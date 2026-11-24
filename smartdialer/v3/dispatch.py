from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from random import Random
from typing import Literal

from sqlalchemy import Connection, Engine, RowMapping, text

from .agents import AgentState, AgentTransitions, HELD
from .borrowers import BorrowerState, BorrowerTransitions
from .calls import RANK, CallState, CallTransitions, Lease, TransitionResult
from .campaigns import Campaign, read_campaign
from .clock import Clock
from .config import DispatchConfig, ExcessAnswerPolicy
from .db import audit, log
from .observations import record_status
from .health import record_operation, start_permitted
from .providers import ProviderRejected, ProviderStatus, ProviderTimeout, TelecomProvider
from .safety_policy import SPECULATION_CANCEL_REQUESTED, cancel_requested, speculative_start_allowed


@dataclass(frozen=True)
class Work:
    lease: Lease
    provider: str
    reference: str
    new_call: bool


@dataclass(frozen=True)
class Operation:
    kind: Literal["initiate", "cancel", "bridge"]
    phone: str | None = None
    agent_id: int | None = None


class Dispatcher:
    def __init__(
        self,
        engine: Engine,
        clock: Clock,
        providers: Mapping[str, TelecomProvider],
        owner: str,
        *,
        config: DispatchConfig | None = None,
        rng: Random | None = None,
        on_boundary: Callable[[str], None] | None = None,
    ) -> None:
        self.engine = engine
        self.clock = clock
        self.providers = providers
        self.owner = owner
        self.config = config if config is not None else DispatchConfig()
        self.rng = rng if rng is not None else Random()
        self.on_boundary = on_boundary

    def step(self) -> bool:
        work = self._claim()
        if work is None:
            return False
        self._boundary("after_claim")
        provider = self.providers.get(work.provider)
        if provider is None:
            self._unavailable_provider(work)
            return True

        if work.new_call:
            operation = self._prepare(work, None, new=True)
        else:
            try:
                remote = provider.get_call_status(work.reference)
            except Exception as error:
                self._uncertain(work, "status", error)
                return True
            operation = self._prepare(work, remote)
        if operation is not None:
            self._send(work, operation, provider)
        return True

    def drain(self, limit: int = 100) -> int:
        processed = 0
        while processed < limit and self.step():
            processed += 1
        return processed

    def _claim(self) -> Work | None:
        with self.engine.begin() as connection:
            now = self.clock.now()
            call = connection.execute(
                text(f"""
                    SELECT c.* FROM calls c
                    LEFT JOIN agents a ON a.id = c.agent_id
                    JOIN borrowers b ON b.id = c.borrower_id
                    WHERE c.state_rank BETWEEN 20 AND 99 AND (c.lease_until IS NULL OR c.lease_until <= :now)
                        AND (c.next_retry_at <= :now OR (c.state <> 'UNKNOWN' AND (
                            a.state = 'RECOVERY_HOLD' OR (b.state = 'SUPPRESSED' AND c.state_rank < 60)
                            OR ({SPECULATION_CANCEL_REQUESTED})
                        )))
                    ORDER BY CASE WHEN a.state = 'RECOVERY_HOLD' THEN 0 WHEN c.state_rank = 50 THEN 1
                                  WHEN c.state_rank > 20 THEN 2 ELSE 3 END, c.next_retry_at, c.id
                    LIMIT 1 FOR UPDATE OF c SKIP LOCKED
                """),
                {"now": now},
            ).mappings().one_or_none()
            if call is None:
                return None
            lease = CallTransitions(connection, clock=self.clock).acquire_lease(call, self.owner, now, self.config.lease_seconds)
            return Work(lease, call.provider, call.idempotency_key, call.state == CallState.RESERVED)

    def _prepare(self, work: Work, status: ProviderStatus | None, *, new: bool = False) -> Operation | None:
        with self.engine.begin() as connection:
            now = self.clock.now()
            calls = CallTransitions(connection, clock=self.clock)
            call = calls.lock_owned(work.lease, now)
            if call is None:
                return None
            campaign = read_campaign(connection, call.campaign_id)
            if campaign is None:
                return None
            if new or status is None:
                if call.state_rank <= 25 and call.provider_call_id is None:
                    return self._prepare_start(calls, call, work, campaign, now)
                self._end(calls, call, work, campaign, now, CallState.FAILED, "PROVIDER_CONFIRMED_MISSING")
                return None

            result = record_status(calls, call, campaign, now, status, lease=work.lease)
            if result == TransitionResult.APPLIED and RANK[status.state] == 100:
                return None
            call = calls.lock_owned(work.lease, now, audit_lost=False)
            if call is None:
                return None
            if result == TransitionResult.INVALID:
                self._defer(calls, call, work, campaign, now, "INVALID_PROVIDER_STATUS", "status")
                return None
            return self._prepare_active(calls, call, work, campaign, now)

    def _prepare_start(
        self, calls: CallTransitions, call: RowMapping, work: Work, campaign: Campaign, now: datetime,
    ) -> Operation | None:
        agent = self._agent(calls, call, campaign, now)
        valid_agent = call.is_speculative and agent is None or (
            agent is not None and agent.state in {AgentState.RESERVED, AgentState.DIALING}
        )
        if campaign.status != "ACTIVE" or not valid_agent or not speculative_start_allowed(calls.connection, call, campaign, now):
            if work.new_call:
                self._end(calls, call, work, campaign, now, CallState.CANCELLED, "RESERVATION_UNAVAILABLE")
                return None
            return Operation("cancel")
        borrower = BorrowerTransitions(calls.connection).get(call.borrower_id, lock=True)
        if borrower.state == BorrowerState.SUPPRESSED:
            if work.new_call:
                self._end(calls, call, work, campaign, now, CallState.CANCELLED, "BORROWER_SUPPRESSED")
                return None
            return Operation("cancel")
        if call.dispatch_attempts >= self.config.max_dispatch_attempts:
            self._end(calls, call, work, campaign, now, CallState.FAILED, "DISPATCH_DEAD_LETTER")
            return None

        if not start_permitted(calls.connection, call, campaign, now):
            if work.new_call:
                self._end(calls, call, work, campaign, now, CallState.CANCELLED, "PROVIDER_BREAKER_BLOCKED")
            else:
                self._poll_later(calls, work, now)
            return None

        if not self._rate_slot(calls.connection, call, campaign, now):
            calls.release_lease(work.lease, now, now + timedelta(seconds=self.config.retry_base_seconds))
            return None
        calls.start_attempt(work.lease, now)
        return Operation("initiate", phone=borrower.phone_normalized)

    def _prepare_active(
        self, calls: CallTransitions, call: RowMapping, work: Work, campaign: Campaign, now: datetime,
    ) -> Operation | None:
        agent = self._agent(calls, call, campaign, now)
        borrower = BorrowerTransitions(calls.connection).get(call.borrower_id, lock=True)
        if (borrower.state == BorrowerState.SUPPRESSED and call.state_rank < 60) or cancel_requested(calls.connection, call):
            if call.state_rank == 50:
                calls.mark_excess_answer(work.lease, campaign, now)
            return Operation("cancel")
        if call.state_rank == 50:
            deadline = call.answered_at + timedelta(seconds=min(self.config.answer_hold_seconds, campaign.config.answer_hold_seconds))
            if now >= deadline:
                calls.mark_excess_answer(work.lease, campaign, now)
                return Operation("cancel")
            if call.is_speculative and agent is None:
                owner = calls.claim_speculative_answer(work.lease, campaign, now)
                if owner is not None:
                    return Operation("bridge", agent_id=owner)
                calls.mark_excess_answer(work.lease, campaign, now)
                if campaign.config.excess_answer_policy == ExcessAnswerPolicy.SHORT_HOLD:
                    calls.release_lease(work.lease, now, min(deadline, now + timedelta(seconds=0.1)))
                    return None
                return Operation("cancel")
            if calls.ready_to_bridge(call, agent, now, campaign):
                return Operation("bridge", agent_id=agent.id)
            replacement = calls.reroute(call, campaign, now)
            return Operation("bridge", agent_id=replacement) if replacement is not None else Operation("cancel")
        if (agent is None and not call.is_speculative) or (agent is not None and agent.state == AgentState.RECOVERY_HOLD):
            return Operation("cancel")
        self._poll_later(calls, work, now)
        return None

    def _send(self, work: Work, operation: Operation, provider: TelecomProvider) -> None:
        self._boundary(f"before_{operation.kind}")
        try:
            if operation.kind == "initiate":
                if operation.phone is None:
                    raise ValueError("initiation requires a borrower number")
                status = provider.initiate_call(work.reference, operation.phone, work.lease)
            elif operation.kind == "bridge":
                if operation.agent_id is None:
                    raise ValueError("bridge requires a reserved agent")
                status = provider.bridge(work.reference, work.lease, operation.agent_id)
            else:
                status = provider.cancel_call(work.reference, work.lease)
        except Exception as error:
            self._uncertain(work, operation.kind, error)
            return
        self._boundary(f"after_{operation.kind}")
        self._apply(work, status, operation.kind)

    def _apply(self, work: Work, status: ProviderStatus, operation: str) -> None:
        with self.engine.begin() as connection:
            now = self.clock.now()
            calls = CallTransitions(connection, clock=self.clock)
            call = calls.lock_owned(work.lease, now)
            if call is None:
                return
            campaign = read_campaign(connection, call.campaign_id)
            if campaign is None:
                return
            result = record_status(calls, call, campaign, now, status, lease=work.lease)
            if operation == "initiate" and result != TransitionResult.INVALID:
                record_operation(connection, call, now, work.lease.token, operation, "OK")
            if result == TransitionResult.INVALID:
                self._defer(calls, call, work, campaign, now, "INVALID_PROVIDER_STATUS", operation)
            else:
                current = calls.lock_owned(work.lease, now, audit_lost=False)
                if current is None:
                    return
                if current.state_rank == 50:
                    calls.release_lease(work.lease, now, now)
                else:
                    self._poll_later(calls, work, now)

    def _uncertain(self, work: Work, operation: str, error: Exception) -> None:
        with self.engine.begin() as connection:
            now = self.clock.now()
            calls = CallTransitions(connection, clock=self.clock)
            call = calls.lock_owned(work.lease, now)
            if call is None:
                return
            campaign = read_campaign(connection, call.campaign_id)
            if campaign is None:
                return
            label = "REJECTED" if isinstance(error, ProviderRejected) else "TIMEOUT" if isinstance(error, ProviderTimeout) else "ERROR"
            self._defer(calls, call, work, campaign, now, f"{operation.upper()}_{label}", operation)

    def _defer(
        self, calls: CallTransitions, call: RowMapping, work: Work, campaign: Campaign,
        now: datetime, reason: str, operation: str,
    ) -> None:
        if call.state != CallState.UNKNOWN and not (operation == "status" and call.state_rank == 60):
            calls.advance(call.id, CallState.UNKNOWN, now, campaign, failure_reason=reason, lease=work.lease)
        result = "TIMEOUT" if reason.endswith("_TIMEOUT") else "ERROR"
        record_operation(calls.connection, call, now, work.lease.token, operation, result)
        audit(calls.connection, now, "call", call.id, "PROVIDER_UNCERTAIN", campaign_id=campaign.id,
              provider=work.provider, reason=reason, operation=operation, token=work.lease.token)
        failures = calls.connection.execute(
            text("SELECT count(*) FROM audit_log WHERE entity_type = 'call' AND entity_id = :id AND action = 'PROVIDER_UNCERTAIN'"),
            {"id": str(call.id)},
        ).scalar_one()
        upper = min(self.config.retry_max_seconds, self.config.retry_base_seconds * 2 ** min(failures - 1, 16))
        retry_at = now + timedelta(seconds=self.rng.uniform(0, upper))
        calls.release_lease(work.lease, now, retry_at)
        log("provider_operation_uncertain", call_id=call.id, provider=work.provider, operation=operation, reason=reason, next_retry_at=retry_at)

    def _unavailable_provider(self, work: Work) -> None:
        with self.engine.begin() as connection:
            now = self.clock.now()
            calls = CallTransitions(connection, clock=self.clock)
            call = calls.lock_owned(work.lease, now)
            if call is None:
                return
            campaign = read_campaign(connection, call.campaign_id)
            if campaign is None:
                return
            if work.new_call:
                self._end(calls, call, work, campaign, now, CallState.FAILED, "UNCONFIGURED_PROVIDER")
            else:
                self._defer(calls, call, work, campaign, now, "UNCONFIGURED_PROVIDER", "status")

    def _poll_later(self, calls: CallTransitions, work: Work, now: datetime) -> None:
        calls.release_lease(work.lease, now, now + timedelta(seconds=self.config.status_poll_seconds))

    @staticmethod
    def _agent(calls: CallTransitions, call: RowMapping, campaign: Campaign, now: datetime) -> RowMapping | None:
        if call.agent_id is None:
            return None
        agents = AgentTransitions(calls.connection)
        agent = agents.get(call.agent_id, lock=True)
        if agent.state in HELD and agent.state != AgentState.RECOVERY_HOLD and (
            agent.lease_expires_at is None or agent.lease_expires_at <= now or agent.last_heartbeat_at is None
            or agent.last_heartbeat_at <= now - timedelta(seconds=campaign.config.heartbeat_ttl_seconds)
        ):
            if not agents.move(agent, AgentState.RECOVERY_HOLD, now):
                raise RuntimeError("could not put an expired reservation on hold")
            return agents.get(agent.id)
        return agent

    @staticmethod
    def _end(
        calls: CallTransitions, call: RowMapping, work: Work, campaign: Campaign, now: datetime,
        target: CallState, reason: str,
    ) -> None:
        result = calls.advance(call.id, target, now, campaign, failure_reason=reason, lease=work.lease)
        if result == TransitionResult.APPLIED:
            audit(calls.connection, now, "call", call.id, reason, campaign_id=campaign.id, provider=work.provider)

    @staticmethod
    def _rate_slot(connection: Connection, call: RowMapping, campaign: Campaign, now: datetime) -> bool:
        key = f"dispatch-rate:{campaign.id}:{call.provider}"
        connection.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": key})
        used = connection.execute(
            text("""
                SELECT count(*) FROM audit_log WHERE campaign_id = :campaign AND action = 'INITIATE_PERMITTED'
                    AND occurred_at > :start AND details->>'provider' = :provider
            """),
            dict(campaign=campaign.id, start=now - timedelta(seconds=1), provider=call.provider),
        ).scalar_one()
        if used >= campaign.provider_rate_limit:
            return False
        audit(connection, now, "call", call.id, "INITIATE_PERMITTED", campaign_id=campaign.id, provider=call.provider)
        return True

    def _boundary(self, name: str) -> None:
        if self.on_boundary is not None:
            self.on_boundary(name)
