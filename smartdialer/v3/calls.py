from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import Connection, RowMapping, text

from .agents import AgentState, AgentTransitions
from .borrowers import BorrowerState, BorrowerTransitions
from .campaigns import AVAILABLE_AGENTS, Campaign, availability_params
from .clock import Clock
from .db import audit


class CallState(StrEnum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATING = "INITIATING"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    BUSY = "BUSY"
    NO_ANSWER = "NO_ANSWER"
    UNKNOWN = "UNKNOWN"


class TransitionResult(StrEnum):
    APPLIED = "APPLIED"
    STALE = "STALE"
    INVALID = "INVALID"
    UNKNOWN_CALL = "UNKNOWN_CALL"


RANK = {
    CallState.QUEUED: 10,
    CallState.RESERVED: 20,
    CallState.INITIATING: 25,
    CallState.INITIATED: 30,
    CallState.RINGING: 40,
    CallState.ANSWERED: 50,
    CallState.CONNECTED: 60,
    CallState.COMPLETED: 100,
    CallState.FAILED: 100,
    CallState.CANCELLED: 100,
    CallState.BUSY: 100,
    CallState.NO_ANSWER: 100,
}


@dataclass(frozen=True)
class Lease:
    call_id: UUID
    owner: str
    token: int
    until: datetime


class CallTransitions:
    def __init__(self, connection: Connection, *, clock: Clock | None = None) -> None:
        self.connection = connection
        self.clock = clock

    def reserve(
        self, campaign: Campaign, borrower: RowMapping, agent_id: int | None, approval_id: int, now: datetime,
    ) -> UUID:
        call_id = uuid4()
        attempt = borrower.attempts + 1
        self.connection.execute(
            text("""
                INSERT INTO calls (
                    id, campaign_id, borrower_id, agent_id, safety_decision_id, idempotency_key,
                    state, state_rank, provider, attempt_number, created_at, next_retry_at, is_speculative
                ) VALUES (
                    :id, :campaign_id, :borrower_id, :agent_id, :approval_id, :key,
                    'RESERVED', 20, :provider, :attempt, :now, :now, :speculative
                )
            """),
            dict(
                id=call_id, campaign_id=campaign.id, borrower_id=borrower.id, agent_id=agent_id,
                approval_id=approval_id, key=f"{campaign.id}:{borrower.id}:{attempt}",
                provider=campaign.config.provider, attempt=attempt, now=now, speculative=agent_id is None,
            ),
        )
        return call_id

    def advance(
        self,
        call_id: UUID,
        target: CallState,
        now: datetime,
        campaign: Campaign,
        *,
        occurred_at: datetime | None = None,
        failure_reason: str | None = None,
        lease: Lease | None = None,
        observed_answered_at: datetime | None = None,
        confirmed_bridge_agent: int | None = None,
    ) -> TransitionResult:
        if lease is not None and lease.call_id != call_id:
            return TransitionResult.INVALID
        call = self.lock_owned(lease, now) if lease is not None else self.lock(call_id)
        if call is None:
            return TransitionResult.STALE if lease is not None else TransitionResult.UNKNOWN_CALL
        if campaign.id != call.campaign_id:
            return self._reject(call, target, now, TransitionResult.INVALID, "CAMPAIGN_MISMATCH")
        if target == CallState.UNKNOWN:
            return self._mark_unknown(call, now, failure_reason)
        rank = RANK[target]
        if call.state_rank >= rank:
            return self._reject(call, target, now, TransitionResult.STALE, "RANK_NOT_INCREASING")
        at = occurred_at if occurred_at is not None else now
        if at.utcoffset() is None or not call.created_at <= at <= now:
            return self._reject(call, target, now, TransitionResult.INVALID, "INVALID_TIMESTAMP")
        if observed_answered_at is not None and (
            observed_answered_at.utcoffset() is None or not call.created_at <= observed_answered_at <= at
        ):
            return self._reject(call, target, now, TransitionResult.INVALID, "INVALID_ANSWER_TIMESTAMP")

        agents = AgentTransitions(self.connection)
        agent = agents.get(call.agent_id, lock=True) if call.agent_id is not None else None
        if target == CallState.CONNECTED:
            held = agent is not None and agent.current_call_id == call.id
            accepted_with_reservation = held and confirmed_bridge_agent is not None and (
                agent.id == confirmed_bridge_agent and agent.state in {AgentState.DIALING, AgentState.RECOVERY_HOLD}
            )
            valid = accepted_with_reservation if confirmed_bridge_agent is not None else self.ready_to_bridge(call, agent, now, campaign)
            if call.answered_at is None or at < call.answered_at or not valid:
                return self._reject(call, target, now, TransitionResult.INVALID, "ANSWER_OR_RESERVATION_MISSING")

        self._advance_rank(call, target, rank, at, failure_reason, observed_answered_at)
        if rank == 100:
            self._settle(call, agents, agent, now, campaign)
        elif target == CallState.CONNECTED:
            if agent.state == AgentState.DIALING and not self.ready_to_bridge(call, agent, now, campaign):
                if not agents.move(agent, AgentState.RECOVERY_HOLD, now):
                    raise RuntimeError("failed to retain the stale bridge owner for recovery")
            elif agent.state == AgentState.DIALING and not agents.move(agent, AgentState.CONNECTED, now):
                raise RuntimeError("failed to connect the locked agent")
        elif rank >= 30 and agent is not None and agent.state == AgentState.RESERVED:
            if not agents.move(agent, AgentState.DIALING, now):
                raise RuntimeError("failed to move the locked agent to DIALING")
        return TransitionResult.APPLIED

    @staticmethod
    def ready_to_bridge(call: RowMapping, agent: RowMapping | None, now: datetime, campaign: Campaign) -> bool:
        return (
            agent is not None and agent.state == AgentState.DIALING and agent.current_call_id == call.id
            and agent.lease_expires_at is not None and agent.lease_expires_at > now
            and agent.last_heartbeat_at > now - timedelta(seconds=campaign.config.heartbeat_ttl_seconds)
        )

    def lock(self, call_id: UUID) -> RowMapping | None:
        return self.connection.execute(
            text("SELECT * FROM calls WHERE id = :id FOR UPDATE"), {"id": call_id},
        ).mappings().one_or_none()

    def _advance_rank(
        self, call: RowMapping, target: CallState, rank: int, at: datetime, failure_reason: str | None,
        observed_answered_at: datetime | None,
    ) -> None:
        self.connection.execute(
            text("""
                UPDATE calls SET state = :target, state_rank = :rank, version = version + 1,
                    initiated_at = CASE WHEN :target = 'INITIATED' THEN coalesce(initiated_at, :at) ELSE initiated_at END,
                    ringing_at = CASE WHEN :target = 'RINGING' THEN :at ELSE ringing_at END,
                    answered_at = coalesce(answered_at, CASE WHEN :target = 'ANSWERED' THEN :at ELSE :observed_answered_at END),
                    connected_at = CASE WHEN :target = 'CONNECTED' THEN :at ELSE connected_at END,
                    ended_at = CASE WHEN :rank = 100 THEN :at ELSE ended_at END,
                    abandoned_at = CASE WHEN :rank = 100 AND connected_at IS NULL
                        THEN coalesce(abandoned_at, answered_at, :observed_answered_at) ELSE abandoned_at END,
                    failure_reason = :failure_reason,
                    next_retry_at = CASE WHEN :target = 'ANSWERED' THEN :at ELSE next_retry_at END,
                    lease_owner = CASE WHEN :rank = 100 THEN NULL ELSE lease_owner END,
                    lease_until = CASE WHEN :rank = 100 THEN NULL ELSE lease_until END
                WHERE id = :id AND state_rank < :rank AND state_rank < 100
                RETURNING id
            """),
            dict(id=call.id, target=target, rank=rank, at=at, failure_reason=failure_reason, observed_answered_at=observed_answered_at),
        ).scalar_one()

    def _mark_unknown(self, call: RowMapping, now: datetime, failure_reason: str | None) -> TransitionResult:
        if not 25 <= call.state_rank < 100 or call.state == CallState.UNKNOWN:
            return self._reject(call, CallState.UNKNOWN, now, TransitionResult.STALE, "NO_OPERATION_TO_RECONCILE")
        self.connection.execute(
            text("""
                UPDATE calls SET state = 'UNKNOWN', version = version + 1, failure_reason = :reason
                WHERE id = :id AND version = :version AND state_rank < 100 RETURNING id
            """),
            dict(id=call.id, version=call.version, reason=failure_reason),
        ).scalar_one()
        return TransitionResult.APPLIED

    def acquire_lease(self, call: RowMapping, owner: str, now: datetime, seconds: float) -> Lease:
        until = now + timedelta(seconds=seconds)
        token = self.connection.execute(
            text("""
                UPDATE calls SET lease_owner = :owner, lease_until = :until, fencing_token = fencing_token + 1,
                    version = version + 1
                WHERE id = :id AND version = :version AND (lease_until IS NULL OR lease_until <= :now)
                    AND state_rank < 100 RETURNING fencing_token
            """),
            dict(id=call.id, version=call.version, owner=owner, until=until, now=now),
        ).scalar_one()
        return Lease(call.id, owner, token, until)

    def lock_owned(self, lease: Lease, now: datetime, *, audit_lost: bool = True) -> RowMapping | None:
        call = self.connection.execute(
            text("""
                SELECT * FROM calls WHERE id = :id AND lease_owner = :owner AND fencing_token = :token
                    AND lease_until > :now AND state_rank < 100 FOR UPDATE
            """),
            dict(id=lease.call_id, owner=lease.owner, token=lease.token, now=now),
        ).mappings().one_or_none()
        if self.clock is not None:
            now = self.clock.now()
            if call is not None and call.lease_until <= now:
                call = None
        if call is None and audit_lost:
            audit(self.connection, now, "call", lease.call_id, "FENCE_REJECTED", owner=lease.owner, token=lease.token)
        return call

    def release_lease(self, lease: Lease, now: datetime, next_retry: datetime) -> None:
        self.connection.execute(
            text("""
                UPDATE calls SET lease_owner = NULL, lease_until = NULL, next_retry_at = :next_retry,
                    version = version + 1
                WHERE id = :id AND lease_owner = :owner AND fencing_token = :token AND lease_until > :now
            """),
            dict(id=lease.call_id, owner=lease.owner, token=lease.token, now=now, next_retry=next_retry),
        )

    def start_attempt(self, lease: Lease, now: datetime) -> None:
        self.connection.execute(
            text("""
                UPDATE calls SET state = 'INITIATING', state_rank = 25, dispatch_attempts = dispatch_attempts + 1,
                    version = version + 1, failure_reason = NULL
                WHERE id = :id AND lease_owner = :owner AND fencing_token = :token AND lease_until > :now
                    AND state_rank IN (20,25) RETURNING id
            """),
            dict(id=lease.call_id, owner=lease.owner, token=lease.token, now=now),
        ).scalar_one()

    def bind_provider(self, call: RowMapping, provider_call_id: str, initiated_at: datetime | None) -> bool:
        if call.provider_call_id is not None and call.provider_call_id != provider_call_id:
            return False
        if call.provider_call_id is None or (call.initiated_at is None and initiated_at is not None):
            self.connection.execute(
                text("""
                    UPDATE calls SET provider_call_id = :provider_id, initiated_at = coalesce(initiated_at, :initiated),
                        version = version + 1 WHERE id = :id
                """),
                dict(id=call.id, provider_id=provider_call_id, initiated=initiated_at),
            )
        return True

    def restore_known_state(self, call: RowMapping, target: CallState) -> bool:
        if call.state != CallState.UNKNOWN or RANK[target] != call.state_rank or call.state_rank < 30:
            return False
        self.connection.execute(
            text("UPDATE calls SET state = :state, failure_reason = NULL, version = version + 1 WHERE id = :id"),
            dict(id=call.id, state=target),
        )
        return True

    def claim_speculative_answer(self, lease: Lease, campaign: Campaign, now: datetime) -> int | None:
        call = self.lock_owned(lease, now)
        if call is None or not call.is_speculative or call.agent_id is not None or call.state_rank != 50:
            return None
        replacement = self._available_replacement(campaign, now)
        if replacement is None:
            return None
        self._bind_answer_agent(call, replacement, now)
        return replacement.id

    def mark_excess_answer(self, lease: Lease, campaign: Campaign, now: datetime) -> None:
        applied = self.connection.execute(text("""
            UPDATE calls SET abandoned_at = :now, version = version + 1
            WHERE id = :id AND lease_owner = :owner AND fencing_token = :token AND lease_until > :now
                AND state_rank = 50 AND is_speculative AND agent_id IS NULL AND abandoned_at IS NULL RETURNING id
        """), dict(id=lease.call_id, owner=lease.owner, token=lease.token, now=now)).scalar_one_or_none()
        if applied is not None:
            audit(self.connection, now, "call", lease.call_id, "UNCLAIMED_ANSWER", campaign_id=campaign.id,
                  policy=campaign.config.excess_answer_policy)

    def reroute(self, call: RowMapping, campaign: Campaign, now: datetime) -> int | None:
        if call.state_rank != 50 or call.agent_id is None or self.connection.execute(
            text("SELECT 1 FROM audit_log WHERE entity_type = 'call' AND entity_id = :id AND action = 'AGENT_REROUTED' LIMIT 1"),
            {"id": str(call.id)},
        ).scalar_one_or_none() is not None:
            return None
        agents = AgentTransitions(self.connection)
        source = agents.get(call.agent_id, lock=True)
        if source.state != AgentState.RECOVERY_HOLD:
            return None
        replacement = self._available_replacement(campaign, now)
        if replacement is None:
            return None
        self._bind_answer_agent(call, replacement, now, source=source)
        audit(self.connection, now, "call", call.id, "AGENT_REROUTED", campaign_id=campaign.id, previous=source.id, new=replacement.id)
        return replacement.id

    def _available_replacement(self, campaign: Campaign, now: datetime) -> RowMapping | None:
        return self.connection.execute(
            text(f"""
                SELECT * FROM agents WHERE {AVAILABLE_AGENTS}
                ORDER BY last_call_ended_at NULLS FIRST, id LIMIT 1 FOR UPDATE SKIP LOCKED
            """),
            availability_params(campaign, now),
        ).mappings().one_or_none()

    def _bind_answer_agent(self, call: RowMapping, replacement: RowMapping, now: datetime, *, source: RowMapping | None = None) -> None:
        agents = AgentTransitions(self.connection)
        self.connection.execute(
            text("UPDATE calls SET agent_id = :agent, version = version + 1 WHERE id = :id"),
            dict(id=call.id, agent=replacement.id),
        )
        if source is not None and not agents.move(source, agents.recovery_target(source), now):
            raise RuntimeError("could not release the previous owner")
        if not agents.move(replacement, AgentState.RESERVED, now, call_id=call.id):
            raise RuntimeError("could not reserve the replacement agent")
        if not agents.move(agents.get(replacement.id), AgentState.DIALING, now):
            raise RuntimeError("could not prepare the replacement agent")

    def _settle(
        self, call: RowMapping, agents: AgentTransitions, agent: RowMapping | None, now: datetime, campaign: Campaign,
    ) -> None:
        if agent is not None:
            live = (
                agent.state != AgentState.RECOVERY_HOLD and agent.lease_expires_at is not None
                and agent.lease_expires_at > now
                and agent.last_heartbeat_at > now - timedelta(seconds=campaign.config.heartbeat_ttl_seconds)
            )
            target = agents.recovery_target(agent) if agent.state == AgentState.RECOVERY_HOLD else AgentState.OFFLINE
            wrap_up_ends_at = None
            if live and call.connected_at is not None:
                target = AgentState.WRAP_UP
                wrap_up_ends_at = now + timedelta(seconds=campaign.config.wrap_up_seconds)
            elif live:
                target = AgentState.AVAILABLE
            if not agents.move(agent, target, now, wrap_up_ends_at=wrap_up_ends_at):
                raise RuntimeError("failed to settle the locked agent")

        borrowers = BorrowerTransitions(self.connection)
        borrower = borrowers.get(call.borrower_id, lock=True)
        if borrower.state == BorrowerState.SUPPRESSED:
            return
        if call.connected_at is not None:
            target = BorrowerState.CONTACTED
        elif borrower.attempts >= borrower.max_attempts:
            target = BorrowerState.EXHAUSTED
        else:
            target = BorrowerState.RETRY_SCHEDULED
        if not borrowers.move(borrower, target, now, retry_delay_seconds=campaign.config.retry_delay_seconds):
            raise RuntimeError("failed to settle the locked borrower")

    def _reject(
        self, call: RowMapping, target: CallState, now: datetime, result: TransitionResult, reason: str,
    ) -> TransitionResult:
        audit(
            self.connection, now, "call", call.id, "TRANSITION_REJECTED", campaign_id=call.campaign_id,
            source=call.state, target=target, result=result, reason=reason,
        )
        return result
