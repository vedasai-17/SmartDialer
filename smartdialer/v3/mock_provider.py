import hashlib
import hmac
import json
import os
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from random import Random

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Connection, Engine, RowMapping, text

from .agents import AgentState, AgentTransitions
from .calls import RANK, CallState, CallTransitions, Lease
from .campaigns import read_campaign
from .clock import Clock
from .providers import ProviderRejected, ProviderStatus, ProviderTimeout, ProviderWebhook


class MockProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    setup_min_seconds: float = Field(default=0.2, ge=0)
    setup_max_seconds: float = Field(default=0.2, ge=0)
    ring_seconds: float = Field(default=0.65, ge=0)
    talk_seconds: float = Field(default=1.5, ge=0)
    answer_hold_seconds: float = Field(default=2, gt=0, le=2)
    answer_rate: float = Field(default=0.55, ge=0, le=1)
    failure_rate: float = Field(default=0.01, ge=0, le=1)
    timeout_rate: float = Field(default=0.002, ge=0, le=1)
    duplicate_rate: float = Field(default=0, ge=0, le=1)
    out_of_order_rate: float = Field(default=0, ge=0, le=1)
    lost_terminal_rate: float = Field(default=0, ge=0, le=1)
    outage: bool = False
    timeout_after_accept: bool | None = None
    cancel_timeout_after_accept: bool = False
    bridge_timeout_after_accept: bool = False


PROFILES = {
    "A": MockProfile(),
    "B": MockProfile(
        setup_min_seconds=1, setup_max_seconds=3, failure_rate=0.10, timeout_rate=0.05,
        duplicate_rate=0.075, out_of_order_rate=0.04, lost_terminal_rate=0.03,
    ),
}


class MockCall(BaseModel):
    accepted_at: datetime
    ringing_at: datetime
    outcome_at: datetime
    outcome: CallState
    hold_until: datetime
    talk_seconds: float
    lose_terminal: bool


class MockProvider:
    """A durable mock provider with its own transactions and namespace in call_events."""

    def __init__(self, name: str, engine: Engine, clock: Clock, secret: str, *, seed: int = 17) -> None:
        self.name = name
        self.engine = engine
        self.clock = clock
        self._secret = secret.encode()
        self.seed = seed
        self._namespace = f"mock:{name}"

    def sign(self, raw: bytes) -> str:
        return "sha256=" + hmac.new(self._secret, raw, hashlib.sha256).hexdigest()

    def verify_signature(self, raw: bytes, signature: str) -> bool:
        return signature.isascii() and hmac.compare_digest(self.sign(raw), signature)

    def parse_webhook(self, raw: bytes) -> ProviderWebhook:
        return ProviderWebhook.model_validate_json(raw)

    def configure(self, **overrides: object) -> MockProfile:
        with self.engine.begin() as connection:
            self._lock_reference(connection, "settings")
            profile = MockProfile.model_validate({**self._profile(connection).model_dump(), **overrides})
            if profile.setup_min_seconds > profile.setup_max_seconds:
                raise ValueError("minimum setup time exceeds maximum setup time")
            connection.execute(
                text("""
                    INSERT INTO call_events (provider_name, provider_event_id, event_type, received_at, payload, processing_status)
                    VALUES (:namespace, 'settings', 'MOCK_SETTINGS', :now, CAST(:payload AS jsonb), 'APPLIED')
                    ON CONFLICT (provider_name, provider_event_id) DO UPDATE SET payload = excluded.payload
                """),
                dict(namespace=self._namespace, now=self.clock.now(), payload=profile.model_dump_json()),
            )
        return profile

    def initiate_call(self, client_reference: str, phone: str, lease: Lease) -> ProviderStatus:
        with self.engine.begin() as connection:
            self._lock_reference(connection, client_reference)
            now = self.clock.now()
            profile = self._available_profile(connection)
            records = self._read(connection, client_reference)
            previous = self._snapshot(client_reference, records, now)
            if previous is not None:
                return previous
            row = CallTransitions(connection, clock=self.clock).lock_owned(lease, now, audit_lost=False)
            if row is None or row.idempotency_key != client_reference or row.state_rank != 25:
                raise ProviderRejected("initiation permit expired")
            agent = AgentTransitions(connection).get(row.agent_id, lock=True)
            now = self.clock.now()
            if agent.state not in {AgentState.RESERVED, AgentState.DIALING} or agent.lease_expires_at is None or (
                agent.lease_expires_at <= now or row.lease_until <= now or agent.current_call_id != row.id
            ):
                raise ProviderRejected("initial agent reservation expired")
            if not phone.startswith("999"):
                raise ProviderRejected("mock accepts synthetic 999-prefix numbers only")
            transport = Random(f"{self.seed}:{self.name}:{client_reference}:send:{row.dispatch_attempts}")
            timeout = transport.random() < profile.timeout_rate
            after_accept = profile.timeout_after_accept if profile.timeout_after_accept is not None else transport.random() < 0.5
            if timeout and not after_accept:
                raise ProviderTimeout("initiation timed out before mock acceptance")
            mock_call = self._accept(connection, client_reference, profile, now)
            result = self._snapshot(client_reference, {"accept": mock_call.model_dump(mode="json")}, now)
        if timeout:
            raise ProviderTimeout("mock accepted before the response timed out")
        if result is None:
            raise RuntimeError("mock acceptance did not produce a status")
        return result

    def get_call_status(self, client_reference: str) -> ProviderStatus | None:
        with self.engine.connect() as connection:
            self._available_profile(connection)
            return self._snapshot(client_reference, self._read(connection, client_reference), self.clock.now())

    def cancel_call(self, client_reference: str, lease: Lease) -> ProviderStatus:
        with self.engine.begin() as connection:
            self._lock_reference(connection, client_reference)
            now = self.clock.now()
            profile = self._available_profile(connection)
            records = self._read(connection, client_reference)
            current = self._snapshot(client_reference, records, now)
            if current is not None and RANK[current.state] == 100:
                return current
            row = CallTransitions(connection, clock=self.clock).lock_owned(lease, now, audit_lost=False)
            now = self.clock.now()
            if row is None or row.idempotency_key != client_reference or row.lease_until <= now:
                raise ProviderRejected("cancellation permit expired")
            current = self._snapshot(client_reference, records, now)
            if current is not None and RANK[current.state] == 100:
                return current
            if current is None:
                current = ProviderStatus(self._provider_id(client_reference), CallState.CANCELLED, now)
            result = replace(current, state=CallState.CANCELLED, changed_at=now)
            self._record(connection, f"{client_reference}:cancel", "MOCK_CANCEL", now, {"at": now.isoformat()})
            if "accept" in records and not records["accept"]["lose_terminal"]:
                self._schedule(connection, client_reference, "cancel", result, now)
        if profile.cancel_timeout_after_accept:
            raise ProviderTimeout("mock cancelled before the response timed out")
        return result

    def bridge(self, client_reference: str, lease: Lease, agent_id: int) -> ProviderStatus:
        with self.engine.begin() as connection:
            self._lock_reference(connection, client_reference)
            now = self.clock.now()
            profile = self._available_profile(connection)
            records = self._read(connection, client_reference)
            current = self._snapshot(client_reference, records, now)
            if current is None:
                raise ProviderRejected("provider does not know this reference")
            if current.state != CallState.ANSWERED:
                return current

            # This is the mock's provider-side acceptance point, after the dispatcher committed its lease.
            row = CallTransitions(connection, clock=self.clock).lock_owned(lease, now, audit_lost=False)
            if row is None or row.idempotency_key != client_reference or row.state_rank != 50 or row.agent_id != agent_id:
                raise ProviderRejected("bridge permit expired or its agent changed")
            agent = AgentTransitions(connection).get(agent_id, lock=True)
            campaign = read_campaign(connection, row.campaign_id)
            now = self.clock.now()
            current = self._snapshot(client_reference, records, now)
            if current is not None and current.state != CallState.ANSWERED:
                return current
            if row.lease_until <= now or campaign is None or not CallTransitions.ready_to_bridge(row, agent, now, campaign):
                raise ProviderRejected("agent does not hold a fresh bridge reservation")
            self._record(connection, f"{client_reference}:bridge", "MOCK_BRIDGE", now, {"at": now.isoformat(), "agent_id": agent_id})
            result = replace(current, state=CallState.CONNECTED, changed_at=now, connected_at=now, agent_id=agent_id)
            accepted = MockCall.model_validate(records["accept"])
            if not accepted.lose_terminal:
                end = now + timedelta(seconds=accepted.talk_seconds)
                self._schedule(connection, client_reference, "complete", replace(result, state=CallState.COMPLETED, changed_at=end), end)
        if profile.bridge_timeout_after_accept:
            raise ProviderTimeout("mock bridged before the response timed out")
        return result

    def deliver_ready(self, receive: Callable[[str, bytes, str], object], *, limit: int = 100) -> int:
        with self.engine.connect() as connection:
            pending = connection.execute(
                text("""
                    SELECT id, payload FROM call_events
                    WHERE provider_name = :namespace AND event_type = 'MOCK_CALLBACK'
                        AND processed_at IS NULL AND received_at <= :now
                    ORDER BY received_at, id LIMIT :limit
                """),
                dict(namespace=self._namespace, now=self.clock.now(), limit=limit),
            ).mappings().all()
        for row in pending:
            self._deliver(row, receive)
        return len(pending)

    def _deliver(self, row: RowMapping, receive: Callable[[str, bytes, str], object]) -> None:
        wire = row.payload["wire"]
        with self.engine.connect() as connection:
            records = self._read(connection, wire["client_reference"])
        cancelled = records.get("cancel")
        ignore = row.payload["unbridged"] and "bridge" in records
        if cancelled is not None and wire["event_type"] != "CANCELLED":
            ignore |= datetime.fromisoformat(cancelled["at"]) <= datetime.fromisoformat(wire["timestamp"])
        if not ignore:
            raw = json.dumps(wire).encode()
            receive(self.name, raw, self.sign(raw))
        with self.engine.begin() as connection:
            connection.execute(text("UPDATE call_events SET processed_at = :now WHERE id = :id"), dict(now=self.clock.now(), id=row.id))

    def _accept(self, connection: Connection, ref: str, profile: MockProfile, now: datetime) -> MockCall:
        rng = Random(f"{self.seed}:{self.name}:{ref}")
        ringing = now + timedelta(seconds=rng.uniform(profile.setup_min_seconds, profile.setup_max_seconds))
        outcome_at = ringing + timedelta(seconds=profile.ring_seconds)
        if rng.random() < profile.failure_rate:
            outcome = CallState.FAILED
        elif rng.random() < profile.answer_rate:
            outcome = CallState.ANSWERED
        else:
            outcome = CallState.BUSY if rng.random() < 0.2 else CallState.NO_ANSWER
        hold_until = outcome_at + timedelta(seconds=profile.answer_hold_seconds)
        accepted = MockCall(
            accepted_at=now, ringing_at=ringing, outcome_at=outcome_at, outcome=outcome,
            hold_until=hold_until, talk_seconds=profile.talk_seconds, lose_terminal=rng.random() < profile.lost_terminal_rate,
        )
        self._record(connection, f"{ref}:accept", "MOCK_ACCEPT", now, accepted.model_dump(mode="json"))
        reordered = rng.random() < profile.out_of_order_rate
        result = ProviderStatus(self._provider_id(ref), CallState.RINGING, ringing, initiated_at=now)
        self._schedule(connection, ref, "ring", result, hold_until + timedelta(seconds=0.2) if reordered else ringing)
        if outcome == CallState.ANSWERED:
            answer = replace(result, state=outcome, changed_at=outcome_at, answered_at=outcome_at)
            delivery = hold_until + timedelta(seconds=0.1) if reordered else outcome_at
            self._schedule(connection, ref, "answer", answer, delivery)
            if rng.random() < profile.duplicate_rate:
                self._schedule(connection, ref, "answer-repeat-1", answer, delivery)
                self._schedule(connection, ref, "answer-repeat-2", answer, delivery)
            if not accepted.lose_terminal:
                self._schedule(connection, ref, "unbridged", replace(answer, state=CallState.COMPLETED, changed_at=hold_until), hold_until, unbridged=True)
        elif not accepted.lose_terminal:
            self._schedule(connection, ref, "outcome", replace(result, state=outcome, changed_at=outcome_at), outcome_at)
        return accepted

    def _snapshot(self, ref: str, records: dict[str, dict], now: datetime) -> ProviderStatus | None:
        provider_id = self._provider_id(ref)
        if "accept" not in records:
            if "cancel" in records:
                return ProviderStatus(provider_id, CallState.CANCELLED, datetime.fromisoformat(records["cancel"]["at"]))
            return None
        call = MockCall.model_validate(records["accept"])
        cancellation = datetime.fromisoformat(records["cancel"]["at"]) if "cancel" in records else None
        observed = cancellation if cancellation is not None else now
        result = ProviderStatus(provider_id, CallState.INITIATED, call.accepted_at, initiated_at=call.accepted_at)
        if observed >= call.ringing_at:
            result = replace(result, state=CallState.RINGING, changed_at=call.ringing_at)
        if observed >= call.outcome_at:
            answer = call.outcome_at if call.outcome == CallState.ANSWERED else None
            result = replace(result, state=call.outcome, changed_at=call.outcome_at, answered_at=answer)
        if "bridge" in records:
            connected = datetime.fromisoformat(records["bridge"]["at"])
            result = replace(result, state=CallState.CONNECTED, changed_at=connected, connected_at=connected, agent_id=records["bridge"]["agent_id"])
            end = connected + timedelta(seconds=call.talk_seconds)
            if observed >= end:
                result = replace(result, state=CallState.COMPLETED, changed_at=end)
        elif result.state == CallState.ANSWERED and observed >= call.hold_until:
            result = replace(result, state=CallState.COMPLETED, changed_at=call.hold_until)
        if cancellation is not None:
            result = replace(result, state=CallState.CANCELLED, changed_at=cancellation)
        return result

    def _profile(self, connection: Connection) -> MockProfile:
        payload = connection.execute(text("""
            SELECT payload FROM call_events WHERE provider_name = :namespace AND provider_event_id = 'settings'
        """), {"namespace": self._namespace}).scalar_one_or_none()
        return MockProfile.model_validate(payload) if payload is not None else PROFILES[self.name]

    def _available_profile(self, connection: Connection) -> MockProfile:
        profile = self._profile(connection)
        if profile.outage:
            raise ProviderTimeout("mock provider outage")
        return profile

    def _lock_reference(self, connection: Connection, ref: str) -> None:
        connection.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": f"{self._namespace}:{ref}"})

    def _provider_id(self, ref: str) -> str:
        return f"{self.name}-" + hashlib.sha256(ref.encode()).hexdigest()[:24]

    def _read(self, connection: Connection, ref: str) -> dict[str, dict]:
        records = connection.execute(
            text("SELECT provider_event_id, payload FROM call_events WHERE provider_name = :namespace AND provider_event_id = ANY(:keys)"),
            dict(namespace=self._namespace, keys=[f"{ref}:{kind}" for kind in ("accept", "bridge", "cancel")]),
        ).all()
        return {key.rsplit(":", 1)[-1]: payload for key, payload in records}

    def _record(self, connection: Connection, key: str, event_type: str, at: datetime, payload: dict) -> None:
        connection.execute(
            text("""
                INSERT INTO call_events (provider_name, provider_event_id, event_type, received_at, payload, processing_status)
                VALUES (:namespace, :key, :type, :at, CAST(:payload AS jsonb), 'APPLIED')
            """),
            dict(namespace=self._namespace, key=key, type=event_type, at=at, payload=json.dumps(payload)),
        )

    def _schedule(
        self, connection: Connection, ref: str, suffix: str, status: ProviderStatus, due: datetime, *, unbridged: bool = False,
    ) -> None:
        event_id = f"{status.provider_call_id}:{suffix}"
        wire = {
            "event_id": event_id, "client_reference": ref, "provider_call_id": status.provider_call_id,
            "event_type": status.state, "timestamp": status.changed_at.isoformat(),
            "initiated_at": status.initiated_at.isoformat() if status.initiated_at else None,
            "answered_at": status.answered_at.isoformat() if status.answered_at else None,
            "connected_at": status.connected_at.isoformat() if status.connected_at else None, "agent_id": status.agent_id,
        }
        self._record(connection, f"callback:{event_id}", "MOCK_CALLBACK", due, {"wire": wire, "unbridged": unbridged})


def mock_providers(engine: Engine, clock: Clock, *, seed: int = 17) -> dict[str, MockProvider]:
    return {
        name: MockProvider(name, engine, clock, os.environ.get(f"SMARTDIALER_MOCK_{name}_SECRET", f"local-mock-{name}"), seed=seed)
        for name in PROFILES
    }
