from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from uuid import UUID

import pytest
from sqlalchemy import Engine, text

from smartdialer.v3.__main__ import seed
from smartdialer.v3.agents import AgentState, AgentTransitions
from smartdialer.v3.borrowers import BorrowerState, BorrowerTransitions
from smartdialer.v3.calls import CallState, CallTransitions, TransitionResult
from smartdialer.v3.campaigns import Campaign, read_campaign
from smartdialer.v3.clock import VirtualClock
from smartdialer.v3.config import CampaignConfig
from smartdialer.v3.dialer import Dialer


def reserved_call(engine: Engine, clock: VirtualClock) -> tuple[Campaign, UUID]:
    with engine.begin() as connection:
        campaign_id = seed(connection, clock, "test-campaign", 1, 1, CampaignConfig())
        campaign = read_campaign(connection, campaign_id)
    call_id, = Dialer(engine, clock).tick(campaign_id, 1)
    return campaign, call_id


@pytest.mark.parametrize(("source", "allowed"), [
    ("OFFLINE", "AVAILABLE"),
    ("AVAILABLE", "RESERVED PAUSED OFFLINE"),
    ("RESERVED", "DIALING AVAILABLE OFFLINE RECOVERY_HOLD"),
    ("DIALING", "CONNECTED AVAILABLE OFFLINE RECOVERY_HOLD"),
    ("CONNECTED", "WRAP_UP OFFLINE RECOVERY_HOLD"),
    ("WRAP_UP", "AVAILABLE PAUSED OFFLINE"),
    ("PAUSED", "AVAILABLE OFFLINE"),
    ("RECOVERY_HOLD", "AVAILABLE WRAP_UP OFFLINE PAUSED"),
])
def test_illegal_agent_edges_are_rejected(engine: Engine, clock: VirtualClock, source: str, allowed: str) -> None:
    campaign, call_id = reserved_call(engine, clock)
    now = clock.now()
    with engine.begin() as connection:
        agents = AgentTransitions(connection)
        calls = CallTransitions(connection)
        if source in {"OFFLINE", "AVAILABLE", "PAUSED"}:
            calls.advance(call_id, CallState.CANCELLED, now, campaign)
            if source != "AVAILABLE":
                assert agents.move(agents.get(1), AgentState(source), now)
        if source in {"DIALING", "CONNECTED", "WRAP_UP"}:
            calls.advance(call_id, CallState.ANSWERED, now, campaign)
        if source in {"CONNECTED", "WRAP_UP"}:
            calls.advance(call_id, CallState.CONNECTED, now, campaign)
        if source == "WRAP_UP":
            calls.advance(call_id, CallState.COMPLETED, now, campaign)
        if source == "RECOVERY_HOLD":
            assert agents.move(agents.get(1), AgentState.RECOVERY_HOLD, now)
        original = agents.get(1)
        assert original.state == source
        for target in set(AgentState) - set(allowed.split()):
            assert not agents.move(original, target, now)
        assert agents.get(1).version == original.version


@pytest.mark.parametrize(("source", "allowed"), [
    ("QUEUED", "RESERVED SUPPRESSED"),
    ("RESERVED", "CONTACTED RETRY_SCHEDULED EXHAUSTED SUPPRESSED"),
    ("CONTACTED", "SUPPRESSED"),
    ("RETRY_SCHEDULED", "RESERVED SUPPRESSED"),
    ("EXHAUSTED", "SUPPRESSED"),
    ("SUPPRESSED", ""),
])
def test_illegal_borrower_edges_are_rejected(engine: Engine, clock: VirtualClock, source: str, allowed: str) -> None:
    with engine.begin() as connection:
        seed(connection, clock, "test-campaign", 1, 1, CampaignConfig())
        connection.execute(text("UPDATE borrowers SET state = :state"), {"state": source})
        borrowers = BorrowerTransitions(connection)
        original = borrowers.get(1)
        for target in set(BorrowerState) - set(allowed.split()):
            assert not borrowers.move(original, target, clock.now())
        assert borrowers.get(1).version == original.version


@pytest.mark.parametrize(("source", "stale"), [
    ("RESERVED", "QUEUED RESERVED UNKNOWN"),
    ("INITIATING", "QUEUED RESERVED INITIATING"),
    ("INITIATED", "QUEUED RESERVED INITIATING INITIATED"),
    ("RINGING", "QUEUED RESERVED INITIATING INITIATED RINGING"),
    ("ANSWERED", "QUEUED RESERVED INITIATING INITIATED RINGING ANSWERED"),
    ("CONNECTED", "QUEUED RESERVED INITIATING INITIATED RINGING ANSWERED CONNECTED"),
    ("UNKNOWN", "QUEUED RESERVED INITIATING INITIATED RINGING ANSWERED UNKNOWN"),
    *((state, " ".join(CallState)) for state in ("COMPLETED", "FAILED", "CANCELLED", "BUSY", "NO_ANSWER")),
])
def test_nonadvancing_call_edges_are_rejected(engine: Engine, clock: VirtualClock, source: str, stale: str) -> None:
    campaign, call_id = reserved_call(engine, clock)
    now = clock.now()
    with engine.begin() as connection:
        calls = CallTransitions(connection)
        if source in {"CONNECTED", "UNKNOWN"}:
            assert calls.advance(call_id, CallState.ANSWERED, now, campaign) == TransitionResult.APPLIED
        if source != "RESERVED":
            assert calls.advance(call_id, CallState(source), now, campaign) == TransitionResult.APPLIED
        original = connection.execute(text("SELECT state, version FROM calls")).one()
        assert original.state == source
        for target in stale.split():
            assert calls.advance(call_id, CallState(target), now, campaign) == TransitionResult.STALE
        assert connection.execute(text("SELECT state, version FROM calls")).one() == original


def test_heartbeat_expiry_holds_call_and_late_heartbeat_cannot_resurrect(engine: Engine, clock: VirtualClock) -> None:
    campaign, call_id = reserved_call(engine, clock)
    clock.advance(31)
    now = clock.now()
    with engine.begin() as connection:
        agents = AgentTransitions(connection)
        before = agents.get(1)
        assert agents.expire(before, now)
        hold = agents.get(1)
        assert (hold.state, hold.current_call_id) == (AgentState.RECOVERY_HOLD, call_id)
        assert not agents.move(hold, AgentState.AVAILABLE, now)
        calls = CallTransitions(connection)
        assert calls.advance(call_id, CallState.CANCELLED, now, campaign) == TransitionResult.APPLIED
        offline = agents.get(1)
        assert (offline.state, offline.current_call_id) == (AgentState.OFFLINE, None)
        assert not agents.heartbeat(1, before.version, before.last_heartbeat_at, now, 30)
        assert not agents.heartbeat(1, offline.version, now, now, 30)
        assert agents.get(1).state == AgentState.OFFLINE
        borrower = BorrowerTransitions(connection).get(1)
        assert (borrower.state, borrower.attempts) == (BorrowerState.RETRY_SCHEDULED, 1)


def test_stale_agent_cas_and_duplicate_heartbeat_are_audited(engine: Engine, clock: VirtualClock) -> None:
    with engine.begin() as connection:
        seed(connection, clock, "test-campaign", 1, 1, CampaignConfig())
        agents = AgentTransitions(connection)
        before = agents.get(1)
        clock.advance(2)
        now = clock.now()
        assert agents.heartbeat(1, before.version, now, now, 30)
        assert not agents.heartbeat(1, before.version, now, now, 30)
        assert not agents.move(before, AgentState.PAUSED, now)
        after = agents.get(1)
        assert after.state == AgentState.AVAILABLE
        assert after.version == before.version + 1
        assert after.last_heartbeat_at == now
        reasons = connection.execute(text("SELECT details->>'reason' FROM audit_log ORDER BY id")).scalars().all()
        assert reasons == ["STALE_HEARTBEAT", "STALE_VERSION"]


def test_two_workers_advance_only_one_answer_and_one_terminal(engine: Engine, clock: VirtualClock) -> None:
    campaign, call_id = reserved_call(engine, clock)
    now = clock.now()

    def concurrent_transition(target: CallState) -> list[TransitionResult]:
        barrier = Barrier(4, timeout=20)

        def advance(_: int) -> TransitionResult:
            barrier.wait()
            with engine.begin() as connection:
                return CallTransitions(connection).advance(call_id, target, now, campaign)

        with ThreadPoolExecutor(max_workers=4) as workers:
            return list(workers.map(advance, range(4), timeout=60))

    assert concurrent_transition(CallState.ANSWERED).count(TransitionResult.APPLIED) == 1
    assert concurrent_transition(CallState.COMPLETED).count(TransitionResult.APPLIED) == 1
    with engine.connect() as connection:
        call = connection.execute(text("SELECT version, answered_at, connected_at, abandoned_at FROM calls")).one()
        assert call == (2, now, None, now)
        assert connection.execute(text("SELECT version, attempts FROM borrowers")).one() == (2, 1)
        assert connection.execute(text("SELECT state, current_call_id FROM agents")).one() == ("AVAILABLE", None)


def test_duplicate_provider_event_ids_conflict_concurrently(engine: Engine, clock: VirtualClock) -> None:
    _, call_id = reserved_call(engine, clock)
    barrier = Barrier(4, timeout=20)

    def insert(provider: str) -> int | None:
        barrier.wait()
        with engine.begin() as connection:
            return connection.execute(
                text("""
                    INSERT INTO call_events (call_id, provider_name, provider_event_id, event_type, received_at, payload)
                    VALUES (:call_id, :provider, 'event-1', 'ANSWERED', :now, '{}'::jsonb)
                    ON CONFLICT (provider_name, provider_event_id) DO NOTHING RETURNING id
                """),
                dict(call_id=call_id, provider=provider, now=clock.now()),
            ).scalar_one_or_none()

    with ThreadPoolExecutor(max_workers=4) as workers:
        inserted = list(workers.map(insert, ["A", "A", "A", "B"], timeout=60))
    assert sum(event_id is not None for event_id in inserted) == 2
    with engine.connect() as connection:
        assert connection.execute(text("SELECT provider_name FROM call_events ORDER BY provider_name")).scalars().all() == ["A", "B"]


def test_terminal_jump_blocks_late_answer_and_connection(engine: Engine, clock: VirtualClock) -> None:
    campaign, call_id = reserved_call(engine, clock)
    now = clock.now()
    with engine.begin() as connection:
        calls = CallTransitions(connection)
        assert calls.advance(call_id, CallState.CONNECTED, now, campaign) == TransitionResult.INVALID
        assert calls.advance(call_id, CallState.COMPLETED, now, campaign) == TransitionResult.APPLIED
        for state in (CallState.ANSWERED, CallState.RINGING, CallState.CANCELLED, CallState.CONNECTED):
            assert calls.advance(call_id, state, now, campaign) == TransitionResult.STALE
        assert connection.execute(text("SELECT state, version, answered_at FROM calls")).one() == ("COMPLETED", 1, None)
        assert connection.execute(text("SELECT count(*) FROM audit_log")).scalar_one() == 5


def test_answer_is_not_bridge_and_stale_agent_cannot_connect(engine: Engine, clock: VirtualClock) -> None:
    campaign, call_id = reserved_call(engine, clock)
    with engine.begin() as connection:
        calls = CallTransitions(connection)
        assert calls.advance(call_id, CallState.ANSWERED, clock.now(), campaign) == TransitionResult.APPLIED
        assert connection.execute(text("SELECT state, connected_at FROM calls")).one() == ("ANSWERED", None)
        clock.advance(31)
        assert calls.advance(call_id, CallState.CONNECTED, clock.now(), campaign) == TransitionResult.INVALID
        agents = AgentTransitions(connection)
        held = agents.get(1)
        assert held.lease_expires_at < clock.now() and held.current_call_id == call_id


def test_retry_is_new_logical_attempt_and_exhaustion_is_terminal(engine: Engine, clock: VirtualClock) -> None:
    campaign, first_id = reserved_call(engine, clock)
    with engine.begin() as connection:
        connection.execute(text("UPDATE borrowers SET max_attempts = 2"))
        assert CallTransitions(connection).advance(first_id, CallState.BUSY, clock.now(), campaign) == TransitionResult.APPLIED
        assert BorrowerTransitions(connection).get(1).next_attempt_at == clock.now() + timedelta(seconds=30)
    assert Dialer(engine, clock).tick(campaign.id, 1) == []
    clock.advance(30)
    with engine.begin() as connection:
        agents = AgentTransitions(connection)
        assert agents.heartbeat(1, agents.get(1).version, clock.now(), clock.now(), 30)
    second_id, = Dialer(engine, clock).tick(campaign.id, 1)
    with engine.begin() as connection:
        assert CallTransitions(connection).advance(second_id, CallState.NO_ANSWER, clock.now(), campaign) == TransitionResult.APPLIED
        borrower = BorrowerTransitions(connection).get(1)
        assert (borrower.state, borrower.attempts) == (BorrowerState.EXHAUSTED, 2)
        assert connection.execute(text("SELECT idempotency_key FROM calls ORDER BY attempt_number")).scalars().all() == ["1:1:1", "1:1:2"]
