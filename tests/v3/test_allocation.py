from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from random import Random
from threading import Barrier
from typing import TypeVar
from uuid import uuid4

import pytest
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import IntegrityError

from smartdialer.v3 import safety
from smartdialer.v3.__main__ import seed
from smartdialer.v3.agents import AgentState, AgentTransitions
from smartdialer.v3.allocator import Allocator, ApprovalRejected
from smartdialer.v3.borrowers import BorrowerTransitions
from smartdialer.v3.calls import CallState, CallTransitions
from smartdialer.v3.campaigns import read_campaign
from smartdialer.v3.clock import VirtualClock
from smartdialer.v3.config import CampaignConfig, Mode
from smartdialer.v3.dialer import Dialer
from smartdialer.v3.pacing import PacingInputs, propose
from smartdialer.v3.safety import Action, SafetyController

T = TypeVar("T")
R = TypeVar("R")


def race(items: Iterable[T], operation: Callable[[T], R]) -> list[R]:
    work = list(items)
    gate = Barrier(len(work), timeout=20)

    def start(item: T) -> R:
        gate.wait()
        return operation(item)

    with ThreadPoolExecutor(max_workers=len(work)) as workers:
        return list(workers.map(start, work, timeout=60))


def prepare(engine: Engine, clock: VirtualClock, agents: int, borrowers: int, **config: object) -> int:
    with engine.begin() as connection:
        return seed(connection, clock, "test-campaign", agents, borrowers, CampaignConfig(**config))


def approve_many(engine: Engine, clock: VirtualClock, campaign: int, workers: int, requested: int) -> list[int]:
    controller = SafetyController(engine, clock)
    proposal = propose(PacingInputs(free_agents=requested, batch_limit=requested))
    decisions = [controller.decide(campaign, proposal) for _ in range(workers)]
    assert all(decision.id is not None and decision.approved_calls > 0 for decision in decisions)
    return [decision.id for decision in decisions]


def assert_invariants(connection: Connection) -> None:
    result = connection.execute(text("""
        SELECT
            (SELECT count(*) FROM (SELECT agent_id FROM calls WHERE state_rank < 100
                AND agent_id IS NOT NULL GROUP BY agent_id HAVING count(*) > 1) AS duplicates) AS duplicate_agents,
            (SELECT count(*) FROM (SELECT borrower_id FROM calls WHERE state_rank < 100
                GROUP BY borrower_id HAVING count(*) > 1) AS duplicates) AS duplicate_borrowers,
            (SELECT count(*) FROM calls c LEFT JOIN agents a ON a.id = c.agent_id AND a.current_call_id = c.id
                WHERE c.state_rank < 100 AND c.agent_id IS NOT NULL AND a.id IS NULL) AS invalid_reservations,
            (SELECT count(*) FROM calls c JOIN safety_decisions s ON s.id = c.safety_decision_id
                WHERE s.consumed_at IS NULL OR s.consumed_at <> c.created_at OR s.campaign_id <> c.campaign_id
                  OR s.consumed_at >= s.expires_at) AS invalid_approvals,
            (SELECT count(*) FROM (SELECT provider_name, provider_event_id FROM call_events
                GROUP BY provider_name, provider_event_id HAVING count(*) > 1) AS duplicates) AS duplicate_events,
            (SELECT count(*) FROM calls c LEFT JOIN agents a ON a.id = c.agent_id AND a.current_call_id = c.id
                WHERE c.state = 'CONNECTED' AND (c.answered_at IS NULL OR a.state NOT IN ('CONNECTED','RECOVERY_HOLD')
                    OR a.id IS NULL)) AS invalid_bridges,
            (SELECT count(*) FROM calls WHERE state_rank < 100 AND agent_id IS NOT NULL) AS active_agent_bound,
            (SELECT count(*) FROM agents WHERE current_call_id IS NOT NULL) AS agents_holding_calls,
            (SELECT count(*) FROM calls c JOIN borrowers b ON b.id = c.borrower_id
                WHERE c.state_rank < 100 AND b.state NOT IN ('RESERVED','SUPPRESSED')) AS unreserved_borrowers
    """)).mappings().one()
    assert result.active_agent_bound == result.agents_holding_calls, dict(result)
    assert all(
        value == 0 for name, value in result.items() if name not in {"active_agent_bound", "agents_holding_calls"}
    ), dict(result)


@pytest.mark.parametrize(("agents", "borrowers", "expected"), [(1, 10, 1), (12, 1, 1), (50, 100, 50)])
def test_workers_race_on_unconsumed_approvals(
    engine: Engine, clock: VirtualClock, agents: int, borrowers: int, expected: int,
) -> None:
    campaign = prepare(engine, clock, agents, borrowers)
    ids = approve_many(engine, clock, campaign, workers=8, requested=80)
    allocated = race(ids, Allocator(engine, clock).allocate)
    assert sum(map(len, allocated)) == expected
    with engine.connect() as connection:
        assert_invariants(connection)
        assert connection.execute(text("SELECT count(*) FROM calls WHERE state = 'RESERVED'")).scalar_one() == expected
        assert connection.execute(text("SELECT count(*) FROM safety_decisions WHERE consumed_at IS NOT NULL")).scalar_one() == 8


def test_single_approval_cannot_be_replayed_concurrently(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, 8, 8)
    approval, = approve_many(engine, clock, campaign, workers=1, requested=3)

    def allocate(_: int) -> int:
        try:
            return len(Allocator(engine, clock).allocate(approval))
        except ApprovalRejected as error:
            assert "already consumed" in str(error)
            return 0

    assert sorted(race(range(8), allocate)) == [0, 0, 0, 0, 0, 0, 0, 3]
    with engine.connect() as connection:
        assert_invariants(connection)


def test_each_allocator_rechecks_current_campaign_caps(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, 10, 10)
    first, second = approve_many(engine, clock, campaign, workers=2, requested=10)
    with engine.begin() as connection:
        connection.execute(text("UPDATE campaigns SET max_concurrent_calls = 4, provider_rate_limit = 2"))
    allocator = Allocator(engine, clock)
    assert len(allocator.allocate(first)) == 2
    assert allocator.allocate(second) == []
    clock.advance(1)
    another, = approve_many(engine, clock, campaign, workers=1, requested=10)
    assert len(allocator.allocate(another)) == 2


def test_missing_expired_and_rejected_approvals_cannot_allocate(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, 1, 1, approval_ttl_seconds=2)
    allocator = Allocator(engine, clock)
    approval, = approve_many(engine, clock, campaign, workers=1, requested=1)
    clock.advance(2)
    with pytest.raises(ApprovalRejected, match="validity window"):
        allocator.allocate(approval)
    with pytest.raises(ApprovalRejected, match="missing"):
        allocator.allocate(1_000_000)
    with pytest.raises(ApprovalRejected, match="valid safety decision id"):
        allocator.allocate(None)
    denial = SafetyController(engine, clock).decide(campaign, propose(PacingInputs(0, 50)))
    with pytest.raises(ApprovalRejected, match="does not authorize"):
        allocator.allocate(denial.id)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM calls")).scalar_one() == 0


@pytest.mark.parametrize("table", ["agents", "borrowers"])
def test_locked_candidates_are_skipped(engine: Engine, clock: VirtualClock, table: str) -> None:
    campaign = prepare(engine, clock, 3, 3)
    approval, = approve_many(engine, clock, campaign, workers=1, requested=3)
    with engine.begin() as blocker:
        blocker.execute(text(f"SELECT id FROM {table} WHERE id = 1 FOR UPDATE"))
        assert len(Allocator(engine, clock).allocate(approval)) == 2
    with engine.connect() as connection:
        column = "agent_id" if table == "agents" else "borrower_id"
        assert connection.execute(text(f"SELECT {column} FROM calls ORDER BY {column}")).scalars().all() == [2, 3]
        assert_invariants(connection)


def test_revalidates_borrower_order_eligibility_and_global_suppression(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, 4, 8)
    approval, = approve_many(engine, clock, campaign, workers=1, requested=4)
    now = clock.now()
    with engine.begin() as connection:
        other = seed(connection, clock, "another", 1, 1, CampaignConfig())
        borrowers = BorrowerTransitions(connection)
        phone = borrowers.get(2).phone_normalized
        assert borrowers.suppress(f"+{phone[:3]} ({phone[3:9]}) {phone[9:]}", now)
        assert borrowers.enqueue(other, phone, now) is None
        connection.execute(text("UPDATE borrowers SET priority = 100 WHERE id IN (2,3,4,5,8)"))
        connection.execute(text("UPDATE borrowers SET attempts = max_attempts WHERE id = 3"))
        connection.execute(text("UPDATE borrowers SET next_attempt_at = :now + interval '1 hour' WHERE id = 4"), {"now": now})
        connection.execute(text("UPDATE borrowers SET next_attempt_at = :now - interval '1 second' WHERE id = 8"), {"now": now})
    assert len(Allocator(engine, clock).allocate(approval)) == 4
    with engine.connect() as connection:
        assert connection.execute(text("SELECT borrower_id FROM calls ORDER BY agent_id")).scalars().all() == [8, 5, 1, 6]


def test_safety_trusts_authoritative_counts_and_fails_closed(
    engine: Engine, clock: VirtualClock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign = prepare(engine, clock, 3, 30, heartbeat_ttl_seconds=2)
    controller = SafetyController(engine, clock)
    request = propose(PacingInputs(50, 17))
    decision = controller.decide(campaign, request)
    assert (decision.action, decision.approved_calls) == (Action.REDUCE, 3)
    assert "AVAILABLE_AGENTS" in decision.reasons
    with engine.begin() as connection:
        connection.execute(text("UPDATE campaigns SET mode = 'HYBRID'"))
    fallback = controller.decide(campaign, request)
    assert (fallback.action, fallback.approved_calls, fallback.effective_mode) == (
        Action.FALLBACK_TO_PROGRESSIVE, 3, Mode.STRICT_PROGRESSIVE,
    )
    clock.advance(2)
    stale = controller.decide(campaign, request)
    assert (stale.action, stale.approved_calls) == (Action.REJECT, 0)
    assert Allocator(engine, clock).allocate(decision.id) == []

    def fail(*_: object) -> None:
        raise RuntimeError("injected snapshot failure")

    monkeypatch.setattr(safety, "admission_capacity", fail)
    failed = controller.decide(campaign, request)
    assert (failed.action, failed.approved_calls, failed.reasons) == (Action.REJECT, 0, ("SAFETY_ERROR",))
    with engine.connect() as connection:
        stored = connection.execute(text("SELECT * FROM safety_decisions WHERE id = :id"), {"id": failed.id}).one()
        assert stored.action == "REJECT"
        assert stored.pacing_reasoning["summary"] == request.reasoning


def insert_call_without_allocator(connection: Connection, campaign: int, approval: int | None, now: object) -> None:
    connection.execute(
        text("""
            INSERT INTO calls (
                id, campaign_id, borrower_id, agent_id, safety_decision_id, idempotency_key, state,
                state_rank, provider, attempt_number, created_at, next_retry_at
            ) VALUES (:id, :campaign, 1, 1, :approval, :key, 'RESERVED', 20, 'A', 2, :now, :now)
        """),
        dict(id=uuid4(), campaign=campaign, approval=approval, key=f"{campaign}:1:2", now=now),
    )


def test_database_rejects_missing_unconsumed_or_previously_consumed_approval(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, 2, 2)
    approval, = approve_many(engine, clock, campaign, workers=1, requested=2)
    for invalid_approval in (None, 9999, approval):
        with pytest.raises(IntegrityError, match="valid approval consumed in this transaction"), engine.begin() as connection:
            insert_call_without_allocator(connection, campaign, invalid_approval, clock.now())
    Allocator(engine, clock).allocate(approval)
    with pytest.raises(IntegrityError, match="valid approval consumed in this transaction"), engine.begin() as connection:
        insert_call_without_allocator(connection, campaign, approval, clock.now())


def test_database_rejects_agent_binding_without_matching_reservation(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, 2, 2)
    approval, = approve_many(engine, clock, campaign, workers=1, requested=1)
    with pytest.raises(IntegrityError, match="foreign key constraint"), engine.begin() as connection:
        connection.execute(text("""
            UPDATE safety_decisions SET consumed_at = :now, consumed_in = pg_current_xact_id() WHERE id = :id
        """), {"now": clock.now(), "id": approval})
        insert_call_without_allocator(connection, campaign, approval, clock.now())
    assert len(Allocator(engine, clock).allocate(approval)) == 1


def test_failed_second_pair_rolls_back_the_batch_and_consumption(
    engine: Engine, clock: VirtualClock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign = prepare(engine, clock, 3, 3)
    approval, = approve_many(engine, clock, campaign, workers=1, requested=3)
    allocator = Allocator(engine, clock)
    real_move = BorrowerTransitions.move

    def fail_on_second(service: BorrowerTransitions, borrower, *args, **kwargs) -> bool:
        if borrower.id == 2:
            raise RuntimeError("injected pair failure")
        return real_move(service, borrower, *args, **kwargs)

    with monkeypatch.context() as fault:
        fault.setattr(BorrowerTransitions, "move", fail_on_second)
        with pytest.raises(RuntimeError, match="injected pair failure"):
            allocator.allocate(approval)

    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM calls")).scalar_one() == 0
        assert connection.execute(text("SELECT count(*) FROM agents WHERE state = 'AVAILABLE'")).scalar_one() == 3
        assert connection.execute(text("SELECT count(*) FROM borrowers WHERE attempts = 0")).scalar_one() == 3
        assert connection.execute(text("SELECT consumed_at FROM safety_decisions")).scalar_one() is None
    assert len(allocator.allocate(approval)) == 3


def test_randomized_concurrent_run(engine: Engine, clock: VirtualClock) -> None:
    campaign_id = prepare(engine, clock, 16, 70, retry_delay_seconds=0, wrap_up_seconds=0)
    with engine.begin() as connection:
        connection.execute(text("UPDATE campaigns SET provider_rate_limit = 10000, max_concurrent_calls = 12"))
        campaign = read_campaign(connection, campaign_id)

    def operate(seed_value: int) -> None:
        rng = Random(seed_value)
        dialer = Dialer(engine, clock)
        for _ in range(14):
            for call_id in dialer.tick(campaign_id, rng.randint(1, 6)):
                with engine.begin() as connection:
                    calls = CallTransitions(connection)
                    now = clock.now()
                    if rng.random() < 0.4:
                        calls.advance(call_id, CallState.ANSWERED, now, campaign)
                        if rng.random() < 0.5:
                            calls.advance(call_id, CallState.CONNECTED, now, campaign)
                    agent_id = connection.execute(
                        text("SELECT agent_id FROM calls WHERE id = :id"), {"id": call_id},
                    ).scalar_one()
                    calls.advance(call_id, CallState.COMPLETED, now, campaign)
                    calls.advance(call_id, CallState.RINGING, now, campaign)
                    agents = AgentTransitions(connection)
                    agent = agents.get(agent_id)
                    if agent.state == AgentState.WRAP_UP:
                        assert agents.move(agent, AgentState.AVAILABLE, now)

    race(range(8), operate)
    with engine.connect() as connection:
        assert_invariants(connection)
        completed = connection.execute(text("SELECT count(*) FROM calls WHERE state = 'COMPLETED'")).scalar_one()
        assert completed > 16
        assert connection.execute(text("SELECT count(*) FROM calls WHERE state_rank < 100")).scalar_one() == 0
        assert connection.execute(text("SELECT count(*) FROM audit_log WHERE action = 'TRANSITION_REJECTED'")).scalar_one() == completed
