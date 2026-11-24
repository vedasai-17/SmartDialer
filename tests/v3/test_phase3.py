from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from random import Random
from threading import Barrier

import pytest
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import IntegrityError

from smartdialer.v3.__main__ import seed
from smartdialer.v3.agents import AgentState, AgentTransitions
from smartdialer.v3.allocator import Allocator
from smartdialer.v3.clock import VirtualClock
from smartdialer.v3.config import BreakerConfig, CampaignConfig, DispatchConfig, ExcessAnswerPolicy, Mode, PredictionConfig
from smartdialer.v3.dialer import Dialer
from smartdialer.v3.dispatch import Dispatcher
from smartdialer.v3.events import EventInbox, EventProcessor
from smartdialer.v3.mock_provider import MockProvider, mock_providers
from smartdialer.v3.pacing import PacingInputs, PacingProposal, propose
from smartdialer.v3.reaper import Reaper
from smartdialer.v3.safety import Action, SafetyController, SafetyDecision
from smartdialer.v3.safety_policy import overflow_probability, risk_limited_count


def history(connection: Connection, campaign: int, at: datetime, total: int, answers: int, *, abandoned: int = 0) -> None:
    """Load resolved synthetic measurements without skipping the call insert approval trigger."""
    created = at - timedelta(seconds=2)
    approval = connection.execute(text("""
        INSERT INTO safety_decisions (campaign_id, action, requested_calls, approved_calls, effective_mode,
            reasons, pacing_inputs, pacing_reasoning, safety_inputs, decided_at, expires_at, consumed_at, consumed_in)
        VALUES (:campaign, 'APPROVE', :total, :total, 'BOUNDED_PREDICTIVE', ARRAY['HISTORY_FIXTURE'],
            '{}'::jsonb, '{}'::jsonb, jsonb_build_object('approved_speculative', CAST(:total AS integer)),
            :created, :created + interval '1 second', :created, pg_current_xact_id())
        RETURNING id
    """), {"campaign": campaign, "total": total, "created": created}).scalar_one()
    params = {"campaign": campaign, "total": total, "approval": approval, "created": created, "at": at,
              "answers": answers, "abandoned": abandoned}
    connection.execute(text("""
        WITH history_borrowers AS (
            INSERT INTO borrowers (campaign_id, phone_normalized, queued_at, next_attempt_at, state, attempts)
            SELECT :campaign, '988' || lpad((:approval * 10000 + n)::text, 12, '0'), :created, :created, 'CONTACTED', 1
            FROM generate_series(1, CAST(:total AS integer)) n RETURNING id, campaign_id
        )
        INSERT INTO calls (id, campaign_id, borrower_id, safety_decision_id, idempotency_key, state, state_rank,
            is_speculative, provider, attempt_number, created_at, next_retry_at)
        SELECT gen_random_uuid(), b.campaign_id, b.id, :approval, b.campaign_id || ':' || b.id || ':' || '1', 'RESERVED',
            20, true, c.config->>'provider', 1, :created, :created
        FROM history_borrowers b JOIN campaigns c ON c.id = b.campaign_id
    """), params)
    connection.execute(text("""
        WITH ordered AS (
            SELECT id, row_number() OVER (ORDER BY borrower_id) AS ord FROM calls WHERE safety_decision_id = :approval
        ), outcomes AS (
            SELECT id, ord, (ord * :answers / :total) > ((ord - 1) * :answers / :total) AS answered,
                ord * :answers / :total <= :abandoned AS abandoned
            FROM ordered
        )
        UPDATE calls c SET state = CASE WHEN o.answered THEN 'COMPLETED' ELSE 'NO_ANSWER' END,
            state_rank = 100, initiated_at = :created, ringing_at = :created + interval '0.2 second',
            answered_at = CASE WHEN o.answered THEN :at + o.ord * interval '0.001 second' END,
            connected_at = CASE WHEN o.answered AND NOT o.abandoned THEN :at + o.ord * interval '0.001 second' END,
            ended_at = :at + o.ord * interval '0.001 second' + interval '0.3 second',
            abandoned_at = CASE WHEN o.answered AND o.abandoned THEN :at + o.ord * interval '0.001 second' END,
            agent_id = CASE WHEN o.answered AND NOT o.abandoned THEN (SELECT min(id) FROM agents WHERE campaign_id = :campaign) END
        FROM outcomes o WHERE c.id = o.id
    """), params)


def prepare(
    engine: Engine, clock: VirtualClock, *, agents: int = 5, wraps: int = 3, wrap_at: float = 1.0,
    mode: Mode = Mode.BOUNDED_PREDICTIVE, speculation: bool = True,
    policy: ExcessAnswerPolicy = ExcessAnswerPolicy.SAFE_TERMINATE,
    prediction: PredictionConfig | None = None, breaker: BreakerConfig | None = None,
    reference: int = 15, recent: int = 15, with_history: bool = True,
    risk_budget: float = 0.07, max_speculative: int = 4, hold_seconds: float = 2,
) -> int:
    settings = prediction or PredictionConfig(
        short_window_seconds=20, long_window_seconds=240, max_observation_age_seconds=10,
        min_answer_samples=12, min_recent_samples=8, min_abandonment_samples=12,
        abandonment_threshold=0.20, abandonment_resume_threshold=0.15,
    )
    config = CampaignConfig(heartbeat_ttl_seconds=600, wrap_up_seconds=30, speculation_enabled=speculation,
                            prediction=settings, breaker=breaker or BreakerConfig(min_samples=100),
                            excess_answer_policy=policy, answer_hold_seconds=hold_seconds)
    now = clock.now()
    with engine.begin() as connection:
        campaign = seed(connection, clock, "phase-three", agents, 30, config, mode=mode,
                        max_concurrent_calls=agents + 15, provider_rate_limit=agents + 15,
                        max_speculative_calls=max_speculative, risk_budget=risk_budget)
        if wraps:
            connection.execute(text("""
                UPDATE agents SET state = 'WRAP_UP', wrap_up_ends_at = :end
                WHERE id IN (SELECT id FROM agents WHERE campaign_id = :id ORDER BY id DESC LIMIT :wraps)
            """), {"end": now + timedelta(seconds=wrap_at), "id": campaign, "wraps": wraps})
        if with_history:
            history(connection, campaign, now - timedelta(seconds=90), 100, reference)
            history(connection, campaign, now - timedelta(seconds=4), 100, recent)
    return campaign


def decide(engine: Engine, clock: VirtualClock, campaign: int, desired: int) -> SafetyDecision:
    # Deliberately impossible pacing forecasts should have no authority over Safety's inputs.
    proposal = PacingProposal(desired, PacingInputs(999, desired, predictive=True, soon_free=999, answer_ewma=0), "test request")
    return SafetyController(engine, clock).decide(campaign, proposal)


def evidence(engine: Engine, decision: SafetyDecision) -> dict[str, object]:
    with engine.connect() as connection:
        return connection.execute(text("SELECT safety_inputs FROM safety_decisions WHERE id = :id"), {"id": decision.id}).scalar_one()


def services(engine: Engine, clock: VirtualClock, **profile: object) -> tuple[dict[str, MockProvider], EventInbox, EventProcessor, Dispatcher]:
    providers = mock_providers(engine, clock)
    providers["A"].configure(**{"answer_rate": 1, "failure_rate": 0, "timeout_rate": 0, "talk_seconds": 30,
                                "ring_seconds": 0.3, **profile})
    inbox = EventInbox(engine, clock, providers)
    processor = EventProcessor(engine, clock)
    worker = Dispatcher(engine, clock, providers, "phase3", config=DispatchConfig(status_poll_seconds=1), rng=Random(17))
    return providers, inbox, processor, worker


@pytest.mark.parametrize(("probability", "recent_answers", "expected"), [(0, 0, 50), (1, 100, 8)])
def test_pacing_formula_handles_probability_edges(probability: float, recent_answers: int, expected: int) -> None:
    result = propose(PacingInputs(8, 50, predictive=True, answer_ewma=probability, recent_answers=recent_answers,
                                  recent_samples=100, horizon_seconds=2))
    assert result.requested_calls == expected
    assert "expected_inflight_answers=0.00" in result.reasoning


def test_explainable_pacing_subtracts_inflight_and_includes_only_forecast_as_soon_free() -> None:
    inputs = PacingInputs(8, 50, predictive=True, soon_free=3, in_flight=5, answer_ewma=0.5,
                          recent_answers=20, recent_samples=100, horizon_seconds=2.1)
    proposal = propose(inputs)
    assert proposal.requested_calls == 17 and "forecast within 2.1s" in proposal.reasoning
    assert risk_limited_count(0, 1, 0.5, 0.25, 8) == 2
    assert overflow_probability(0, 3, 1, 0.5) == pytest.approx(0.5)
    assert risk_limited_count(2, 0, 1, 0.99, 5) == 0


def test_safety_recomputes_risk_and_tracks_hard_and_speculative_counts(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, agents=6, wraps=1, risk_budget=0.08, max_speculative=5)
    decision = decide(engine, clock, campaign, 20)
    data = evidence(engine, decision)
    assert decision.action == Action.REDUCE
    assert decision.approved_hard == data["free_agents"] == 5
    assert decision.approved_speculative > 0
    assert data["p_safe"] >= data["recent_answer_upper"] > 0
    assert data["risk_forecast_seconds"] > 0 and data["risk_soon_free"] == 1
    assert data["risk_if_approved"] <= 0.08 < data["risk_if_requested"]
    assert "OVERFLOW_RISK_BUDGET" in decision.reasons
    ids = Allocator(engine, clock).allocate(decision.id)
    assert len(ids) == decision.approved_calls
    with engine.connect() as connection:
        counts = connection.execute(text("""
            SELECT count(*) FILTER (WHERE NOT is_speculative AND agent_id IS NOT NULL),
                   count(*) FILTER (WHERE is_speculative AND agent_id IS NULL)
            FROM calls WHERE state = 'RESERVED'
        """)).one()
    assert counts == (decision.approved_hard, decision.approved_speculative)


def test_concurrent_allocations_recheck_the_speculative_cap(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, max_speculative=2)
    decisions = [decide(engine, clock, campaign, 8) for _ in range(4)]
    assert all(decision.approved_speculative == 2 for decision in decisions)
    barrier = Barrier(4, timeout=20)

    def allocate(decision: SafetyDecision) -> int:
        barrier.wait()
        return len(Allocator(engine, clock).allocate(decision.id))

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(allocate, decisions, timeout=40)) == 4
    with engine.begin() as connection:
        assert connection.execute(text("SELECT count(*) FROM calls WHERE state = 'RESERVED' AND is_speculative")).scalar_one() == 2


def test_hard_admission_preserves_room_for_existing_speculative_answers(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, wraps=3, wrap_at=1, max_speculative=4)
    allocate_live(engine, clock, campaign, 4, 2)
    clock.advance(1.01)
    assert Reaper(engine, clock).tick(limit=1, wrapups_only=True) == 1
    hold = decide(engine, clock, campaign, 5)
    assert evidence(engine, hold)["free_agents"] == 1
    assert evidence(engine, hold)["seats_for_existing_speculation"] == 1
    assert hold.approved_hard == hold.approved_speculative == 0
    assert "EXISTING_SPECULATIVE_EXPOSURE" in hold.reasons


def test_disabling_speculation_before_allocation_cannot_reuse_issued_speculative_budget(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock)
    approval = decide(engine, clock, campaign, 5)
    assert approval.approved_speculative == 3
    with engine.begin() as connection:
        connection.execute(text("UPDATE campaigns SET config = jsonb_set(config, '{speculation_enabled}', 'false')"))
    assert len(Allocator(engine, clock).allocate(approval.id)) == approval.approved_hard
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM calls WHERE state = 'RESERVED' AND is_speculative")).scalar_one() == 0


def test_maximum_overdial_multiplier_caps_new_speculation(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock)
    with engine.begin() as connection:
        connection.execute(text("UPDATE campaigns SET config = jsonb_set(config, '{prediction,max_overdial_multiplier}', '1.5')"))
    decision = decide(engine, clock, campaign, 10)
    assert decision.approved_hard == 2 and decision.approved_speculative == 1
    assert "MAX_OVERDIAL_MULTIPLIER" in decision.reasons


def test_database_cannot_spend_a_hard_only_approval_on_speculation(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, mode=Mode.HYBRID, speculation=False)
    approval = decide(engine, clock, campaign, 5)
    assert approval.effective_mode == Mode.HYBRID and approval.approved_speculative == 0
    with pytest.raises(IntegrityError, match="approved capacity class"), engine.begin() as connection:
        params = {"id": campaign, "approval": approval.id, "now": clock.now()}
        connection.execute(text("""
            UPDATE safety_decisions SET consumed_at = :now, consumed_in = pg_current_xact_id() WHERE id = :approval
        """), params)
        connection.execute(text("""
            INSERT INTO calls (id, campaign_id, borrower_id, safety_decision_id, idempotency_key,
                state, state_rank, is_speculative, provider, attempt_number, created_at, next_retry_at)
            VALUES (gen_random_uuid(), :id, 1, :approval, CAST(:id AS text) || ':' || '1' || ':' || '1',
                'RESERVED', 20, true, 'A', 1, :now, :now)
        """), params)


@pytest.mark.parametrize(("reference", "recent", "reason"), [
    (70, 10, "ANSWER_RATE_SHIFT_DOWN"), (20, 70, "ANSWER_RATE_SHIFT_UP"),
])
def test_large_answer_rate_shift_falls_back(engine: Engine, clock: VirtualClock, reference: int, recent: int, reason: str) -> None:
    campaign = prepare(engine, clock, reference=reference, recent=recent)
    decision = decide(engine, clock, campaign, 10)
    assert (decision.action, decision.effective_mode, decision.approved_speculative) == (Action.FALLBACK_TO_PROGRESSIVE, Mode.STRICT_PROGRESSIVE, 0)
    assert reason in decision.reasons
    if recent > reference:
        facts = evidence(engine, decision)
        assert facts["p_safe"] > facts["recent_answer_upper"]


def test_moderate_upward_shift_widens_probability_and_reduces_speculation(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, agents=6, wraps=1, reference=15, recent=35, risk_budget=0.15)
    decision = decide(engine, clock, campaign, 9)
    facts = evidence(engine, decision)
    assert decision.effective_mode == Mode.BOUNDED_PREDICTIVE
    assert "ANSWER_RATE_SHIFT_UP" in decision.reasons and "OVERFLOW_RISK_BUDGET" in decision.reasons
    assert facts["p_safe"] >= facts["recent_answer_upper"] + 0.19
    assert decision.approved_speculative == 1


def test_missing_or_stale_prediction_falls_back_in_hybrid(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, mode=Mode.HYBRID, with_history=False)
    missing = decide(engine, clock, campaign, 5)
    assert missing.action == Action.FALLBACK_TO_PROGRESSIVE
    assert "PREDICTION_STALE_OR_MISSING" in missing.reasons
    with engine.begin() as connection:
        history(connection, campaign, clock.now() - timedelta(seconds=12), 100, 20)
        history(connection, campaign, clock.now() - timedelta(seconds=90), 100, 20)
    stale = decide(engine, clock, campaign, 5)
    assert stale.effective_mode == Mode.STRICT_PROGRESSIVE and "PREDICTION_STALE_OR_MISSING" in stale.reasons


def test_hybrid_prestages_then_reserves_at_wrapup_without_speculating(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, agents=3, wraps=3, wrap_at=1, mode=Mode.HYBRID, speculation=False)
    dialer = Dialer(engine, clock)
    assert dialer.tick(campaign, 10) == []
    with engine.connect() as connection:
        row = connection.execute(text("""
            SELECT effective_mode, safety_inputs FROM safety_decisions WHERE campaign_id = :id ORDER BY id DESC LIMIT 1
        """), {"id": campaign}).one()
        assert row.effective_mode == "HYBRID" and len(row.safety_inputs["prestaged_borrower_ids"]) == 3
        assert connection.execute(text("SELECT count(*) FROM calls WHERE state = 'RESERVED'")).scalar_one() == 0
    assert dialer.delay(campaign, 10) == pytest.approx(1)
    clock.advance(1)
    assert len(dialer.tick(campaign, 10)) == 3
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM calls WHERE state = 'RESERVED' AND is_speculative")).scalar_one() == 0
        assert connection.execute(text("SELECT count(*) FROM audit_log WHERE action = 'PRESTAGED_BORROWERS_RESERVED'")).scalar_one() == 1


def allocate_live(engine: Engine, clock: VirtualClock, campaign: int, desired: int, speculative: int) -> None:
    approval = decide(engine, clock, campaign, desired)
    assert approval.approved_speculative == speculative
    assert len(Allocator(engine, clock).allocate(approval.id)) == approval.approved_calls


def call_states(engine: Engine, *, speculative: bool | None = None) -> list[tuple]:
    with engine.connect() as connection:
        return connection.execute(text("""
            SELECT state, agent_id, abandoned_at, connected_at, ended_at, answered_at
            FROM calls WHERE borrower_id <= 30 AND (CAST(:speculative AS boolean) IS NULL OR is_speculative = :speculative)
            ORDER BY borrower_id
        """), {"speculative": speculative}).all()


def test_two_speculative_answers_race_for_one_agent(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, wraps=3, wrap_at=0.4)
    allocate_live(engine, clock, campaign, 4, 2)
    providers, inbox, processor, worker = services(engine, clock)
    assert worker.drain() == 4
    clock.advance(0.5)
    assert Reaper(engine, clock).tick(limit=1, wrapups_only=True) == 1
    providers["A"].deliver_ready(inbox.receive)
    assert processor.drain() == 8
    barrier = Barrier(2, timeout=20)

    def bridge(index: int) -> None:
        barrier.wait()
        Dispatcher(engine, clock, providers, f"race-{index}").drain()

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(bridge, range(2), timeout=30))
    rows = call_states(engine, speculative=True)
    assert {row.state for row in rows} == {"CONNECTED", "CANCELLED"}
    connected, = [row for row in rows if row.state == "CONNECTED"]
    rejected, = [row for row in rows if row.state == "CANCELLED"]
    assert connected.agent_id is not None and connected.abandoned_at is None
    assert rejected.agent_id is None and rejected.connected_at is None and rejected.abandoned_at is not None
    with engine.connect() as connection:
        assert connection.execute(text("""
            SELECT count(*) FROM calls c JOIN agents a ON a.id = c.agent_id AND a.current_call_id = c.id
            WHERE c.state = 'CONNECTED'
        """)).scalar_one() == 3


@pytest.mark.parametrize("gets_agent", [True, False])
def test_short_hold_counts_unclaimed_answer_and_enforces_deadline(engine: Engine, clock: VirtualClock, gets_agent: bool) -> None:
    campaign = prepare(engine, clock, policy=ExcessAnswerPolicy.SHORT_HOLD, wrap_at=0.9, hold_seconds=1)
    allocate_live(engine, clock, campaign, 3, 1)
    providers, inbox, processor, worker = services(engine, clock)
    assert worker.drain() == 3
    clock.advance(0.5)
    providers["A"].deliver_ready(inbox.receive)
    processor.drain()
    worker.drain()
    waiting, = call_states(engine, speculative=True)
    assert waiting.state == "ANSWERED" and waiting.agent_id is None and waiting.abandoned_at is not None
    if gets_agent:
        clock.advance(0.5)
        Reaper(engine, clock).tick(wrapups_only=True)
    else:
        clock.advance(1)
    worker.drain()
    final, = call_states(engine, speculative=True)
    assert final.abandoned_at == waiting.abandoned_at
    if gets_agent:
        assert final.state == "CONNECTED" and final.agent_id is not None
    else:
        assert final.state in {"CANCELLED", "COMPLETED"} and final.ended_at - final.answered_at <= timedelta(seconds=1)


def test_excess_answer_policy_forces_progressive_on_next_decision(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, policy=ExcessAnswerPolicy.PROGRESSIVE_FALLBACK)
    allocate_live(engine, clock, campaign, 3, 1)
    providers, inbox, processor, worker = services(engine, clock)
    worker.drain()
    clock.advance(0.5)
    providers["A"].deliver_ready(inbox.receive)
    processor.drain()
    worker.drain()
    clock.advance(0.5)
    Reaper(engine, clock).tick(wrapups_only=True)
    decision = decide(engine, clock, campaign, 4)
    assert decision.action == Action.FALLBACK_TO_PROGRESSIVE and decision.approved_speculative == 0
    assert "EXCESS_ANSWER_PROGRESSIVE_FALLBACK" in decision.reasons
    assert evidence(engine, decision)["abandonment_guard"]["cooldown_until"] is not None


def test_abandonment_upper_bound_requires_cooldown_and_hysteresis(engine: Engine, clock: VirtualClock) -> None:
    config = PredictionConfig(
        short_window_seconds=60, long_window_seconds=240, max_observation_age_seconds=30,
        min_answer_samples=12, min_recent_samples=5, min_abandonment_samples=10,
        abandonment_window_seconds=10, abandonment_threshold=0.2, abandonment_resume_threshold=0.10,
        abandonment_cooldown_seconds=20,
    )
    campaign = prepare(engine, clock, prediction=config)
    with engine.begin() as connection:
        history(connection, campaign, clock.now() - timedelta(seconds=1), 100, 15, abandoned=10)
    tripped = decide(engine, clock, campaign, 4)
    assert tripped.action == Action.FALLBACK_TO_PROGRESSIVE and "ABANDONMENT_UPPER_BOUND" in tripped.reasons
    clock.advance(11)
    with engine.begin() as connection:
        history(connection, campaign, clock.now() - timedelta(seconds=1), 500, 75)
    cooling = decide(engine, clock, campaign, 4)
    assert evidence(engine, cooling)["abandonment_guard"]["upper"] < 0.10
    assert cooling.action == Action.FALLBACK_TO_PROGRESSIVE
    clock.advance(10)
    with engine.begin() as connection:
        history(connection, campaign, clock.now() - timedelta(seconds=1), 500, 75)
    recovered = decide(engine, clock, campaign, 4)
    assert recovered.effective_mode == Mode.BOUNDED_PREDICTIVE
    assert evidence(engine, recovered)["abandonment_guard"]["cooldown_until"] is None


def test_agent_drop_from_100_to_60_cancels_speculative_ringing_only(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, agents=103, wraps=2, wrap_at=1.5)
    with engine.begin() as connection:
        agents = AgentTransitions(connection)
        for agent_id in range(2, 102):
            assert agents.request_presence(agent_id, AgentState.PAUSED, clock.now(), 600)
    allocate_live(engine, clock, campaign, 3, 2)
    providers, inbox, processor, worker = services(engine, clock, ring_seconds=5)
    worker.drain()
    clock.advance(0.25)
    providers["A"].deliver_ready(inbox.receive)
    processor.drain()
    assert all(row.state == "RINGING" for row in call_states(engine))
    with engine.begin() as connection:
        agents = AgentTransitions(connection)
        for agent_id in range(2, 102):
            assert agents.request_presence(agent_id, AgentState.AVAILABLE, clock.now(), 600)
    assert evidence(engine, decide(engine, clock, campaign, 0))["free_agents"] == 100
    clock.advance(0.25)
    with engine.begin() as connection:
        agents = AgentTransitions(connection)
        for agent_id in range(62, 102):
            assert agents.request_presence(agent_id, AgentState.PAUSED, clock.now(), 600)
    blocked = decide(engine, clock, campaign, 100)
    assert (blocked.action, blocked.approved_calls) == (Action.REJECT, 0)
    assert "SUDDEN_AGENT_DROP" in blocked.reasons
    assert evidence(engine, blocked)["availability_drop_fraction"] == pytest.approx(0.4)
    assert worker.drain() == 2
    assert all(row.state == "CANCELLED" and row.abandoned_at is None for row in call_states(engine, speculative=True))
    hard, = call_states(engine, speculative=False)
    assert hard.state == "RINGING"


def test_provider_outage_opens_breaker_and_recovery_requires_one_probe(engine: Engine, clock: VirtualClock) -> None:
    config = BreakerConfig(min_samples=2, error_fraction=0.5, cooldown_seconds=2, window_seconds=20, probe_calls=1)
    campaign = prepare(engine, clock, agents=5, wraps=0, mode=Mode.HYBRID, breaker=config)
    providers, inbox, processor, worker = services(engine, clock)
    dialer = Dialer(engine, clock)
    assert len(dialer.tick(campaign, 1)) == 1
    assert worker.step()
    clock.advance(0.5)
    providers["A"].deliver_ready(inbox.receive)
    processor.drain()
    assert worker.step() and call_states(engine)[0].state == "CONNECTED"
    assert len(dialer.tick(campaign, 1)) == 1
    providers["A"].configure(outage=True)
    assert worker.step()
    assert [row.state for row in call_states(engine)] == ["CONNECTED", "UNKNOWN"]
    opened = decide(engine, clock, campaign, 5)
    assert evidence(engine, opened)["breaker"]["state"] == "OPEN"
    assert "PROVIDER_BREAKER_OPEN" in opened.reasons and opened.approved_calls == 0
    assert dialer.tick(campaign, 5) == []
    clock.advance(1)
    worker.drain()
    assert call_states(engine)[0].state == "CONNECTED"
    providers["A"].configure(outage=False)
    clock.advance(1.1)
    probe = decide(engine, clock, campaign, 5)
    assert evidence(engine, probe)["breaker"]["state"] == "HALF_OPEN"
    assert probe.approved_calls == 1 and probe.approved_speculative == 0
    assert len(Allocator(engine, clock).allocate(probe.id)) == 1
    assert decide(engine, clock, campaign, 5).approved_calls == 0
    worker.drain()
    clock.advance(0.25)
    providers["A"].deliver_ready(inbox.receive)
    processor.drain()
    closed = decide(engine, clock, campaign, 5)
    assert evidence(engine, closed)["breaker"]["state"] == "CLOSED"
    assert call_states(engine)[0].state == "CONNECTED"


def test_provider_latency_window_degrades_hybrid(engine: Engine, clock: VirtualClock) -> None:
    config = BreakerConfig(min_samples=2, slow_fraction=0.5, max_setup_seconds=1, window_seconds=2)
    campaign = prepare(engine, clock, wraps=0, mode=Mode.HYBRID, breaker=config)
    _, _, _, worker = services(engine, clock, setup_min_seconds=4, setup_max_seconds=4)
    allocate_live(engine, clock, campaign, 2, 0)
    assert worker.drain() == 2
    clock.advance(1.1)
    decision = decide(engine, clock, campaign, 4)
    breaker = evidence(engine, decision)["breaker"]
    assert breaker["state"] == "OPEN" and breaker["slow_setups"] == 2
    assert decision.effective_mode == Mode.STRICT_PROGRESSIVE and decision.approved_calls == 0


def test_failed_half_open_probe_reopens_the_breaker(engine: Engine, clock: VirtualClock) -> None:
    campaign = prepare(engine, clock, wraps=0, mode=Mode.STRICT_PROGRESSIVE, with_history=False,
                       breaker=BreakerConfig(min_samples=1, cooldown_seconds=2))
    _, _, _, worker = services(engine, clock, outage=True)
    allocate_live(engine, clock, campaign, 1, 0)
    assert worker.step()
    assert evidence(engine, decide(engine, clock, campaign, 5))["breaker"]["state"] == "OPEN"
    clock.advance(2.1)
    probe = decide(engine, clock, campaign, 5)
    assert probe.approved_calls == 1 and evidence(engine, probe)["breaker"]["state"] == "HALF_OPEN"
    assert len(Allocator(engine, clock).allocate(probe.id)) == 1
    worker.drain()
    reopened = decide(engine, clock, campaign, 5)
    assert evidence(engine, reopened)["breaker"]["state"] == "OPEN"
    assert reopened.approved_calls == 0 and "PROVIDER_BREAKER_OPEN" in reopened.reasons
