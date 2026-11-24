import asyncio
import json
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from multiprocessing.synchronize import Event as ProcessEvent
from threading import Barrier, Event

import pytest
from sqlalchemy import Engine, RowMapping, create_engine, text

from smartdialer.v3.agents import AgentState, AgentTransitions
from smartdialer.v3.borrowers import BorrowerTransitions
from smartdialer.v3.calls import CallState, CallTransitions, Lease
from smartdialer.v3.campaigns import read_campaign
from smartdialer.v3.clock import VirtualClock
from smartdialer.v3.config import BreakerConfig, CampaignConfig, DispatchConfig
from smartdialer.v3.dialer import Dialer
from smartdialer.v3.dispatch import Dispatcher
from smartdialer.v3.events import EventInbox, EventProcessor
from smartdialer.v3.invariants import verify_invariants
from smartdialer.v3.lab import isolated_database
from smartdialer.v3.load_test import run_workload
from smartdialer.v3.mock_provider import MockProvider, mock_providers
from smartdialer.v3.pacing import PacingInputs, PacingProposal, propose
from smartdialer.v3.providers import ProviderStatus, ProviderTimeout
from smartdialer.v3.safety import SafetyController
from smartdialer.v3.seed import seed
from smartdialer.v3.simulation import Simulation, SimulationConfig, TIERS

WORKER_CONFIG = DispatchConfig(lease_seconds=1)


def seed_campaign(engine: Engine, clock: VirtualClock) -> int:
    with engine.begin() as connection:
        return seed(connection, clock, "red-team", 1, 1, CampaignConfig(breaker=BreakerConfig(min_samples=100)))


def call(engine: Engine) -> RowMapping:
    with engine.connect() as connection:
        return connection.execute(text("SELECT * FROM calls")).mappings().one()


def prepare_provider(engine: Engine, clock: VirtualClock, **parameters: object) -> dict[str, MockProvider]:
    providers = mock_providers(engine, clock)
    providers["A"].configure(**{"setup_min_seconds": 0.2, "setup_max_seconds": 0.2, "ring_seconds": 0.3,
                                "answer_rate": 1, "failure_rate": 0, "timeout_rate": 0, "talk_seconds": 0.5,
                                **parameters})
    return providers


def count_provider(engine: Engine, kind: str) -> int:
    with engine.connect() as connection:
        return connection.execute(text("SELECT count(*) FROM call_events WHERE event_type = :kind"), {"kind": kind}).scalar_one()


def _crash_target(url: str, schema: str, now: datetime, campaign: int, window: str, ready: ProcessEvent) -> None:
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"}, hide_parameters=True)
    clock = VirtualClock(now)

    def boundary(name: str) -> None:
        if name == window:
            ready.set()
            Event().wait(45)

    try:
        if window == "after_commit":
            assert len(Dialer(engine, clock).tick(campaign, 1)) == 1
            boundary(window)
            return
        providers = mock_providers(engine, clock)
        if window == "provider_accepted_response_lost":
            provider = providers["A"]
            initiate = provider.initiate_call

            def drop_response(reference: str, phone: str, lease: Lease) -> ProviderStatus:
                try:
                    return initiate(reference, phone, lease)
                except ProviderTimeout:
                    boundary(window)
                    raise

            provider.initiate_call = drop_response
        Dispatcher(engine, clock, providers, "killed-worker", config=WORKER_CONFIG, on_boundary=boundary).step()
    finally:
        engine.dispose()


@pytest.mark.parametrize("window", ["after_commit", "after_claim", "before_initiate", "provider_accepted_response_lost", "after_initiate"])
def test_kill_at_every_dispatch_boundary(engine: Engine, clock: VirtualClock, window: str) -> None:
    campaign = seed_campaign(engine, clock)
    if window != "after_commit":
        assert len(Dialer(engine, clock).tick(campaign, 1)) == 1
    lost = window == "provider_accepted_response_lost"
    providers = prepare_provider(engine, clock, timeout_rate=1 if lost else 0, timeout_after_accept=lost)
    with engine.connect() as connection:
        schema = connection.execute(text("SELECT current_schema()")).scalar_one()
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    worker = context.Process(target=_crash_target, args=(engine.url.render_as_string(hide_password=False), schema,
                                                         clock.now(), campaign, window, ready))
    worker.start()
    try:
        assert ready.wait(30), f"worker missed {window}, exit code {worker.exitcode}"
    finally:
        if worker.is_alive():
            worker.kill()
        worker.join(10)
    assert worker.exitcode not in (None, 0)
    assert count_provider(engine, "MOCK_ACCEPT") == int(lost or window == "after_initiate")
    providers["A"].configure(timeout_rate=0)
    if window != "after_commit":
        clock.advance(1.1)
    dispatcher = Dispatcher(engine, clock, providers, "replacement", config=WORKER_CONFIG)
    inbox = EventInbox(engine, clock, providers)
    processor = EventProcessor(engine, clock)
    for _ in range(20):
        providers["A"].deliver_ready(inbox.receive)
        processor.drain()
        dispatcher.drain()
        clock.advance(0.2)
    assert call(engine).state == "COMPLETED" and count_provider(engine, "MOCK_ACCEPT") == 1
    with engine.connect() as connection:
        assert not any(verify_invariants(connection, drained=True).values())


def test_suppression_in_the_prepare_to_send_gap_prevents_provider_acceptance(engine: Engine, clock: VirtualClock) -> None:
    campaign = seed_campaign(engine, clock)
    assert len(Dialer(engine, clock).tick(campaign, 1)) == 1
    providers = prepare_provider(engine, clock)

    def withdraw(name: str) -> None:
        if name == "before_initiate":
            with engine.begin() as connection:
                borrowers = BorrowerTransitions(connection)
                assert borrowers.suppress(borrowers.get(1).phone_normalized, clock.now())

    assert Dispatcher(engine, clock, providers, "send", on_boundary=withdraw).step()
    assert count_provider(engine, "MOCK_ACCEPT") == 0
    clock.advance(0.5)
    Dispatcher(engine, clock, providers, "reconcile").drain()
    assert call(engine).state == "CANCELLED"
    with engine.connect() as connection:
        assert connection.execute(text("SELECT state, attempts FROM borrowers")).one() == ("SUPPRESSED", 1)
        assert not any(verify_invariants(connection, drained=True).values())


def test_status_reconciliation_catches_a_reordered_answer_before_the_hold_expires(engine: Engine, clock: VirtualClock) -> None:
    campaign = seed_campaign(engine, clock)
    assert len(Dialer(engine, clock).tick(campaign, 1)) == 1
    providers = prepare_provider(engine, clock, setup_min_seconds=2.7, setup_max_seconds=2.7, out_of_order_rate=1)
    dispatcher = Dispatcher(engine, clock, providers, "poll")
    assert dispatcher.step()
    for _ in range(4):
        clock.advance(1)
        dispatcher.drain()
    assert call(engine).state == "CONNECTED"
    clock.advance(1)
    inbox = EventInbox(engine, clock, providers)
    providers["A"].deliver_ready(inbox.receive)
    EventProcessor(engine, clock).drain()
    assert call(engine).state == "COMPLETED" and call(engine).abandoned_at is None


def test_postgres_availability_wins_over_a_stale_advisory_snapshot(engine: Engine, clock: VirtualClock) -> None:
    campaign = seed_campaign(engine, clock)
    stale = PacingProposal(1, PacingInputs(0, 1), "advisory snapshot reports the agent as reserved")
    assert SafetyController(engine, clock).decide(campaign, stale).approved_hard == 1
    with engine.begin() as connection:
        agents = AgentTransitions(connection)
        before = agents.get(1)
        assert agents.request_presence(1, AgentState.OFFLINE, clock.now(), 30)
        assert not agents.heartbeat(1, before.version, clock.now(), clock.now(), 30)
    blocked = SafetyController(engine, clock).decide(campaign, propose(PacingInputs(1, 1)))
    assert blocked.approved_calls == 0
    with engine.connect() as connection:
        assert connection.execute(text("SELECT state FROM agents")).scalar_one() == "OFFLINE"


def test_two_complete_dialer_workers_cannot_take_the_same_single_agent(engine: Engine, clock: VirtualClock) -> None:
    campaign = seed_campaign(engine, clock)
    gate = Barrier(2, timeout=10)

    def reserve(_: int) -> int:
        gate.wait()
        return len(Dialer(engine, clock).tick(campaign, 1))

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(reserve, range(2), timeout=20)) == [0, 1]
    with engine.connect() as connection:
        assert verify_invariants(connection, drained=False)["active_calls"] == 1


def test_late_start_cannot_override_a_new_worker_lease(engine: Engine, clock: VirtualClock) -> None:
    campaign = seed_campaign(engine, clock)
    assert len(Dialer(engine, clock).tick(campaign, 1)) == 1
    providers = prepare_provider(engine, clock)

    def replace_worker(name: str) -> None:
        if name == "before_initiate":
            clock.advance(1.1)
            assert Dispatcher(engine, clock, providers, "replacement", config=WORKER_CONFIG).step()

    assert Dispatcher(engine, clock, providers, "expired", config=WORKER_CONFIG, on_boundary=replace_worker).step()
    row = call(engine)
    assert row.state == "INITIATED" and row.fencing_token == 2 and count_provider(engine, "MOCK_ACCEPT") == 1
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM audit_log WHERE action = 'FENCE_REJECTED'")).scalar_one() == 1


def test_concurrent_replay_of_delayed_answer_cannot_repeat_a_rank(engine: Engine, clock: VirtualClock) -> None:
    campaign = seed_campaign(engine, clock)
    assert len(Dialer(engine, clock).tick(campaign, 1)) == 1
    providers = prepare_provider(engine, clock)
    assert Dispatcher(engine, clock, providers, "start").step()
    clock.advance(0.5)
    inbox = EventInbox(engine, clock, providers)
    processor = EventProcessor(engine, clock)
    providers["A"].deliver_ready(inbox.receive)
    processor.drain()
    first = call(engine)
    assert first.state == "ANSWERED"
    with engine.begin() as connection:
        CallTransitions(connection).advance(first.id, CallState.UNKNOWN, clock.now(), read_campaign(connection, campaign))
    replay = json.dumps({"event_id": "red-team-replay", "event_type": "ANSWERED", "timestamp": clock.now().isoformat(),
                         "provider_call_id": first.provider_call_id}).encode()
    signature = providers["A"].sign(replay)
    gate = Barrier(6, timeout=10)

    def send(_: int) -> str:
        gate.wait()
        return inbox.receive("A", replay, signature)

    with ThreadPoolExecutor(max_workers=6) as pool:
        dispositions = list(pool.map(send, range(6), timeout=20))
    assert dispositions.count("PENDING") == 1 and dispositions.count("DUPLICATE") == 5
    assert processor.drain() == 1 and call(engine).state == "UNKNOWN"
    assert Dispatcher(engine, clock, providers, "poll").step()
    assert call(engine).state == "CONNECTED"
    clock.advance(1)
    providers["A"].deliver_ready(inbox.receive)
    processor.drain()
    with engine.connect() as connection:
        assert not any(verify_invariants(connection, drained=True).values())


def test_seeded_simulation_produces_identical_reports(engine: Engine) -> None:
    url = engine.url.render_as_string(hide_password=False)
    settings = SimulationConfig(seconds=25, warmup_seconds=10, agents=2, cadence_seconds=5)
    runs = []
    for _ in range(2):
        with isolated_database(url) as isolated:
            runs.append(Simulation(isolated, settings, "A", "B", TIERS[1]).run())
    assert runs[0] == runs[1]


def test_load_workload_measures_real_webhook_replays_and_drains(engine: Engine) -> None:
    report = asyncio.run(asyncio.wait_for(run_workload(engine, agents=3, borrowers=6, workers=2, batch=2), timeout=30))
    assert report["allocations"] == report["completed"] == 6
    assert report["webhook_dispositions"]["DUPLICATE"] > 0
    assert not any(report["invariants"].values())
