import asyncio
import json
import multiprocessing
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.synchronize import Event as ProcessEvent
from random import Random
from threading import Barrier, Event
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import Engine, RowMapping, create_engine, text

from smartdialer.v3.__main__ import seed
from smartdialer.v3.agents import AgentState, AgentTransitions
from smartdialer.v3.clock import VirtualClock
from smartdialer.v3.config import BreakerConfig, CampaignConfig, DispatchConfig
from smartdialer.v3.dialer import Dialer
from smartdialer.v3.dispatch import Dispatcher
from smartdialer.v3.events import EventInbox, EventProcessor, create_app
from smartdialer.v3.mock_provider import MockProvider, mock_providers
from smartdialer.v3.reaper import Reaper

TEST_DISPATCH = DispatchConfig(lease_seconds=1, status_poll_seconds=1, max_dispatch_attempts=3)


@dataclass
class Rig:
    engine: Engine
    clock: VirtualClock
    campaign_id: int
    call_id: UUID
    provider: MockProvider
    providers: dict[str, MockProvider]
    inbox: EventInbox
    processor: EventProcessor

    def worker(self, name: str = "test", on_boundary: Callable[[str], None] | None = None) -> Dispatcher:
        return Dispatcher(self.engine, self.clock, self.providers, name, config=TEST_DISPATCH, rng=Random(17), on_boundary=on_boundary)

    def deliver(self) -> int:
        self.provider.deliver_ready(self.inbox.receive)
        return self.processor.drain()

    def row(self) -> RowMapping:
        with self.engine.connect() as connection:
            return connection.execute(text("SELECT * FROM calls WHERE id = :id"), {"id": self.call_id}).mappings().one()

    def presence(self, state: AgentState, agent_id: int = 1) -> None:
        with self.engine.begin() as connection:
            assert AgentTransitions(connection).request_presence(agent_id, state, self.clock.now(), 30)

    def counts(self, event_type: str) -> int:
        with self.engine.connect() as connection:
            return connection.execute(
                text("SELECT count(*) FROM call_events WHERE event_type = :type"), {"type": event_type},
            ).scalar_one()

    async def send(self, client: AsyncClient, data: dict[str, object], *, signature: str | None = None) -> Response:
        raw = json.dumps(data).encode()
        return await client.post(f"/webhooks/{self.provider.name}", content=raw,
                                 headers={"X-SmartDialer-Signature": self.provider.sign(raw) if signature is None else signature})


def prepare(engine: Engine, clock: VirtualClock, provider: str = "A", *, agents: int = 1, heartbeat_ttl: int = 30) -> Rig:
    providers = mock_providers(engine, clock)
    mock = providers[provider]
    mock.configure(answer_rate=1, failure_rate=0, timeout_rate=0, out_of_order_rate=0,
                   duplicate_rate=0, lost_terminal_rate=0, talk_seconds=0.75, ring_seconds=0.3)
    with engine.begin() as connection:
        campaign = seed(connection, clock, "phase-two", agents, 2, CampaignConfig(
            provider=provider, heartbeat_ttl_seconds=heartbeat_ttl, breaker=BreakerConfig(min_samples=100),
        ))
    call_id, = Dialer(engine, clock).tick(campaign, 1)
    return Rig(engine, clock, campaign, call_id, mock, providers, EventInbox(engine, clock, providers), EventProcessor(engine, clock))


def assert_settled(rig: Rig, *, agent_state: str, borrower_state: str) -> None:
    with rig.engine.connect() as connection:
        assert connection.execute(text("SELECT state, current_call_id FROM agents WHERE id = 1")).one() == (agent_state, None)
        assert connection.execute(text("SELECT state, attempts FROM borrowers WHERE id = 1")).one() == (borrower_state, 1)
        assert rig.row().lease_owner is None


@pytest.mark.parametrize("provider", ["A", "B"])
def test_same_dialer_dispatcher_and_processor_connect_on_both_mocks(engine: Engine, clock: VirtualClock, provider: str) -> None:
    rig = prepare(engine, clock, provider)
    worker = rig.worker()
    for _ in range(24):
        rig.deliver()
        worker.drain(10)
        clock.advance(0.25)
    row = rig.row()
    assert row.state == "COMPLETED"
    assert row.answered_at <= row.connected_at <= row.ended_at
    assert row.abandoned_at is None
    assert row.dispatch_attempts == 1 and rig.counts("MOCK_ACCEPT") == 1 and rig.counts("MOCK_BRIDGE") == 1
    assert_settled(rig, agent_state="WRAP_UP", borrower_state="CONTACTED")


def test_three_answer_webhooks_apply_once_and_answer_alone_is_not_a_bridge(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    rig.provider.configure(duplicate_rate=1)
    dispatcher = rig.worker()
    assert dispatcher.step()
    clock.advance(0.5)
    rig.deliver()
    assert rig.row().state == "ANSWERED" and rig.row().connected_at is None
    with engine.connect() as connection:
        applied = connection.execute(text("""
            SELECT processing_status FROM call_events WHERE event_type = 'ANSWERED' ORDER BY id
        """)).scalars().all()
        assert applied == ["APPLIED", "STALE", "STALE"]
    assert dispatcher.step()
    clock.advance(1)
    rig.deliver()
    assert rig.row().state == "COMPLETED" and rig.counts("MOCK_BRIDGE") == 1
    assert_settled(rig, agent_state="WRAP_UP", borrower_state="CONTACTED")


def test_completed_before_answered_and_ringing_finalizes_once_and_audits_late_events(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    rig.provider.configure(out_of_order_rate=1)
    assert rig.worker().step()
    clock.advance(3)
    assert rig.deliver() == 3
    with engine.connect() as connection:
        received = connection.execute(text("SELECT event_type, processing_status FROM call_events WHERE provider_name = 'A' ORDER BY id")).all()
        assert received == [("COMPLETED", "APPLIED"), ("ANSWERED", "STALE"), ("RINGING", "STALE")]
        assert connection.execute(text("SELECT count(*) FROM audit_log WHERE entity_type = 'call_event' AND action = 'STALE'")).scalar_one() == 2
    row = rig.row()
    assert row.state == "COMPLETED" and row.abandoned_at == row.answered_at and row.abandoned_at is not None
    assert_settled(rig, agent_state="AVAILABLE", borrower_state="RETRY_SCHEDULED")


def test_accepted_initiation_timeout_reconciles_without_another_send(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    rig.provider.configure(timeout_rate=1, timeout_after_accept=True)
    dispatcher = rig.worker()
    assert dispatcher.step()
    assert (rig.row().state, rig.row().failure_reason) == ("UNKNOWN", "INITIATE_TIMEOUT")
    assert rig.row().next_retry_at > clock.now()
    assert rig.counts("MOCK_ACCEPT") == 1
    clock.advance(0.3)
    assert dispatcher.step()
    assert rig.row().state == "RINGING" and rig.row().dispatch_attempts == 1
    clock.advance(0.3)
    rig.deliver()
    assert dispatcher.step()
    clock.advance(1)
    rig.deliver()
    assert rig.row().state == "COMPLETED" and rig.counts("MOCK_ACCEPT") == 1


def test_absence_required_for_resend_and_exhaustion_is_audited(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    rig.provider.configure(timeout_rate=1, timeout_after_accept=False)
    worker = rig.worker()
    assert worker.step()
    first_retry = rig.row().next_retry_at - clock.now()
    assert 0 < first_retry.total_seconds() <= TEST_DISPATCH.retry_base_seconds
    assert not worker.step()
    rig.provider.configure(outage=True)
    clock.advance(0.8)
    assert worker.step()
    second_retry = rig.row().next_retry_at - clock.now()
    assert 0 < second_retry.total_seconds() <= 2 * TEST_DISPATCH.retry_base_seconds
    assert rig.row().dispatch_attempts == 1
    rig.provider.configure(outage=False)
    for _ in range(3):
        clock.advance(2)
        assert worker.step()
    row = rig.row()
    assert (row.state, row.failure_reason, row.dispatch_attempts) == ("FAILED", "DISPATCH_DEAD_LETTER", 3)
    assert rig.counts("MOCK_ACCEPT") == 0
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM audit_log WHERE action = 'DISPATCH_DEAD_LETTER'")).scalar_one() == 1
    assert_settled(rig, agent_state="AVAILABLE", borrower_state="RETRY_SCHEDULED")


def test_two_dispatchers_send_only_one_logical_call(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    gate = Barrier(2, timeout=10)

    def dispatch(index: int) -> bool:
        gate.wait()
        return rig.worker(f"worker-{index}").step()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(dispatch, range(2), timeout=20)) == [False, True]
    assert rig.row().dispatch_attempts == 1 and rig.counts("MOCK_ACCEPT") == 1


@pytest.mark.parametrize("take_over", [False, True])
def test_expired_worker_late_write_is_fenced(engine: Engine, clock: VirtualClock, take_over: bool) -> None:
    rig = prepare(engine, clock)
    rig.provider.configure(setup_min_seconds=5, setup_max_seconds=5)
    accepted, resume = Event(), Event()

    def pause(boundary: str) -> None:
        if boundary == "after_initiate":
            accepted.set()
            assert resume.wait(20)

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(rig.worker("expired", pause).step)
        try:
            assert accepted.wait(15)
            assert rig.row().state == "INITIATING"
            clock.advance(1.5)
            if take_over:
                assert rig.worker("replacement").step()
        finally:
            resume.set()
        assert pending.result(timeout=15)
    assert rig.row().state == ("INITIATED" if take_over else "INITIATING")
    assert rig.row().fencing_token == (2 if take_over else 1)
    with engine.connect() as connection:
        rejected = connection.execute(text("SELECT details->>'owner' FROM audit_log WHERE action = 'FENCE_REJECTED'")).scalars().all()
        assert rejected == ["expired"]
    assert rig.counts("MOCK_ACCEPT") == 1 and rig.row().dispatch_attempts == 1


def _child_worker(url: str, search_path: str, now: datetime, boundary: str, ready: ProcessEvent) -> None:
    engine = create_engine(url, hide_parameters=True, connect_args={"options": f"-csearch_path={search_path}"})
    clock = VirtualClock(now)

    def block(site: str) -> None:
        if site == boundary:
            ready.set()
            Event().wait(30)

    if boundary == "after_answer":
        assert EventProcessor(engine, clock).process_one()
        block("after_answer")
    else:
        Dispatcher(engine, clock, mock_providers(engine, clock), "child", config=TEST_DISPATCH, on_boundary=block).step()
    engine.dispose()


def kill_worker(rig: Rig, boundary: str) -> None:
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    with rig.engine.connect() as connection:
        schema = connection.execute(text("SELECT current_schema()")).scalar_one()
    process = context.Process(target=_child_worker, args=(rig.engine.url.render_as_string(hide_password=False), schema,
                                                          rig.clock.now(), boundary, ready))
    process.start()
    try:
        assert ready.wait(20), f"child missed {boundary} (exit code {process.exitcode})"
    finally:
        if process.is_alive():
            process.kill()
        process.join(10)
    assert process.exitcode not in (None, 0)


@pytest.mark.parametrize("boundary", ["after_claim", "before_initiate", "after_initiate"])
def test_killed_dispatch_worker_recovers_after_lease_expiry_without_leaks(engine: Engine, clock: VirtualClock, boundary: str) -> None:
    rig = prepare(engine, clock)
    kill_worker(rig, boundary)
    assert rig.counts("MOCK_ACCEPT") == int(boundary == "after_initiate")
    assert not rig.worker("early").step()
    clock.advance(1.1)
    replacement = rig.worker("replacement")
    for _ in range(20):
        rig.deliver()
        replacement.drain(5)
        clock.advance(0.15)
    assert rig.row().state == "COMPLETED"
    assert rig.counts("MOCK_ACCEPT") == 1
    assert_settled(rig, agent_state="WRAP_UP", borrower_state="CONTACTED")


def test_killed_event_worker_after_answer_can_finish_from_durable_completion(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    assert rig.worker().step()
    clock.advance(0.2)
    assert rig.deliver() == 1
    clock.advance(0.3)
    rig.provider.deliver_ready(rig.inbox.receive)
    kill_worker(rig, "after_answer")
    assert rig.row().state == "ANSWERED"
    clock.advance(2.1)
    assert rig.deliver() == 1
    assert rig.row().state == "COMPLETED" and rig.row().abandoned_at is not None
    assert_settled(rig, agent_state="AVAILABLE", borrower_state="RETRY_SCHEDULED")


@pytest.mark.parametrize("state", [AgentState.OFFLINE, AgentState.PAUSED])
@pytest.mark.parametrize("ringing", [False, True])
def test_agent_requests_during_setup_cancel_without_an_extra_borrower_attempt(
    engine: Engine, clock: VirtualClock, state: AgentState, ringing: bool,
) -> None:
    rig = prepare(engine, clock)
    worker = rig.worker()
    assert worker.step()
    if ringing:
        clock.advance(0.25)
        rig.deliver()
        assert rig.row().state == "RINGING"
    rig.presence(state)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT state, current_call_id FROM agents")).one() == ("RECOVERY_HOLD", rig.call_id)
    assert worker.step()
    assert rig.row().state == "CANCELLED" and rig.row().abandoned_at is None
    assert rig.counts("MOCK_CANCEL") == 1
    assert_settled(rig, agent_state=state, borrower_state="RETRY_SCHEDULED")


@pytest.mark.parametrize("state", [AgentState.OFFLINE, AgentState.PAUSED])
@pytest.mark.parametrize("agents", [1, 2])
def test_agent_requests_after_answer_reroute_once_or_count_abandonment(
    engine: Engine, clock: VirtualClock, state: AgentState, agents: int,
) -> None:
    rig = prepare(engine, clock, agents=agents)
    worker = rig.worker()
    assert worker.step()
    clock.advance(0.5)
    rig.deliver()
    assert rig.row().state == "ANSWERED"
    rig.presence(state)
    assert worker.step()
    if agents == 1:
        assert rig.row().state == "CANCELLED" and rig.row().abandoned_at is not None
        assert rig.counts("MOCK_BRIDGE") == 0
        assert_settled(rig, agent_state=state, borrower_state="RETRY_SCHEDULED")
    else:
        assert rig.row().state == "CONNECTED" and rig.row().agent_id == 2
        assert rig.counts("MOCK_BRIDGE") == 1
        clock.advance(1)
        rig.deliver()
        assert rig.row().state == "COMPLETED" and rig.row().abandoned_at is None
        assert_settled(rig, agent_state=state, borrower_state="CONTACTED")
        with engine.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM audit_log WHERE action = 'AGENT_REROUTED'")).scalar_one() == 1


def test_cancel_timeout_preserves_ownership_until_reconciliation(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    rig.provider.configure(cancel_timeout_after_accept=True)
    worker = rig.worker()
    assert worker.step()
    rig.presence(AgentState.PAUSED)
    assert worker.step()
    assert (rig.row().state, rig.row().failure_reason) == ("UNKNOWN", "CANCEL_TIMEOUT")
    with engine.connect() as connection:
        assert connection.execute(text("SELECT state, current_call_id FROM agents")).one() == ("RECOVERY_HOLD", rig.call_id)
    clock.advance(0.3)
    assert worker.step()
    assert rig.row().state == "CANCELLED" and rig.counts("MOCK_CANCEL") == 1
    assert_settled(rig, agent_state="PAUSED", borrower_state="RETRY_SCHEDULED")


def test_lost_terminal_callback_and_bridge_ack_are_reconciled(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    rig.provider.configure(lost_terminal_rate=1, bridge_timeout_after_accept=True)
    worker = rig.worker()
    assert worker.step()
    clock.advance(0.5)
    rig.deliver()
    assert worker.step()
    assert (rig.row().state, rig.row().state_rank) == ("UNKNOWN", 50)
    clock.advance(1)
    assert rig.deliver() == 0
    assert worker.step()
    assert rig.row().state == "COMPLETED" and rig.row().connected_at is not None
    assert rig.counts("MOCK_BRIDGE") == 1
    assert_settled(rig, agent_state="WRAP_UP", borrower_state="CONTACTED")


def test_mock_gate_rejects_bridge_when_agent_drops_after_permit(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    assert rig.worker().step()
    clock.advance(0.5)
    rig.deliver()

    def disconnect(boundary: str) -> None:
        if boundary == "before_bridge":
            rig.presence(AgentState.OFFLINE)

    assert rig.worker(on_boundary=disconnect).step()
    assert rig.row().state == "UNKNOWN" and rig.row().connected_at is None
    assert rig.counts("MOCK_BRIDGE") == 0
    clock.advance(0.4)
    assert rig.worker("replacement").step()
    assert rig.row().state == "CANCELLED" and rig.row().abandoned_at is not None
    assert_settled(rig, agent_state="OFFLINE", borrower_state="RETRY_SCHEDULED")


def test_agent_drop_after_accepted_bridge_preserves_binding_until_settled(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock, agents=2)
    assert rig.worker().step()
    clock.advance(0.5)
    rig.deliver()

    def disconnect(boundary: str) -> None:
        if boundary == "after_bridge":
            rig.presence(AgentState.OFFLINE)

    assert rig.worker(on_boundary=disconnect).step()
    row = rig.row()
    assert row.state == "CONNECTED" and row.agent_id == 1
    with engine.connect() as connection:
        assert connection.execute(text("SELECT state, current_call_id FROM agents WHERE id = 1")).one() == ("RECOVERY_HOLD", rig.call_id)
    assert rig.worker("reconciler").step()
    row = rig.row()
    assert row.state == "CANCELLED" and row.connected_at is not None and row.abandoned_at is None
    assert rig.counts("MOCK_BRIDGE") == 1
    assert_settled(rig, agent_state="OFFLINE", borrower_state="CONTACTED")


def test_expired_cancellation_cannot_disconnect_a_reassigned_call(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock, agents=2)
    assert rig.worker().step()
    rig.presence(AgentState.PAUSED, agent_id=2)
    clock.advance(0.5)
    rig.deliver()
    rig.presence(AgentState.OFFLINE)

    def let_replacement_bridge(boundary: str) -> None:
        if boundary == "before_cancel":
            clock.advance(1.1)
            rig.presence(AgentState.AVAILABLE, agent_id=2)
            assert rig.worker("replacement").step()
            assert rig.row().state == "CONNECTED" and rig.row().agent_id == 2

    assert rig.worker("stale", let_replacement_bridge).step()
    assert rig.row().state == "CONNECTED" and rig.counts("MOCK_CANCEL") == 0
    clock.advance(1)
    rig.deliver()
    assert rig.row().state == "COMPLETED" and rig.row().abandoned_at is None


def test_webhook_late_answer_counts_abandonment_without_reopening_the_call(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    assert rig.worker().step()
    clock.advance(1)
    provider_call_id = rig.row().provider_call_id
    event = {"provider_call_id": provider_call_id, "timestamp": clock.now().isoformat()}

    async def check() -> None:
        async with AsyncClient(transport=ASGITransport(app=create_app(rig.inbox)), base_url="http://test") as client:
            assert (await rig.send(client, {**event, "event_type": "COMPLETED"})).json() == {"status": "PENDING"}
            assert rig.processor.drain() == 1
            version = rig.row().version
            assert (await rig.send(client, {**event, "event_type": "ANSWERED"})).json() == {"status": "PENDING"}
            assert rig.processor.drain() == 1
            metrics = (await client.get("/metrics")).json()
            assert metrics["abandoned"] == metrics["answered"] == 1
            assert metrics["events"] == {"APPLIED": 1, "STALE": 1}
            assert rig.row().version == version

    asyncio.run(check())
    assert_settled(rig, agent_state="AVAILABLE", borrower_state="RETRY_SCHEDULED")


def test_reaper_marks_expired_agent_held_then_dispatcher_reconciles(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock, heartbeat_ttl=1)
    rig.provider.configure(setup_min_seconds=3, setup_max_seconds=3)
    worker = rig.worker()
    assert worker.step()
    clock.advance(1.1)
    assert Reaper(engine, clock).tick() == 1
    with engine.connect() as connection:
        assert connection.execute(text("SELECT state, current_call_id FROM agents")).one() == ("RECOVERY_HOLD", rig.call_id)
    assert worker.step()
    assert rig.row().state == "CANCELLED"
    assert_settled(rig, agent_state="OFFLINE", borrower_state="RETRY_SCHEDULED")


def test_webhook_persists_invalid_and_unknown_input_and_acknowledges_only_commits(engine: Engine, clock: VirtualClock) -> None:
    rig = prepare(engine, clock)
    assert rig.worker().step()
    row = rig.row()
    payload = {"provider_call_id": row.provider_call_id, "event_type": "RINGING", "timestamp": clock.now().isoformat(), "sequence": 8}
    async def check() -> None:
        async with AsyncClient(transport=ASGITransport(app=create_app(rig.inbox)), base_url="http://test") as client:
            assert (await rig.send(client, payload, signature="bad")).status_code == 401
            for invalid in ({"event_type": "MYSTERY"}, {"timestamp": "not-a-time"}, {"event_id": "null\u0000byte"}):
                assert (await rig.send(client, {**payload, **invalid})).json() == {"status": "INVALID"}
            unknown = await rig.send(client, {**payload, "provider_call_id": "unknown-call"})
            assert unknown.status_code == 200 and unknown.json() == {"status": "UNKNOWN_CALL"}
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE call_events ADD CONSTRAINT test_ingestion_unavailable CHECK (provider_name <> 'A') NOT VALID"))
            try:
                assert (await rig.send(client, payload)).status_code == 503
            finally:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE call_events DROP CONSTRAINT test_ingestion_unavailable"))
            assert (await rig.send(client, payload)).json() == {"status": "PENDING"}
            assert (await rig.send(client, payload)).json() == {"status": "DUPLICATE"}
            assert rig.processor.drain() == 1
            assert (await client.get("/metrics")).json()["events"] == {"INVALID": 3, "UNKNOWN_CALL": 1, "APPLIED": 1}

    asyncio.run(check())
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM audit_log WHERE entity_type = 'call_event'")).scalar_one() == 4
