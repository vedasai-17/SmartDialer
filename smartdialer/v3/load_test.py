import asyncio
from math import ceil
from time import perf_counter

from httpx import ASGITransport, AsyncClient
from sqlalchemy import Engine, text

from .clock import VirtualClock
from .config import CampaignConfig
from .dialer import Dialer
from .dispatch import Dispatcher
from .events import EventInbox, EventProcessor, create_app
from .invariants import verify_invariants
from .lab import isolated_database
from .mock_provider import mock_providers
from .reaper import Reaper
from .seed import seed
from .simulation import START


def percentile_95(values: list[float]) -> float:
    return sorted(values)[ceil(len(values) * 0.95) - 1]


async def run_workload(engine: Engine, agents: int, borrowers: int, workers: int, batch: int) -> dict[str, object]:
    # Freeze domain time and remove mock latency so wall time reflects the DB and HTTP workload.
    clock = VirtualClock(START)
    with engine.begin() as connection:
        campaign = seed(connection, clock, "load", agents, borrowers, CampaignConfig(wrap_up_seconds=0),
                        provider_rate_limit=borrowers)
    providers = mock_providers(engine, clock)
    provider = providers["A"]
    provider.configure(setup_min_seconds=0, setup_max_seconds=0, ring_seconds=0, talk_seconds=0,
                       answer_rate=1, failure_rate=0, timeout_rate=0)
    inbox = EventInbox(engine, clock, providers)
    dispatcher = Dispatcher(engine, clock, providers, "load")
    reaper = Reaper(engine, clock)
    processors = [EventProcessor(engine, clock) for _ in range(workers)]
    loop = asyncio.get_running_loop()
    allocation_done = asyncio.Event()
    allocation_times = []
    acknowledgments = {"PENDING": 0, "DUPLICATE": 0}
    allocated = 0
    started = perf_counter()
    last_allocation = started

    def snapshots() -> tuple[int, int, int, int]:
        with engine.connect() as connection:
            return tuple(connection.execute(text("""
                SELECT (SELECT count(*) FROM borrowers WHERE state = 'QUEUED'),
                    (SELECT count(*) FROM calls WHERE state_rank < 100),
                    (SELECT count(*) FROM call_events WHERE event_type = 'MOCK_CALLBACK' AND processed_at IS NULL
                        AND received_at <= :now),
                    (SELECT count(*) FROM call_events WHERE processing_status = 'PENDING')
            """), {"now": clock.now()}).one())

    async def allocate() -> None:
        nonlocal allocated, last_allocation
        dialer = Dialer(engine, clock)
        while True:
            before = perf_counter()
            ids = await asyncio.to_thread(dialer.tick, campaign, batch)
            after = perf_counter()
            allocation_times.append((after - before) * 1000)
            if ids:
                allocated += len(ids)
                last_allocation = after
            else:
                queued, _, _, _ = await asyncio.to_thread(snapshots)
                if not queued:
                    return
                await asyncio.sleep(0.001)

    async def allocate_all() -> None:
        async with asyncio.TaskGroup() as tasks:
            for _ in range(workers):
                tasks.create_task(allocate())
        allocation_done.set()

    async with AsyncClient(transport=ASGITransport(app=create_app(inbox)), base_url="http://load.local") as client:
        async def submit(name: str, payload: bytes, signature: str) -> None:
            responses = await asyncio.gather(*(client.post(f"/webhooks/{name}", content=payload,
                                              headers={"X-SmartDialer-Signature": signature})
                                               for _ in range(max(2, workers))))
            for response in responses:
                response.raise_for_status()
                result = response.json()["status"]
                if result not in acknowledgments:
                    raise AssertionError(f"unexpected webhook disposition: {result}")
                acknowledgments[result] += 1

        def receive(name: str, payload: bytes, signature: str) -> None:
            asyncio.run_coroutine_threadsafe(submit(name, payload, signature), loop).result(timeout=30)

        async def serve() -> None:
            while True:
                dispatched = await asyncio.to_thread(dispatcher.drain, batch * workers)
                posted = await asyncio.to_thread(provider.deliver_ready, receive, limit=batch * workers)
                if posted:
                    await asyncio.gather(*(asyncio.to_thread(processor.drain, batch) for processor in processors))
                await asyncio.to_thread(reaper.tick, limit=agents)
                _, active, callbacks, pending = await asyncio.to_thread(snapshots)
                if pending and not posted:
                    await asyncio.gather(*(asyncio.to_thread(processor.drain, batch) for processor in processors))
                if allocation_done.is_set() and not (active or callbacks or pending):
                    return
                if not dispatched and not posted:
                    await asyncio.sleep(0.001)

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(allocate_all())
            tasks.create_task(serve())

    elapsed = perf_counter() - started
    with engine.connect() as connection:
        invariants = verify_invariants(connection, drained=True)
        completed = connection.execute(text("SELECT count(*) FROM calls WHERE connected_at IS NOT NULL AND state = 'COMPLETED'")).scalar_one()
    if allocated != borrowers or completed != borrowers:
        raise AssertionError(f"load workload allocated {allocated} and completed {completed}, expected {borrowers}")
    requests = sum(acknowledgments.values())
    return {"agents": agents, "borrowers": borrowers, "workers": workers, "batch": batch,
            "allocations": allocated, "completed": completed, "allocation_requests": len(allocation_times),
            "allocations_per_second": allocated / (last_allocation - started),
            "allocation_p95_ms": percentile_95(allocation_times), "elapsed_seconds": elapsed,
            "webhook_requests": requests, "webhooks_per_second": requests / elapsed,
            "webhook_dispositions": acknowledgments, "invariants": invariants}


def load_test(url: str, agents: int, borrowers: int, workers: int, batch: int, timeout: int) -> dict[str, object]:
    with isolated_database(url) as engine:
        return asyncio.run(asyncio.wait_for(run_workload(engine, agents, borrowers, workers, batch), timeout=timeout))
