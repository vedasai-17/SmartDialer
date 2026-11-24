import json
import math
import multiprocessing as mp
import os
import tempfile
import time
from pathlib import Path

from .dialer import Dialer
from .pacing import Snapshot, propose
from .provider import Profile
from .simulation import SCENARIOS, compare, seed_data, stats
from .store import Store


NOW = 1_760_000_000
DEMO_PROFILE = Profile(answer_rate=1, talk_seconds=4, noisy=False)


def require(condition, description):
    if not condition:
        raise AssertionError(description)


def _allocator(path, ready, gate, results, batch):
    with Store(path) as store:
        dialer = Dialer(store)
        ready.put(True)
        if not gate.wait(15):
            raise TimeoutError('Benchmark start gate timed out')
        samples, reserved = [], 0
        proposal = propose('progressive', Snapshot(batch), batch)
        while True:
            begin = time.perf_counter()
            admitted = dialer.safety.admit(proposal, 'A', NOW, limit=batch)
            samples.append(time.perf_counter() - begin)
            if not admitted:
                break
            reserved += len(admitted)
            # Give competing workers a chance to acquire the single SQLite writer.
            time.sleep(0.001)
        results.put((samples, reserved))


def load_test(agents=1000, workers=4, borrowers=None, batch=8):
    borrower_count = agents * 2 if borrowers is None else borrowers
    context = mp.get_context('spawn')
    with tempfile.TemporaryDirectory(prefix='smartdialer-load-') as directory:
        path = str(Path(directory) / 'load.sqlite3')
        with Store(path) as store:
            store.initialize()
            seed_data(store, agents, borrower_count)
        ready, gate, results = context.Queue(), context.Event(), context.Queue()
        processes = [context.Process(target=_allocator, args=(path, ready, gate, results, batch))
                     for _ in range(workers)]
        try:
            for process in processes:
                process.start()
            for _ in processes:
                ready.get(timeout=15)
            begin = time.perf_counter()
            gate.set()
            reports = [results.get(timeout=30) for _ in processes]
            elapsed = time.perf_counter() - begin
            for process in processes:
                process.join(5)
                require(process.exitcode == 0, 'Allocation worker failed')
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join()
            for queue in (ready, results):
                queue.close()
        latencies = sorted(sample for samples, _ in reports for sample in samples)
        with Store(path) as store:
            metrics = stats(store)
        require(metrics['reservations'] == min(agents, borrower_count) and not metrics['duplicate_active'],
                'Unexpected reservation count or duplicate assignment')
        return {'agents': agents, 'borrowers': borrower_count, 'workers': workers, 'batch_size': batch,
                'reservations': metrics['reservations'], 'duplicate_active': metrics['duplicate_active'],
                'per_worker_reservations': [count for _, count in reports],
                'allocation_batches': len(latencies), 'elapsed_seconds': round(elapsed, 4),
                'reservations_per_second': round(metrics['reservations'] / elapsed, 2),
                'p95_allocation_batch_ms': round(1000 * latencies[math.ceil(0.95 * len(latencies)) - 1], 3)}


def _service_worker(path, offsets, crash_at=None):
    def crash_hook(hit):
        if hit == crash_at:
            os._exit(97)

    with Store(path) as store:
        dialer = Dialer(store, DEMO_PROFILE, crash_hook)
        for offset in offsets:
            dialer.service(NOW + offset)


def _run_service_process(path, offsets, crash_at=None):
    process = mp.get_context('spawn').Process(target=_service_worker, args=(path, offsets, crash_at))
    process.start()
    try:
        process.join(15)
        expected_exit = 97 if crash_at else 0
        require(process.exitcode == expected_exit,
                f'Worker at {crash_at or "recovery"} exited with {process.exitcode}, expected {expected_exit}')
        return process.exitcode
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        process.close()


def crash_demo(point, recover_after_hangup=False):
    with tempfile.TemporaryDirectory(prefix='smartdialer-crash-') as directory:
        path = str(Path(directory) / 'crash.sqlite3')
        with Store(path) as store:
            store.initialize()
            seed_data(store, 1, 1)
            dialer = Dialer(store, DEMO_PROFILE)
            call_id, = dialer.safety.admit(propose('progressive', Snapshot(1)), 'A', NOW)
            if point == 'after-answer':
                dialer.dispatch(NOW)
        crash_exit = _run_service_process(path, [0 if point == 'after-start' else 5], point)
        with Store(path) as store:
            before = store.db.execute('SELECT state FROM calls WHERE id = ?', (call_id,)).fetchone()[0]
            require(before == ('RESERVED' if point == 'after-start' else 'ANSWERED'), 'Crash occurred outside expected window')
        recovery_exit = _run_service_process(path, range(20 if recover_after_hangup else 6, 30))
        with Store(path) as store:
            provider_calls = store.db.execute('SELECT COUNT(*) FROM mock_calls WHERE started_at IS NOT NULL').fetchone()[0]
            metrics = stats(store)
            require(provider_calls == metrics['terminal'] == metrics['reservations'] == 1,
                    'Crash recovery lost or duplicated a call')
            require(metrics['bridged'] == int(not recover_after_hangup) and metrics['unserved'] == int(recover_after_hangup),
                    'Recovery bridged a completed call or failed to bridge an active answer')
            bridge_attempts = store.db.execute("SELECT attempts FROM jobs WHERE kind = 'BRIDGE' AND call_id = ?",
                                               (call_id,)).fetchone()[0]
            require(bridge_attempts == int(not recover_after_hangup), 'Replacement worker claimed a stale bridge job')
            finalizations = store.db.execute("SELECT COUNT(*) FROM transitions WHERE state = 'COMPLETED'").fetchone()[0]
            agent_state = store.db.execute('SELECT state FROM agents WHERE id = 1').fetchone()[0]
            require(finalizations == 1 and agent_state == 'AVAILABLE', 'Reservation was not released exactly once')
            return {'crash_point': point, 'worker_exit_code': crash_exit, 'state_after_crash': before,
                    'replacement_exit_code': recovery_exit, 'provider_calls': provider_calls, 'bridged': metrics['bridged'],
                    'bridge_attempts': bridge_attempts, 'unserved': metrics['unserved'], 'finalizations': finalizations,
                    'agent_state_after_recovery': agent_state}


def outage_demo():
    with Store(':memory:') as store:
        store.initialize()
        seed_data(store, 4, 12)
        dialer = Dialer(store, DEMO_PROFILE)
        first, = dialer.tick(NOW, 'progressive', 'B', batch_limit=1)
        with store.write() as db:
            db.execute("UPDATE mock_settings SET outage_until = ? WHERE provider = 'B'", (NOW + 19,))
        pending = dialer.tick(NOW + 1, 'predictive', 'B')
        for offset in range(2, 13):
            dialer.tick(NOW + offset, 'predictive', 'B')
        require(not dialer.safety.admit(propose('progressive', Snapshot(4)), 'B', NOW + 12),
                'Circuit should independently reject an optimistic pacing proposal')
        circuit = dict(store.db.execute("SELECT * FROM provider_health WHERE provider = 'B'").fetchone())
        during = stats(store)
        existing = store.db.execute('SELECT state FROM calls WHERE id = ?', (first,)).fetchone()[0]
        require(circuit['open_until'] > NOW + 12 and during['initiated'] == 1, 'Provider circuit failed to block starts')
        require(existing == 'COMPLETED', 'Provider start outage interrupted an existing call')
        for offset in range(13, 55):
            dialer.tick(NOW + offset, 'predictive', 'B')
        recovered = [store.db.execute('SELECT started_at FROM calls WHERE id = ?', (call_id,)).fetchone()[0]
                     for call_id in pending]
        require(all(value and value >= NOW + 19 for value in recovered), 'Attempts did not recover using their original IDs')
        return {'started_during_outage': during['initiated'] - 1, 'existing_call': existing,
                'circuit_rejections': during['safety_decisions']['provider_circuit_open'],
                'retried_original_attempts': len(recovered), 'first_recovery_after_seconds': min(recovered) - NOW}


def agent_loss_demo():
    with Store(':memory:') as store:
        store.initialize()
        seed_data(store, 100, 120)
        dialer = Dialer(store, DEMO_PROFILE)
        initial = dialer.tick(NOW, 'progressive', 'A', batch_limit=30)
        begin = time.perf_counter()
        moves = dialer.set_agents(range(1, 41), 'offline', NOW + 1)
        approved = dialer.safety.admit(propose('predictive', Snapshot(100), 100), 'A', NOW + 1, limit=100)
        response_ms = 1000 * (time.perf_counter() - begin)
        require(moves['reassigned'] == len(initial) == len(approved) == 30, 'Drop did not honor reassigned capacity')
    with Store(':memory:') as store:
        store.initialize()
        seed_data(store, 1, 1)
        dialer = Dialer(store, DEMO_PROFILE)
        dialer.tick(NOW, 'progressive', 'A')
        dialer.set_agents([1], 'offline', NOW + 5)
        dialer.dispatch(NOW + 5)
        dialer.deliver(NOW + 5)
        require(stats(store)['unserved'] == 1, 'Cancellation race should retain evidence of the unserved answer')
    return {'online_before': 100, 'online_after': 60, 'reassigned': moves['reassigned'],
            'new_approved_with_30_active': len(approved), 'reaction_ms': round(response_ms, 3),
            'cancellation_race_answered_without_agent': 1}


def event_demo():
    with Store(':memory:') as store:
        store.initialize()
        seed_data(store, 2, 2)
        dialer = Dialer(store, DEMO_PROFILE)
        first, second = dialer.safety.admit(propose('progressive', Snapshot(2)), 'A', NOW)
        with store.write() as db:
            def event(call_id, event_id, kind):
                return {'call_id': call_id, 'event_id': event_id, 'kind': kind, 'happened_at': NOW + 3,
                        'started_at': NOW, 'answered_at': NOW + 2 if kind == 'ANSWERED' else None,
                        'connected_at': None}

            answer = event(first, 'answer-1', 'ANSWERED')
            for duplicate in (answer, answer, event(first, 'answer-2', 'ANSWERED')):
                dialer.apply_event(db, duplicate, NOW + 3)
            dialer.apply_event(db, event(first, 'first-complete', 'COMPLETED'), NOW + 4)
            for rank, kind in enumerate(('COMPLETED', 'ANSWERED', 'RINGING')):
                dialer.apply_event(db, event(second, f'late-{rank}', kind), NOW + 5 + rank)
        answers = store.db.execute("SELECT COUNT(*) FROM transitions WHERE state = 'ANSWERED'").fetchone()[0]
        bridges = store.db.execute("SELECT COUNT(*) FROM jobs WHERE kind = 'BRIDGE'").fetchone()[0]
        states = [row[0] for row in store.db.execute('SELECT state FROM calls ORDER BY borrower_id')]
        require(answers == bridges == 1 and states == ['COMPLETED', 'COMPLETED'], 'Duplicate or late event changed state twice')
        return {'first_answer_transitions': answers, 'bridge_jobs_enqueued': bridges, 'out_of_order_final_state': states[1],
                'unserved_answers_retained': stats(store)['unserved']}


def failure_checks():
    return {'crash_after_provider_acceptance': crash_demo('after-start'),
            'crash_after_answer': crash_demo('after-answer'),
            'completion_while_worker_is_down': crash_demo('after-answer', recover_after_hangup=True),
            'provider_outage': outage_demo(), 'agent_loss': agent_loss_demo(),
            'duplicate_and_out_of_order': event_demo()}


def benchmark():
    simulation = compare(SCENARIOS)
    for run in simulation['runs']:
        case = f"{run['scenario']} {run['mode']} / {run['ranking']}"
        require(not run['duplicate_active'], f'{case} produced duplicate active reservations')
        require(not run['pending_after_drain'], f'{case} still has unresolved attempts after draining')
    return {'simulation': simulation, 'failures': failure_checks(),
            'allocation_load': [load_test(agents, workers=4) for agents in (100, 1000)]}


def pacing_example():
    with Store(':memory:') as store:
        store.initialize()
        seed_data(store, 10, 150)
        # Leave 50 borrowers eligible and give the forecaster 100 resolved attempts.
        with store.write() as db:
            db.execute('UPDATE borrowers SET done = 1, attempts = 1 WHERE id > 50')
            for index in range(100):
                started = NOW - 240 + 2 * index
                answered = index < 57
                db.execute('''INSERT INTO calls(id, agent_id, borrower_id, provider, attempt_no, state,
                              admitted_at, started_at, answered_at, connected_at, ended_at)
                              VALUES (?, ?, ?, 'A', 1, ?, ?, ?, ?, ?, ?)''',
                           (f'history-{index}', 1 + index % 10, 51 + index, 'COMPLETED' if answered else 'FAILED',
                            started, started, started + 4 if answered else None, started + 4 if answered else None,
                            started + (16 if answered else 4)))
        dialer = Dialer(store, DEMO_PROFILE)
        require(dialer.snapshot('A', NOW + 301).samples == 0, 'Expired attempts still affect pacing')
        dialer.tick(NOW, 'predictive', 'A')
        decision = store.db.execute('SELECT proposal, approved, reason FROM decisions').fetchone()
        proposal = json.loads(decision['proposal'])
        dialer.service(NOW + 5)
        dialer.service(NOW + 12)
        batch = store.db.execute('''SELECT COUNT(started_at) AS initiated, COUNT(connected_at) AS bridged,
            COUNT(*) FILTER (WHERE state = 'COMPLETED') AS completed FROM calls WHERE admitted_at = ?''', (NOW,)).fetchone()
        require(proposal['requested'] == 17 and
                decision['approved'] == batch['initiated'] == batch['bridged'] == batch['completed'] == 10,
                'Unexpected pacing example')
        return {'strategy': proposal['strategy'], 'inputs': proposal['snapshot'], 'answer_rate': proposal['answer_rate'],
                'proposed': proposal['requested'], 'approved': decision['approved'], 'reason': decision['reason'], **dict(batch)}
