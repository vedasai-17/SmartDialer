import multiprocessing as mp
import tempfile
import unittest
from pathlib import Path

from smartdialer.dialer import Dialer
from smartdialer.ml import AnswerModel, evaluate, train
from smartdialer.pacing import Snapshot, propose
from smartdialer.simulation import seed_data, stats
from smartdialer.validation import (DEMO_PROFILE, NOW, agent_loss_demo, crash_demo, event_demo,
                                    load_test, outage_demo, pacing_example)
from smartdialer.store import Store


def _run_progressive_tick(path, ready):
    with Store(path) as store:
        ready.wait(timeout=10)
        Dialer(store, DEMO_PROFILE).tick(NOW, 'progressive', 'A')


class SafetyTests(unittest.TestCase):
    def test_progressive_workers_respect_capacity_through_wrap_up(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'progressive.sqlite3')
            with Store(path) as store:
                store.initialize()
                seed_data(store, 10, 50)

            context = mp.get_context('spawn')
            ready = context.Barrier(2)
            workers = [context.Process(target=_run_progressive_tick, args=(path, ready)) for _ in range(2)]
            try:
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join(timeout=15)
                    self.assertEqual(worker.exitcode, 0)
            finally:
                for worker in workers:
                    if worker.is_alive():
                        worker.terminate()
                        worker.join()

            with Store(path) as store:
                first_batch = store.db.execute('''SELECT agent_id, borrower_id FROM calls
                    WHERE started_at IS NOT NULL AND ended_at IS NULL''').fetchall()
                self.assertEqual(len(first_batch), 10)
                self.assertEqual(len({row['agent_id'] for row in first_batch}), 10)
                self.assertEqual(len({row['borrower_id'] for row in first_batch}), 10)
                self.assertEqual(stats(store)['reservations'], 10)

                dialer = Dialer(store, DEMO_PROFILE)
                dialer.service(NOW + 5)
                self.assertEqual(stats(store)['bridged'], 10)
                dialer.service(NOW + 11)
                self.assertEqual(stats(store)['terminal'], 10)
                self.assertEqual(store.db.execute("SELECT COUNT(*) FROM agents WHERE state = 'WRAP_UP'").fetchone()[0], 10)
                self.assertFalse(dialer.tick(NOW + 12, 'progressive', 'A'))
                self.assertEqual(len(dialer.tick(NOW + 13, 'progressive', 'A')), 10)
                self.assertEqual(stats(store)['duplicate_active'], 0)

    def test_concurrent_workers_share_one_agent(self):
        self.assertEqual(load_test(1, 2, 2, 1)['reservations'], 1)

    def test_concurrent_workers_share_one_borrower(self):
        self.assertEqual(load_test(2, 2, 1, 1)['reservations'], 1)

    def test_pacing_is_clamped(self):
        example = pacing_example()
        self.assertEqual((example['proposed'], example['approved'], example['initiated']), (17, 10, 10))
        self.assertEqual((example['strategy'], example['inputs']['samples'], example['inputs']['answers']),
                         ('predictive', 100, 57))
        self.assertEqual((example['bridged'], example['completed']), (10, 10))

    def test_stale_history_and_ringing_adjustment(self):
        fallback = propose('predictive', Snapshot(10, samples=9, answers=2))
        self.assertEqual((fallback.requested, fallback.strategy), (10, 'progressive_fallback'))
        base = propose('predictive', Snapshot(10, samples=100, answers=50))
        with_ringing = propose('predictive', Snapshot(10, ringing=4, samples=100, answers=50))
        with_waiting_answer = propose('predictive', Snapshot(10, samples=100, answers=50, awaiting_bridge=1))
        self.assertEqual((base.requested, with_ringing.requested, with_waiting_answer.requested), (20, 16, 18))

    def test_forecast_reads_ringing_pending_answers_and_wrap_up_from_live_state(self):
        with Store(':memory:') as store:
            store.initialize()
            seed_data(store, 2, 3)
            dialer = Dialer(store, DEMO_PROFILE)
            dialer.tick(NOW, 'progressive', 'A', batch_limit=1)
            dialer.service(NOW + 1)
            ringing = dialer.snapshot('A', NOW + 1)
            self.assertEqual((ringing.free, ringing.ringing, ringing.covered_ringing), (1, 1, 1))

            dialer.providers['A'].advance(NOW + 5)
            dialer.deliver(NOW + 5)
            waiting = dialer.snapshot('A', NOW + 5)
            self.assertEqual((waiting.ringing, waiting.awaiting_bridge, waiting.covered_awaiting_bridge), (0, 1, 1))

            dialer.service(NOW + 5)
            dialer.service(NOW + 11)
            wrapping = dialer.snapshot('A', NOW + 11)
            self.assertEqual((wrapping.free, wrapping.near_releases), (1, 1))
            free = dialer.snapshot('A', NOW + 13)
            self.assertEqual((free.free, free.near_releases), (2, 0))

    def test_pause_defers_until_wrap_up(self):
        with Store(':memory:') as store:
            store.initialize()
            seed_data(store, 1, 2)
            dialer = Dialer(store, DEMO_PROFILE)
            dialer.tick(NOW, 'progressive', 'A')
            dialer.service(NOW + 5)
            dialer.set_agents([1], 'paused', NOW + 5)
            dialer.service(NOW + 10)
            self.assertEqual(store.db.execute('SELECT state FROM agents').fetchone()[0], 'WRAP_UP')
            dialer.service(NOW + 12)
            self.assertEqual(store.db.execute('SELECT state FROM agents').fetchone()[0], 'PAUSED')
            self.assertFalse(dialer.tick(NOW + 13, 'progressive', 'A'))
            dialer.set_agents([1], 'online', NOW + 14)
            self.assertEqual(len(dialer.tick(NOW + 14, 'progressive', 'A')), 1)


class FailureTests(unittest.TestCase):
    def test_crash_at_both_commit_boundaries(self):
        for point in ('after-start', 'after-answer'):
            with self.subTest(point=point):
                self.assertEqual(crash_demo(point)['finalizations'], 1)

    def test_completion_while_worker_is_down_skips_stale_bridge(self):
        recovery = crash_demo('after-answer', recover_after_hangup=True)
        self.assertEqual((recovery['bridged'], recovery['unserved']), (0, 1))

    def test_outage_preserves_existing_calls_and_pauses_new_starts(self):
        self.assertGreater(outage_demo()['circuit_rejections'], 0)

    def test_dropout_and_cancellation_race(self):
        self.assertEqual(agent_loss_demo()['cancellation_race_answered_without_agent'], 1)

    def test_duplicate_and_out_of_order_callbacks(self):
        self.assertEqual(event_demo()['out_of_order_final_state'], 'COMPLETED')

    def test_accepted_timeout_reuses_attempt_id(self):
        with Store(':memory:') as store:
            store.initialize()
            seed_data(store, 1, 1)
            dialer = Dialer(store, DEMO_PROFILE)
            store.db.execute("UPDATE mock_settings SET timeout_next = 1 WHERE provider = 'B'")
            call_id, = dialer.tick(NOW, 'progressive', 'B')
            self.assertIsNotNone(dialer.providers['B'].status(call_id)['started_at'])
            self.assertIsNone(store.db.execute('SELECT started_at FROM calls').fetchone()[0])
            for offset in range(1, 25):
                dialer.service(NOW + offset)
            self.assertEqual(store.db.execute('SELECT COUNT(*) FROM mock_calls').fetchone()[0], 1)
            self.assertEqual((stats(store)['bridged'], stats(store)['terminal']), (1, 1))


class RankingTests(unittest.TestCase):
    def test_ranking_obeys_priority_and_falls_back_for_invalid_model(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'model.json'
            self.assertIsNone(AnswerModel.load(path))
            for invalid in ('{"version":1,"weights":"12345"}', '{"version":1,"weights":[NaN,0,0,0,0]}'):
                path.write_text(invalid, encoding='utf-8')
                fallback = AnswerModel.load(path)
                self.assertIsNone(fallback)
            model, metrics = train()
            model.save(path)
            saved = AnswerModel.load(path)
            self.assertEqual(saved, model)
            self.assertEqual(evaluate(saved), metrics)
            self.assertLess(metrics['held_out_brier'], metrics['constant_baseline_brier'])
        cases = [('ml', saved, 0, 0, 2), ('priority', saved, 1, 0, 1),
                 ('cooldown', saved, 0, NOW + 30, 1), ('fallback', fallback, 0, 0, 1)]
        for name, ranker, priority, eligible_at, expected in cases:
            with self.subTest(name=name), Store(':memory:') as store:
                store.initialize()
                seed_data(store, 1, 2)
                store.db.execute('UPDATE borrowers SET past_answers = 0, priority = ? WHERE id = 1', (priority,))
                store.db.execute('UPDATE borrowers SET past_answers = 12, eligible_at = ? WHERE id = 2', (eligible_at,))
                Dialer(store).tick(NOW, 'predictive', 'A', ranker)
                self.assertEqual(store.db.execute('SELECT borrower_id FROM calls').fetchone()[0], expected)


if __name__ == '__main__':
    unittest.main()
