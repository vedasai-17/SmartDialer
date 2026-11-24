import argparse
import json
import os
import time
from pathlib import Path

from .dialer import Dialer
from .ml import AnswerModel, train
from .simulation import SCENARIOS, compare, seed_data, stats
from .store import Store
from .validation import benchmark, crash_demo, failure_checks, load_test, pacing_example


def positive(raw):
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError('expected a positive integer')
    return value


def write_report(encoded, path):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(encoded + '\n', encoding='utf-8')


def report(data, path=None):
    encoded = json.dumps(data, indent=2, sort_keys=True)
    if path:
        write_report(encoded, path)
    print(encoded)


def worker(args):
    def crash_hook(point):
        if point == args.crash_at:
            os._exit(97)

    model = AnswerModel.load(args.model)
    if args.model and not model:
        print('Model unavailable. Using queue order.', flush=True)
    ranking = 'ML' if model else 'queue order'
    print(f'Worker PID {os.getpid()} using {args.mode}, provider {args.provider}, ranking {ranking}', flush=True)
    end = time.monotonic() + args.seconds if args.seconds else float('inf')
    with Store(args.db) as store:
        dialer = Dialer(store, crash_hook=crash_hook)
        while time.monotonic() < end:
            dialer.tick(time.time(), args.mode, args.provider, model)
            time.sleep(0.2)


def show_comparison(data, path=None):
    if path:
        write_report(json.dumps(data, indent=2, sort_keys=True), path)
    print('Scenario  Mode / ranker       Util %   Started  Bridged  Unserved  Requested  Approved')
    for run in data['runs']:
        strategy = f"{run['mode']} / {run['ranking']}"
        print(f"{run['scenario']:>4}      {strategy:<20} {run['utilization_pct']:>6.2f}   "
              f"{run['initiated']:>7}  {run['bridged']:>7}  {run['unserved']:>8}  "
              f"{run['requested']:>9}  {run['approved']:>8}")
    print('Held-out model Brier score and constant baseline (lower is better)')
    report(data['training'])
    if path:
        print(f'Decision traces and minute-by-minute pacing written to {path}')


def main():
    parser = argparse.ArgumentParser(prog='python -m smartdialer', description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    seed = commands.add_parser('seed', help='initialize a new database with synthetic records')
    seed.add_argument('--db', default='.smartdialer/local.sqlite3')
    seed.add_argument('--agents', type=positive, default=10)
    seed.add_argument('--borrowers', type=positive, default=50)
    work = commands.add_parser('worker', help='run one dialer worker against a seeded database')
    work.add_argument('--db', default=seed.get_default('db'))
    work.add_argument('--mode', choices=('progressive', 'predictive'), default='progressive')
    work.add_argument('--provider', choices=('A', 'B'), default='A')
    work.add_argument('--model')
    work.add_argument('--seconds', type=positive, help='exit after this many real seconds')
    work.add_argument('--crash-at', choices=('after-start', 'after-answer'))
    status = commands.add_parser('status', help='show call counts and the latest pacing decisions')
    status.add_argument('--db', default=seed.get_default('db'))
    agents = commands.add_parser('agents', help='change agent presence in a running campaign')
    agents.add_argument('--db', default=seed.get_default('db'))
    agents.add_argument('state', choices=('online', 'offline', 'paused'))
    agents.add_argument('ids', type=positive, nargs='+')
    ml = commands.add_parser('train', help='train and save an answer prediction model')
    ml.add_argument('--output', default='.smartdialer/answer-model.json')
    ml.add_argument('--seed', type=int, default=17)
    sim = commands.add_parser('simulate', help='compare three strategies on the assignment scenarios')
    sim.add_argument('--scenario', choices=('all', *SCENARIOS), default='all')
    sim.add_argument('--seconds', type=positive, default=600)
    sim.add_argument('--seed', type=int, default=17)
    sim.add_argument('--model', help='compare against an existing trained answer model')
    sim.add_argument('--report', default='.smartdialer/simulation.json')
    load = commands.add_parser('load-test', help='benchmark transactional allocation across processes')
    load.add_argument('--agents', type=positive, default=1000)
    load.add_argument('--borrowers', type=positive)
    load.add_argument('--workers', type=positive, default=4)
    load.add_argument('--batch', type=positive, default=8)
    load.add_argument('--report')
    failures = commands.add_parser('failures', help='run deterministic failure and recovery demonstrations')
    failures.add_argument('--report')
    bench = commands.add_parser('benchmark', help='run all simulations, failure checks, and allocation benchmarks')
    bench.add_argument('--report', default='.smartdialer/phase6.json')
    commands.add_parser('pacing-example', help='demonstrate a proposal of 17 and an approval of 10')
    commands.add_parser('demo', help='run a short demo of safety, recovery, pacing and ML')
    args = parser.parse_args()
    if args.command in ('worker', 'status', 'agents') and not Path(args.db).is_file():
        parser.error('database does not exist. Run seed with the same --db path first.')

    if args.command == 'seed':
        with Store(args.db) as store:
            store.initialize()
            if store.db.execute('SELECT 1 FROM agents LIMIT 1').fetchone():
                parser.error('this database is already seeded. Pass a new --db path for another run.')
            seed_data(store, args.agents, args.borrowers)
        print(f'Seeded {args.agents} agents and {args.borrowers} borrowers in {args.db}')
    elif args.command == 'worker':
        try:
            worker(args)
        except KeyboardInterrupt:
            pass
    elif args.command == 'status':
        with Store(args.db) as store:
            recent = [dict(row) for row in store.db.execute('SELECT * FROM decisions ORDER BY seq DESC LIMIT 5')]
            for item in recent:
                item['proposal'] = json.loads(item['proposal'])
            metrics = stats(store)
            report({**metrics, 'active_calls': metrics['reservations'] - metrics['terminal'],
                    'recent_decisions': recent})
    elif args.command == 'agents':
        with Store(args.db) as store:
            report(Dialer(store).set_agents(args.ids, args.state, time.time()))
    elif args.command == 'train':
        model, metrics = train(args.seed)
        model.save(args.output)
        report({**metrics, 'model': args.output})
    elif args.command == 'simulate':
        scenarios = SCENARIOS if args.scenario == 'all' else (args.scenario,)
        model = AnswerModel.load(args.model)
        if args.model and model is None:
            parser.error('the simulation model could not be loaded. Run train or check the --model path.')
        show_comparison(compare(scenarios, args.seconds, args.seed, model=model), args.report)
    elif args.command == 'load-test':
        report(load_test(args.agents, args.workers, args.borrowers, args.batch), args.report)
    elif args.command == 'failures':
        report(failure_checks(), args.report)
    elif args.command == 'benchmark':
        data = benchmark()
        write_report(json.dumps(data, indent=2, sort_keys=True), args.report)
        show_comparison(data['simulation'])
        print(f"Failure checks passed: {len(data['failures'])}")
        print('Agents  Workers  Reserved  Duplicate  Reservations/s  p95 batch ms')
        for run in data['allocation_load']:
            print(f"{run['agents']:>6}  {run['workers']:>7}  {run['reservations']:>8}  {run['duplicate_active']:>9}  "
                  f"{run['reservations_per_second']:>14.2f}  {run['p95_allocation_batch_ms']:>12.3f}")
        print(f'Complete benchmark report written to {args.report}')
    elif args.command == 'pacing-example':
        report(pacing_example())
    elif args.command == 'demo':
        report({'two_workers_one_agent': load_test(1, 2, 2, 1),
                'worker_crash': crash_demo('after-start'), 'pacing': pacing_example()})
        show_comparison(compare(('A',), 360))


if __name__ == '__main__':
    main()
