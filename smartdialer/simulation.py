from dataclasses import asdict

from .dialer import Dialer
from .ml import evaluate, stable_uniform, train
from .provider import Profile
from .store import Store


SIM_START = 1_760_000_000
SCENARIOS = {'A': (0.20, 120), 'B': (0.50, 90), 'C': (0.70, 180), 'D': (0.70, 180)}


def seed_data(store, agents, borrowers, seed=17):
    with store.write() as db:
        db.executemany('INSERT INTO agents(id) VALUES (?)', ((index,) for index in range(1, agents + 1)))
        db.executemany('INSERT INTO borrowers(id, past_calls, past_answers) VALUES (?, 12, ?)',
                       ((index, int(13 * stable_uniform(seed, index, 'history'))) for index in range(1, borrowers + 1)))


def stats(store, start=None, end=None, scheduled_agent_seconds=None):
    db = store.db
    counts = dict(db.execute('''SELECT COUNT(*) AS reservations, COUNT(started_at) AS initiated,
        COUNT(answered_at) AS answered, COUNT(connected_at) AS bridged, COUNT(ended_at) AS terminal,
        COALESCE(SUM(ended_at IS NOT NULL AND answered_at IS NOT NULL AND connected_at IS NULL), 0) AS unserved
        FROM calls''').fetchone())
    counts['safety_decisions'] = {
        row['reason']: row['n'] for row in db.execute('SELECT reason, COUNT(*) AS n FROM decisions GROUP BY reason')
    }
    counts['requested'], counts['approved'] = db.execute('''SELECT
        COALESCE(SUM(CAST(json_extract(proposal, '$.requested') AS INTEGER)), 0), COALESCE(SUM(approved), 0)
        FROM decisions''').fetchone()
    counts['duplicate_active'] = sum(db.execute(f'''SELECT COUNT(*) FROM (SELECT {column} FROM calls
        WHERE ended_at IS NULL GROUP BY {column} HAVING COUNT(*) > 1)''').fetchone()[0]
        for column in ('agent_id', 'borrower_id'))
    if scheduled_agent_seconds:
        talk = sum(max(0, min(end, row['ended_at'] or end) - max(start, row['connected_at']))
                   for row in db.execute('SELECT connected_at, ended_at FROM calls WHERE connected_at IS NOT NULL'))
        counts['utilization_pct'] = round(100 * talk / scheduled_agent_seconds, 2)
    counts['bridged_per_100_starts'] = round(100 * counts['bridged'] / max(1, counts['initiated']), 2)
    return counts


def run_scenario(scenario, mode, model, duration, seed, agents=16):
    start = SIM_START
    base_rate, talk = SCENARIOS[scenario]
    change = start + duration // 2 if scenario == 'D' else None
    provider = 'B' if scenario in ('B', 'D') else 'A'
    profile = Profile(seed=seed, answer_rate=base_rate, talk_seconds=talk, change_at=change,
                      changed_rate=0.10, changed_talk=70)
    offline_count = int(agents * 0.4) if change else 0
    with Store(':memory:') as store:
        store.initialize()
        seed_data(store, agents, max(600, duration * agents // 12), seed)
        dialer = Dialer(store, profile)
        for offset in range(duration):
            now = start + offset
            if now == change:
                dialer.set_agents(range(1, offline_count + 1), 'offline', now)
                with store.write() as db:
                    db.execute('UPDATE mock_settings SET outage_until = ? WHERE provider = ?', (now + 25, provider))
            dialer.tick(now, mode, provider, model)
        # Finish existing calls so delayed callbacks enter the safety counts.
        for offset in range(duration, duration + 400):
            dialer.service(start + offset)
            if not store.db.execute('SELECT 1 FROM calls WHERE ended_at IS NULL LIMIT 1').fetchone():
                break
        staffed = agents * duration - offline_count * (duration - duration // 2)
        metrics = stats(store, start, start + duration, staffed)
        metrics['pending_after_drain'] = metrics['reservations'] - metrics['terminal']
        trace = store.db.execute('''SELECT at, proposal, approved, reason FROM decisions
            WHERE json_extract(proposal, '$.strategy') = 'predictive'
            AND json_extract(proposal, '$.requested') > approved ORDER BY seq LIMIT 1''').fetchone()
        buckets = [dict(row) for row in store.db.execute('''SELECT CAST((at - ?) / 60 AS INTEGER) AS minute,
            SUM(json_extract(proposal, '$.requested')) AS requested, SUM(approved) AS approved,
            SUM(reason = 'provider_circuit_open') AS circuit_rejections FROM decisions GROUP BY minute ORDER BY minute''',
                                                         (start,))]
        return {'scenario': scenario, 'profile': asdict(profile), 'provider': provider,
                'mode': mode, 'ranking': 'ml' if model else 'queue', **metrics,
                'sample_reduction': dict(trace) if trace else None, 'minute_pacing': buckets}


def compare(scenarios, duration=600, seed=17, model=None):
    if model is None:
        model, training = train(seed)
        model_source = 'trained_for_run'
    else:
        training = evaluate(model, seed)
        model_source = 'saved_model'
    results, ranking_effects = [], []
    for scenario in scenarios:
        for mode, ranker in [('progressive', None), ('predictive', None), ('predictive', model)]:
            results.append(run_scenario(scenario, mode, ranker, duration, seed))
        queue, ranked = results[-2:]
        ranking_effects.append({
            'scenario': scenario,
            'utilization_change_pp': round(ranked['utilization_pct'] - queue['utilization_pct'], 2),
            'initiated_change': ranked['initiated'] - queue['initiated'],
            'bridged_change': ranked['bridged'] - queue['bridged'],
            'unserved_change': ranked['unserved'] - queue['unserved'],
        })
    return {'seed': seed, 'simulated_seconds': duration, 'model_source': model_source,
            'training': training, 'runs': results, 'ranking_effects': ranking_effects}
