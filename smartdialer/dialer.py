import json
import uuid
from dataclasses import asdict

from .pacing import Snapshot, propose
from .provider import MockProvider, Profile


TERMINAL = ('COMPLETED', 'FAILED', 'CANCELLED')
SETUP = ('RESERVED', 'INITIATED', 'RINGING', 'ANSWERED')
LEASE_SECONDS = 5
WRAP_SECONDS = 2


def record_state(db, call_id, state, now):
    db.execute('UPDATE calls SET state = ? WHERE id = ?', (state, call_id))
    db.execute('INSERT INTO transitions(call_id, state, at) VALUES (?, ?, ?)', (call_id, state, now))


def enqueue(db, kind, call_id, now):
    db.execute('INSERT OR IGNORE INTO jobs(kind, call_id, due_at) VALUES (?, ?, ?)', (kind, call_id, now))


def finish_wrap_up(db, now):
    db.execute('''UPDATE agents SET state = CASE WHEN paused THEN 'PAUSED' ELSE 'AVAILABLE' END,
                  wrap_until = NULL WHERE online = 1 AND state = 'WRAP_UP' AND wrap_until <= ?''', (now,))


class SafetyController:
    def __init__(self, store):
        self.store = store

    def admit(self, proposal, provider, now, model=None, limit=50):
        call_ids = []
        if not proposal.requested:
            return call_ids
        with self.store.write() as db:
            finish_wrap_up(db, now)
            health = db.execute('SELECT * FROM provider_health WHERE provider = ?', (provider,)).fetchone()
            budget = min(limit, proposal.requested)
            if max(health['open_until'], health['probe_until']) > now:
                budget, reason = 0, 'provider_circuit_open'
            elif health['open_until']:
                pending = db.execute('''SELECT 1 FROM calls c JOIN jobs j ON j.call_id = c.id
                    WHERE c.provider = ? AND c.ended_at IS NULL AND c.cancel_requested = 0
                        AND j.kind = 'START' AND j.done = 0 LIMIT 1''', (provider,)).fetchone()
                budget, reason = (0 if pending else 1), 'provider_half_open'
            else:
                reason = 'approved'
            if budget:
                agents = db.execute('''SELECT id FROM agents WHERE online = 1 AND paused = 0
                    AND state = 'AVAILABLE' ORDER BY id LIMIT ?''', (budget,)).fetchall()
                if len(agents) < budget and reason == 'approved':
                    reason = 'confirmed_agent_capacity'
                if agents:
                    borrowers = db.execute('''SELECT b.* FROM borrowers b WHERE b.done = 0 AND b.eligible_at <= ?
                        AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.borrower_id = b.id AND c.ended_at IS NULL)
                        ORDER BY b.priority DESC, b.eligible_at, b.id LIMIT ?''',
                                           (now, max(256, len(agents)) if model else len(agents))).fetchall()
                    if model:
                        borrowers.sort(key=lambda b: (-b['priority'], -model.score(now, b), b['eligible_at'], b['id']))
                    if len(borrowers) < len(agents) and reason == 'approved':
                        reason = 'eligible_borrower_capacity'
                    for agent, borrower in zip(agents, borrowers):
                        call_id = uuid.uuid4().hex
                        db.execute('''INSERT INTO calls(id, agent_id, borrower_id, provider, attempt_no, admitted_at)
                                      VALUES (?, ?, ?, ?, ?, ?)''',
                                   (call_id, agent['id'], borrower['id'], provider, borrower['attempts'] + 1, now))
                        db.execute("UPDATE agents SET state = 'RESERVED' WHERE id = ?", (agent['id'],))
                        db.execute('UPDATE borrowers SET attempts = attempts + 1 WHERE id = ?', (borrower['id'],))
                        record_state(db, call_id, 'RESERVED', now)
                        enqueue(db, 'START', call_id, now)
                        call_ids.append(call_id)
            db.execute('INSERT INTO decisions(at, mode, proposal, approved, reason) VALUES (?, ?, ?, ?, ?)',
                       (now, proposal.strategy, json.dumps(asdict(proposal)), len(call_ids), reason))
        return call_ids


class Dialer:
    def __init__(self, store, profile=Profile(), crash_hook=None):
        self.store = store
        self.safety = SafetyController(store)
        self.providers = {name: MockProvider(store, name, profile) for name in ('A', 'B')}
        self.crash_hook = crash_hook or (lambda _: None)

    def snapshot(self, provider, now):
        db = self.store.db
        free = db.execute('''SELECT COUNT(*) FROM agents WHERE online = 1 AND paused = 0
                          AND (state = 'AVAILABLE' OR (state = 'WRAP_UP' AND wrap_until <= ?))''', (now,)).fetchone()[0]
        history = db.execute('''SELECT COUNT(*) AS samples, SUM(answered_at IS NOT NULL) AS answers,
            AVG(answered_at - started_at) AS setup FROM calls WHERE provider = ? AND ended_at BETWEEN ? AND ?
            AND started_at IS NOT NULL AND (state IN ('COMPLETED', 'FAILED') OR answered_at IS NOT NULL)''',
                             (provider, now - 300, now)).fetchone()
        horizon = max(2, min(20, history['setup'] or (4 if provider == 'A' else 8)))
        in_flight = db.execute('''SELECT
            COUNT(*) FILTER (WHERE c.state <> 'ANSWERED') AS ringing,
            COUNT(*) FILTER (WHERE c.state <> 'ANSWERED' AND a.online = 1 AND c.cancel_requested = 0) AS covered_ringing,
            COUNT(*) FILTER (WHERE c.state = 'ANSWERED') AS awaiting_bridge,
            COUNT(*) FILTER (WHERE c.state = 'ANSWERED' AND a.online = 1 AND c.cancel_requested = 0) AS covered_awaiting_bridge
            FROM calls c JOIN agents a ON a.id = c.agent_id WHERE c.provider = ? AND c.ended_at IS NULL
            AND c.state IN ('RESERVED', 'INITIATED', 'RINGING', 'ANSWERED')''', (provider,)).fetchone()
        releases = db.execute('''SELECT
            (SELECT COUNT(*) FROM calls c JOIN agents a ON a.id = c.agent_id
             WHERE c.state = 'CONNECTED' AND c.ended_at IS NULL AND c.cancel_requested = 0 AND a.online = 1
             AND a.paused = 0 AND c.expected_end_at > ? AND c.expected_end_at + ? <= ?) +
            (SELECT COUNT(*) FROM agents WHERE online = 1 AND paused = 0 AND state = 'WRAP_UP'
             AND wrap_until > ? AND wrap_until <= ?)''', (now, WRAP_SECONDS, now + horizon, now, now + horizon)).fetchone()[0]
        return Snapshot(free=free, near_releases=releases, samples=history['samples'],
                        answers=history['answers'] or 0, horizon=round(horizon, 3), **dict(in_flight))

    def tick(self, now, mode, provider, model=None, batch_limit=50):
        self.service(now)
        proposal = propose(mode, self.snapshot(provider, now), batch_limit)
        admitted = self.safety.admit(proposal, provider, now, model, batch_limit)
        if admitted:
            self.dispatch(now, len(admitted))
        return admitted

    def service(self, now):
        for provider in self.providers.values():
            provider.advance(now)
        self.deliver(now)
        self.dispatch(now)
        self.deliver(now)
        if self.store.db.execute("SELECT 1 FROM agents WHERE state = 'WRAP_UP' AND wrap_until <= ? LIMIT 1",
                                 (now,)).fetchone():
            with self.store.write() as db:
                finish_wrap_up(db, now)

    def deliver(self, now, limit=256):
        select = 'SELECT * FROM mock_events WHERE delivered = 0 AND deliver_at <= ? ORDER BY deliver_at, seq LIMIT ?'
        if not self.store.db.execute(select, (now, 1)).fetchone():
            return
        with self.store.write() as db:
            events = db.execute(select, (now, limit)).fetchall()
            for event in events:
                self.apply_event(db, event, now)
                db.execute('UPDATE mock_events SET delivered = 1 WHERE seq = ?', (event['seq'],))
        if any(event['kind'] == 'ANSWERED' for event in events):
            self.crash_hook('after-answer')

    def apply_event(self, db, event, now):
        call_id = event['call_id']
        changed = db.execute('INSERT OR IGNORE INTO inbox(event_id, call_id, kind, received_at) VALUES (?, ?, ?, ?)',
                             (event['event_id'], call_id, event['kind'], now)).rowcount
        if not changed:
            return
        prior = db.execute('SELECT * FROM calls WHERE id = ?', (call_id,)).fetchone()
        db.execute('''UPDATE calls SET started_at = COALESCE(started_at, ?), answered_at = COALESCE(answered_at, ?),
                      connected_at = COALESCE(connected_at, ?) WHERE id = ?''',
                   (event['started_at'], event['answered_at'], event['connected_at'], call_id))
        call = db.execute('SELECT * FROM calls WHERE id = ?', (call_id,)).fetchone()
        if call['ended_at'] is not None:
            newly_started = prior['started_at'] is None and call['started_at'] is not None
            newly_answered = prior['answered_at'] is None and call['answered_at'] is not None
            if newly_started or newly_answered:
                db.execute('''UPDATE borrowers SET past_calls = past_calls + ?, past_answers = past_answers + ?,
                              done = MAX(done, ?) WHERE id = ?''',
                           (newly_started, newly_answered, newly_answered, call['borrower_id']))
            return
        kind, state = event['kind'], call['state']
        if kind in TERMINAL:
            self._finish(db, call, kind, event['happened_at'], now)
        elif kind == 'RINGING' and state in ('RESERVED', 'INITIATED'):
            record_state(db, call_id, kind, now)
            db.execute("UPDATE agents SET state = 'DIALING' WHERE id = ? AND online = 1", (call['agent_id'],))
        elif kind == 'ANSWERED' and state in ('RESERVED', 'INITIATED', 'RINGING'):
            record_state(db, call_id, kind, now)
            db.execute("UPDATE agents SET state = 'DIALING' WHERE id = ? AND online = 1", (call['agent_id'],))
            if not call['cancel_requested']:
                enqueue(db, 'BRIDGE', call_id, now)
        elif kind == 'CONNECTED' and state in SETUP:
            record_state(db, call_id, kind, now)
            duration = db.execute('''SELECT AVG(ended_at - connected_at) FROM calls WHERE provider = ?
                AND ended_at >= ? AND state = 'COMPLETED' AND connected_at IS NOT NULL''',
                                  (call['provider'], now - 900)).fetchone()[0]
            db.execute('UPDATE calls SET expected_end_at = ? WHERE id = ?',
                       (event['happened_at'] + (duration or 120), call_id))
            db.execute("UPDATE agents SET state = 'CONNECTED' WHERE id = ? AND online = 1", (call['agent_id'],))

    @staticmethod
    def _finish(db, call, kind, happened, now):
        record_state(db, call['id'], kind, now)
        db.execute('UPDATE calls SET ended_at = ? WHERE id = ?', (happened, call['id']))
        db.execute('UPDATE jobs SET done = 1 WHERE call_id = ?', (call['id'],))
        db.execute('''UPDATE agents SET state = CASE WHEN online = 0 THEN 'OFFLINE'
                       WHEN ? THEN 'WRAP_UP' WHEN paused = 1 THEN 'PAUSED' ELSE 'AVAILABLE' END,
                       wrap_until = CASE WHEN online = 1 AND ? THEN ? END WHERE id = ?''',
                   (call['connected_at'] is not None, call['connected_at'] is not None,
                    now + WRAP_SECONDS, call['agent_id']))
        answered = call['answered_at'] is not None
        db.execute('''UPDATE borrowers SET done = ?, eligible_at = ?, past_calls = past_calls + ?,
                      past_answers = past_answers + ? WHERE id = ?''',
                   (answered or call['attempt_no'] >= 3, now + 30,
                    call['started_at'] is not None, answered, call['borrower_id']))

    def set_agents(self, ids, state, now):
        ids = sorted(set(ids))
        moves = {'reassigned': 0, 'cancellation_requested': 0}
        with self.store.write() as db:
            for agent_id in ids:
                if state == 'offline':
                    db.execute("UPDATE agents SET online = 0, state = 'OFFLINE', wrap_until = NULL WHERE id = ?", (agent_id,))
                elif state == 'paused':
                    db.execute("UPDATE agents SET paused = 1, state = CASE WHEN state = 'AVAILABLE' THEN 'PAUSED' ELSE state END WHERE id = ?",
                               (agent_id,))
                elif state == 'online':
                    db.execute('''UPDATE agents SET online = 1, paused = 0, state = CASE
                        WHEN EXISTS (SELECT 1 FROM calls c WHERE c.agent_id = agents.id AND c.ended_at IS NULL
                            AND c.state = 'CONNECTED') THEN 'CONNECTED'
                        WHEN EXISTS (SELECT 1 FROM calls c WHERE c.agent_id = agents.id AND c.ended_at IS NULL
                            AND c.state <> 'RESERVED') THEN 'DIALING'
                        WHEN EXISTS (SELECT 1 FROM calls c WHERE c.agent_id = agents.id AND c.ended_at IS NULL)
                            THEN 'RESERVED' WHEN wrap_until > ? THEN 'WRAP_UP' ELSE 'AVAILABLE' END WHERE id = ?''',
                               (now, agent_id))
                else:
                    raise ValueError(f'Unknown agent command {state}')
            if state == 'offline':
                calls = db.execute('''SELECT c.* FROM calls c JOIN agents a ON a.id = c.agent_id
                            WHERE a.online = 0 AND c.ended_at IS NULL AND c.cancel_requested = 0 ORDER BY c.admitted_at''').fetchall()
                for call in calls:
                    bridge = db.execute("SELECT 1 FROM jobs WHERE kind = 'BRIDGE' AND call_id = ?", (call['id'],)).fetchone()
                    spare = None
                    if not bridge and call['state'] in SETUP:
                        spare = db.execute("SELECT id FROM agents WHERE online = 1 AND paused = 0 AND state = 'AVAILABLE' ORDER BY id LIMIT 1").fetchone()
                    if spare:
                        db.execute('UPDATE calls SET agent_id = ? WHERE id = ?', (spare['id'], call['id']))
                        db.execute('UPDATE agents SET state = ? WHERE id = ?',
                                   ('RESERVED' if call['state'] == 'RESERVED' else 'DIALING', spare['id']))
                        moves['reassigned'] += 1
                    else:
                        db.execute('UPDATE calls SET cancel_requested = 1 WHERE id = ?', (call['id'],))
                        enqueue(db, 'CANCEL', call['id'], now)
                        moves['cancellation_requested'] += 1
        return moves

    def dispatch(self, now, limit=256):
        for _ in range(limit):
            job = self._claim_job(now)
            if not job:
                return
            call = self.store.db.execute('SELECT * FROM calls WHERE id = ?', (job['call_id'],)).fetchone()
            provider = self.providers[call['provider']]
            if job['kind'] == 'START':
                self._dispatch_start(job, provider, now)
            elif job['kind'] == 'BRIDGE':
                if call['ended_at'] is None and not call['cancel_requested'] and call['state'] == 'ANSWERED':
                    provider.bridge(call['id'], call['agent_id'], now)
                self._complete_job(job)
            else:
                if call['ended_at'] is None:
                    provider.cancel(call['id'], now)
                self._complete_job(job)

    def _claim_job(self, now):
        query = '''SELECT * FROM jobs WHERE done = 0 AND due_at <= ? AND lease_until <= ?
                   ORDER BY CASE kind WHEN 'CANCEL' THEN 0 WHEN 'BRIDGE' THEN 1 ELSE 2 END, due_at, rowid LIMIT 1'''
        if not self.store.db.execute(query, (now, now)).fetchone():
            return None
        with self.store.write() as db:
            row = db.execute(query, (now, now)).fetchone()
            if not row:
                return None
            job = dict(row)
            job['token'] = uuid.uuid4().hex
            job['attempts'] += 1
            db.execute('UPDATE jobs SET lease_until = ?, token = ?, attempts = ? WHERE kind = ? AND call_id = ?',
                       (now + LEASE_SECONDS, job['token'], job['attempts'], job['kind'], job['call_id']))
        return job

    def _dispatch_start(self, job, provider, now):
        call_id = job['call_id']
        accepted = provider.status(call_id)
        if accepted is None:
            with self.store.write() as db:
                current = db.execute('''SELECT c.*, a.online, j.token, j.done FROM calls c JOIN agents a ON a.id = c.agent_id
                    JOIN jobs j ON j.call_id = c.id AND j.kind = 'START' WHERE c.id = ?''', (call_id,)).fetchone()
                if current['done'] or current['token'] != job['token']:
                    return
                if current['ended_at'] is not None or current['cancel_requested'] or not current['online']:
                    self._update_job(db, job, done=True)
                    return
                health = db.execute('SELECT * FROM provider_health WHERE provider = ?', (provider.name,)).fetchone()
                blocked_until = max(health['open_until'], health['probe_until'])
                if blocked_until > now:
                    self._update_job(db, job, due_at=blocked_until)
                    return
                if health['open_until']:
                    db.execute('UPDATE provider_health SET probe_until = ?, probe_token = ? WHERE provider = ?',
                               (now + LEASE_SECONDS, job['token'], provider.name))
            try:
                accepted = provider.start(call_id, now)
                self.crash_hook('after-start')
            except TimeoutError:
                with self.store.write() as db:
                    changed = self._update_job(db, job, due_at=now + min(30, 2 ** min(5, job['attempts'])))
                    if changed:
                        health = db.execute('SELECT * FROM provider_health WHERE provider = ?', (provider.name,)).fetchone()
                        failures = health['failures'] + 1
                        until = max(health['open_until'], now + 12) if failures >= 2 else 0
                        db.execute('''UPDATE provider_health SET failures = ?, open_until = ?, probe_until = 0,
                                      probe_token = NULL WHERE provider = ?''', (failures, until, provider.name))
                return
        with self.store.write() as db:
            if not self._update_job(db, job, done=True):
                return
            started = accepted['started_at']
            if started is not None:
                call = db.execute('SELECT * FROM calls WHERE id = ?', (call_id,)).fetchone()
                db.execute('UPDATE calls SET started_at = COALESCE(started_at, ?) WHERE id = ?', (started, call_id))
                if call['state'] == 'RESERVED' and call['ended_at'] is None:
                    record_state(db, call_id, 'INITIATED', now)
                    db.execute("UPDATE agents SET state = 'DIALING' WHERE id = ? AND online = 1", (call['agent_id'],))
                db.execute('''UPDATE provider_health SET failures = 0, open_until = 0, probe_until = 0,
                       probe_token = NULL WHERE provider = ? AND (open_until = 0 OR probe_token = ?)''',
                           (provider.name, job['token']))

    def _complete_job(self, job):
        with self.store.write() as db:
            self._update_job(db, job, done=True)

    @staticmethod
    def _update_job(db, job, *, done=False, due_at=None):
        return db.execute('''UPDATE jobs SET done = ?, due_at = COALESCE(?, due_at), lease_until = 0, token = NULL
                   WHERE kind = ? AND call_id = ? AND token = ? AND done = 0''',
                          (done, due_at, job['kind'], job['call_id'], job['token'])).rowcount
