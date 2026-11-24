from dataclasses import dataclass
from typing import Protocol

from .ml import answer_probability, features, stable_uniform


@dataclass(frozen=True)
class Profile:
    seed: int = 23
    answer_rate: float = 0.5
    talk_seconds: float = 90
    change_at: float | None = None
    changed_rate: float = 0.7
    changed_talk: float = 180
    noisy: bool = True


class TelecomProvider(Protocol):
    def start(self, call_id, now): ...
    def status(self, call_id): ...
    def cancel(self, call_id, now): ...
    def bridge(self, call_id, agent_id, now): ...


class MockProvider:
    """Separate transactions model a provider that survives a dialer process crash."""

    def __init__(self, store, name, profile):
        self.store = store
        self.name = name
        self.profile = profile

    def status(self, call_id):
        return self.store.db.execute('SELECT * FROM mock_calls WHERE call_id = ?', (call_id,)).fetchone()

    def start(self, call_id, now):
        with self.store.write() as db:
            existing = self.status(call_id)
            if existing:
                return existing
            settings = db.execute('SELECT * FROM mock_settings WHERE provider = ?', (self.name,)).fetchone()
            if settings['outage_until'] > now:
                raise TimeoutError(f'Provider {self.name} start endpoint is unavailable')
            borrower = db.execute('''
                SELECT b.*, c.attempt_no, c.ended_at, c.cancel_requested, a.online,
                    EXISTS (SELECT 1 FROM jobs j WHERE j.call_id = c.id AND j.kind = 'START') AS approved
                FROM calls c JOIN borrowers b ON b.id = c.borrower_id JOIN agents a ON a.id = c.agent_id
                WHERE c.id = ? AND c.provider = ?
            ''', (call_id, self.name)).fetchone()
            if not borrower or not borrower['approved']:
                raise ValueError('Provider start requires an approved attempt')
            if borrower['ended_at'] is not None or borrower['cancel_requested'] or not borrower['online']:
                db.execute("INSERT INTO mock_calls(call_id, provider, state) VALUES (?, ?, 'CANCELLED')",
                           (call_id, self.name))
                call = self.status(call_id)
                self._emit(db, call, 'CANCELLED', now)
                return call
            key = (self.profile.seed, borrower['id'], borrower['attempt_no'])
            fault = stable_uniform(*key, 'provider timeout')
            if self.name == 'A' and self.profile.noisy and fault < 0.01:
                db.execute("INSERT INTO mock_calls(call_id, provider, state) VALUES (?, ?, 'FAILED')",
                           (call_id, self.name))
                call = self.status(call_id)
                self._emit(db, call, 'FAILED', now)
                return call
            noisy = self.profile.noisy and self.name == 'B'
            if noisy and fault < 0.04:
                attempts = db.execute("SELECT attempts FROM jobs WHERE kind = 'START' AND call_id = ?",
                                      (call_id,)).fetchone()[0]
                if attempts == 1:
                    raise TimeoutError('Request expired before reaching the provider')
            changed = self.profile.change_at is not None and now >= self.profile.change_at
            rate = self.profile.changed_rate if changed else self.profile.answer_rate
            talk = self.profile.changed_talk if changed else self.profile.talk_seconds
            inputs = features(now, borrower['past_calls'], borrower['past_answers'], borrower['attempt_no'] - 1)
            probability = rate if rate in (0, 1) else answer_probability(inputs, rate)
            answers = stable_uniform(*key, 'answer') < probability
            delay = (2 if self.name == 'A' else 6) + int(stable_uniform(*key, 'setup') * 3)
            talk *= 0.7 + 0.6 * stable_uniform(*key, 'talk')
            db.execute('''INSERT INTO mock_calls(call_id, provider, state, started_at, outcome_at, will_answer,
                          talk_seconds) VALUES (?, ?, 'RINGING', ?, ?, ?, ?)''',
                       (call_id, self.name, now, now + delay, answers, talk))
            call = self.status(call_id)
            delivery = now + delay + 2 if noisy and stable_uniform(*key, 'late ring') < 0.35 else now + 1
            self._emit(db, call, 'RINGING', now + 1, delivery)
            timeout = settings['timeout_next'] > 0 or (noisy and 0.04 <= fault < 0.12)
            if settings['timeout_next'] > 0:
                db.execute('UPDATE mock_settings SET timeout_next = timeout_next - 1 WHERE provider = ?', (self.name,))
        if timeout:
            raise TimeoutError('Provider accepted the start, but its response timed out')
        return call

    def bridge(self, call_id, agent_id, now):
        with self.store.write() as db:
            call = self._advance_call(db, self.status(call_id), now)
            if not call or call['state'] != 'ANSWERED':
                return call
            eligible = db.execute('''SELECT 1 FROM agents a JOIN calls c ON c.agent_id = a.id
                WHERE a.id = ? AND a.online = 1 AND c.id = ? AND c.ended_at IS NULL
                    AND c.cancel_requested = 0 AND c.state = 'ANSWERED' ''', (agent_id, call_id)).fetchone()
            if eligible:
                db.execute('''UPDATE mock_calls SET state = 'CONNECTED', connected_at = ?, finish_at = ?,
                              bridged_agent = ? WHERE call_id = ?''', (now, now + call['talk_seconds'], agent_id, call_id))
                call = self.status(call_id)
                self._emit(db, call, 'CONNECTED', now)
            return call

    def cancel(self, call_id, now):
        with self.store.write() as db:
            call = self._advance_call(db, self.status(call_id), now)
            if call and call['state'] in ('COMPLETED', 'FAILED', 'CANCELLED'):
                return call
            if call:
                db.execute("UPDATE mock_calls SET state = 'CANCELLED' WHERE call_id = ?", (call_id,))
            else:
                # A tombstone fences delayed start requests for this same attempt.
                db.execute("INSERT INTO mock_calls(call_id, provider, state) VALUES (?, ?, 'CANCELLED')",
                           (call_id, self.name))
            call = self.status(call_id)
            self._emit(db, call, 'CANCELLED', now)
            return call

    def advance(self, now, limit=256):
        pending = self.store.db.execute('''SELECT 1 FROM mock_calls WHERE provider = ? AND
            ((state = 'RINGING' AND outcome_at <= ?) OR (state IN ('ANSWERED', 'CONNECTED') AND finish_at <= ?))
            LIMIT 1''', (self.name, now, now)).fetchone()
        if not pending:
            return
        with self.store.write() as db:
            for call in db.execute('''SELECT * FROM mock_calls WHERE provider = ? AND
                ((state = 'RINGING' AND outcome_at <= ?) OR (state IN ('ANSWERED', 'CONNECTED') AND finish_at <= ?))
                LIMIT ?''', (self.name, now, now, limit)).fetchall():
                self._advance_call(db, call, now)

    def _advance_call(self, db, call, now):
        if not call:
            return None
        call_id = call['call_id']
        if call['state'] == 'RINGING' and call['outcome_at'] <= now:
            outcome = call['outcome_at']
            if call['will_answer']:
                db.execute("UPDATE mock_calls SET state = 'ANSWERED', answered_at = ?, finish_at = ? WHERE call_id = ?",
                           (outcome, outcome + 12, call_id))
                call = self.status(call_id)
                self._emit(db, call, 'ANSWERED', outcome)
                if self.name == 'B' and self.profile.noisy:
                    self._emit(db, call, 'ANSWERED', outcome, outcome + 0.25, suffix='duplicate')
            else:
                db.execute("UPDATE mock_calls SET state = 'FAILED' WHERE call_id = ?", (call_id,))
                call = self.status(call_id)
                self._emit(db, call, 'FAILED', outcome)

        if call['state'] in ('ANSWERED', 'CONNECTED') and call['finish_at'] <= now:
            db.execute("UPDATE mock_calls SET state = 'COMPLETED' WHERE call_id = ?", (call_id,))
            self._emit(db, call, 'COMPLETED', call['finish_at'])
            call = self.status(call_id)
        return call

    @staticmethod
    def _emit(db, call, kind, happened, delivery=None, suffix=''):
        event_id = f"{call['call_id']}:{kind}:{suffix}"
        db.execute('''INSERT OR IGNORE INTO mock_events(event_id, call_id, kind, happened_at, deliver_at,
                       started_at, answered_at, connected_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                   (event_id, call['call_id'], kind, happened, happened if delivery is None else delivery,
                    call['started_at'], call['answered_at'], call['connected_at']))
