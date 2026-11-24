import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS agents (
    id INTEGER PRIMARY KEY,
    online INTEGER NOT NULL DEFAULT 1,
    paused INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'AVAILABLE',
    wrap_until REAL
);
CREATE INDEX IF NOT EXISTS ready_agents ON agents(id) WHERE online = 1 AND paused = 0 AND state = 'AVAILABLE';
CREATE INDEX IF NOT EXISTS wrapping_agents ON agents(wrap_until) WHERE state = 'WRAP_UP';
CREATE TABLE IF NOT EXISTS borrowers (
    id INTEGER PRIMARY KEY,
    priority INTEGER NOT NULL DEFAULT 0,
    eligible_at REAL NOT NULL DEFAULT 0,
    done INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    past_calls INTEGER NOT NULL,
    past_answers INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS borrower_queue ON borrowers(done, priority DESC, eligible_at, id);
CREATE TABLE IF NOT EXISTS calls (
    id TEXT PRIMARY KEY,
    agent_id INTEGER NOT NULL REFERENCES agents,
    borrower_id INTEGER NOT NULL REFERENCES borrowers,
    provider TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'RESERVED',
    admitted_at REAL NOT NULL,
    started_at REAL,
    answered_at REAL,
    connected_at REAL,
    ended_at REAL,
    expected_end_at REAL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    UNIQUE (borrower_id, attempt_no)
);
CREATE UNIQUE INDEX IF NOT EXISTS active_agent ON calls(agent_id) WHERE ended_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS active_borrower ON calls(borrower_id) WHERE ended_at IS NULL;
CREATE INDEX IF NOT EXISTS call_history ON calls(provider, ended_at);
CREATE TABLE IF NOT EXISTS jobs (
    kind TEXT NOT NULL,
    call_id TEXT NOT NULL REFERENCES calls,
    due_at REAL NOT NULL,
    lease_until REAL NOT NULL DEFAULT 0,
    token TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    done INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (kind, call_id)
);
CREATE INDEX IF NOT EXISTS pending_jobs ON jobs(done, due_at, lease_until);
CREATE TABLE IF NOT EXISTS provider_health (
    provider TEXT PRIMARY KEY,
    failures INTEGER NOT NULL DEFAULT 0,
    open_until REAL NOT NULL DEFAULT 0,
    probe_until REAL NOT NULL DEFAULT 0,
    probe_token TEXT
);
INSERT OR IGNORE INTO provider_health(provider) VALUES ('A'), ('B');
CREATE TABLE IF NOT EXISTS mock_settings (
    provider TEXT PRIMARY KEY,
    outage_until REAL NOT NULL DEFAULT 0,
    timeout_next INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO mock_settings(provider) VALUES ('A'), ('B');
CREATE TABLE IF NOT EXISTS mock_calls (
    call_id TEXT PRIMARY KEY REFERENCES calls,
    provider TEXT NOT NULL,
    state TEXT NOT NULL,
    started_at REAL,
    outcome_at REAL,
    will_answer INTEGER,
    talk_seconds REAL,
    answered_at REAL,
    connected_at REAL,
    finish_at REAL,
    bridged_agent INTEGER
);
CREATE INDEX IF NOT EXISTS mock_active ON mock_calls(state, outcome_at, finish_at);
CREATE TABLE IF NOT EXISTS mock_events (
    seq INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    call_id TEXT NOT NULL REFERENCES calls,
    kind TEXT NOT NULL,
    happened_at REAL NOT NULL,
    deliver_at REAL NOT NULL,
    started_at REAL,
    answered_at REAL,
    connected_at REAL,
    delivered INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS deliverable_events ON mock_events(delivered, deliver_at, seq);
CREATE TABLE IF NOT EXISTS inbox (
    event_id TEXT PRIMARY KEY,
    call_id TEXT NOT NULL REFERENCES calls,
    kind TEXT NOT NULL,
    received_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS transitions (
    seq INTEGER PRIMARY KEY,
    call_id TEXT NOT NULL REFERENCES calls,
    state TEXT NOT NULL,
    at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS decisions (
    seq INTEGER PRIMARY KEY,
    at REAL NOT NULL,
    mode TEXT NOT NULL,
    proposal TEXT NOT NULL,
    approved INTEGER NOT NULL,
    reason TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path):
        if str(path) != ':memory:':
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=15, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys = ON')
        self.db.execute('PRAGMA busy_timeout = 15000')

    def initialize(self):
        self.db.execute('PRAGMA journal_mode = WAL')
        self.db.executescript(SCHEMA)

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextmanager
    def write(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield self.db
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
