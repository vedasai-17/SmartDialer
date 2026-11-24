import os
from collections.abc import Iterator
from datetime import UTC, datetime
import pytest
from sqlalchemy import Engine, text

from smartdialer.v3.clock import VirtualClock
from smartdialer.v3.config import Settings
from smartdialer.v3.lab import isolated_database


@pytest.fixture(scope="session")
def migrated_database() -> Iterator[Engine]:
    url = os.environ.get("SMARTDIALER_TEST_DATABASE_URL", Settings().database_url)
    with isolated_database(url) as engine:
        yield engine


@pytest.fixture
def engine(migrated_database: Engine) -> Engine:
    with migrated_database.begin() as connection:
        connection.execute(text("""
            TRUNCATE TABLE call_events, audit_log, calls, safety_decisions, agents, borrowers, campaigns
            RESTART IDENTITY
        """))
    return migrated_database


@pytest.fixture
def clock() -> VirtualClock:
    return VirtualClock(datetime(2026, 1, 1, tzinfo=UTC))
