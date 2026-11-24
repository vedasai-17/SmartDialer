import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text

from smartdialer.v3.clock import VirtualClock
from smartdialer.v3.config import Settings


@pytest.fixture(scope="session")
def migrated_database() -> Iterator[Engine]:
    url = os.environ.get("SMARTDIALER_TEST_DATABASE_URL", Settings().database_url)
    base = create_engine(url, hide_parameters=True)
    schema = f"test_dialer_{uuid4().hex}"
    with base.begin() as connection:
        server_version = int(connection.execute(text("SHOW server_version_num")).scalar_one())
        assert 160000 <= server_version < 170000, "the integration suite needs PostgreSQL 16"
        connection.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"}, hide_parameters=True)
    try:
        with engine.connect() as connection:
            config = Config(Path(__file__).resolve().parents[2] / "alembic.ini")
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield engine
    finally:
        engine.dispose()
        with base.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        base.dispose()


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
