from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text

# Alembic installs process-wide context proxies while a migration command is running.
MIGRATION_LOCK = Lock()


@contextmanager
def isolated_database(url: str) -> Iterator[Engine]:
    """Migrate a disposable schema without reading or changing campaign data in other schemas."""
    base = create_engine(url, hide_parameters=True)
    schema = f"smartdialer_lab_{uuid4().hex}"
    engine = None
    created = False
    try:
        with base.begin() as connection:
            version = int(connection.execute(text("SHOW server_version_num")).scalar_one())
            if not 160000 <= version < 170000:
                raise ValueError("SmartDialer verification requires PostgreSQL 16")
            connection.execute(text(f"CREATE SCHEMA {schema}"))
        created = True
        engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"}, hide_parameters=True)
        with MIGRATION_LOCK, engine.connect() as connection:
            config = Config(Path(__file__).resolve().parents[2] / "alembic.ini")
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        try:
            if created:
                with base.begin() as connection:
                    connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        finally:
            base.dispose()
