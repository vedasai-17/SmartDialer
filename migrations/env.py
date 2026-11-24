from alembic import context
from sqlalchemy import Connection

from smartdialer.v3.config import Settings
from smartdialer.v3.db import database


def run_migrations() -> None:
    settings = Settings()
    if context.is_offline_mode():
        context.configure(url=settings.database_url, literal_binds=True)
        with context.begin_transaction():
            context.run_migrations()
    else:
        supplied_connection = context.config.attributes.get("connection")
        if supplied_connection is not None:
            migrate(supplied_connection)
            return
        engine = database(settings)
        try:
            with engine.connect() as connection:
                migrate(connection)
        finally:
            engine.dispose()


def migrate(connection: Connection) -> None:
    context.configure(connection=connection, transaction_per_migration=True)
    with context.begin_transaction():
        context.run_migrations()


run_migrations()
