import json
import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import Connection, Engine, create_engine, text

from .config import Settings

logger = logging.getLogger("smartdialer.v3")


def database(settings: Settings) -> Engine:
    return create_engine(settings.database_url, pool_pre_ping=True, hide_parameters=True)


def log(event: str, **fields: object) -> None:
    logger.info(json.dumps({"event": event, **fields}, default=str))


def audit(
    connection: Connection,
    now: datetime,
    entity_type: str,
    entity_id: int | UUID,
    action: str,
    *,
    campaign_id: int | None = None,
    **details: object,
) -> None:
    connection.execute(
        text("""
            INSERT INTO audit_log (occurred_at, campaign_id, entity_type, entity_id, action, details)
            VALUES (:now, :campaign_id, :entity_type, :entity_id, :action, CAST(:details AS jsonb))
        """),
        dict(
            now=now, campaign_id=campaign_id, entity_type=entity_type, entity_id=str(entity_id),
            action=action, details=json.dumps(details, default=str),
        ),
    )
