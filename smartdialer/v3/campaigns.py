from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import Connection, text

from .config import CampaignConfig, Mode

AVAILABLE_AGENTS = """
    campaign_id = :campaign_id AND state = 'AVAILABLE'
    AND last_heartbeat_at > :fresh_after AND last_heartbeat_at <= :now
    AND lease_expires_at > :now
"""


@dataclass(frozen=True)
class Campaign:
    id: int
    mode: Mode
    status: str
    max_concurrent_calls: int
    provider_rate_limit: int
    config: CampaignConfig


@dataclass(frozen=True)
class AdmissionCapacity:
    free_agents: int
    concurrent_slots: int
    provider_slots: int

    @property
    def limit(self) -> int:
        return min(self.free_agents, self.concurrent_slots, self.provider_slots)


def read_campaign(connection: Connection, campaign_id: int, *, lock: bool = False) -> Campaign | None:
    lock_clause = " FOR NO KEY UPDATE" if lock else ""
    row = connection.execute(
        text("""
            SELECT id, mode, status, max_concurrent_calls, provider_rate_limit, config
            FROM campaigns WHERE id = :id
        """ + lock_clause),
        {"id": campaign_id},
    ).mappings().one_or_none()
    if row is None:
        return None
    return Campaign(
        id=row.id, mode=Mode(row.mode), status=row.status, max_concurrent_calls=row.max_concurrent_calls,
        provider_rate_limit=row.provider_rate_limit, config=CampaignConfig.model_validate(row.config),
    )


def availability_params(campaign: Campaign, now: datetime) -> dict[str, int | datetime]:
    return {
        "campaign_id": campaign.id,
        "now": now,
        "fresh_after": now - timedelta(seconds=campaign.config.heartbeat_ttl_seconds),
    }


def admission_capacity(connection: Connection, campaign: Campaign, now: datetime) -> AdmissionCapacity:
    row = connection.execute(
        text(f"""
            SELECT (SELECT count(*) FROM agents WHERE {AVAILABLE_AGENTS}) AS free_agents,
                   (SELECT count(*) FROM calls WHERE campaign_id = :campaign_id AND state_rank < 100) AS active,
                   (SELECT count(*) FROM calls WHERE campaign_id = :campaign_id AND provider = :provider
                        AND created_at > :rate_window_start) AS recent
        """),
        {
            **availability_params(campaign, now),
            "provider": campaign.config.provider,
            "rate_window_start": now - timedelta(seconds=1),
        },
    ).one()
    return AdmissionCapacity(
        row.free_agents,
        max(0, campaign.max_concurrent_calls - row.active),
        max(0, campaign.provider_rate_limit - row.recent),
    )
