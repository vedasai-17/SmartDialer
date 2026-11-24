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
    max_speculative_calls: int = 0
    risk_budget: float = 0


@dataclass(frozen=True)
class AdmissionCapacity:
    free_agents: int
    concurrent_slots: int
    provider_slots: int
    staffed_agents: int = 0
    active_calls: int = 0
    active_speculative: int = 0
    in_flight: int = 0
    speculative_in_flight: int = 0
    waiting_speculative_answers: int = 0

    @property
    def limit(self) -> int:
        return min(self.free_agents, self.concurrent_slots, self.provider_slots)


def read_campaign(connection: Connection, campaign_id: int, *, lock: bool = False) -> Campaign | None:
    lock_clause = " FOR NO KEY UPDATE" if lock else ""
    row = connection.execute(
        text("""
            SELECT id, mode, status, max_concurrent_calls, provider_rate_limit, config, max_speculative_calls, risk_budget
            FROM campaigns WHERE id = :id
        """ + lock_clause),
        {"id": campaign_id},
    ).mappings().one_or_none()
    if row is None:
        return None
    return Campaign(
        id=row.id, mode=Mode(row.mode), status=row.status, max_concurrent_calls=row.max_concurrent_calls,
        provider_rate_limit=row.provider_rate_limit, config=CampaignConfig.model_validate(row.config),
        max_speculative_calls=row.max_speculative_calls, risk_budget=row.risk_budget,
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
            WITH presence AS (
                SELECT count(*) FILTER (WHERE {AVAILABLE_AGENTS}) AS free_agents, count(*) AS staffed_agents
                FROM agents WHERE campaign_id = :campaign_id AND last_heartbeat_at > :fresh_after
                    AND last_heartbeat_at <= :now AND lease_expires_at > :now
                    AND state NOT IN ('OFFLINE', 'PAUSED', 'RECOVERY_HOLD')
            ), active AS (
                SELECT count(*) AS active,
                    count(*) FILTER (WHERE is_speculative) AS active_speculative,
                    count(*) FILTER (WHERE state_rank BETWEEN 30 AND 40) AS in_flight,
                    count(*) FILTER (WHERE is_speculative AND agent_id IS NULL AND state_rank < 50) AS speculative_in_flight,
                    count(*) FILTER (WHERE is_speculative AND agent_id IS NULL AND state_rank = 50) AS waiting_answers
                FROM calls WHERE campaign_id = :campaign_id AND state_rank < 100
            )
            SELECT presence.*, active.*,
                (SELECT count(*) FROM calls WHERE campaign_id = :campaign_id AND provider = :provider
                    AND created_at > :rate_window_start) AS recent
            FROM presence CROSS JOIN active
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
        row.staffed_agents, row.active, row.active_speculative, row.in_flight,
        row.speculative_in_flight, row.waiting_answers,
    )


def upcoming_wrapups(connection: Connection, campaign: Campaign, now: datetime, horizon: float) -> list[datetime]:
    return list(connection.execute(text("""
        SELECT wrap_up_ends_at FROM agents WHERE campaign_id = :campaign_id AND state = 'WRAP_UP'
            AND wrap_up_ends_at BETWEEN :now AND :horizon AND lease_expires_at > wrap_up_ends_at
            AND last_heartbeat_at > :fresh_after AND last_heartbeat_at <= :now
        ORDER BY wrap_up_ends_at, id
    """), {**availability_params(campaign, now), "horizon": now + timedelta(seconds=horizon)}).scalars())
