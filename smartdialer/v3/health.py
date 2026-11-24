import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import Connection, RowMapping, text

from .campaigns import Campaign


class BreakerState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass(frozen=True)
class Breaker:
    state: BreakerState
    since: datetime
    probe_slots: int = 0
    operations: int = 0
    errors: int = 0
    timeouts: int = 0
    setup_samples: int = 0
    slow_setups: int = 0

    def evidence(self) -> dict[str, object]:
        return {**asdict(self), "since": self.since.isoformat()}


def record_operation(
    connection: Connection, call: RowMapping, now: datetime, token: int, operation: str, result: str,
) -> None:
    connection.execute(text("""
        INSERT INTO call_events (call_id, provider_name, provider_event_id, event_type, received_at,
            processed_at, payload, processing_status)
        VALUES (:call, :namespace, :key, 'PROVIDER_OPERATION', :now, :now, CAST(:details AS jsonb), 'APPLIED')
        ON CONFLICT (provider_name, provider_event_id) DO NOTHING
    """), {"call": call.id, "namespace": f"dispatch:{call.provider}", "key": f"{call.id}:{token}:{operation}",
           "now": now, "details": json.dumps({"operation": operation, "result": result})})


def provider_breaker(connection: Connection, campaign: Campaign, now: datetime) -> Breaker:
    config = campaign.config.breaker
    start = now - timedelta(seconds=config.window_seconds)
    previous = connection.execute(text("""
        SELECT safety_inputs->'breaker' FROM safety_decisions
        WHERE campaign_id = :campaign AND decided_at <= :now AND safety_inputs->>'provider' = :provider
            AND safety_inputs ? 'breaker' ORDER BY decided_at DESC, id DESC LIMIT 1
    """), {"campaign": campaign.id, "provider": campaign.config.provider, "now": now}).scalar_one_or_none()
    state = BreakerState(previous["state"]) if previous is not None else BreakerState.CLOSED
    since = datetime.fromisoformat(previous["since"]) if previous is not None else start
    if state == BreakerState.OPEN:
        ready = since + timedelta(seconds=config.cooldown_seconds)
        if now < ready:
            return Breaker(state, since)
        return Breaker(BreakerState.HALF_OPEN, ready, config.probe_calls)
    if state == BreakerState.HALF_OPEN:
        return _probe_health(connection, campaign, now, since)

    # New successful probes start a fresh evidence window. Old outage events cannot reopen the breaker.
    stats = _recent_health(connection, campaign, now, max(start, since))
    if ((stats.operations >= config.min_samples and stats.errors / stats.operations >= config.error_fraction)
            or (stats.setup_samples >= config.min_samples and stats.slow_setups / stats.setup_samples >= config.slow_fraction)):
        return Breaker(BreakerState.OPEN, now, operations=stats.operations, errors=stats.errors,
                       timeouts=stats.timeouts, setup_samples=stats.setup_samples, slow_setups=stats.slow_setups)
    return Breaker(BreakerState.CLOSED, since, operations=stats.operations, errors=stats.errors,
                   timeouts=stats.timeouts, setup_samples=stats.setup_samples, slow_setups=stats.slow_setups)


def _recent_health(connection: Connection, campaign: Campaign, now: datetime, start: datetime) -> RowMapping:
    return connection.execute(text("""
        WITH operations AS (
            SELECT count(*) AS operations,
                count(*) FILTER (WHERE e.payload->>'result' <> 'OK' OR (e.payload->>'operation' = 'initiate'
                    AND c.state = 'FAILED' AND c.ended_at <= :now)) AS errors,
                count(*) FILTER (WHERE e.payload->>'result' = 'TIMEOUT') AS timeouts
            FROM call_events e JOIN calls c ON c.id = e.call_id
            WHERE e.provider_name = :namespace AND e.event_type = 'PROVIDER_OPERATION'
                AND e.received_at > :start AND e.received_at <= :now AND c.campaign_id = :campaign
        ), setups AS (
            SELECT count(*) AS setup_samples, count(*) FILTER (WHERE
                extract(epoch FROM (coalesce(ringing_at, :now) - initiated_at)) > :max_setup
            ) AS slow_setups FROM calls WHERE campaign_id = :campaign AND provider = :provider
                AND initiated_at > :start AND initiated_at <= :now
                AND (ringing_at IS NOT NULL OR (state_rank BETWEEN 30 AND 40
                    AND extract(epoch FROM (:now - initiated_at)) > :max_setup))
        ) SELECT operations.*, setups.* FROM operations CROSS JOIN setups
    """), {"namespace": f"dispatch:{campaign.config.provider}", "campaign": campaign.id,
           "provider": campaign.config.provider, "start": start, "now": now,
           "max_setup": campaign.config.breaker.max_setup_seconds}).mappings().one()


def _probe_health(connection: Connection, campaign: Campaign, now: datetime, since: datetime) -> Breaker:
    config = campaign.config.breaker
    counts = connection.execute(text("""
        WITH probes AS (
            SELECT c.*, EXISTS (
                SELECT 1 FROM call_events e WHERE e.call_id = c.id AND e.provider_name = :namespace
                    AND e.event_type = 'PROVIDER_OPERATION' AND e.payload->>'result' <> 'OK'
            ) AS operation_error
            FROM calls c JOIN safety_decisions d ON d.id = c.safety_decision_id
            WHERE c.campaign_id = :campaign AND c.provider = :provider AND c.created_at <= :now
                AND d.safety_inputs->'breaker'->>'state' = 'HALF_OPEN'
                AND d.safety_inputs->'breaker'->>'since' = :since
                AND (c.state_rank < 100 OR c.dispatch_attempts > 0)
        )
        SELECT count(*) AS probes, count(*) FILTER (WHERE operation_error OR state = 'FAILED'
                   OR (initiated_at IS NOT NULL AND extract(epoch FROM
                        (coalesce(ringing_at, ended_at, :now) - initiated_at)) > :max_setup)
                ) AS failed,
            count(*) FILTER (WHERE ringing_at IS NOT NULL OR answered_at IS NOT NULL
                OR state IN ('BUSY','NO_ANSWER')) AS healthy
        FROM probes
    """), {"campaign": campaign.id, "provider": campaign.config.provider, "since": since.isoformat(),
           "namespace": f"dispatch:{campaign.config.provider}", "now": now,
           "max_setup": config.max_setup_seconds}).mappings().one()
    if counts.failed:
        return Breaker(BreakerState.OPEN, now, operations=counts.probes, errors=counts.failed)
    if counts.healthy >= config.probe_calls:
        return Breaker(BreakerState.CLOSED, now, operations=counts.probes)
    return Breaker(BreakerState.HALF_OPEN, since, max(0, config.probe_calls - counts.probes))


def start_permitted(connection: Connection, call: RowMapping, campaign: Campaign, now: datetime) -> bool:
    breaker = provider_breaker(connection, campaign, now)
    if breaker.state == BreakerState.CLOSED:
        return True
    if breaker.state == BreakerState.OPEN:
        return False
    return bool(connection.execute(text("""
        SELECT 1 FROM safety_decisions WHERE id = :id AND safety_inputs->'breaker'->>'state' = 'HALF_OPEN'
            AND safety_inputs->'breaker'->>'since' = :since
    """), {"id": call.safety_decision_id, "since": breaker.since.isoformat()}).scalar_one_or_none())
