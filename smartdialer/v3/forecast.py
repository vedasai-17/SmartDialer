from dataclasses import dataclass
from datetime import datetime, timedelta
from math import floor
from statistics import fmean

from sqlalchemy import Connection, RowMapping, text

from .campaigns import AdmissionCapacity, Campaign, upcoming_wrapups
from .config import Mode
from .pacing import PacingInputs, Quantiles


@dataclass(frozen=True)
class Sample:
    yes: int
    total: int

    @property
    def rate(self) -> float:
        return self.yes / self.total if self.total else 0


@dataclass(frozen=True)
class Forecast:
    recent: Sample
    reference: Sample
    ewma: float | None
    latest_observation: datetime | None
    timing_samples: int
    setup: Quantiles | None
    ring: Quantiles | None
    answer_latency: Quantiles | None
    average_talk: float | None
    abandonment: Sample
    latest_abandonment: datetime | None


def quantiles(values: list[float]) -> Quantiles | None:
    if not values:
        return None
    ordered = sorted(values)

    def percentile(fraction: float) -> float:
        position = fraction * (len(ordered) - 1)
        index = floor(position)
        return ordered[index] + (position - index) * (ordered[min(index + 1, len(ordered) - 1)] - ordered[index])

    return percentile(0.01), percentile(0.50), percentile(0.95)


def read_forecast(connection: Connection, campaign: Campaign, now: datetime) -> Forecast:
    config = campaign.config.prediction
    long_start = now - timedelta(seconds=config.long_window_seconds)
    abandonment_start = now - timedelta(seconds=config.abandonment_window_seconds)
    rows = connection.execute(text("""
        SELECT c.state, c.state_rank, c.initiated_at, c.ringing_at, c.connected_at, c.ended_at, c.abandoned_at,
            coalesce(c.answered_at, (
                SELECT min(e.provider_timestamp) FROM call_events e
                WHERE e.call_id = c.id AND e.provider_name = c.provider AND e.event_type = 'ANSWERED'
                    AND e.processing_status IN ('APPLIED','STALE')
                    AND e.provider_timestamp BETWEEN c.created_at AND c.ended_at
            )) AS answer_at
        FROM calls c WHERE c.campaign_id = :campaign AND c.provider = :provider
            AND (c.answered_at > :since OR c.ended_at > :since OR c.abandoned_at > :since)
        ORDER BY c.created_at, c.borrower_id, c.attempt_number
    """), {"campaign": campaign.id, "provider": campaign.config.provider,
           "since": min(long_start, abandonment_start)}).mappings().all()
    return summarize(rows, campaign, now, long_start, abandonment_start)


def summarize(
    rows: list[RowMapping], campaign: Campaign, now: datetime, long_start: datetime, abandonment_start: datetime,
) -> Forecast:
    config = campaign.config.prediction
    recent_start = now - timedelta(seconds=config.short_window_seconds)
    observations = []
    setups, rings, answers, talks = [], [], [], []
    abandonment_samples = 0
    abandonment_times = []
    for row in rows:
        if row.answer_at is not None:
            at, answered = row.answer_at, True
        elif row.state in {"BUSY", "NO_ANSWER"}:
            at, answered = row.ended_at, False
        else:
            continue
        if answered and abandonment_start < at <= now:
            # Until a live answer is bridged or classified, keep it out of the denominator.
            if row.connected_at is not None or row.abandoned_at is not None or row.state_rank == 100:
                abandonment_samples += 1
            if row.abandoned_at is not None or (row.state_rank == 100 and row.connected_at is None):
                abandonment_times.append(at)
        if not long_start < at <= now:
            continue
        observations.append((at, int(answered)))
        if row.initiated_at is not None and row.ringing_at is not None and row.initiated_at <= row.ringing_at <= at:
            setups.append((row.ringing_at - row.initiated_at).total_seconds())
            rings.append((at - row.ringing_at).total_seconds())
            if answered:
                answers.append((at - row.initiated_at).total_seconds())
        if row.connected_at is not None and row.ended_at is not None and row.connected_at <= row.ended_at:
            talks.append((row.ended_at - row.connected_at).total_seconds())

    recent = [answered for at, answered in observations if at > recent_start]
    reference = [answered for at, answered in observations if at <= recent_start]
    observations.sort(key=lambda observation: observation[0])
    ewma = None
    for _, answered in observations:
        ewma = answered if ewma is None else config.ewma_alpha * answered + (1 - config.ewma_alpha) * ewma
    return Forecast(
        Sample(sum(recent), len(recent)), Sample(sum(reference), len(reference)), ewma,
        observations[-1][0] if observations else None, len(answers),
        quantiles(setups), quantiles(rings), quantiles(answers), fmean(talks) if talks else None,
        Sample(len(abandonment_times), abandonment_samples), max(abandonment_times, default=None),
    )


def pacing_inputs(
    connection: Connection, campaign: Campaign, now: datetime, capacity: AdmissionCapacity,
    batch_limit: int, health: str,
) -> PacingInputs:
    forecast = read_forecast(connection, campaign, now)
    horizon = forecast.answer_latency[1] if forecast.answer_latency is not None else None
    soon = len(upcoming_wrapups(connection, campaign, now, horizon)) if horizon is not None else 0
    return PacingInputs(
        capacity.free_agents, batch_limit, predictive=True, soon_free=soon, in_flight=capacity.in_flight,
        answer_ewma=forecast.ewma, recent_answers=forecast.recent.yes, recent_samples=forecast.recent.total,
        setup_seconds=forecast.setup, ring_seconds=forecast.ring, horizon_seconds=horizon,
        average_talk_seconds=forecast.average_talk, provider_health=health,
        fill_current=campaign.mode == Mode.HYBRID,
        hard_only=campaign.mode == Mode.HYBRID and not campaign.config.speculation_enabled,
    )
