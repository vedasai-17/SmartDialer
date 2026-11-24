from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from math import floor, isfinite

from scipy.stats import binom
from sqlalchemy import Connection, RowMapping, text

from .campaigns import AdmissionCapacity, Campaign, upcoming_wrapups
from .config import ExcessAnswerPolicy, Mode
from .forecast import Forecast, read_forecast
from .health import BreakerState, provider_breaker
from .pacing import wilson_upper

SPECULATION_CANCEL_REQUESTED = """
    c.is_speculative AND c.agent_id IS NULL AND EXISTS (
        SELECT 1 FROM safety_decisions d WHERE d.campaign_id = c.campaign_id
            AND (d.safety_inputs->>'cancel_speculation_through')::timestamptz >= c.created_at
    )
"""


@dataclass(frozen=True)
class Limits:
    hard: int
    speculative: int
    mode: Mode
    reasons: tuple[str, ...]
    evidence: dict[str, object]

    @property
    def total(self) -> int:
        return self.hard + self.speculative


def overflow_probability(existing: int, new: int, capacity: int, probability: float) -> float:
    if capacity < 0:
        return 1.0
    result = float(binom.sf(capacity, existing + new, probability))
    if not isfinite(result):
        raise ValueError("non-finite binomial overflow probability")
    return result


def risk_limited_count(existing: int, capacity: int, probability: float, budget: float, limit: int) -> int:
    if overflow_probability(existing, 0, capacity, probability) > budget:
        return 0
    low, high = 0, limit
    while low < high:
        candidate = (low + high + 1) // 2
        if overflow_probability(existing, candidate, capacity, probability) <= budget:
            low = candidate
        else:
            high = candidate - 1
    return low


def seats_for_existing(exposure: int, probability: float, budget: float) -> int:
    low, high = 0, exposure
    while low < high:
        seats = (low + high) // 2
        if overflow_probability(exposure, 0, seats, probability) > budget:
            low = seats + 1
        else:
            high = seats
    return low


def evaluate_limits(
    connection: Connection, campaign: Campaign, capacity: AdmissionCapacity, requested: int, now: datetime,
) -> Limits:
    evidence: dict[str, object] = {**asdict(capacity), "provider": campaign.config.provider}
    reasons = []
    maximum = min(requested, capacity.concurrent_slots, capacity.provider_slots)
    hard = min(maximum, max(0, capacity.free_agents - capacity.waiting_speculative_answers))
    for reason, limit in (("CAMPAIGN_CONCURRENCY", capacity.concurrent_slots),
                          ("PROVIDER_RATE_LIMIT", capacity.provider_slots)):
        if limit < requested:
            reasons.append(reason)
    if requested == 0:
        reasons.append("NO_REQUEST")
    if campaign.status != "ACTIVE":
        reasons.append("CAMPAIGN_INACTIVE")

    previous = _previous_inputs(connection, campaign, now)
    if "abandonment_guard" in previous:
        evidence["abandonment_guard"] = previous["abandonment_guard"]
    dropped = _agent_drop(connection, campaign, capacity, now, previous, evidence)
    breaker = provider_breaker(connection, campaign, now)
    evidence["breaker"] = breaker.evidence()
    if dropped:
        reasons.append("SUDDEN_AGENT_DROP")
    if breaker.state == BreakerState.OPEN:
        reasons.append("PROVIDER_BREAKER_OPEN")
    if campaign.status != "ACTIVE" or dropped or breaker.state == BreakerState.OPEN:
        return Limits(0, 0, Mode.STRICT_PROGRESSIVE, tuple(reasons), evidence)
    if breaker.state == BreakerState.HALF_OPEN:
        return Limits(min(hard, breaker.probe_slots), 0, Mode.STRICT_PROGRESSIVE,
                      tuple([*reasons, "PROVIDER_BREAKER_HALF_OPEN"]), evidence)
    if capacity.free_agents < requested:
        reasons.append("AVAILABLE_AGENTS")
    if campaign.mode == Mode.STRICT_PROGRESSIVE:
        return Limits(hard, 0, campaign.mode, tuple(reasons), evidence)

    forecast = read_forecast(connection, campaign, now)
    probability, degraded = _forecast_quality(campaign, forecast, now, evidence, reasons)
    guard_latched = _abandonment_guard(campaign, forecast, previous, now, evidence, reasons)
    if degraded or guard_latched:
        return Limits(hard, 0, Mode.STRICT_PROGRESSIVE, tuple([*reasons, "PROGRESSIVE_FALLBACK"]), evidence)

    horizon = forecast.answer_latency[0]
    ready_times = upcoming_wrapups(connection, campaign, now, horizon)
    evidence["risk_forecast_seconds"] = horizon
    evidence["risk_soon_free"] = len(ready_times)
    if campaign.mode == Mode.HYBRID:
        _prestage(connection, campaign, now, forecast.answer_latency[1], evidence)
    if not campaign.config.speculation_enabled:
        mode = campaign.mode if campaign.mode == Mode.HYBRID else Mode.STRICT_PROGRESSIVE
        return Limits(hard, 0, mode, tuple([*reasons, "SPECULATION_DISABLED"]), evidence)

    existing_seats = seats_for_existing(capacity.speculative_in_flight, probability, campaign.risk_budget)
    spare = max(0, existing_seats + capacity.waiting_speculative_answers - len(ready_times))
    if hard > capacity.free_agents - spare:
        hard = max(0, capacity.free_agents - spare)
        reasons.append("EXISTING_SPECULATIVE_EXPOSURE")
    evidence["seats_for_existing_speculation"] = existing_seats
    cap_by_multiplier = floor(capacity.free_agents * campaign.config.prediction.max_overdial_multiplier)
    cap_by_speculation = max(0, campaign.max_speculative_calls - capacity.active_speculative)
    if cap_by_multiplier < maximum:
        reasons.append("MAX_OVERDIAL_MULTIPLIER")
    room = max(0, min(maximum, cap_by_multiplier) - hard)
    if cap_by_speculation < room:
        reasons.append("MAX_SPECULATIVE_CALLS")
    candidate = min(room, cap_by_speculation)
    overflow_capacity = capacity.free_agents - hard + len(ready_times) - capacity.waiting_speculative_answers
    speculative = 0
    if candidate > 0 and campaign.risk_budget > 0:
        speculative = risk_limited_count(capacity.speculative_in_flight, overflow_capacity, probability, campaign.risk_budget, candidate)
    if speculative < candidate:
        reasons.append("OVERFLOW_RISK_BUDGET")
    evidence.update({
        "overflow_capacity": overflow_capacity, "risk_budget": campaign.risk_budget,
        "risk_if_requested": overflow_probability(capacity.speculative_in_flight, candidate, overflow_capacity, probability),
        "risk_if_approved": overflow_probability(capacity.speculative_in_flight, speculative, overflow_capacity, probability),
    })
    return Limits(hard, speculative, campaign.mode, tuple(reasons), evidence)


def _previous_inputs(connection: Connection, campaign: Campaign, now: datetime) -> dict[str, object]:
    return connection.execute(text("""
        SELECT safety_inputs FROM safety_decisions WHERE campaign_id = :id AND decided_at <= :now
            AND safety_inputs ? 'staffed_agents' ORDER BY decided_at DESC, id DESC LIMIT 1
    """), {"id": campaign.id, "now": now}).scalar_one_or_none() or {}


def _agent_drop(
    connection: Connection, campaign: Campaign, capacity: AdmissionCapacity, now: datetime,
    previous: dict[str, object], evidence: dict[str, object],
) -> bool:
    config = campaign.config.prediction
    baseline = connection.execute(text("""
        SELECT max(least(
            greatest(0, (safety_inputs->>'free_agents')::int - :free),
            greatest(0, (safety_inputs->>'staffed_agents')::int - :staffed)
        )::double precision / nullif((safety_inputs->>'free_agents')::int, 0))
        FROM safety_decisions WHERE campaign_id = :id AND decided_at > :since AND decided_at <= :now
            AND safety_inputs ? 'staffed_agents'
    """), {"free": capacity.free_agents, "staffed": capacity.staffed_agents, "id": campaign.id,
           "since": now - timedelta(seconds=config.agent_drop_window_seconds), "now": now}).scalar_one()
    fraction = baseline or 0.0
    drop_until = previous.get("agent_drop_until")
    cancel_through = previous.get("cancel_speculation_through")
    if fraction > config.agent_drop_fraction:
        if drop_until is None or now >= datetime.fromisoformat(drop_until):
            drop_until = (now + timedelta(seconds=config.agent_drop_window_seconds)).isoformat()
            cancel_through = now.isoformat()
    evidence.update({"availability_drop_fraction": fraction, "agent_drop_until": drop_until,
                     "cancel_speculation_through": cancel_through})
    return drop_until is not None and now < datetime.fromisoformat(drop_until)


def _forecast_quality(
    campaign: Campaign, forecast: Forecast, now: datetime, evidence: dict[str, object], reasons: list[str],
) -> tuple[float, bool]:
    config = campaign.config.prediction
    evidence.update({"recent_answer_sample": asdict(forecast.recent), "reference_answer_sample": asdict(forecast.reference),
                     "answer_ewma": forecast.ewma, "timing_samples": forecast.timing_samples,
                     "setup_quantiles": forecast.setup, "ring_quantiles": forecast.ring,
                     "answer_latency_quantiles": forecast.answer_latency, "average_talk_seconds": forecast.average_talk})
    stale = forecast.latest_observation is None or now - forecast.latest_observation > timedelta(seconds=config.max_observation_age_seconds)
    if stale:
        reasons.append("PREDICTION_STALE_OR_MISSING")
    insufficient = (forecast.recent.total < config.min_recent_samples or forecast.reference.total < config.min_answer_samples
                    or forecast.timing_samples < config.min_recent_samples or forecast.average_talk is None)
    if insufficient:
        reasons.append("LOW_PREDICTION_CONFIDENCE")
    recent_upper = wilson_upper(forecast.recent.yes, forecast.recent.total) if forecast.recent.total else 1.0
    reference_upper = wilson_upper(forecast.reference.yes, forecast.reference.total) if forecast.reference.total else 1.0
    shift = forecast.recent.rate - forecast.reference.rate
    probability = max(recent_upper, reference_upper, forecast.ewma or 0.0, 1e-6)
    shifted = not insufficient and abs(shift) >= config.rate_shift_warning
    if shifted:
        reasons.append("ANSWER_RATE_SHIFT_UP" if shift > 0 else "ANSWER_RATE_SHIFT_DOWN")
        probability = min(1.0, probability + max(0.0, shift))
    evidence.update({"answer_rate_shift": shift, "p_safe": probability, "recent_answer_upper": recent_upper,
                     "reference_answer_upper": reference_upper})
    return probability, stale or insufficient or (shifted and abs(shift) >= config.rate_shift_fallback)


def _abandonment_guard(
    campaign: Campaign, forecast: Forecast, previous: dict[str, object], now: datetime,
    evidence: dict[str, object], reasons: list[str],
) -> bool:
    config = campaign.config.prediction
    sample = forecast.abandonment
    upper = wilson_upper(sample.yes, sample.total) if sample.total else None
    enough = sample.total >= config.min_abandonment_samples
    prior = previous.get("abandonment_guard") or {}
    cooldown = datetime.fromisoformat(prior["cooldown_until"]) if prior.get("cooldown_until") else None
    seen = datetime.fromisoformat(prior["latest_policy_abandonment"]) if prior.get("latest_policy_abandonment") else None
    latest = forecast.latest_abandonment
    policy_fallback = (campaign.config.excess_answer_policy == ExcessAnswerPolicy.PROGRESSIVE_FALLBACK
                       and latest is not None and (seen is None or latest > seen))
    if policy_fallback:
        seen = latest
        reasons.append("EXCESS_ANSWER_PROGRESSIVE_FALLBACK")
    breached = enough and upper > config.abandonment_threshold
    if policy_fallback or (breached and cooldown is None):
        fresh_deadline = now + timedelta(seconds=config.abandonment_cooldown_seconds)
        cooldown = max(cooldown, fresh_deadline) if cooldown is not None else fresh_deadline
    if not enough:
        reasons.append("ABANDONMENT_SAMPLE_TOO_SMALL")
    if breached:
        reasons.append("ABANDONMENT_UPPER_BOUND")
    if cooldown is not None and now >= cooldown and enough and upper <= config.abandonment_resume_threshold:
        cooldown = None
    if cooldown is not None:
        reasons.append("ABANDONMENT_COOLDOWN_OR_HYSTERESIS")
    evidence["abandonment_guard"] = {"answered": sample.total, "abandoned": sample.yes, "upper": upper,
                                     "cooldown_until": cooldown.isoformat() if cooldown else None,
                                     "latest_policy_abandonment": seen.isoformat() if seen else None}
    return cooldown is not None or (campaign.config.speculation_enabled and not enough)


def _prestage(
    connection: Connection, campaign: Campaign, now: datetime, horizon: float, evidence: dict[str, object],
) -> None:
    ready = upcoming_wrapups(connection, campaign, now, horizon)
    if not ready:
        return
    evidence["prestage_next_ready_at"] = ready[0].isoformat()
    evidence["prestaged_borrower_ids"] = list(connection.execute(text("""
        SELECT b.id FROM borrowers b WHERE b.campaign_id = :id AND b.state IN ('QUEUED','RETRY_SCHEDULED')
            AND b.attempts < b.max_attempts AND b.next_attempt_at <= :horizon
            AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.borrower_id = b.id AND c.state_rank < 100)
        ORDER BY b.priority DESC, b.next_attempt_at, b.queued_at, b.attempts, b.id LIMIT :limit
    """), {"id": campaign.id, "horizon": ready[-1], "limit": min(len(ready), 50)}).scalars())


def cancel_requested(connection: Connection, call: RowMapping) -> bool:
    return call.is_speculative and call.agent_id is None and bool(connection.execute(text(f"""
        SELECT 1 FROM calls c WHERE c.id = :id AND ({SPECULATION_CANCEL_REQUESTED})
    """), {"id": call.id}).scalar_one_or_none())


def speculative_start_allowed(connection: Connection, call: RowMapping, campaign: Campaign, now: datetime) -> bool:
    if not call.is_speculative:
        return True
    if campaign.mode == Mode.STRICT_PROGRESSIVE or not campaign.config.speculation_enabled or cancel_requested(connection, call):
        return False
    return bool(connection.execute(text("""
        SELECT issued.expires_at > :now AND issued.effective_mode = :mode AND latest.effective_mode = :mode
            AND (NOT :stop_on_excess OR NOT EXISTS (
                SELECT 1 FROM calls c WHERE c.campaign_id = :campaign AND c.provider = :provider
                    AND c.abandoned_at BETWEEN issued.decided_at AND :now
            ))
        FROM safety_decisions issued CROSS JOIN LATERAL (
            SELECT effective_mode FROM safety_decisions WHERE campaign_id = :campaign AND decided_at <= :now
            ORDER BY decided_at DESC, id DESC LIMIT 1
        ) latest WHERE issued.id = :issued
    """), {"issued": call.safety_decision_id, "campaign": campaign.id, "now": now,
           "mode": campaign.mode, "provider": campaign.config.provider,
           "stop_on_excess": campaign.config.excess_answer_policy == ExcessAnswerPolicy.PROGRESSIVE_FALLBACK}).scalar_one_or_none())
