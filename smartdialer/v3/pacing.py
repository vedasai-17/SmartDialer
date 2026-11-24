from dataclasses import dataclass
from math import floor, isfinite, sqrt

Quantiles = tuple[float, float, float]


@dataclass(frozen=True)
class PacingInputs:
    free_agents: int
    batch_limit: int
    predictive: bool = False
    soon_free: int = 0
    in_flight: int = 0
    answer_ewma: float | None = None
    recent_answers: int = 0
    recent_samples: int = 0
    setup_seconds: Quantiles | None = None
    ring_seconds: Quantiles | None = None
    horizon_seconds: float | None = None
    average_talk_seconds: float | None = None
    provider_health: str = "CLOSED"
    fill_current: bool = False
    hard_only: bool = False


@dataclass(frozen=True)
class PacingProposal:
    requested_calls: int
    inputs: PacingInputs
    reasoning: str


def wilson_upper(successes: int, samples: int, z: float = 1.96) -> float:
    if samples <= 0 or not 0 <= successes <= samples:
        raise ValueError("a Wilson bound needs a valid nonempty sample")
    rate = successes / samples
    squared = z * z
    margin = z * sqrt((rate * (1 - rate) + squared / (4 * samples)) / samples)
    return min(1.0, (rate + squared / (2 * samples) + margin) / (1 + squared / samples))


def propose(inputs: PacingInputs) -> PacingProposal:
    requested = max(0, min(inputs.free_agents, inputs.batch_limit))
    progressive_reason = f"free={inputs.free_agents}, batch_limit={inputs.batch_limit}, request={requested}"
    if inputs.provider_health == "OPEN":
        return PacingProposal(0, inputs, f"free={inputs.free_agents}, provider breaker OPEN, request=0")
    if not inputs.predictive:
        return PacingProposal(requested, inputs, progressive_reason)
    if (inputs.answer_ewma is None or not isfinite(inputs.answer_ewma) or not 0 <= inputs.answer_ewma <= 1
            or inputs.recent_samples <= 0 or not 0 <= inputs.recent_answers <= inputs.recent_samples
            or inputs.horizon_seconds is None or not isfinite(inputs.horizon_seconds) or inputs.horizon_seconds < 0
            or inputs.provider_health != "CLOSED"):
        return PacingProposal(requested, inputs, f"{progressive_reason}, progressive proposal while forecasts or provider are degraded")
    upper = wilson_upper(inputs.recent_answers, inputs.recent_samples)
    probability = max(inputs.answer_ewma, upper, 1e-6)
    expected = probability * max(0, inputs.in_flight)
    capacity = max(0, inputs.free_agents) + max(0, inputs.soon_free)
    need = max(0.0, capacity - expected)
    raw = floor(need / probability + 1e-12)
    if not inputs.hard_only:
        desired = max(inputs.free_agents, raw) if inputs.fill_current else raw
        requested = max(0, min(inputs.batch_limit, desired))
    return PacingProposal(
        requested, inputs,
        f"free={inputs.free_agents}, soon_free={inputs.soon_free} (forecast within {inputs.horizon_seconds:.3g}s), "
        f"in_flight={inputs.in_flight}, p_ewma={inputs.answer_ewma:.3f}, p_upper={upper:.3f}, "
        f"expected_inflight_answers={expected:.2f}, need={need:.2f}, formula_request={raw}, "
        f"batch_limit={inputs.batch_limit}, hard_only={inputs.hard_only}, request={requested}",
    )
