from dataclasses import dataclass
from math import floor


@dataclass(frozen=True)
class Snapshot:
    free: int
    ringing: int = 0
    covered_ringing: int = 0
    near_releases: int = 0
    samples: int = 0
    answers: int = 0
    horizon: float = 5
    awaiting_bridge: int = 0
    covered_awaiting_bridge: int = 0


@dataclass(frozen=True)
class Proposal:
    requested: int
    strategy: str
    snapshot: Snapshot
    answer_rate: float | None = None
    expected_ringing_answers: float | None = None
    effective_answer_rate: float | None = None
    expected_answer_room: float | None = None


def propose(mode, snapshot, batch_limit=50):
    if mode == 'progressive' or snapshot.samples < 10:
        strategy = 'progressive' if mode == 'progressive' else 'progressive_fallback'
        return Proposal(min(snapshot.free, batch_limit), strategy, snapshot)

    rate = snapshot.answers / snapshot.samples
    bounded_rate = max(0.10, rate)
    expected_ringing = snapshot.ringing * bounded_rate
    capacity = snapshot.free + snapshot.near_releases + snapshot.covered_ringing + snapshot.covered_awaiting_bridge
    room = max(0, capacity - expected_ringing - snapshot.awaiting_bridge)
    requested = min(batch_limit, floor(room / bounded_rate)) if snapshot.free or snapshot.near_releases else 0
    return Proposal(requested, 'predictive', snapshot, rate, round(expected_ringing, 3), bounded_rate, round(room, 3))
