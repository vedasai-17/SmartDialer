from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@dataclass
class VirtualClock:
    time: datetime

    def __post_init__(self) -> None:
        if self.time.utcoffset() is None:
            raise ValueError("the clock needs a timezone-aware start")
        self.time = self.time.astimezone(UTC)

    def now(self) -> datetime:
        return self.time

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("the clock cannot move backwards")
        self.time += timedelta(seconds=seconds)
