from datetime import datetime
from uuid import UUID

from sqlalchemy import Connection, Engine, text

from .allocator import Allocator, ApprovalRejected
from .campaigns import admission_capacity, read_campaign
from .clock import Clock
from .config import Mode
from .db import log
from .forecast import pacing_inputs
from .health import provider_breaker
from .pacing import PacingInputs, propose
from .reaper import Reaper
from .safety import SafetyController


class Dialer:
    def __init__(self, engine: Engine, clock: Clock) -> None:
        self.engine = engine
        self.clock = clock
        self.safety = SafetyController(engine, clock)
        self.allocator = Allocator(engine, clock)
        self.reaper = Reaper(engine, clock)

    def tick(self, campaign_id: int, batch_limit: int) -> list[UUID]:
        inputs = PacingInputs(0, batch_limit)
        try:
            with self.engine.connect() as connection:
                campaign = read_campaign(connection, campaign_id)
                staged_at = self._staged_ready_at(connection, campaign_id) if campaign is not None and campaign.mode == Mode.HYBRID else None
            if staged_at is not None and staged_at <= self.clock.now():
                self.reaper.tick(campaign_id=campaign_id, wrapups_only=True)
            if campaign is not None:
                with self.engine.connect() as connection:
                    now = self.clock.now()
                    capacity = admission_capacity(connection, campaign, now)
                    health = provider_breaker(connection, campaign, now).state
                    if campaign.mode == Mode.STRICT_PROGRESSIVE:
                        inputs = PacingInputs(capacity.free_agents, batch_limit, provider_health=health)
                    else:
                        inputs = pacing_inputs(connection, campaign, now, capacity, batch_limit, health)
        except Exception as error:
            log("pacing_input_unavailable", campaign_id=campaign_id, error_type=type(error).__name__)
        proposal = propose(inputs)
        decision = self.safety.decide(campaign_id, proposal)
        if decision.id is None or decision.approved_calls == 0:
            return []
        try:
            return self.allocator.allocate(decision.id)
        except ApprovalRejected as error:
            log("allocation_skipped", approval_id=decision.id, reason=str(error))
            return []

    def delay(self, campaign_id: int, interval: float) -> float:
        """Wake at a forecast wrap-up deadline when it is sooner than the regular tick."""
        try:
            with self.engine.connect() as connection:
                staged_at = self._staged_ready_at(connection, campaign_id)
        except Exception as error:
            log("pacing_schedule_unavailable", campaign_id=campaign_id, error_type=type(error).__name__)
            return interval
        remaining = (staged_at - self.clock.now()).total_seconds() if staged_at is not None else interval
        return max(0.0, min(interval, remaining))

    @staticmethod
    def _staged_ready_at(connection: Connection, campaign_id: int) -> datetime | None:
        ready_at = connection.execute(text("""
            SELECT CASE WHEN d.effective_mode = 'HYBRID' AND c.mode = 'HYBRID' AND c.status = 'ACTIVE'
                AND jsonb_array_length(coalesce(d.safety_inputs->'prestaged_borrower_ids', '[]'::jsonb)) > 0
                THEN d.safety_inputs->>'prestage_next_ready_at' END
            FROM safety_decisions d JOIN campaigns c ON c.id = d.campaign_id WHERE d.campaign_id = :campaign
            ORDER BY d.decided_at DESC, d.id DESC LIMIT 1
        """), {"campaign": campaign_id}).scalar_one_or_none()
        return datetime.fromisoformat(ready_at) if ready_at is not None else None
