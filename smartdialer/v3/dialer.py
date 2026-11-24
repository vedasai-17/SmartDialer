from uuid import UUID

from sqlalchemy import Engine

from .allocator import Allocator, ApprovalRejected
from .campaigns import admission_capacity, read_campaign
from .clock import Clock
from .db import log
from .pacing import PacingInputs, propose
from .safety import SafetyController


class Dialer:
    def __init__(self, engine: Engine, clock: Clock) -> None:
        self.engine = engine
        self.clock = clock
        self.safety = SafetyController(engine, clock)
        self.allocator = Allocator(engine, clock)

    def tick(self, campaign_id: int, batch_limit: int) -> list[UUID]:
        free_agents = 0
        try:
            with self.engine.connect() as connection:
                campaign = read_campaign(connection, campaign_id)
                if campaign is not None:
                    free_agents = admission_capacity(connection, campaign, self.clock.now()).free_agents
        except Exception as error:
            log("pacing_input_unavailable", campaign_id=campaign_id, error_type=type(error).__name__)
        proposal = propose(PacingInputs(free_agents, batch_limit))
        decision = self.safety.decide(campaign_id, proposal)
        if decision.id is None or decision.approved_calls == 0:
            return []
        try:
            return self.allocator.allocate(decision.id)
        except ApprovalRejected as error:
            log("allocation_skipped", approval_id=decision.id, reason=str(error))
            return []
