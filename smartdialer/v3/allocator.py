from datetime import datetime
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import Connection, Engine, text

from .agents import AgentState, AgentTransitions
from .borrowers import BorrowerState, BorrowerTransitions
from .calls import CallTransitions
from .campaigns import AVAILABLE_AGENTS, Campaign, admission_capacity, availability_params, read_campaign
from .clock import Clock
from .config import Mode
from .db import audit, log


class ApprovalRejected(ValueError):
    pass


class Allocator:
    def __init__(self, engine: Engine, clock: Clock) -> None:
        self.engine = engine
        self.clock = clock

    def allocate(self, approval_id: int) -> list[UUID]:
        if type(approval_id) is not int or approval_id <= 0:
            raise ApprovalRejected("a valid safety decision id is required")
        with self.engine.begin() as connection:
            call_ids, rejection = self._allocate(connection, approval_id)
            if rejection:
                audit(connection, self.clock.now(), "safety_decision", approval_id, "ALLOCATION_REJECTED", reason=rejection)
        if rejection:
            raise ApprovalRejected(rejection)
        log("allocation_committed", approval_id=approval_id, allocated_calls=len(call_ids))
        return call_ids

    def _allocate(self, connection: Connection, approval_id: int) -> tuple[list[UUID], str | None]:
        campaign_id = connection.execute(
            text("SELECT campaign_id FROM safety_decisions WHERE id = :id"), {"id": approval_id},
        ).scalar_one_or_none()
        if campaign_id is None:
            return [], "safety decision is missing"
        # Serialize campaign-wide cap checks. Rows claimed below are still skipped when locked elsewhere.
        try:
            campaign = read_campaign(connection, campaign_id, lock=True)
        except ValidationError:
            return [], "campaign configuration is invalid"
        if campaign is None or campaign.status != "ACTIVE":
            return [], "campaign is inactive"
        approval = connection.execute(
            text("SELECT * FROM safety_decisions WHERE id = :id FOR UPDATE"), {"id": approval_id},
        ).mappings().one()
        now = self.clock.now()
        if approval.consumed_at is not None:
            return [], "safety decision is already consumed"
        if not approval.decided_at <= now < approval.expires_at:
            return [], "safety decision is outside its validity window"
        if approval.approved_calls <= 0 or approval.effective_mode != Mode.STRICT_PROGRESSIVE:
            return [], "safety decision does not authorize progressive calls"

        limit = min(approval.approved_calls, admission_capacity(connection, campaign, now).limit)
        connection.execute(
            text("""
                UPDATE safety_decisions SET consumed_at = :now, consumed_in = pg_current_xact_id()
                WHERE id = :id AND consumed_at IS NULL AND expires_at > :now RETURNING id
            """),
            dict(id=approval_id, now=now),
        ).scalar_one()
        if limit == 0:
            return [], None
        return self._reserve_pairs(connection, campaign, approval_id, now, limit), None

    @staticmethod
    def _reserve_pairs(
        connection: Connection, campaign: Campaign, approval_id: int, now: datetime, limit: int,
    ) -> list[UUID]:
        agents = connection.execute(
            text(f"""
                SELECT * FROM agents WHERE {AVAILABLE_AGENTS}
                ORDER BY last_call_ended_at NULLS FIRST, id
                LIMIT :limit FOR UPDATE SKIP LOCKED
            """),
            {**availability_params(campaign, now), "limit": limit},
        ).mappings().all()
        if not agents:
            return []
        borrowers = connection.execute(
            text("""
                SELECT b.* FROM borrowers b
                WHERE b.campaign_id = :campaign_id AND b.state IN ('QUEUED', 'RETRY_SCHEDULED')
                    AND b.attempts < b.max_attempts AND b.next_attempt_at <= :now
                    AND NOT EXISTS (
                        SELECT 1 FROM calls WHERE borrower_id = b.id AND state_rank < 100
                    )
                ORDER BY b.priority DESC, b.next_attempt_at, b.queued_at, b.attempts, b.id
                LIMIT :limit FOR UPDATE OF b SKIP LOCKED
            """),
            dict(campaign_id=campaign.id, now=now, limit=len(agents)),
        ).mappings().all()

        agent_transitions = AgentTransitions(connection)
        borrower_transitions = BorrowerTransitions(connection)
        calls = CallTransitions(connection)
        call_ids = []
        for agent, borrower in zip(agents, borrowers):
            call_id = calls.reserve(campaign, borrower, agent.id, approval_id, now)
            if not agent_transitions.move(agent, AgentState.RESERVED, now, call_id=call_id):
                raise RuntimeError("failed to reserve the locked agent")
            if not borrower_transitions.move(borrower, BorrowerState.RESERVED, now):
                raise RuntimeError("failed to reserve the locked borrower")
            call_ids.append(call_id)
        return call_ids
