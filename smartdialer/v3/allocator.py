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
from .safety_policy import evaluate_limits


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
        if approval.approved_calls <= 0:
            return [], "safety decision does not authorize calls"
        if approval.safety_inputs.get("provider", campaign.config.provider) != campaign.config.provider:
            return [], "provider changed after approval"

        capacity = admission_capacity(connection, campaign, now)
        limits = evaluate_limits(connection, campaign, capacity, approval.approved_calls, now)
        hard = min(limits.hard, approval.safety_inputs.get("approved_hard", approval.approved_calls))
        speculative = 0
        if approval.effective_mode == limits.mode == campaign.mode and campaign.mode != Mode.STRICT_PROGRESSIVE:
            speculative = min(limits.speculative, approval.safety_inputs.get("approved_speculative", 0))
        current_breaker = limits.evidence["breaker"]
        issued_breaker = approval.safety_inputs.get("breaker", {})
        if current_breaker["state"] == "HALF_OPEN" and (
            issued_breaker.get("state") != "HALF_OPEN" or issued_breaker.get("since") != current_breaker["since"]
        ):
            hard = 0
        connection.execute(
            text("""
                UPDATE safety_decisions SET consumed_at = :now, consumed_in = pg_current_xact_id()
                WHERE id = :id AND consumed_at IS NULL AND expires_at > :now RETURNING id
            """),
            dict(id=approval_id, now=now),
        ).scalar_one()
        if hard + speculative == 0:
            return [], None
        mode = Mode.HYBRID if approval.effective_mode == limits.mode == Mode.HYBRID else Mode.STRICT_PROGRESSIVE
        return self._reserve_pairs(connection, campaign, approval_id, now, hard, speculative, mode), None

    @staticmethod
    def _reserve_pairs(
        connection: Connection, campaign: Campaign, approval_id: int, now: datetime, hard: int,
        speculative: int, mode: Mode,
    ) -> list[UUID]:
        agents = connection.execute(
            text(f"""
                SELECT * FROM agents WHERE {AVAILABLE_AGENTS}
                ORDER BY last_call_ended_at NULLS FIRST, id
                LIMIT :limit FOR UPDATE SKIP LOCKED
            """),
            {**availability_params(campaign, now), "limit": hard},
        ).mappings().all()
        owners = [*agents, *([None] * speculative)]
        if not owners:
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
            dict(campaign_id=campaign.id, now=now, limit=len(owners)),
        ).mappings().all()

        agent_transitions = AgentTransitions(connection)
        borrower_transitions = BorrowerTransitions(connection)
        calls = CallTransitions(connection)
        call_ids = []
        for agent, borrower in zip(owners, borrowers):
            call_id = calls.reserve(campaign, borrower, agent.id if agent is not None else None, approval_id, now)
            if agent is not None and not agent_transitions.move(agent, AgentState.RESERVED, now, call_id=call_id):
                raise RuntimeError("failed to reserve the locked agent")
            if not borrower_transitions.move(borrower, BorrowerState.RESERVED, now):
                raise RuntimeError("failed to reserve the locked borrower")
            call_ids.append(call_id)
        if mode == Mode.HYBRID and call_ids:
            staged = connection.execute(text("""
                SELECT safety_inputs->'prestaged_borrower_ids' FROM safety_decisions
                WHERE campaign_id = :id AND id < :approval AND effective_mode = 'HYBRID'
                    AND (safety_inputs->>'prestage_next_ready_at')::timestamptz <= :now
                    AND decided_at > :now - interval '1 minute'
                ORDER BY decided_at DESC, id DESC LIMIT 1
            """), {"id": campaign.id, "approval": approval_id, "now": now}).scalar_one_or_none() or []
            selected = [borrower.id for borrower in borrowers[:len(agents)] if borrower.id in staged]
            if selected:
                audit(connection, now, "safety_decision", approval_id, "PRESTAGED_BORROWERS_RESERVED",
                      campaign_id=campaign.id, borrower_ids=selected)
        return call_ids
