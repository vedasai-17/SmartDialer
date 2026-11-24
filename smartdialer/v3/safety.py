import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import Connection, Engine, text

from .campaigns import admission_capacity, read_campaign
from .clock import Clock
from .config import Mode
from .db import log
from .pacing import PacingProposal
from .safety_policy import evaluate_limits


class Action(StrEnum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_TO_PROGRESSIVE = "FALLBACK_TO_PROGRESSIVE"


@dataclass(frozen=True)
class SafetyDecision:
    id: int | None
    action: Action
    approved_calls: int
    effective_mode: Mode
    reasons: tuple[str, ...]
    approved_hard: int = 0
    approved_speculative: int = 0


class SafetyController:
    def __init__(self, engine: Engine, clock: Clock) -> None:
        self.engine = engine
        self.clock = clock

    def decide(self, campaign_id: int, proposal: PacingProposal) -> SafetyDecision:
        requested = proposal.requested_calls
        if type(requested) is not int or not 0 <= requested <= 2_147_483_647:
            decision = self._reject(campaign_id, proposal, 0, "INVALID_REQUEST")
        else:
            try:
                decision = self._decide(campaign_id, proposal, requested)
            except Exception as error:
                log("safety_error", campaign_id=campaign_id, error_type=type(error).__name__)
                decision = self._reject(campaign_id, proposal, requested, "SAFETY_ERROR")
        log(
            "safety_decision", campaign_id=campaign_id, approval_id=decision.id,
            action=decision.action, approved_calls=decision.approved_calls, approved_speculative=decision.approved_speculative,
            reasons=decision.reasons,
        )
        return decision

    def _decide(self, campaign_id: int, proposal: PacingProposal, requested: int) -> SafetyDecision:
        with self.engine.begin() as connection:
            campaign = read_campaign(connection, campaign_id, lock=True)
            if campaign is None:
                return SafetyDecision(None, Action.REJECT, 0, Mode.STRICT_PROGRESSIVE, ("CAMPAIGN_NOT_FOUND",))
            now = self.clock.now()
            capacity = admission_capacity(connection, campaign, now)
            limits = evaluate_limits(connection, campaign, capacity, requested, now)
            action = Action.APPROVE
            if limits.total == 0:
                action = Action.REJECT
            elif limits.mode != campaign.mode:
                action = Action.FALLBACK_TO_PROGRESSIVE
            elif limits.total < requested:
                action = Action.REDUCE
            decision = SafetyDecision(None, action, limits.total, limits.mode, limits.reasons or ("WITHIN_LIMITS",),
                                      limits.hard, limits.speculative)
            return self._record(
                connection, campaign_id, proposal, requested, decision, now,
                campaign.config.approval_ttl_seconds, limits.evidence,
            )

    def _reject(self, campaign_id: int, proposal: PacingProposal, requested: int, reason: str) -> SafetyDecision:
        decision = SafetyDecision(None, Action.REJECT, 0, Mode.STRICT_PROGRESSIVE, (reason,))
        try:
            with self.engine.begin() as connection:
                if connection.execute(text("SELECT 1 FROM campaigns WHERE id = :id"), {"id": campaign_id}).scalar():
                    return self._record(connection, campaign_id, proposal, requested, decision, self.clock.now(), 5, {})
        except Exception as error:
            log("safety_rejection_record_failed", campaign_id=campaign_id, error_type=type(error).__name__)
        return decision

    @staticmethod
    def _record(
        connection: Connection,
        campaign_id: int,
        proposal: PacingProposal,
        requested: int,
        decision: SafetyDecision,
        now: datetime,
        ttl_seconds: int,
        safety_inputs: dict[str, object],
    ) -> SafetyDecision:
        decision_id = connection.execute(
            text("""
                INSERT INTO safety_decisions (
                    campaign_id, action, requested_calls, approved_calls, effective_mode, reasons,
                    pacing_inputs, pacing_reasoning, safety_inputs, decided_at, expires_at
                ) VALUES (
                    :campaign_id, :action, :requested, :approved, :mode, :reasons,
                    CAST(:pacing_inputs AS jsonb), CAST(:reasoning AS jsonb), CAST(:safety_inputs AS jsonb),
                    :now, :expires_at
                ) RETURNING id
            """),
            dict(
                campaign_id=campaign_id, action=decision.action, requested=requested,
                approved=decision.approved_calls, mode=decision.effective_mode, reasons=list(decision.reasons),
                pacing_inputs=json.dumps(asdict(proposal.inputs)), reasoning=json.dumps({"summary": proposal.reasoning}),
                safety_inputs=json.dumps({**safety_inputs, "approved_hard": decision.approved_hard,
                                          "approved_speculative": decision.approved_speculative}),
                now=now, expires_at=now + timedelta(seconds=ttl_seconds),
            ),
        ).scalar_one()
        return SafetyDecision(decision_id, decision.action, decision.approved_calls, decision.effective_mode, decision.reasons,
                              decision.approved_hard, decision.approved_speculative)
