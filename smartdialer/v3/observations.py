from datetime import datetime

from sqlalchemy import RowMapping

from .calls import RANK, CallState, CallTransitions, Lease, TransitionResult
from .campaigns import Campaign
from .db import audit
from .providers import ProviderStatus


def record_status(
    calls: CallTransitions,
    call: RowMapping,
    campaign: Campaign,
    now: datetime,
    status: ProviderStatus,
    *,
    lease: Lease | None = None,
) -> TransitionResult:
    if call.state_rank == 100:
        return calls.advance(call.id, status.state, now, campaign, occurred_at=status.changed_at)
    problem = _invalid_status(call, status, now)
    if problem is None and not calls.bind_provider(call, status.provider_call_id, status.initiated_at):
        problem = "PROVIDER_ID_MISMATCH"
    if problem is not None:
        audit(calls.connection, now, "call", call.id, "INVALID_PROVIDER_STATUS", campaign_id=campaign.id, reason=problem)
        return TransitionResult.INVALID

    if status.connected_at is not None and call.state_rank < 60:
        if call.answered_at is None:
            result = calls.advance(call.id, CallState.ANSWERED, now, campaign, occurred_at=status.answered_at, lease=lease)
            if result != TransitionResult.APPLIED:
                return result
        result = calls.advance(call.id, CallState.CONNECTED, now, campaign, occurred_at=status.connected_at,
                               lease=lease, confirmed_bridge_agent=status.agent_id)
        if result != TransitionResult.APPLIED or status.state == CallState.CONNECTED:
            return result

    if calls.restore_known_state(call, status.state):
        return TransitionResult.APPLIED
    return calls.advance(
        call.id, status.state, now, campaign, occurred_at=status.changed_at, lease=lease,
        observed_answered_at=status.answered_at if RANK[status.state] == 100 else None,
        confirmed_bridge_agent=status.agent_id if status.state == CallState.CONNECTED else None,
    )


def _invalid_status(call: RowMapping, status: ProviderStatus, now: datetime) -> str | None:
    if status.state not in RANK or RANK[status.state] < 30:
        return "UNKNOWN_PROVIDER_STATUS"
    if call.state_rank < 25:
        return "PROVIDER_OPERATION_NOT_STARTED"
    times = (status.changed_at, status.initiated_at, status.answered_at, status.connected_at)
    if any(at is not None and (at.utcoffset() is None or not call.created_at <= at <= now) for at in times):
        return "INVALID_TIMESTAMP"
    if any(at is not None and at > status.changed_at for at in times[1:]):
        return "INVALID_EVENT_TIMELINE"
    if RANK[status.state] == 100 and any(
        at is not None and at > status.changed_at for at in (call.answered_at, call.connected_at)
    ):
        return "TERMINAL_PRECEDES_RECORDED_CALL_ACTIVITY"
    if status.state == CallState.CONNECTED and status.connected_at is None:
        return "BRIDGE_EVIDENCE_MISSING"
    if status.connected_at is not None and RANK[status.state] < 60:
        return "PREBRIDGE_EVENT_HAS_BRIDGE_EVIDENCE"
    if status.answered_at is not None and status.initiated_at is not None and status.answered_at < status.initiated_at:
        return "INVALID_ANSWER_TIMESTAMP"
    if status.connected_at is not None and (
        status.answered_at is None or status.connected_at < status.answered_at or status.agent_id != call.agent_id
    ):
        return "BRIDGE_EVIDENCE_MISMATCH"
    return None
