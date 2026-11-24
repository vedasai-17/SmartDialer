from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID

from sqlalchemy import Connection, RowMapping, text

from .db import audit


class AgentState(StrEnum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"
    RECOVERY_HOLD = "RECOVERY_HOLD"


A = AgentState
HELD = frozenset({A.RESERVED, A.DIALING, A.CONNECTED, A.RECOVERY_HOLD})
ALLOWED = {
    A.OFFLINE: {A.AVAILABLE},
    A.AVAILABLE: {A.RESERVED, A.PAUSED, A.OFFLINE},
    A.RESERVED: {A.DIALING, A.AVAILABLE, A.OFFLINE, A.RECOVERY_HOLD},
    A.DIALING: {A.CONNECTED, A.AVAILABLE, A.OFFLINE, A.RECOVERY_HOLD},
    A.CONNECTED: {A.WRAP_UP, A.OFFLINE, A.RECOVERY_HOLD},
    A.WRAP_UP: {A.AVAILABLE, A.PAUSED, A.OFFLINE},
    A.PAUSED: {A.AVAILABLE, A.OFFLINE},
    A.RECOVERY_HOLD: {A.AVAILABLE, A.WRAP_UP, A.OFFLINE, A.PAUSED},
}


class AgentTransitions:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def get(self, agent_id: int, *, lock: bool = False) -> RowMapping:
        query = "SELECT * FROM agents WHERE id = :id"
        if lock:
            query += " FOR UPDATE"
        return self.connection.execute(text(query), {"id": agent_id}).mappings().one()

    def move(
        self,
        agent: RowMapping,
        target: AgentState,
        now: datetime,
        *,
        call_id: UUID | None = None,
        wrap_up_ends_at: datetime | None = None,
        login_ttl_seconds: int | None = None,
    ) -> bool:
        source = AgentState(agent.state)
        if target not in ALLOWED[source]:
            return self._reject(agent, target, now, "ILLEGAL_TRANSITION")
        if login_ttl_seconds is not None and (source != A.OFFLINE or target != A.AVAILABLE or login_ttl_seconds <= 0):
            return self._reject(agent, target, now, "INVALID_LOGIN")
        if target == A.RESERVED and call_id is None:
            return self._reject(agent, target, now, "CALL_REQUIRED")
        if target == A.WRAP_UP and (wrap_up_ends_at is None or wrap_up_ends_at < now):
            return self._reject(agent, target, now, "WRAP_UP_DEADLINE_REQUIRED")
        if source == A.WRAP_UP and target == A.AVAILABLE and agent.wrap_up_ends_at > now:
            return self._reject(agent, target, now, "WRAP_UP_IN_PROGRESS")

        releasing = source in HELD and target not in HELD
        if releasing and not self._call_released(agent):
            return self._reject(agent, target, now, "ACTIVE_RESERVATION")
        heartbeat = now if login_ttl_seconds is not None else agent.last_heartbeat_at
        lease = now + timedelta(seconds=login_ttl_seconds) if login_ttl_seconds is not None else agent.lease_expires_at
        if target in {A.AVAILABLE, A.RESERVED, A.CONNECTED} and (lease is None or lease <= now):
            return self._reject(agent, target, now, "HEARTBEAT_EXPIRED")
        if target == A.CONNECTED and self._call_rank(agent.current_call_id) != 60:
            return self._reject(agent, target, now, "CALL_NOT_CONNECTED")

        if target == A.RESERVED:
            current_call = call_id
        elif target in HELD:
            current_call = agent.current_call_id
        else:
            current_call = None
        applied = self.connection.execute(
            text("""
                UPDATE agents SET state = :target, version = version + 1, current_call_id = :call_id,
                    last_heartbeat_at = :heartbeat, lease_expires_at = :lease,
                    wrap_up_ends_at = :wrap_up, last_call_ended_at = :last_ended
                WHERE id = :id AND state = :source AND version = :version
                RETURNING id
            """),
            dict(
                id=agent.id, source=source, version=agent.version, target=target, call_id=current_call,
                heartbeat=heartbeat, lease=lease, wrap_up=wrap_up_ends_at if target == A.WRAP_UP else None,
                last_ended=now if releasing else agent.last_call_ended_at,
            ),
        ).scalar_one_or_none()
        if applied is None:
            return self._reject(agent, target, now, "STALE_VERSION")
        return True

    def heartbeat(
        self, agent_id: int, version: int, observed_at: datetime, now: datetime, ttl_seconds: int,
    ) -> bool:
        agent = self.get(agent_id)
        if agent.state == A.OFFLINE or agent.version != version:
            return self._reject(agent, AgentState(agent.state), now, "STALE_HEARTBEAT")
        if ttl_seconds <= 0 or not now - timedelta(seconds=ttl_seconds) < observed_at <= now:
            return self._reject(agent, AgentState(agent.state), now, "INVALID_HEARTBEAT_TIME")
        applied = self.connection.execute(
            text("""
                UPDATE agents SET last_heartbeat_at = :observed, lease_expires_at = :lease, version = version + 1
                WHERE id = :id AND state = :source AND version = :version
                    AND (last_heartbeat_at IS NULL OR last_heartbeat_at < :observed)
                RETURNING id
            """),
            dict(
                id=agent.id, source=agent.state, version=version, observed=observed_at,
                lease=observed_at + timedelta(seconds=ttl_seconds),
            ),
        ).scalar_one_or_none()
        if applied is None:
            return self._reject(agent, AgentState(agent.state), now, "STALE_HEARTBEAT")
        return True

    def expire(self, agent: RowMapping, now: datetime) -> bool:
        state = AgentState(agent.state)
        if state in {A.OFFLINE, A.RECOVERY_HOLD} or (agent.lease_expires_at is not None and agent.lease_expires_at > now):
            return False
        return self.move(agent, A.RECOVERY_HOLD if state in HELD else A.OFFLINE, now)

    def request_presence(self, agent_id: int, target: AgentState, now: datetime, ttl_seconds: int) -> bool:
        agent = self.get(agent_id, lock=True)
        if target not in {A.OFFLINE, A.PAUSED, A.AVAILABLE}:
            return self._reject(agent, target, now, "INVALID_PRESENCE_REQUEST")
        if agent.state == target:
            return True
        if agent.state in HELD and target in {A.OFFLINE, A.PAUSED}:
            if agent.state != A.RECOVERY_HOLD and not self.move(agent, A.RECOVERY_HOLD, now):
                return False
            audit(self.connection, now, "agent", agent.id, "PRESENCE_REQUESTED", campaign_id=agent.campaign_id,
                  call_id=agent.current_call_id, target=target)
            return True
        login_ttl = ttl_seconds if agent.state == A.OFFLINE and target == A.AVAILABLE else None
        if not self.move(agent, target, now, login_ttl_seconds=login_ttl):
            return False
        audit(self.connection, now, "agent", agent.id, "PRESENCE_CHANGED", campaign_id=agent.campaign_id, target=target)
        return True

    def recovery_target(self, agent: RowMapping) -> AgentState:
        target = self.connection.execute(
            text("""
                SELECT details->>'target' FROM audit_log
                WHERE entity_type = 'agent' AND entity_id = :id AND action = 'PRESENCE_REQUESTED'
                    AND details->>'call_id' = :call_id ORDER BY id DESC LIMIT 1
            """),
            dict(id=str(agent.id), call_id=str(agent.current_call_id)),
        ).scalar_one_or_none()
        return A.PAUSED if target == A.PAUSED else A.OFFLINE

    def _call_released(self, agent: RowMapping) -> bool:
        call = self.connection.execute(
            text("SELECT state_rank, agent_id FROM calls WHERE id = :id"), {"id": agent.current_call_id},
        ).one_or_none()
        return call is not None and (call.state_rank == 100 or call.agent_id != agent.id)

    def _call_rank(self, call_id: UUID) -> int | None:
        return self.connection.execute(
            text("SELECT state_rank FROM calls WHERE id = :id"), {"id": call_id},
        ).scalar_one_or_none()

    def _reject(self, agent: RowMapping, target: AgentState, now: datetime, reason: str) -> bool:
        audit(
            self.connection, now, "agent", agent.id, "TRANSITION_REJECTED", campaign_id=agent.campaign_id,
            source=agent.state, target=target, version=agent.version, reason=reason,
        )
        return False
