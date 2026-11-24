from sqlalchemy import Engine, text

from .agents import AgentState, AgentTransitions
from .clock import Clock


class Reaper:
    def __init__(self, engine: Engine, clock: Clock) -> None:
        self.engine = engine
        self.clock = clock

    def tick(self, *, limit: int = 100) -> int:
        with self.engine.begin() as connection:
            now = self.clock.now()
            rows = connection.execute(
                text("""
                    SELECT * FROM agents
                    WHERE state NOT IN ('OFFLINE', 'RECOVERY_HOLD')
                        AND (lease_expires_at IS NULL OR lease_expires_at <= :now
                             OR (state = 'WRAP_UP' AND wrap_up_ends_at <= :now))
                    ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED
                """),
                dict(now=now, limit=limit),
            ).mappings().all()
            transitions = AgentTransitions(connection)
            for agent in rows:
                if agent.lease_expires_at is None or agent.lease_expires_at <= now:
                    transitions.expire(agent, now)
                else:
                    transitions.move(agent, AgentState.AVAILABLE, now)
        return len(rows)
