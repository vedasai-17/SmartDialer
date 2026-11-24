from sqlalchemy import Connection, text

from .agents import AgentState, AgentTransitions
from .borrowers import BorrowerTransitions
from .clock import Clock
from .config import CampaignConfig, Mode


def seed(
    connection: Connection, clock: Clock, name: str, agents: int, borrowers: int, config: CampaignConfig,
    *, mode: Mode = Mode.STRICT_PROGRESSIVE, max_speculative_calls: int = 0, risk_budget: float = 0,
    max_concurrent_calls: int | None = None, provider_rate_limit: int | None = None,
) -> int:
    campaign_id = connection.execute(text("""
        INSERT INTO campaigns (name, mode, max_concurrent_calls, provider_rate_limit, max_speculative_calls, risk_budget, config)
        VALUES (:name, :mode, :concurrent, :rate, :speculative, :risk, CAST(:config AS jsonb)) RETURNING id
    """), dict(name=name, mode=mode, concurrent=agents if max_concurrent_calls is None else max_concurrent_calls,
               rate=agents if provider_rate_limit is None else provider_rate_limit, speculative=max_speculative_calls,
               risk=risk_budget, config=config.model_dump_json())).scalar_one()
    now = clock.now()
    agent_transitions = AgentTransitions(connection)
    borrowers_service = BorrowerTransitions(connection)
    for _ in range(agents):
        agent_id = connection.execute(text("INSERT INTO agents (campaign_id) VALUES (:id) RETURNING id"),
                                      {"id": campaign_id}).scalar_one()
        if not agent_transitions.move(agent_transitions.get(agent_id), AgentState.AVAILABLE, now,
                                      login_ttl_seconds=config.heartbeat_ttl_seconds):
            raise RuntimeError("could not seed agent presence")
    for offset in range(borrowers):
        synthetic = f"999{campaign_id:06d}{offset:06d}"
        if borrowers_service.enqueue(campaign_id, synthetic, now) is None:
            raise RuntimeError("synthetic borrower already exists")
    return campaign_id
