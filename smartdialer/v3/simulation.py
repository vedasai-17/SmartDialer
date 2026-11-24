from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import ceil
from random import Random

from sqlalchemy import Connection, Engine, RowMapping, text

from .agents import AgentState, AgentTransitions
from .clock import VirtualClock
from .config import BreakerConfig, CampaignConfig, DispatchConfig, Mode, PredictionConfig
from .dialer import Dialer
from .dispatch import Dispatcher
from .events import EventInbox, EventProcessor
from .invariants import verify_invariants
from .lab import isolated_database
from .mock_provider import PROFILES, mock_providers
from .reaper import Reaper
from .seed import seed

SCENARIOS = {"A": (0.20, 120), "B": (0.50, 90), "C": (0.70, 180), "D": (0.70, 180)}
TIERS = ("progressive", "just-in-time", "bounded-speculation")
START = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class SimulationConfig:
    seconds: int = 360
    warmup_seconds: int = 180
    agents: int = 6
    cadence_seconds: int = 5
    seed: int = 17
    risk_budget: float = 0.01


class Simulation:
    def __init__(self, engine: Engine, settings: SimulationConfig, scenario: str, provider: str, tier: str) -> None:
        self.engine = engine
        self.settings = settings
        self.scenario = scenario
        self.provider_name = provider
        self.tier = tier
        self.clock = VirtualClock(START)
        self.start = START + timedelta(seconds=settings.warmup_seconds)
        self.end = self.start + timedelta(seconds=settings.seconds)
        self.drain_end = self.end + timedelta(seconds=360)
        self.ttl = settings.warmup_seconds + settings.seconds + 400
        self.providers = mock_providers(engine, self.clock, seed=settings.seed)
        self.provider = self.providers[provider]
        self.inbox = EventInbox(engine, self.clock, self.providers)
        self.processor = EventProcessor(engine, self.clock)
        self.dispatcher = Dispatcher(engine, self.clock, self.providers, "simulation", rng=Random(settings.seed),
                                     config=DispatchConfig(status_poll_seconds=20))
        self.dialer = Dialer(engine, self.clock)
        self.reaper = Reaper(engine, self.clock)
        self.campaign = self._seed()
        self.roster_reduction = 0
        self.changes: list[dict[str, object]] = []

    def _seed(self) -> int:
        probability, talk = SCENARIOS[self.scenario]
        self.provider.configure(answer_rate=probability, talk_seconds=talk)
        # Preserve the normal abandonment guardrail. Shorter answer windows suit the simulated workload.
        prediction = PredictionConfig(short_window_seconds=75, long_window_seconds=900, max_observation_age_seconds=60,
                                      min_answer_samples=8, min_recent_samples=4)
        config = CampaignConfig(provider=self.provider_name, heartbeat_ttl_seconds=self.ttl, speculation_enabled=self.tier == TIERS[2],
                                prediction=prediction, breaker=BreakerConfig(window_seconds=60, cooldown_seconds=20))
        borrowers = max(100, ceil(self.settings.agents * (self.settings.seconds + self.settings.warmup_seconds) / 6))
        with self.engine.begin() as connection:
            return seed(connection, self.clock, f"simulation-{self.scenario}", self.settings.agents, borrowers, config,
                        max_concurrent_calls=self.settings.agents * 2, provider_rate_limit=self.settings.agents * 2,
                        max_speculative_calls=max(1, self.settings.agents // 2), risk_budget=self.settings.risk_budget)

    def run(self) -> dict[str, object]:
        cadence = timedelta(seconds=self.settings.cadence_seconds)
        next_pace = START
        activity = self._activity()
        changes = iter(self._schedule())
        change = next(changes, None)
        started = False
        while True:
            now = self.clock.now()
            if not started and now >= self.start:
                started = True
                next_pace = now
                if self.tier != TIERS[0]:
                    with self.engine.begin() as connection:
                        connection.execute(text("UPDATE campaigns SET mode = 'HYBRID' WHERE id = :id"), {"id": self.campaign})
            changed = False
            while change is not None and change[0] <= now:
                self._change(change[1], change[2])
                changed = True
                change = next(changes, None)
            if self.roster_reduction and now < self.end:
                changed |= self._retire_available()
            released = 0
            if activity.next_wrap is not None and activity.next_wrap <= now:
                released = self.reaper.tick(limit=self.settings.agents, campaign_id=self.campaign, wrapups_only=True)
            delivered = 0
            if activity.next_callback is not None and activity.next_callback <= now:
                delivered = self.provider.deliver_ready(self.inbox.receive)
                self.processor.drain(limit=max(100, delivered))
            progressed = 0
            if changed or delivered or (activity.next_call is not None and activity.next_call <= now):
                progressed = self.dispatcher.drain(limit=max(100, self.settings.agents * 4))
            current = self._activity()
            if now < self.end:
                jit = started and self.tier != TIERS[0] and (released > 0 or current.free > activity.free)
                if now >= next_pace or jit or changed:
                    if now >= next_pace:
                        next_pace = now + cadence
                    reserved = self.dialer.tick(self.campaign, self.settings.agents * 2)
                    if reserved:
                        progressed += self.dispatcher.drain(limit=max(100, self.settings.agents * 4))
                        current = self._activity()
            elif current.active == 0 and current.next_callback is None:
                break
            if now >= self.drain_end:
                raise RuntimeError(f"simulation failed to drain {self.scenario}/{self.provider_name}/{self.tier}")
            upcoming = [current.next_call, current.next_callback, current.next_wrap, self.drain_end]
            if not started:
                upcoming.append(self.start)
            if now < self.end:
                upcoming.extend([next_pace, self.end])
            if change is not None:
                upcoming.append(change[0])
            next_time = min(at for at in upcoming if at is not None)
            if next_time <= now and not (changed or released or delivered or progressed):
                raise RuntimeError("simulation scheduled work without making progress")
            self.clock.advance(max(0.0, (next_time - now).total_seconds()))
            activity = current
        with self.engine.connect() as connection:
            invariants = verify_invariants(connection, drained=True)
            return self._report(connection, invariants)

    def _activity(self) -> RowMapping:
        with self.engine.connect() as connection:
            return connection.execute(text("""
                SELECT (SELECT count(*) FROM agents WHERE campaign_id = :campaign AND state = 'AVAILABLE') AS free,
                    (SELECT count(*) FROM calls WHERE campaign_id = :campaign AND state_rank < 100) AS active,
                    (SELECT min(wrap_up_ends_at) FROM agents WHERE campaign_id = :campaign AND state = 'WRAP_UP') AS next_wrap,
                    (SELECT min(greatest(next_retry_at, lease_until)) FROM calls WHERE campaign_id = :campaign
                        AND state_rank >= 20 AND state_rank < 100) AS next_call,
                    (SELECT min(received_at) FROM call_events WHERE provider_name = :namespace
                        AND event_type = 'MOCK_CALLBACK' AND processed_at IS NULL) AS next_callback
            """), {"campaign": self.campaign, "namespace": f"mock:{self.provider_name}"}).mappings().one()

    def _schedule(self) -> list[tuple[datetime, str, object]]:
        if self.scenario != "D":
            return []
        controls = [(0.25, "answer-rate", (0.10, 70)), (0.34, "latency", True), (0.40, "outage", True),
                    (0.53, "outage", False), (0.56, "latency", False), (0.58, "answer-rate", (0.20, 90)),
                    (0.63, "agent-drop", ceil(self.settings.agents * 0.4)), (0.74, "answer-rate", (0.70, 180))]
        return [(self.start + timedelta(seconds=self.settings.seconds * cut), kind, value) for cut, kind, value in controls]

    def _change(self, kind: str, value: object) -> None:
        self.changes.append({"at_seconds": self._seconds(self.clock.now()), "kind": kind, "value": value})
        if kind == "answer-rate":
            probability, talk = value
            self.provider.configure(answer_rate=probability, talk_seconds=talk)
        elif kind == "latency":
            normal = PROFILES[self.provider_name]
            self.provider.configure(setup_min_seconds=5 if value else normal.setup_min_seconds,
                                    setup_max_seconds=6 if value else normal.setup_max_seconds)
        elif kind == "outage":
            self.provider.configure(outage=value)
        else:
            self.roster_reduction = int(value)

    def _retire_available(self) -> bool:
        with self.engine.begin() as connection:
            ids = list(connection.execute(text("""
                SELECT id FROM agents WHERE campaign_id = :campaign AND state IN ('AVAILABLE','WRAP_UP','RESERVED','DIALING')
                ORDER BY CASE state WHEN 'AVAILABLE' THEN 0 WHEN 'WRAP_UP' THEN 1 ELSE 2 END, id LIMIT :limit
            """), {"campaign": self.campaign, "limit": self.roster_reduction}).scalars())
            transitions = AgentTransitions(connection)
            removed = sum(transitions.request_presence(agent, AgentState.PAUSED, self.clock.now(), self.ttl) for agent in ids)
        if removed:
            self.roster_reduction -= removed
            self.changes.append({"at_seconds": self._seconds(self.clock.now()), "kind": "agents-removed", "value": removed})
        return removed > 0

    def _seconds(self, at: datetime) -> float:
        return (at - self.start).total_seconds()

    def _report(self, connection: Connection, invariants: dict[str, int]) -> dict[str, object]:
        params = {"campaign": self.campaign, "start": self.start, "end": self.end}
        outcomes = connection.execute(text("""
            WITH started AS (
                SELECT c.*, (c.answered_at IS NOT NULL OR EXISTS (SELECT 1 FROM call_events e
                    WHERE e.call_id = c.id AND e.provider_name = c.provider AND e.event_type = 'ANSWERED'
                        AND e.processing_status IN ('APPLIED','STALE') AND e.provider_timestamp BETWEEN c.created_at AND c.ended_at
                )) AS observed_answer FROM calls c WHERE campaign_id = :campaign
                    AND initiated_at >= :start AND initiated_at < :end
            )
            SELECT count(*) AS initiated, count(*) FILTER (WHERE is_speculative) AS speculative_started,
                count(*) FILTER (WHERE connected_at IS NOT NULL) AS connected,
                count(*) FILTER (WHERE observed_answer) AS answered,
                count(*) FILTER (WHERE abandoned_at IS NOT NULL OR (observed_answer AND connected_at IS NULL)) AS abandoned
            FROM started
        """), params).mappings().one()
        talk = connection.execute(text("""
            SELECT coalesce(sum(extract(epoch FROM (least(ended_at, :end) - greatest(connected_at, :start)))), 0)
            FROM calls WHERE campaign_id = :campaign AND connected_at < :end AND ended_at > :start
        """), params).scalar_one()
        staffed = self.settings.agents * self.settings.seconds - sum(
            change["value"] * (self.settings.seconds - change["at_seconds"])
            for change in self.changes if change["kind"] == "agents-removed"
        )
        decisions = connection.execute(text("""
            SELECT decided_at, action, requested_calls, approved_calls, effective_mode, reasons, pacing_reasoning, safety_inputs
            FROM safety_decisions WHERE campaign_id = :campaign AND decided_at >= :start AND decided_at < :end
            ORDER BY decided_at, id
        """), params).mappings().all()
        actions: dict[str, int] = {}
        reasons: dict[str, int] = {}
        breakers = []
        previous = None
        for decision in decisions:
            actions[decision.action] = actions.get(decision.action, 0) + 1
            for reason in decision.reasons:
                reasons[reason] = reasons.get(reason, 0) + 1
            state = (decision.safety_inputs.get("breaker") or {}).get("state")
            if state is not None and state != previous:
                breakers.append({"at_seconds": self._seconds(decision.decided_at), "state": state})
                previous = state
        samples = sorted((decision for decision in decisions if decision.requested_calls > 0),
                         key=lambda row: (row.requested_calls - row.approved_calls, row.requested_calls), reverse=True)[:2]
        explanations = [{"at_seconds": self._seconds(row.decided_at), "requested": row.requested_calls,
                         "approved": row.approved_calls, "mode": row.effective_mode, "reasons": row.reasons,
                         "pacing": row.pacing_reasoning, "safety": row.safety_inputs} for row in samples]
        answered = outcomes.answered
        return {"scenario": self.scenario, "provider": self.provider_name, "tier": self.tier,
                "utilization": float(talk) / staffed, "staffed_agent_seconds": staffed, **dict(outcomes),
                "abandonment_rate": outcomes.abandoned / answered if answered else 0.0,
                "requested": sum(row.requested_calls for row in decisions), "approved": sum(row.approved_calls for row in decisions),
                "safety_actions": actions, "safety_reasons": reasons, "breaker_transitions": breakers,
                "decision_samples": explanations, "changes": self.changes, "invariants": invariants}


def compare(url: str, settings: SimulationConfig, scenarios: list[str], providers: list[str], jobs: int) -> dict[str, object]:
    parameters = [(scenario, provider, tier) for scenario in scenarios for provider in providers for tier in TIERS]

    def run(case: tuple[str, str, str]) -> dict[str, object]:
        with isolated_database(url) as engine:
            return Simulation(engine, settings, *case).run()

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        results = list(pool.map(run, parameters))
    acceptance = []
    for scenario in scenarios:
        for provider in providers:
            baseline, jit, speculative = [row for row in results if row["scenario"] == scenario and row["provider"] == provider]
            if scenario == "D":
                responses = sorted({edge["state"] for row in (baseline, jit, speculative) for edge in row["breaker_transitions"]})
                acceptance.append({"scenario": scenario, "provider": provider, "safety_response": responses,
                                   "passed": "OPEN" in responses})
            else:
                winners = [row["tier"] for row in (jit, speculative)
                           if row["utilization"] > baseline["utilization"] and row["abandonment_rate"] <= 0.025]
                acceptance.append({"scenario": scenario, "provider": provider, "improving_tiers_within_guardrail": winners,
                                   "passed": bool(winners)})
    return {"settings": vars(settings), "scenario_inputs": {key: SCENARIOS[key] for key in scenarios},
            "results": results, "acceptance": acceptance}


def ladder(report: dict[str, object]) -> str:
    lines = ["Scenario Provider Tier                    Util  Starts  Conn  Aband  Aband%   Req/Appr  Safety actions  Breaker"]
    for row in report["results"]:
        actions = ",".join(f"{name}={n}" for name, n in sorted(row["safety_actions"].items()))
        breakers = "->".join(edge["state"] for edge in row["breaker_transitions"])
        lines.append(f"{row['scenario']:8} {row['provider']:8} {row['tier']:21} {row['utilization']:6.1%}"
                     f" {row['initiated']:7} {row['connected']:5} {row['abandoned']:6} {row['abandonment_rate']:7.2%}"
                     f"   {row['requested']}/{row['approved']}  {actions}  {breakers}")
        if row["decision_samples"]:
            sample = row["decision_samples"][0]
            lines.append(f"   Why {sample['requested']} approved {sample['approved']} | {sample['pacing']['summary']}"
                         f" | {', '.join(sample['reasons'])}")
    return "\n".join(lines)
