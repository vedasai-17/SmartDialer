import argparse
import json
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

from sqlalchemy import Connection, Engine, text

from .agents import AgentState, AgentTransitions
from .campaigns import read_campaign
from .clock import Clock, SystemClock
from .config import CampaignConfig, ExcessAnswerPolicy, Mode, Settings
from .db import database
from .dialer import Dialer
from .seed import seed


def positive(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return value


def run_loop(action: Callable[[], object], seconds: int | None, *, interval: float | Callable[[], float]) -> None:
    from .db import log

    deadline = time.monotonic() + seconds if seconds is not None else float("inf")
    while time.monotonic() < deadline:
        try:
            action()
        except Exception as error:
            log("worker_iteration_failed", error_type=type(error).__name__)
        time.sleep(interval() if callable(interval) else interval)


def status(connection: Connection, campaign_id: int) -> dict[str, object]:
    params = {"id": campaign_id}
    calls = connection.execute(
        text("SELECT state, count(*) FROM calls WHERE campaign_id = :id GROUP BY state"), params,
    ).all()
    decisions = connection.execute(
        text("""
            SELECT id, action, requested_calls, approved_calls, effective_mode, reasons, pacing_inputs, pacing_reasoning, safety_inputs,
                decided_at, consumed_at FROM safety_decisions
            WHERE campaign_id = :id ORDER BY id DESC LIMIT 5
        """), params,
    ).mappings()
    agents = connection.execute(
        text("SELECT state, count(*) FROM agents WHERE campaign_id = :id GROUP BY state"), params,
    ).all()
    return {"campaign_id": campaign_id, "calls": dict(calls), "agents": dict(agents), "decisions": [dict(row) for row in decisions]}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SmartDialer v3 PostgreSQL prototype")
    commands = parser.add_subparsers(dest="command", required=True)
    seed_command = commands.add_parser("seed", help="create a campaign with synthetic records")
    seed_command.add_argument("--name", default="progressive-demo")
    seed_command.add_argument("--agents", type=positive, default=10)
    seed_command.add_argument("--borrowers", type=positive, default=50)
    seed_command.add_argument("--heartbeat-ttl", type=positive, default=300)
    seed_command.add_argument("--provider", choices=("A", "B"), default="A")
    seed_command.add_argument("--mode", type=Mode, choices=list(Mode), default=Mode.STRICT_PROGRESSIVE)
    seed_command.add_argument("--enable-speculation", action="store_true")
    seed_command.add_argument("--max-speculative-calls", type=int, default=0)
    seed_command.add_argument("--risk-budget", type=float, default=0)
    seed_command.add_argument("--max-concurrent-calls", type=positive)
    seed_command.add_argument("--provider-rate-limit", type=positive)
    seed_command.add_argument("--excess-answer-policy", type=ExcessAnswerPolicy, choices=list(ExcessAnswerPolicy),
                              default=ExcessAnswerPolicy.SAFE_TERMINATE)
    tick = commands.add_parser("tick", help="run pacing, safety, and allocation once")
    tick.add_argument("--campaign-id", type=positive, required=True)
    tick.add_argument("--batch-limit", type=positive, default=50)
    show = commands.add_parser("status", help="show campaign call counts and safety decisions")
    show.add_argument("--campaign-id", type=positive, required=True)
    dial = commands.add_parser("dialer", help="continuously pace and allocate")
    dial.add_argument("--campaign-id", type=positive, required=True)
    dial.add_argument("--batch-limit", type=positive, default=50)
    dial.add_argument("--seconds", type=positive)
    dispatch = commands.add_parser("dispatcher", help="dispatch and reconcile leased calls")
    dispatch.add_argument("--worker-id", default=f"{os.getpid()}-{uuid4().hex[:8]}")
    dispatch.add_argument("--seconds", type=positive)
    events = commands.add_parser("events", help="serve signed webhooks and process the inbox")
    events.add_argument("--port", type=positive, default=8000)
    presence = commands.add_parser("agents", help="change agent presence")
    presence.add_argument("state", choices=("online", "offline", "paused"))
    presence.add_argument("ids", nargs="+", type=positive)
    mock = commands.add_parser("mock-config", help="change a shared mock provider profile")
    mock.add_argument("--provider", choices=("A", "B"), required=True)
    mock.add_argument("--set", action="append", default=[], metavar="FIELD=VALUE")
    simulation = commands.add_parser("simulate", help="compare the three dialing tiers with a virtual clock and PostgreSQL")
    simulation.add_argument("--scenario", choices=("all", "A", "B", "C", "D"), default="all")
    simulation.add_argument("--provider", choices=("both", "A", "B"), default="both")
    simulation.add_argument("--agents", type=positive, default=6)
    simulation.add_argument("--seconds", type=positive, default=360)
    simulation.add_argument("--warmup-seconds", type=positive, default=180)
    simulation.add_argument("--cadence-seconds", type=positive, default=5)
    simulation.add_argument("--seed", type=int, default=17)
    simulation.add_argument("--jobs", type=positive, default=2)
    simulation.add_argument("--report", type=Path)
    simulation.add_argument("--require-acceptance", action="store_true")
    load = commands.add_parser("load-test", help="measure concurrent allocation and webhook throughput")
    load.add_argument("--agents", type=positive, default=100)
    load.add_argument("--borrowers", type=positive)
    load.add_argument("--workers", type=positive, default=4)
    load.add_argument("--batch", type=positive, default=10)
    load.add_argument("--timeout", type=positive, default=300)
    load.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.command in {"simulate", "load-test"}:
        run_lab_command(args, Settings().database_url)
        return
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    engine = database(Settings())
    clock = SystemClock()
    try:
        if args.command == "seed":
            if args.max_speculative_calls < 0 or not 0 <= args.risk_budget <= 1:
                parser.error("speculative cap must be nonnegative and risk budget must be between zero and one")
            with engine.begin() as connection:
                config = CampaignConfig(heartbeat_ttl_seconds=args.heartbeat_ttl, provider=args.provider,
                                        speculation_enabled=args.enable_speculation, excess_answer_policy=args.excess_answer_policy)
                campaign_id = seed(connection, clock, args.name, args.agents, args.borrowers, config, mode=args.mode,
                                   max_speculative_calls=args.max_speculative_calls, risk_budget=args.risk_budget,
                                   max_concurrent_calls=args.max_concurrent_calls, provider_rate_limit=args.provider_rate_limit)
            result = {"campaign_id": campaign_id, "agents": args.agents, "borrowers": args.borrowers}
        elif args.command == "tick":
            call_ids = Dialer(engine, clock).tick(args.campaign_id, args.batch_limit)
            result = {"campaign_id": args.campaign_id, "calls_reserved": len(call_ids)}
        elif args.command == "status":
            with engine.connect() as connection:
                result = status(connection, args.campaign_id)
        elif args.command == "dialer":
            dialer = Dialer(engine, clock)
            run_loop(lambda: dialer.tick(args.campaign_id, args.batch_limit), args.seconds,
                     interval=lambda: dialer.delay(args.campaign_id, 0.2))
            return
        elif args.command == "agents":
            states = {"online": AgentState.AVAILABLE, "offline": AgentState.OFFLINE, "paused": AgentState.PAUSED}
            changed = []
            with engine.begin() as connection:
                transitions = AgentTransitions(connection)
                for agent_id in sorted(set(args.ids)):
                    agent = transitions.get(agent_id)
                    campaign = read_campaign(connection, agent.campaign_id)
                    if campaign is not None and transitions.request_presence(
                        agent_id, states[args.state], clock.now(), campaign.config.heartbeat_ttl_seconds,
                    ):
                        changed.append(agent_id)
            result = {"state_requested": args.state, "agents": changed}
        else:
            run_provider_command(args, engine, clock, parser)
            return
        print(json.dumps(result, default=str))
    except KeyboardInterrupt:
        pass
    finally:
        engine.dispose()


def run_provider_command(args: argparse.Namespace, engine: Engine, clock: Clock, parser: argparse.ArgumentParser) -> None:
    from .mock_provider import MockProfile, mock_providers

    providers = mock_providers(engine, clock)
    if args.command == "dispatcher":
        from .dispatch import Dispatcher
        from .reaper import Reaper

        worker = Dispatcher(engine, clock, providers, args.worker_id)
        reaper = Reaper(engine, clock)

        def work() -> None:
            reaper.tick()
            worker.drain(limit=50)

        run_loop(work, args.seconds, interval=0.05)
    elif args.command == "events":
        import uvicorn

        from .events import EventInbox, EventProcessor, create_app

        inbox = EventInbox(engine, clock, providers)
        processor = EventProcessor(engine, clock)

        def receive() -> None:
            for provider in providers.values():
                provider.deliver_ready(inbox.receive)
            processor.drain()

        uvicorn.run(create_app(inbox, receive), host="127.0.0.1", port=args.port, access_log=False)
    else:
        overrides = {}
        for override in args.set:
            field, separator, value = override.partition("=")
            if not separator or field not in MockProfile.model_fields:
                parser.error(f"invalid mock setting {field!r}")
            try:
                overrides[field] = json.loads(value)
            except json.JSONDecodeError:
                overrides[field] = value
        print(providers[args.provider].configure(**overrides).model_dump_json())


def run_lab_command(args: argparse.Namespace, url: str) -> None:
    if args.command == "simulate":
        from .simulation import SCENARIOS, SimulationConfig, compare, ladder

        settings = SimulationConfig(args.seconds, args.warmup_seconds, args.agents, args.cadence_seconds, args.seed)
        result = compare(url, settings, list(SCENARIOS) if args.scenario == "all" else [args.scenario],
                         ["A", "B"] if args.provider == "both" else [args.provider], args.jobs)
        print(ladder(result))
        for acceptance in result["acceptance"]:
            print(json.dumps(acceptance))
    else:
        from .load_test import load_test

        result = load_test(url, args.agents, args.borrowers or args.agents * 2, args.workers, args.batch, args.timeout)
        print(json.dumps(result, indent=2))
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if args.command == "simulate" and args.require_acceptance and not all(row["passed"] for row in result["acceptance"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
