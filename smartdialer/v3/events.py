import asyncio
import json
import re
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import SQLAlchemyError

from .calls import CallState, CallTransitions, TransitionResult
from .campaigns import read_campaign
from .clock import Clock
from .db import audit, log
from .observations import record_status
from .providers import ProviderWebhook, TelecomProvider

WEBHOOK_STATES = frozenset({
    CallState.RINGING, CallState.ANSWERED, CallState.COMPLETED, CallState.FAILED,
    CallState.CANCELLED, CallState.BUSY, CallState.NO_ANSWER,
})


class UnknownProvider(ValueError):
    pass


class InvalidSignature(ValueError):
    pass


def event_key(event: ProviderWebhook) -> str:
    if event.event_id is not None:
        return event.event_id
    data = [event.provider_call_id, event.event_type, event.timestamp.astimezone(UTC).isoformat(), event.sequence]
    return "sha256:" + sha256(json.dumps(data, separators=(",", ":")).encode()).hexdigest()


class EventInbox:
    def __init__(self, engine: Engine, clock: Clock, providers: Mapping[str, TelecomProvider]) -> None:
        self.engine = engine
        self.clock = clock
        self.providers = providers

    def receive(self, provider_name: str, raw: bytes, signature: str) -> str:
        provider = self.providers.get(provider_name)
        if provider is None:
            raise UnknownProvider(provider_name)
        if not provider.verify_signature(raw, signature):
            raise InvalidSignature(provider_name)
        now = self.clock.now()
        try:
            event = provider.parse_webhook(raw)
        except ValidationError:
            return self._store_invalid(provider_name, raw, now)

        key = event_key(event)
        with self.engine.begin() as connection:
            call_id = self._resolve(connection, provider_name, event)
            reason = None
            if event.event_type not in WEBHOOK_STATES:
                status, reason = "INVALID", "UNKNOWN_EVENT_TYPE"
            elif event.timestamp > now:
                status, reason = "INVALID", "FUTURE_TIMESTAMP"
            elif call_id is None:
                status, reason = "UNKNOWN_CALL", "PROVIDER_CALL_UNRESOLVED"
            else:
                status = "PENDING"
            return self._store(
                connection, provider_name, key, event.event_type, now, event.model_dump_json(), status,
                call_id=call_id, timestamp=event.timestamp, sequence=event.sequence, reason=reason,
            )

    @staticmethod
    def _resolve(connection: Connection, provider: str, event: ProviderWebhook) -> UUID | None:
        matches = connection.execute(
            text("""
                SELECT id, idempotency_key, provider_call_id FROM calls
                WHERE provider = :provider AND (provider_call_id = :provider_id OR idempotency_key = :reference)
            """),
            dict(provider=provider, provider_id=event.provider_call_id, reference=event.client_reference),
        ).all()
        if len(matches) != 1:
            return None
        row = matches[0]
        if event.client_reference is not None and event.client_reference != row.idempotency_key:
            return None
        if row.provider_call_id is not None and event.provider_call_id != row.provider_call_id:
            return None
        return row.id

    def _store_invalid(self, provider: str, raw: bytes, now: datetime) -> str:
        digest = sha256(raw).hexdigest()
        try:
            parsed = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            parsed = None
        item = parsed if isinstance(parsed, dict) else {}
        event_id = item.get("event_id")
        key = event_id if isinstance(event_id, str) and re.fullmatch(r"[!-~]{1,128}", event_id) else f"invalid:{digest}"
        event_type = item.get("event_type")
        event_type = event_type if isinstance(event_type, str) and re.fullmatch(r"[!-~]{1,64}", event_type) else "UNPARSEABLE"
        with self.engine.begin() as connection:
            return self._store(connection, provider, key, event_type, now, json.dumps({"body_sha256": digest}),
                               "INVALID", reason="INVALID_WEBHOOK_PAYLOAD")

    @staticmethod
    def _store(
        connection: Connection, provider: str, key: str, event_type: str, now: datetime, payload: str, status: str,
        *, call_id: UUID | None = None, timestamp: datetime | None = None, sequence: int | None = None,
        reason: str | None = None,
    ) -> str:
        event_id = connection.execute(
            text("""
                INSERT INTO call_events (call_id, provider_name, provider_event_id, provider_sequence, event_type,
                    provider_timestamp, received_at, processed_at, payload, processing_status)
                VALUES (:call_id, :provider, :key, :sequence, :type, :timestamp, :now, :processed,
                    CAST(:payload AS jsonb), :status)
                ON CONFLICT (provider_name, provider_event_id) DO NOTHING RETURNING id
            """),
            dict(call_id=call_id, provider=provider, key=key, sequence=sequence, type=event_type,
                 timestamp=timestamp, now=now, processed=now if status != "PENDING" else None, payload=payload, status=status),
        ).scalar_one_or_none()
        if event_id is None:
            return "DUPLICATE"
        if reason is not None:
            audit(connection, now, "call_event", event_id, status, provider=provider, reason=reason)
            log("webhook_anomaly", provider=provider, event_id=event_id, status=status, reason=reason)
        return status


class EventProcessor:
    def __init__(self, engine: Engine, clock: Clock) -> None:
        self.engine = engine
        self.clock = clock

    def process_one(self) -> bool:
        with self.engine.begin() as connection:
            event = connection.execute(text("""
                SELECT * FROM call_events WHERE processing_status = 'PENDING'
                ORDER BY received_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
            """)).mappings().one_or_none()
            if event is None:
                return False
            now = self.clock.now()
            calls = CallTransitions(connection)
            call = calls.lock(event.call_id) if event.call_id is not None else None
            if call is None:
                result = TransitionResult.UNKNOWN_CALL
            else:
                try:
                    campaign = read_campaign(connection, call.campaign_id)
                    payload = ProviderWebhook.model_validate(event.payload)
                    if campaign is None or call.provider != event.provider_name or payload.event_type not in WEBHOOK_STATES:
                        result = TransitionResult.INVALID
                    else:
                        result = record_status(calls, call, campaign, now, payload.status())
                except ValidationError:
                    result = TransitionResult.INVALID
            connection.execute(
                text("UPDATE call_events SET processing_status = :status, processed_at = :now WHERE id = :id"),
                dict(status=result, now=now, id=event.id),
            )
            if result != TransitionResult.APPLIED:
                audit(connection, now, "call_event", event.id, result, call_id=event.call_id, provider=event.provider_name)
            return True

    def drain(self, limit: int = 100) -> int:
        processed = 0
        while processed < limit and self.process_one():
            processed += 1
        return processed


def create_app(inbox: EventInbox, background: Callable[[], None] | None = None) -> FastAPI:
    async def process_background(stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.to_thread(background)
            except Exception as error:
                log("event_worker_error", error_type=type(error).__name__)
            await asyncio.sleep(0.05)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        stop = asyncio.Event()
        task = asyncio.create_task(process_background(stop)) if background is not None else None
        try:
            yield
        finally:
            stop.set()
            if task is not None:
                await task

    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None, lifespan=lifespan)

    @app.post("/webhooks/{provider}")
    async def webhook(provider: str, request: Request) -> dict[str, str]:
        raw = await request.body()
        signature = request.headers.get("X-SmartDialer-Signature", "")
        try:
            status = await asyncio.to_thread(inbox.receive, provider, raw, signature)
        except UnknownProvider as error:
            raise HTTPException(404, "unknown provider") from error
        except InvalidSignature as error:
            raise HTTPException(401, "invalid signature") from error
        except SQLAlchemyError as error:
            log("webhook_insert_failed", provider=provider, error_type=type(error).__name__)
            raise HTTPException(503, "event could not be committed") from error
        return {"status": status}

    @app.get("/metrics")
    def metrics() -> dict[str, object]:
        with inbox.engine.connect() as connection:
            calls = connection.execute(text("SELECT state, count(*) FROM calls GROUP BY state")).all()
            events = connection.execute(text("""
                SELECT processing_status, count(*) FROM call_events WHERE provider_name NOT LIKE 'mock:%'
                    AND event_type <> 'PROVIDER_OPERATION'
                GROUP BY processing_status
            """)).all()
            outcomes = connection.execute(text("""
                WITH answers AS (
                    SELECT c.id, c.connected_at, c.state_rank, c.abandoned_at,
                        (c.answered_at IS NOT NULL OR EXISTS (
                            SELECT 1 FROM call_events e WHERE e.call_id = c.id AND e.provider_name = c.provider
                                AND e.event_type = 'ANSWERED' AND e.processing_status IN ('APPLIED','STALE')
                                AND e.provider_timestamp BETWEEN c.created_at AND c.ended_at
                        )) AS answered
                    FROM calls c
                )
                SELECT count(*) FILTER (WHERE answered) AS answered,
                    count(*) FILTER (WHERE abandoned_at IS NOT NULL OR
                        (answered AND state_rank = 100 AND connected_at IS NULL)) AS abandoned
                FROM answers
            """)).one()
            return {"calls": dict(calls), "events": dict(events), "answered": outcomes.answered,
                    "abandoned": outcomes.abandoned, "abandonment_rate": outcomes.abandoned / outcomes.answered if outcomes.answered else 0}

    return app
