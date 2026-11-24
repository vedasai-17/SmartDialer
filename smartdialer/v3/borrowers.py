import re
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import Connection, RowMapping, text

from .db import audit


class BorrowerState(StrEnum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    CONTACTED = "CONTACTED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    EXHAUSTED = "EXHAUSTED"
    SUPPRESSED = "SUPPRESSED"


B = BorrowerState
ALLOWED = {
    B.QUEUED: {B.RESERVED, B.SUPPRESSED},
    B.RESERVED: {B.CONTACTED, B.RETRY_SCHEDULED, B.EXHAUSTED, B.SUPPRESSED},
    B.CONTACTED: {B.SUPPRESSED},
    B.RETRY_SCHEDULED: {B.RESERVED, B.SUPPRESSED},
    B.EXHAUSTED: {B.SUPPRESSED},
    B.SUPPRESSED: set(),
}


def normalize_phone(phone: str) -> str:
    digits = re.sub(r"[+().\s-]", "", phone)
    if not re.fullmatch(r"[1-9][0-9]{7,14}", digits):
        raise ValueError("a country code and 8–15 digits are required")
    return digits


class BorrowerTransitions:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def get(self, borrower_id: int, *, lock: bool = False) -> RowMapping:
        query = "SELECT * FROM borrowers WHERE id = :id"
        if lock:
            query += " FOR UPDATE"
        return self.connection.execute(text(query), {"id": borrower_id}).mappings().one()

    def enqueue(self, campaign_id: int, phone: str, now: datetime, *, priority: int = 0) -> int | None:
        return self.connection.execute(
            text("""
                INSERT INTO borrowers (campaign_id, phone_normalized, priority, queued_at, next_attempt_at)
                VALUES (:campaign_id, :phone, :priority, :now, :now)
                ON CONFLICT (phone_normalized) DO NOTHING RETURNING id
            """),
            dict(campaign_id=campaign_id, phone=normalize_phone(phone), priority=priority, now=now),
        ).scalar_one_or_none()

    def suppress(self, phone: str, now: datetime) -> bool:
        borrower = self.connection.execute(
            text("SELECT * FROM borrowers WHERE phone_normalized = :phone FOR UPDATE"),
            {"phone": normalize_phone(phone)},
        ).mappings().one_or_none()
        if borrower is None or borrower.state == B.SUPPRESSED:
            return False
        return self.move(borrower, B.SUPPRESSED, now)

    def move(
        self, borrower: RowMapping, target: BorrowerState, now: datetime, *, retry_delay_seconds: int = 0,
    ) -> bool:
        source = BorrowerState(borrower.state)
        if target not in ALLOWED[source]:
            return self._reject(borrower, target, now, "ILLEGAL_TRANSITION")
        if target == B.RESERVED:
            if borrower.attempts >= borrower.max_attempts or borrower.next_attempt_at > now:
                return self._reject(borrower, target, now, "BORROWER_INELIGIBLE")
            call_attempt = self.connection.execute(
                text("SELECT attempt_number FROM calls WHERE borrower_id = :id AND state = 'RESERVED'"),
                {"id": borrower.id},
            ).scalar_one_or_none()
            if call_attempt != borrower.attempts + 1:
                return self._reject(borrower, target, now, "RESERVED_CALL_REQUIRED")
        if target == B.RETRY_SCHEDULED and borrower.attempts >= borrower.max_attempts:
            return self._reject(borrower, target, now, "ATTEMPTS_EXHAUSTED")
        if target == B.EXHAUSTED and borrower.attempts < borrower.max_attempts:
            return self._reject(borrower, target, now, "ATTEMPTS_REMAIN")
        if source == B.RESERVED and target != B.SUPPRESSED and self.connection.execute(
            text("SELECT 1 FROM calls WHERE borrower_id = :id AND state_rank < 100"), {"id": borrower.id},
        ).scalar_one_or_none() is not None:
            return self._reject(borrower, target, now, "ACTIVE_CALL")

        reserved = target == B.RESERVED
        applied = self.connection.execute(
            text("""
                UPDATE borrowers SET state = :target, version = version + 1, attempts = :attempts,
                    next_attempt_at = :next_attempt_at, last_attempt_at = :last_attempt_at
                WHERE id = :id AND state = :source AND version = :version RETURNING id
            """),
            dict(
                id=borrower.id, source=source, version=borrower.version, target=target,
                attempts=borrower.attempts + int(reserved), last_attempt_at=now if reserved else borrower.last_attempt_at,
                next_attempt_at=now + timedelta(seconds=retry_delay_seconds) if target == B.RETRY_SCHEDULED
                else borrower.next_attempt_at,
            ),
        ).scalar_one_or_none()
        if applied is None:
            return self._reject(borrower, target, now, "STALE_VERSION")
        if target == B.SUPPRESSED:
            audit(self.connection, now, "borrower", borrower.id, "SUPPRESSED", campaign_id=borrower.campaign_id)
        return True

    def _reject(self, borrower: RowMapping, target: BorrowerState, now: datetime, reason: str) -> bool:
        audit(
            self.connection, now, "borrower", borrower.id, "TRANSITION_REJECTED", campaign_id=borrower.campaign_id,
            source=borrower.state, target=target, version=borrower.version, reason=reason,
        )
        return False
