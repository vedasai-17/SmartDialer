from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from pydantic import AwareDatetime, BaseModel, Field

from .calls import CallState, Lease


class ProviderTimeout(TimeoutError):
    """The operation may have reached the provider."""


class ProviderRejected(Exception):
    """The provider definitively declined the operation."""


@dataclass(frozen=True)
class ProviderStatus:
    provider_call_id: str
    state: CallState
    changed_at: datetime
    initiated_at: datetime | None = None
    answered_at: datetime | None = None
    connected_at: datetime | None = None
    agent_id: int | None = None


class ProviderWebhook(BaseModel):
    event_id: str | None = Field(default=None, min_length=1, max_length=128, pattern=r"^[\x21-\x7e]+$")
    event_type: str = Field(min_length=1, max_length=64, pattern=r"^[\x21-\x7e]+$")
    provider_call_id: str = Field(min_length=1, max_length=128, pattern=r"^[\x21-\x7e]+$")
    client_reference: str | None = Field(default=None, min_length=1, max_length=128, pattern=r"^[\x21-\x7e]+$")
    timestamp: AwareDatetime
    sequence: int | None = Field(default=None, ge=0, le=2**63 - 1)
    initiated_at: AwareDatetime | None = None
    answered_at: AwareDatetime | None = None
    connected_at: AwareDatetime | None = None
    agent_id: int | None = Field(default=None, gt=0, le=2**63 - 1)

    def status(self) -> ProviderStatus:
        return ProviderStatus(
            self.provider_call_id, CallState(self.event_type), self.timestamp, self.initiated_at,
            self.answered_at, self.connected_at, self.agent_id,
        )


class TelecomProvider(Protocol):
    name: str

    def initiate_call(self, client_reference: str, phone: str, lease: Lease) -> ProviderStatus: ...

    def cancel_call(self, client_reference: str, lease: Lease) -> ProviderStatus: ...

    def get_call_status(self, client_reference: str) -> ProviderStatus | None: ...

    def bridge(self, client_reference: str, lease: Lease, agent_id: int) -> ProviderStatus: ...

    def verify_signature(self, raw: bytes, signature: str) -> bool: ...

    def parse_webhook(self, raw: bytes) -> ProviderWebhook: ...
