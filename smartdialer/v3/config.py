import os
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Mode(StrEnum):
    STRICT_PROGRESSIVE = "STRICT_PROGRESSIVE"
    BOUNDED_PREDICTIVE = "BOUNDED_PREDICTIVE"
    HYBRID = "HYBRID"


class Settings(BaseModel):
    database_url: str = Field(
        default_factory=lambda: os.environ.get(
            "SMARTDIALER_DATABASE_URL", "postgresql+psycopg://smartdialer:smartdialer@127.0.0.1:5432/smartdialer"
        ),
        repr=False,
    )


class CampaignConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: str = Field(default="A", min_length=1)
    heartbeat_ttl_seconds: int = Field(default=30, gt=0)
    approval_ttl_seconds: int = Field(default=5, gt=0, le=30)
    retry_delay_seconds: int = Field(default=30, ge=0)
    wrap_up_seconds: int = Field(default=5, ge=0)
    speculation_enabled: bool = False


class DispatchConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    lease_seconds: float = Field(default=3, gt=0)
    status_poll_seconds: float = Field(default=3, gt=0)
    max_dispatch_attempts: int = Field(default=4, gt=0)
    retry_base_seconds: float = Field(default=0.25, gt=0)
    retry_max_seconds: float = Field(default=8, gt=0)
    answer_hold_seconds: float = Field(default=2, gt=0, le=2)
