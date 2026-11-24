import os
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Mode(StrEnum):
    STRICT_PROGRESSIVE = "STRICT_PROGRESSIVE"
    BOUNDED_PREDICTIVE = "BOUNDED_PREDICTIVE"
    HYBRID = "HYBRID"


class ExcessAnswerPolicy(StrEnum):
    SAFE_TERMINATE = "SAFE_TERMINATE"
    SHORT_HOLD = "SHORT_HOLD"
    PROGRESSIVE_FALLBACK = "PROGRESSIVE_FALLBACK"


class PredictionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    short_window_seconds: int = Field(default=120, gt=0)
    long_window_seconds: int = Field(default=900, gt=0)
    max_observation_age_seconds: int = Field(default=90, gt=0)
    min_answer_samples: int = Field(default=20, ge=2)
    min_recent_samples: int = Field(default=8, ge=2)
    ewma_alpha: float = Field(default=0.15, gt=0, le=1)
    max_overdial_multiplier: float = Field(default=3, ge=1, le=10)
    abandonment_window_seconds: int = Field(default=1800, gt=0)
    min_abandonment_samples: int = Field(default=150, ge=2)
    abandonment_threshold: float = Field(default=0.025, gt=0, lt=1)
    abandonment_resume_threshold: float = Field(default=0.02, gt=0, lt=1)
    abandonment_cooldown_seconds: int = Field(default=120, gt=0)
    agent_drop_fraction: float = Field(default=0.30, gt=0, le=1)
    agent_drop_window_seconds: int = Field(default=5, gt=0)
    rate_shift_warning: float = Field(default=0.15, gt=0, le=1)
    rate_shift_fallback: float = Field(default=0.35, gt=0, le=1)

    @model_validator(mode="after")
    def validate_windows(self) -> "PredictionConfig":
        if self.short_window_seconds >= self.long_window_seconds:
            raise ValueError("the short answer window must be shorter than the long window")
        if self.abandonment_resume_threshold >= self.abandonment_threshold:
            raise ValueError("the abandonment resume threshold must be below the stop threshold")
        if self.rate_shift_warning >= self.rate_shift_fallback:
            raise ValueError("the answer-rate warning must be below the fallback threshold")
        return self


class BreakerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    window_seconds: int = Field(default=30, gt=0)
    min_samples: int = Field(default=3, ge=1)
    error_fraction: float = Field(default=0.5, gt=0, le=1)
    slow_fraction: float = Field(default=0.5, gt=0, le=1)
    max_setup_seconds: float = Field(default=4, gt=0)
    cooldown_seconds: int = Field(default=15, gt=0)
    probe_calls: int = Field(default=1, ge=1, le=5)


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
    excess_answer_policy: ExcessAnswerPolicy = ExcessAnswerPolicy.SAFE_TERMINATE
    answer_hold_seconds: float = Field(default=2, gt=0, le=2)
    prediction: PredictionConfig = Field(default_factory=PredictionConfig)
    breaker: BreakerConfig = Field(default_factory=BreakerConfig)


class DispatchConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    lease_seconds: float = Field(default=3, gt=0)
    status_poll_seconds: float = Field(default=3, gt=0)
    max_dispatch_attempts: int = Field(default=4, gt=0)
    retry_base_seconds: float = Field(default=0.25, gt=0)
    retry_max_seconds: float = Field(default=8, gt=0)
    answer_hold_seconds: float = Field(default=2, gt=0, le=2)
