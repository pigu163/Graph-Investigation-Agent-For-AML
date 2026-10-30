"""Validate external investigation requests and model decisions."""
from datetime import datetime
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Account identifiers remain strings: leading zeroes are meaningful.
NonEmptyText = Annotated[str, Field(min_length=1)]


class InvestigationRequest(BaseModel):
    """Source-local event times are distinct from the aware storage cutoff."""
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    dataset: NonEmptyText
    bank: NonEmptyText
    account: NonEmptyText
    question: NonEmptyText
    start: datetime
    end: datetime
    as_of: datetime | None = None

    @field_validator("start", "end")
    @classmethod
    def event_time_is_naive(cls, value: datetime) -> datetime:
        if value.utcoffset() is not None:
            raise ValueError("Source event time must not include a timezone")
        return value

    @field_validator("as_of")
    @classmethod
    def cutoff_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("Storage cutoff must include a timezone")
        return value

    @model_validator(mode="after")
    def ordered_window(self) -> Self:
        if self.start >= self.end:
            raise ValueError("Start must precede end")
        return self


class ModelDecision(BaseModel):
    """Validate model output before checking action against the current state."""
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, strict=True)

    action: NonEmptyText
    purpose: Annotated[str, Field(min_length=1, max_length=500)]
