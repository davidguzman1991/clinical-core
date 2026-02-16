from __future__ import annotations

import re
from datetime import datetime

from pydantic import BaseModel, Field, field_validator

_ICD10_CODE_RE = re.compile(r"^[A-Z0-9][A-Z0-9\.]{1,9}$")


class ICD10SelectionRequest(BaseModel):
    original_query: str = Field(..., min_length=1)
    normalized_query: str = Field(..., min_length=1)
    selected_icd: str = Field(..., min_length=1, max_length=10)
    user_id: str | None = Field(default=None, max_length=128)
    session_id: str | None = Field(default=None, max_length=128)

    @field_validator("original_query", "normalized_query", mode="before")
    @classmethod
    def _strip_text(cls, value: object) -> str:
        if value is None:
            raise ValueError("field is required")
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("field must not be empty")
        return cleaned

    @field_validator("selected_icd", mode="before")
    @classmethod
    def _normalize_selected_icd(cls, value: object) -> str:
        if value is None:
            raise ValueError("selected_icd is required")
        code = str(value).strip().upper()
        if not code:
            raise ValueError("selected_icd must not be empty")
        if not _ICD10_CODE_RE.fullmatch(code):
            raise ValueError("selected_icd has invalid format")
        return code

    @field_validator("user_id", "session_id", mode="before")
    @classmethod
    def _strip_optional(cls, value: object) -> str | None:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None


class ICD10SelectionResponse(BaseModel):
    success: bool
    message: str
    selected_icd: str
    timestamp: datetime
