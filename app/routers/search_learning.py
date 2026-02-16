"""Search learning router for Clinical Core.

Clinical Core is a reusable clinical engine. This router captures clinician search
behavior to enable future ranking/personalization and AI-assisted suggestions.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.search_normalization import normalize_text

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["search-learning"])


class SearchLogIn(BaseModel):
    query: str = Field(..., min_length=1)
    selected_term: str | None = Field(default=None)
    selected_icd: str | None = Field(default=None)
    specialty: Optional[str] = Field(default=None)
    user_id: Optional[str] = Field(default=None)


@router.post("/log")
def log_search(payload: SearchLogIn, db: Session = Depends(get_db)) -> dict:
    original_query = payload.query.strip()
    normalized_query = normalize_text(original_query)
    selected_term = payload.selected_term.strip().lower() if payload.selected_term else None
    selected_icd = payload.selected_icd.strip().upper() if payload.selected_icd else None
    specialty = payload.specialty.strip() if payload.specialty is not None else None
    user_id = payload.user_id.strip() if payload.user_id is not None else None

    if not original_query:
        raise HTTPException(status_code=400, detail="query must not be empty")
    if not normalized_query:
        raise HTTPException(status_code=400, detail="query must contain searchable terms")
    if not selected_term and not selected_icd:
        raise HTTPException(status_code=400, detail="selected_term or selected_icd is required")
    if specialty is not None and not specialty:
        raise HTTPException(status_code=400, detail="specialty must not be empty when provided")
    if user_id is not None and not user_id:
        raise HTTPException(status_code=400, detail="user_id must not be empty when provided")

    selected_term = selected_term or selected_icd

    try:
        table = Table(
            "clinical_search_logs",
            MetaData(),
            autoload_with=db.get_bind(),
        )

        db.execute(
            table.insert().values(
                user_id=user_id,
                query=original_query,
                normalized_query=normalized_query,
                selected_term=selected_term,
                selected_icd=selected_icd,
                specialty=specialty,
            )
        )
        db.commit()

        return {"message": "search log saved"}

    except SQLAlchemyError:
        db.rollback()
        logger.exception("Failed to insert clinical_search_logs")
        raise HTTPException(status_code=500, detail="failed to save search log")
