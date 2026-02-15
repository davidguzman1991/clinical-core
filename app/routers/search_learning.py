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

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["search-learning"])


class SearchLogIn(BaseModel):
    query: str = Field(..., min_length=1)
    selected_term: str = Field(..., min_length=1)
    specialty: Optional[str] = Field(default=None)


@router.post("/log")
def log_search(payload: SearchLogIn, db: Session = Depends(get_db)) -> dict:
    query = payload.query.strip().lower()
    selected_term = payload.selected_term.strip().lower()
    specialty = payload.specialty.strip() if payload.specialty is not None else None

    if not query:
        raise HTTPException(status_code=400, detail="query must not be empty")
    if not selected_term:
        raise HTTPException(status_code=400, detail="selected_term must not be empty")
    if specialty is not None and not specialty:
        raise HTTPException(status_code=400, detail="specialty must not be empty when provided")

    try:
        table = Table(
            "clinical_search_logs",
            MetaData(),
            autoload_with=db.get_bind(),
        )

        db.execute(
            table.insert().values(
                query=query,
                selected_term=selected_term,
                specialty=specialty,
            )
        )
        db.commit()

        return {"message": "search log saved"}

    except SQLAlchemyError:
        db.rollback()
        logger.exception("Failed to insert clinical_search_logs")
        raise HTTPException(status_code=500, detail="failed to save search log")
