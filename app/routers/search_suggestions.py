"""Search suggestions router for Clinical Core.

Suggestions are generated from prior clinician selections recorded in
clinical_search_logs.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import MetaData, Table, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.session import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["search-learning"])


@router.get("/suggest")
def suggest(
    query: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
) -> list[dict]:
    q = query.strip().lower()
    if not q:
        raise HTTPException(status_code=400, detail="query must not be empty")

    try:
        table = Table(
            "clinical_search_logs",
            MetaData(),
            autoload_with=db.get_bind(),
        )

        selected_term_col = table.c.selected_term
        query_col = table.c.query

        stmt = (
            select(
                selected_term_col.label("selected_term"),
                func.count().label("frequency"),
            )
            .where(
                func.coalesce(query_col, "").ilike(f"%{q}%")
                | func.coalesce(selected_term_col, "").ilike(f"%{q}%")
            )
            .group_by(selected_term_col)
            .order_by(func.count().desc())
            .limit(5)
        )

        rows = db.execute(stmt).all()
        return [{"selected_term": r.selected_term, "frequency": int(r.frequency)} for r in rows]

    except AttributeError:
        logger.exception("clinical_search_logs table missing expected columns")
        raise HTTPException(status_code=500, detail="search log schema mismatch")
    except SQLAlchemyError:
        logger.exception("Failed to fetch suggestions")
        raise HTTPException(status_code=500, detail="failed to fetch suggestions")
