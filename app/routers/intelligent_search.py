"""Intelligent hybrid search router for Clinical Core.

This endpoint provides a reusable clinical search mechanism that combines:
- trigram similarity (pg_trgm)
- global usage frequency
- per-user usage frequency
- specialty usage frequency

It is designed to remain domain-agnostic and compatible with future AI modules.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import MetaData, Table, case, func, literal, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.session import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["intelligent-search"])


@router.get("/intelligent")
def intelligent_search(
    q: str = Query(..., min_length=1, description="Search query"),
    user_id: Optional[int] = Query(default=None, description="Optional user identifier for personalization"),
    specialty: Optional[str] = Query(default=None, description="Optional specialty for contextual ranking"),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    query = q.strip().lower()
    if not query:
        raise HTTPException(status_code=400, detail="q must not be empty")

    specialty_norm = specialty.strip().lower() if specialty is not None else None
    if specialty is not None and not specialty_norm:
        raise HTTPException(status_code=400, detail="specialty must not be empty when provided")

    try:
        bind = db.get_bind()
        metadata = MetaData()

        clinical_dictionary = Table(
            "clinical_dictionary",
            metadata,
            autoload_with=bind,
        )
        clinical_search_logs = Table(
            "clinical_search_logs",
            metadata,
            autoload_with=bind,
        )

        term_norm = func.coalesce(clinical_dictionary.c.term_normalized, "")
        category = clinical_dictionary.c.category

        # Base terms with similarity.
        base_terms = (
            select(
                term_norm.label("term"),
                category.label("category"),
                func.similarity(term_norm, query).label("sim"),
            )
            .where(term_norm != "")
            .cte("base_terms")
        )

        selected_term = func.coalesce(clinical_search_logs.c.selected_term, "")

        # Frequency aggregates with conditional counts.
        global_freq = func.count().label("global_frequency")

        user_freq = (
            func.count().filter(clinical_search_logs.c.user_id == user_id).label("user_frequency")
            if user_id is not None
            else literal(0).label("user_frequency")
        )

        specialty_freq = (
            func.count().filter(func.lower(clinical_search_logs.c.specialty) == specialty_norm).label(
                "specialty_frequency"
            )
            if specialty_norm is not None
            else literal(0).label("specialty_frequency")
        )

        freq = (
            select(
                func.lower(selected_term).label("term"),
                global_freq,
                user_freq,
                specialty_freq,
            )
            .where(selected_term != "")
            .group_by(func.lower(selected_term))
            .cte("freq")
        )

        # Final score.
        sim = func.coalesce(base_terms.c.sim, literal(0.0))
        gf = func.coalesce(freq.c.global_frequency, literal(0))
        uf = func.coalesce(freq.c.user_frequency, literal(0))
        sf = func.coalesce(freq.c.specialty_frequency, literal(0))

        score = (
            literal(0.5) * sim
            + literal(0.2) * gf
            + literal(0.2) * uf
            + literal(0.1) * sf
        ).label("score")

        # Use ILIKE-compatible filtering on term; similarity remains for ranking.
        filtered = (
            select(
                base_terms.c.term,
                base_terms.c.category,
                score,
            )
            .select_from(base_terms.outerjoin(freq, freq.c.term == base_terms.c.term))
            .where(base_terms.c.term.ilike(f"%{query}%") | (sim > 0.2))
            .order_by(score.desc(), base_terms.c.term.asc())
            .limit(10)
        )

        rows = db.execute(filtered).all()
        return [
            {
                "term": r.term,
                "category": r.category,
                "score": float(r.score) if r.score is not None else 0.0,
            }
            for r in rows
        ]

    except AttributeError:
        logger.exception("Schema mismatch for clinical_dictionary/clinical_search_logs")
        raise HTTPException(status_code=500, detail="schema mismatch")
    except SQLAlchemyError:
        logger.exception("Failed to execute intelligent search")
        raise HTTPException(status_code=500, detail="failed to execute intelligent search")
