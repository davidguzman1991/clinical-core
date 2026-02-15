"""Clinical dictionary search service.

Clinical Core exposes reusable clinical search behavior based on a curated
clinical dictionary.

This service is intentionally domain-agnostic and optimized for PostgreSQL.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any, Optional

from sqlalchemy import MetaData, Table, case, func, literal, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_MULTISPACE_RE = re.compile(r"\s+")


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_query(value: str) -> str:
    text = value.strip().lower()
    text = _strip_accents(text)
    text = _MULTISPACE_RE.sub(" ", text)
    return text


def search_clinical_dictionary(
    db: Session,
    query: str,
    *,
    specialty: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict[str, Any]]:
    q = normalize_query(query)
    if not q:
        return []

    dialect = getattr(getattr(db, "bind", None), "dialect", None)
    is_postgres = getattr(dialect, "name", "") == "postgresql"

    bind = db.get_bind()
    md = MetaData()
    clinical_dictionary = Table("clinical_dictionary", md, autoload_with=bind)
    clinical_search_logs = Table("clinical_search_logs", md, autoload_with=bind)

    term_raw = clinical_dictionary.c.term_raw
    term_norm = func.coalesce(clinical_dictionary.c.term_normalized, "")
    category = clinical_dictionary.c.category
    suggested_icd = getattr(clinical_dictionary.c, "suggested_icd", None)

    specialty_norm = normalize_query(specialty) if specialty is not None else None

    sim = func.similarity(term_norm, q) if is_postgres and len(q) >= 3 else literal(0.0)

    # Usage logs are optional/"future". We compute a cheap global frequency when available.
    # Convention: selected_term may store either the selected item or, for pure searches,
    # the query string (best-effort). This keeps schema stable while enabling future ranking.
    freq_cte = (
        select(
            func.lower(func.coalesce(clinical_search_logs.c.selected_term, "")).label("term_key"),
            func.count().label("global_frequency"),
        )
        .where(func.coalesce(clinical_search_logs.c.selected_term, "") != "")
        .group_by(func.lower(func.coalesce(clinical_search_logs.c.selected_term, "")))
        .cte("freq")
    )

    specialty_boost = (
        case((func.lower(func.coalesce(category, "")) == specialty_norm, literal(0.05)), else_=literal(0.0))
        if specialty_norm
        else literal(0.0)
    )

    global_freq = func.coalesce(freq_cte.c.global_frequency, literal(0))

    score = (
        literal(0.7) * sim
        + literal(0.2) * global_freq
        + literal(0.1) * specialty_boost
    ).label("score")

    base = (
        select(
            term_raw.label("term"),
            category.label("category"),
            (suggested_icd.label("suggested_icd") if suggested_icd is not None else literal(None).label("suggested_icd")),
            score,
        )
        .select_from(clinical_dictionary.outerjoin(freq_cte, freq_cte.c.term_key == func.lower(term_norm)))
        .where(
            (term_norm.ilike(f"%{q}%"))
            | (term_raw.ilike(f"%{q}%"))
            | ((sim > 0.2) if is_postgres and len(q) >= 3 else literal(False))
        )
        .order_by(score.desc(), term_norm.asc())
        .limit(limit)
        .offset(offset)
    )

    # Debug: log compiled SQL (best-effort).
    try:
        compiled = base.compile(dialect=bind.dialect, compile_kwargs={"literal_binds": True})
        logger.info("clinical_dictionary query=%r specialty=%r", q, specialty_norm)
        logger.info("clinical_dictionary SQL: %s", str(compiled))
    except Exception:
        logger.exception("Failed to compile SQL for logging")

    try:
        rows = db.execute(base).all()
    except SQLAlchemyError:
        logger.exception("Similarity query failed; falling back to ILIKE-only search")
        fallback = (
            select(
                term_raw.label("term"),
                category.label("category"),
                (suggested_icd.label("suggested_icd") if suggested_icd is not None else literal(None).label("suggested_icd")),
            )
            .select_from(clinical_dictionary)
            .where((term_norm.ilike(f"%{q}%")) | (term_raw.ilike(f"%{q}%")))
            .order_by(term_norm.asc())
            .limit(limit)
            .offset(offset)
        )

        try:
            compiled_fb = fallback.compile(dialect=bind.dialect, compile_kwargs={"literal_binds": True})
            logger.info("clinical_dictionary fallback SQL: %s", str(compiled_fb))
        except Exception:
            logger.exception("Failed to compile fallback SQL for logging")

        rows = db.execute(fallback).all()

    return [
        {
            "term": r.term,
            "category": r.category,
            "suggested_icd": r.suggested_icd,
        }
        for r in rows
    ]
