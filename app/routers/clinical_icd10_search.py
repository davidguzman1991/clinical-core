"""Async clinical ICD-10 search router.

Endpoint:
- GET /clinical/icd10/search?q=...
"""

from __future__ import annotations

import re
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import case, func, literal, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.clinical.icd10.models import ICD10
from app.db.async_session import get_async_db

router = APIRouter(prefix="/clinical/icd10", tags=["clinical-icd10-search"])

SIMILARITY_THRESHOLD = 0.2
DEFAULT_LIMIT = 10
MAX_LIMIT = 50


class ICD10SearchResult(BaseModel):
    code: str
    description: str
    score: float


def _normalize_query(value: str) -> str:
    return " ".join(value.strip().split())


def normalize_icd_input(q: str) -> str:
    if not q:
        return q

    q = q.strip().upper()

    # Insertar punto automático ICD10
    if re.match(r"^[A-Z][0-9]{3}$", q):
        return q[:3] + "." + q[3:]

    return q


async def _run_icd10_search(
    q: str,
    limit: int,
    db: AsyncSession,
) -> List[ICD10SearchResult]:
    query = _normalize_query(q)
    if not query:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="q must not be empty",
        )

    code_query = query.upper()
    use_similarity = len(query) >= 3
    code_expr = func.coalesce(ICD10.code, "")
    desc_expr = func.coalesce(ICD10.description, "")
    terms_expr = func.coalesce(ICD10.search_terms, "")

    exact_code_match = func.lower(code_expr) == func.lower(literal(code_query))
    term_match = or_(
        code_expr.ilike(f"%{query}%"),
        desc_expr.ilike(f"%{query}%"),
        terms_expr.ilike(f"%{query}%"),
    )

    similarity_score = (
        func.greatest(
            func.similarity(code_expr, code_query),
            func.similarity(desc_expr, query),
            func.similarity(terms_expr, query),
        )
        if use_similarity
        else literal(0.0)
    )
    similarity_filter = (similarity_score > SIMILARITY_THRESHOLD) if use_similarity else literal(False)

    rank_bucket = case(
        (exact_code_match, literal(0)),
        (term_match, literal(1)),
        else_=literal(2),
    )

    score = case(
        (exact_code_match, literal(3.0) + similarity_score),
        (term_match, literal(2.0) + similarity_score),
        else_=literal(1.0) + similarity_score,
    ).label("score")

    stmt = (
        select(
            ICD10.code.label("code"),
            ICD10.description.label("description"),
            score,
        )
        .where(or_(term_match, similarity_filter))
        .order_by(rank_bucket.asc(), score.desc(), ICD10.code.asc())
        .limit(limit)
    )

    rows = (await db.execute(stmt)).all()
    return [
        ICD10SearchResult(
            code=row.code,
            description=row.description,
            score=float(row.score),
        )
        for row in rows
    ]


@router.get("/search", response_model=List[ICD10SearchResult])
async def search_icd10(
    q: str = Query(..., min_length=1, description="Clinical query (code or terms)"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: AsyncSession = Depends(get_async_db),
) -> List[ICD10SearchResult]:
    # Normalizacion previa compatible con texto y codigo ICD-10.
    q = normalize_icd_input(q)
    return await _run_icd10_search(q=q, limit=limit, db=db)


@router.get("/search-advanced")
async def search_icd10_advanced(
    q: str = Query(..., min_length=1, description="Clinical query (code or terms)"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: AsyncSession = Depends(get_async_db),
) -> list:
    # Normalizacion comun para compatibilidad de codigo ICD y terminos clinicos.
    q = normalize_icd_input(q)
    q = _normalize_query(q)
    if not q:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="q must not be empty",
        )

    # SQL avanzada con prioridad al diccionario clinical_terms.
    # Nota: se mantiene LIMIT 20 para estabilidad del ranking y latencia.
    sql = """
SELECT
    i.code,
    i.description,
    (
        ts_rank(i.search_vector, query) * 3 +
        similarity(i.description, :q) * 2 +
        similarity(coalesce(ct.term, ''), :q) * 4 +
        CASE
            WHEN unaccent(i.description) ILIKE '%' || unaccent(:q) || '%' THEN 2
            ELSE 0
        END +
        CASE
            WHEN ct.term IS NOT NULL THEN 5
            ELSE 0
        END
    ) AS score
FROM icd10 i
LEFT JOIN clinical_terms ct
    ON ct.icd10_code = i.code,
    plainto_tsquery('spanish', unaccent(:q)) query
WHERE
    i.search_vector @@ query
    OR i.description % :q
    OR ct.term ILIKE '%' || :q || '%'
ORDER BY score DESC
LIMIT 20;
"""

    # AsyncSession requiere await; el patron es equivalente al solicitado.
    result = await db.execute(text(sql), {"q": q})
    return result.fetchall()
