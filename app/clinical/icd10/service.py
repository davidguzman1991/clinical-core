"""ICD-10 search service for Clinical Core.

This service encapsulates ICD-10 retrieval/search logic behind a stable API.
Consumers (Receta Fácil, Web Diabetes, CALMA, etc.) should integrate via the HTTP
endpoints, not by importing this code directly.

Search strategy:
- Prefix matches are ranked highest (clinician-friendly autocomplete).
- Substring matches are ranked next (description or curated search_terms).
- Trigram similarity is used as a fuzzy fallback when running on PostgreSQL with
  pg_trgm enabled.
"""

from __future__ import annotations

from typing import List, Optional

from sqlalchemy import case, func, literal, or_, select
from sqlalchemy.orm import Session

from app.clinical.icd10.models import ICD10


def search_icd10_in_session(db: Session, query: str, limit: int = 20) -> List[ICD10]:
    q = query.strip()
    if not q:
        return []

    dialect = getattr(getattr(db, "bind", None), "dialect", None)
    dialect_name = getattr(dialect, "name", "")
    use_trigram = dialect_name == "postgresql" and len(q) >= 3

    prefix_match = ICD10.description.ilike(f"{q}%")
    description_substring_match = ICD10.description.ilike(f"%{q}%")
    search_terms_substring_match = func.coalesce(ICD10.search_terms, "").ilike(f"%{q}%")
    substring_match = or_(description_substring_match, search_terms_substring_match)

    rank_bucket = case(
        (prefix_match, literal(0)),
        (substring_match, literal(1)),
        else_=literal(2),
    )

    similarity_score = (
        func.greatest(
            func.similarity(ICD10.description, q),
            func.similarity(func.coalesce(ICD10.search_terms, ""), q),
        )
        if use_trigram
        else literal(0.0)
    )

    stmt = (
        select(ICD10)
        .where(
            or_(
                ICD10.code.ilike(f"%{q}%"),
                prefix_match,
                substring_match,
                (similarity_score > 0.2) if use_trigram else literal(False),
            )
        )
        .order_by(rank_bucket.asc(), similarity_score.desc(), ICD10.code.asc())
        .limit(limit)
    )

    return db.execute(stmt).scalars().all()


def get_icd10_by_code_in_session(db: Session, code: str) -> Optional[ICD10]:
    c = code.strip()
    if not c:
        return None

    stmt = select(ICD10).where(ICD10.code == c)
    return db.execute(stmt).scalar_one_or_none()
