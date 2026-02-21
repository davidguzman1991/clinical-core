from __future__ import annotations

import logging
import re
import unicodedata

from sqlalchemy import bindparam, func, select
from sqlalchemy.orm import Session

from app.models.clinical_ontology import ClinicalOntology

logger = logging.getLogger(__name__)

_MULTISPACE_RE = re.compile(r"\s+")


def _normalize_query(value: str) -> str:
    text = (value or "").strip().lower()
    nfkd = unicodedata.normalize("NFKD", text)
    stripped = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return _MULTISPACE_RE.sub(" ", stripped).strip()


def detect_shadow_ontology(query: str, db: Session) -> None:
    """Passive ontology detector; logs matches and never raises."""
    try:
        normalized_query = _normalize_query(query)
        if not normalized_query:
            return

        normalized_col = func.lower(func.coalesce(ClinicalOntology.normalized_term, ""))
        query_pattern = bindparam("query_pattern", value=f"%{normalized_query}%")

        stmt = (
            select(ClinicalOntology.system, ClinicalOntology.normalized_term)
            .where(normalized_col != "")
            .where(query_pattern.ilike(func.concat("%", normalized_col, "%")))
        )

        rows = db.execute(stmt).all()
        for system, term in rows:
            if not term:
                continue
            logger.info("SHADOW_ONTOLOGY_MATCH: system=%s term=%s", system or "", term)
    except Exception:
        # Shadow mode must never alter the normal search pipeline.
        return
