from __future__ import annotations

import logging
import re
import unicodedata
from typing import Dict, List

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


def apply_anatomical_boost(
    results: List[Dict],
    query: str,
    db: Session,
) -> List[Dict]:
    """Apply passive ontology-based anatomical boost on in-memory results."""
    try:
        normalized_query = _normalize_query(query)
        if not normalized_query or not results:
            return results

        normalized_col = func.lower(func.coalesce(ClinicalOntology.normalized_term, ""))
        pattern = bindparam("query_pattern", value=f"%{normalized_query}%")
        stmt = (
            select(ClinicalOntology.system, ClinicalOntology.normalized_term)
            .where(normalized_col != "")
            .where(pattern.ilike(func.concat("%", normalized_col, "%")))
            .order_by(func.length(normalized_col).desc())
        )
        matches = db.execute(stmt).all()
        if not matches:
            return results

        detected_system = ""
        for system, term in matches:
            if system and term:
                detected_system = str(system).strip().lower()
                break
        if not detected_system:
            return results

        logger.info(f"ANATOMICAL_BOOST_APPLIED: system={detected_system}")

        boosted_any = False
        for result in results:
            tags = str(result.get("tags", "") or "").lower()
            if detected_system in tags:
                similarity = float(result.get("similarity", 0.0) or 0.0)
                result["similarity"] = similarity + 0.15
                boosted_any = True

        if not boosted_any:
            return results

        return sorted(results, key=lambda r: float(r.get("similarity", 0.0) or 0.0), reverse=True)
    except Exception:
        return results
