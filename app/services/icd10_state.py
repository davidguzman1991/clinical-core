from __future__ import annotations

import logging

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.icd10 import ICD10

logger = logging.getLogger(__name__)


def check_icd10_loaded(session: Session) -> bool:
    """Return True when ICD-10 has at least one row."""
    try:
        count = session.execute(select(func.count()).select_from(ICD10)).scalar_one()
        return bool(count and count > 0)
    except Exception:
        logger.exception("Failed to check ICD10 load state")
        return False
