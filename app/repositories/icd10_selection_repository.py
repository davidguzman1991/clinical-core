from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.clinical_search_log import ClinicalSearchLog
from app.models.icd10 import ICD10


class ICD10SelectionRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def icd10_exists(self, code: str) -> bool:
        stmt = select(ICD10.code).where(ICD10.code == code).limit(1)
        return self.db.execute(stmt).scalar_one_or_none() is not None

    def insert_selection_log(
        self,
        *,
        original_query: str,
        normalized_query: str,
        selected_icd: str,
        user_id: str | None,
        session_id: str | None,
    ) -> ClinicalSearchLog:
        row = ClinicalSearchLog(
            user_id=user_id,
            query=original_query,
            normalized_query=normalized_query,
            selected_term=selected_icd,
            selected_icd=selected_icd,
            session_id=session_id,
            specialty=None,
        )
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row
