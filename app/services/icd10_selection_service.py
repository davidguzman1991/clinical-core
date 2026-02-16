from __future__ import annotations

from app.repositories.icd10_selection_repository import ICD10SelectionRepository
from app.schemas.icd10_selection import ICD10SelectionRequest, ICD10SelectionResponse
from app.services.search_normalization import normalize_text


class ICD10SelectionValidationError(ValueError):
    pass


class ICD10CodeNotFoundError(LookupError):
    pass


class ICD10SelectionService:
    def __init__(self, repository: ICD10SelectionRepository) -> None:
        self.repository = repository

    def record_selection(self, payload: ICD10SelectionRequest) -> ICD10SelectionResponse:
        original_query = payload.original_query.strip()
        normalized_query = normalize_text(payload.normalized_query)
        if not normalized_query:
            raise ICD10SelectionValidationError("normalized_query must contain searchable terms")

        selected_icd = payload.selected_icd.strip().upper()
        if not self.repository.icd10_exists(selected_icd):
            raise ICD10CodeNotFoundError(selected_icd)

        row = self.repository.insert_selection_log(
            original_query=original_query,
            normalized_query=normalized_query,
            selected_icd=selected_icd,
            user_id=payload.user_id,
            session_id=payload.session_id,
        )

        return ICD10SelectionResponse(
            success=True,
            message="ICD10 selection logged",
            selected_icd=selected_icd,
            timestamp=row.created_at,
        )
