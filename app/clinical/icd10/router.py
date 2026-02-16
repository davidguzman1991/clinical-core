"""FastAPI router for ICD-10.

This router exposes ICD-10 functionality as reusable APIs for Clinical Core.
It must remain independent from product-specific domains (users/auth/prescriptions).
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.clinical.icd10.service import get_icd10_by_code_in_session
from app.db.session import get_db
from app.repositories.icd10_selection_repository import ICD10SelectionRepository
from app.repositories.search_repository import ClinicalSearchRepository
from app.schemas.icd10_selection import ICD10SelectionRequest, ICD10SelectionResponse
from app.services.icd10_selection_service import (
    ICD10CodeNotFoundError,
    ICD10SelectionService,
    ICD10SelectionValidationError,
)
from app.services.search_service import ClinicalSearchService

router = APIRouter()


@router.get("/search")
def search(
    q: str = Query(default=""),
    limit: int = Query(default=20, ge=1, le=100),
    user_id: str | None = Query(default=None, description="Optional user id for usage-based ranking"),
    specialty: str | None = Query(default=None, description="Optional specialty for context logging"),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    service = ClinicalSearchService(repository=ClinicalSearchRepository(db))
    results = service.search(q, limit=limit, user_id=user_id, specialty=specialty)
    return [
        {
            "code": r.code,
            "description": r.description,
            "score": r.score,
            "match_type": r.match_type,
        }
        for r in results
    ]


@router.post("/select", response_model=ICD10SelectionResponse, status_code=status.HTTP_201_CREATED)
def select_icd10(
    payload: ICD10SelectionRequest,
    db: Session = Depends(get_db),
) -> ICD10SelectionResponse:
    service = ICD10SelectionService(repository=ICD10SelectionRepository(db))
    try:
        return service.record_selection(payload)
    except ICD10SelectionValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except ICD10CodeNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"ICD10 code not found: {exc}") from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="failed to log ICD10 selection") from exc


@router.get("/{code}")
def get_by_code(code: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    item = get_icd10_by_code_in_session(db, code=code)
    if not item:
        raise HTTPException(status_code=404, detail="ICD10 code not found")

    return {"code": item.code, "description": item.description}
