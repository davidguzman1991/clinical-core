"""FastAPI router for ICD-10.

This router exposes ICD-10 functionality as reusable APIs for Clinical Core.
It must remain independent from product-specific domains (users/auth/prescriptions).
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.clinical.icd10.service import get_icd10_by_code_in_session, search_icd10_in_session
from app.db.session import get_db

router = APIRouter()


@router.get("/search")
def search(
    q: str = Query(default=""),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    results = search_icd10_in_session(db, query=q, limit=limit)
    return [{"code": r.code, "description": r.description} for r in results]


@router.get("/{code}")
def get_by_code(code: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    item = get_icd10_by_code_in_session(db, code=code)
    if not item:
        raise HTTPException(status_code=404, detail="ICD10 code not found")

    return {"code": item.code, "description": item.description}
