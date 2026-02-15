"""ICD-10 model import shim.

Clinical Core's ICD-10 model lives in the reusable clinical module.
This shim provides a stable import path (app.models.icd10.ICD10) for:
- Alembic model registration
- seed/loader scripts

Do not import app.db.models from this module.
"""

from app.clinical.icd10.models import ICD10

__all__ = ["ICD10"]
