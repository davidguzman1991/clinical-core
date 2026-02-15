from app.db.base import Base

# Import all models here
from app.models.icd10 import ICD10
from app.models.clinical_dictionary import ClinicalDictionary
from app.models.clinical_search_log import ClinicalSearchLog

__all__ = ["Base", "ICD10", "ClinicalDictionary", "ClinicalSearchLog"]
