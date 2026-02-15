"""ICD-10 SQLAlchemy models for Clinical Core.

Clinical Core is a reusable clinical engine intended to be shared across multiple
healthcare products. This module must remain domain-agnostic and must not depend
on application-specific concepts such as users, authentication, prescriptions, or
consultations.
"""

from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ICD10(Base):
    __tablename__ = "icd10"

    code: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    description: Mapped[str] = mapped_column(String, nullable=False)
    search_terms: Mapped[str | None] = mapped_column(Text, nullable=True)
