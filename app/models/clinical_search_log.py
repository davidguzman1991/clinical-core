"""SQLAlchemy model for clinical_search_logs.

The table may already exist in the database; this model exists to standardize
metadata for migrations and future extensions (user personalization, ranking,
ML features).
"""

from __future__ import annotations

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ClinicalSearchLog(Base):
    __tablename__ = "clinical_search_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    query: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    selected_term: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    specialty: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
