from __future__ import annotations

from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ClinicalDictionary(Base):
    __tablename__ = "clinical_dictionary"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    term_raw: Mapped[str] = mapped_column(String, nullable=False, index=True)
    term_normalized: Mapped[str] = mapped_column(String, nullable=False, index=True)
    category: Mapped[str | None] = mapped_column(String, nullable=True)
    suggested_icd: Mapped[str | None] = mapped_column(String(10), nullable=True)

    __table_args__ = (
        Index(
            "idx_clinical_dictionary_trgm",
            "term_normalized",
            postgresql_using="gin",
            postgresql_ops={"term_normalized": "gin_trgm_ops"},
        ),
    )
