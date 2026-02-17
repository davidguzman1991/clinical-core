from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, func, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ClinicalDictionary(Base):
    __tablename__ = "clinical_dictionary"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    term: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    icd10_code: Mapped[str] = mapped_column(String(10), ForeignKey("icd10.code"), nullable=False, index=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        Index(
            "idx_clinical_dictionary_trgm",
            "term",
            postgresql_using="gin",
            postgresql_ops={"term": "gin_trgm_ops"},
        ),
    )
