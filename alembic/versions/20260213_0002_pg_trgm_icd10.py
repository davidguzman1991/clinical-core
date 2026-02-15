"""Enable pg_trgm and add trigram index for ICD-10 search.

Revision ID: 20260213_0002
Revises: 20260213_0001
Create Date: 2026-02-13

"""

from __future__ import annotations

from alembic import op


revision = "20260213_0002"
down_revision = "20260213_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.create_index(
        "ix_icd10_desc_terms_trgm",
        "icd10",
        ["description", "search_terms"],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={
            "description": "gin_trgm_ops",
            "search_terms": "gin_trgm_ops",
        },
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.drop_index("ix_icd10_desc_terms_trgm", table_name="icd10")
