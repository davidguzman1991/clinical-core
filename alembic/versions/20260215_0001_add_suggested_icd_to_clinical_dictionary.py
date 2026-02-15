"""Add ClinicalDictionary.suggested_icd.

Revision ID: 20260215_0001
Revises: 745b67db564d
Create Date: 2026-02-15

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260215_0001"
down_revision = "745b67db564d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [c["name"] for c in inspector.get_columns("clinical_dictionary")]
    if "suggested_icd" not in columns:
        op.add_column(
            "clinical_dictionary",
            sa.Column("suggested_icd", sa.String(length=10), nullable=True),
        )


def downgrade() -> None:
    op.drop_column("clinical_dictionary", "suggested_icd")
