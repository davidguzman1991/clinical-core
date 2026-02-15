"""Add ClinicalDictionary.suggested_icd.

Revision ID: 20260215_0001
Revises: 745b67db564d
Create Date: 2026-02-15

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260215_0001"
down_revision = "745b67db564d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clinical_dictionary", sa.Column("suggested_icd", sa.String(length=10), nullable=True))


def downgrade() -> None:
    op.drop_column("clinical_dictionary", "suggested_icd")
