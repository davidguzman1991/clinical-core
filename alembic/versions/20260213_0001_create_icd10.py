"""Create ICD-10 table.

Revision ID: 20260213_0001
Revises: 
Create Date: 2026-02-13

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260213_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "icd10",
        sa.Column("code", sa.String(), primary_key=True),
        sa.Column("description", sa.String(), nullable=False),
        sa.Column("search_terms", sa.Text(), nullable=True),
    )
    op.create_index(op.f("ix_icd10_code"), "icd10", ["code"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_icd10_code"), table_name="icd10")
    op.drop_table("icd10")
