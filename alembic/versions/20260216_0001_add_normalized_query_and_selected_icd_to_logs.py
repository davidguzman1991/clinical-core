"""Add normalized_query and selected_icd to clinical_search_logs.

Revision ID: 20260216_0001
Revises: 20260215_0001
Create Date: 2026-02-16
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260216_0001"
down_revision = "20260215_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    columns = [c["name"] for c in inspector.get_columns("clinical_search_logs")]

    if "normalized_query" not in columns:
        op.add_column("clinical_search_logs", sa.Column("normalized_query", sa.Text(), nullable=True))
    if "selected_icd" not in columns:
        op.add_column("clinical_search_logs", sa.Column("selected_icd", sa.String(length=10), nullable=True))

    indexes = {idx["name"] for idx in inspector.get_indexes("clinical_search_logs")}
    if "ix_clinical_search_logs_normalized_query" not in indexes:
        op.create_index(
            "ix_clinical_search_logs_normalized_query",
            "clinical_search_logs",
            ["normalized_query"],
            unique=False,
        )
    if "ix_clinical_search_logs_selected_icd" not in indexes:
        op.create_index(
            "ix_clinical_search_logs_selected_icd",
            "clinical_search_logs",
            ["selected_icd"],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index("ix_clinical_search_logs_selected_icd", table_name="clinical_search_logs")
    op.drop_index("ix_clinical_search_logs_normalized_query", table_name="clinical_search_logs")
    op.drop_column("clinical_search_logs", "selected_icd")
    op.drop_column("clinical_search_logs", "normalized_query")
