"""Add session_id and convert user_id to text in clinical_search_logs.

Revision ID: 20260216_0002
Revises: 20260216_0001
Create Date: 2026-02-16
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260216_0002"
down_revision = "20260216_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {c["name"]: c for c in inspector.get_columns("clinical_search_logs")}
    indexes = {idx["name"] for idx in inspector.get_indexes("clinical_search_logs")}

    if "session_id" not in columns:
        op.add_column("clinical_search_logs", sa.Column("session_id", sa.String(length=128), nullable=True))
    if "ix_clinical_search_logs_session_id" not in indexes:
        op.create_index(
            "ix_clinical_search_logs_session_id",
            "clinical_search_logs",
            ["session_id"],
            unique=False,
        )

    user_id_column = columns.get("user_id")
    if user_id_column is not None and isinstance(user_id_column["type"], sa.Integer):
        op.alter_column(
            "clinical_search_logs",
            "user_id",
            existing_type=sa.Integer(),
            type_=sa.String(length=128),
            existing_nullable=True,
            postgresql_using="user_id::text",
        )


def downgrade() -> None:
    op.drop_index("ix_clinical_search_logs_session_id", table_name="clinical_search_logs")
    op.drop_column("clinical_search_logs", "session_id")
    op.alter_column(
        "clinical_search_logs",
        "user_id",
        existing_type=sa.String(length=128),
        type_=sa.Integer(),
        existing_nullable=True,
        postgresql_using="NULLIF(user_id, '')::integer",
    )
