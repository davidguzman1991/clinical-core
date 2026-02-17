"""Rebuild clinical_dictionary for ICD-10 autocomplete.

Revision ID: 20260217_0001
Revises: 20260216_0002
Create Date: 2026-02-17
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import re
from uuid import uuid4

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from app.services.icd10_state import check_icd10_loaded


revision = "20260217_0001"
down_revision = "20260216_0002"
branch_labels = None
depends_on = None

_ICD_CODE_RE = re.compile(r"[^A-Z0-9]")
logger = logging.getLogger(__name__)


def normalize_icd10_official(code: str) -> str:
    """Normalize ICD-10 code to official dotted format when possible."""
    compact = _ICD_CODE_RE.sub("", (code or "").strip().upper())
    if len(compact) > 3 and compact[0].isalpha() and compact[1:3].isdigit():
        return f"{compact[:3]}.{compact[3:]}"
    return compact


def map_root_to_billable(code: str) -> str:
    root = normalize_icd10_official(code).split(".", 1)[0]
    preferred = {
        "E11": "E11.9",
        "I50": "I50.9",
        "I21": "I21.9",
        "O24": "O24.4",
    }
    return preferred.get(root, f"{root}.9" if len(root) == 3 and root[0].isalpha() and root[1:3].isdigit() else root)


def _resolve_billable_code(code: str, existing_codes: set[str]) -> str | None:
    official = normalize_icd10_official(code)
    if official in existing_codes:
        return official

    mapped = normalize_icd10_official(map_root_to_billable(official))
    if mapped in existing_codes:
        logger.info("Mapped ICD10 root '%s' to billable '%s'", code, mapped)
        return mapped

    root = official.split(".", 1)[0]
    candidates = sorted(c for c in existing_codes if c.startswith(f"{root}."))
    if not candidates:
        return None

    for candidate in candidates:
        if candidate.endswith(".9"):
            logger.info("Mapped ICD10 root '%s' to generic billable '%s'", code, candidate)
            return candidate

    logger.info("Mapped ICD10 root '%s' to first available billable '%s'", code, candidates[0])
    return candidates[0]


def upgrade() -> None:
    bind = op.get_bind()
    session = Session(bind=bind)
    try:
        inspector = sa.inspect(bind)
        if not inspector.has_table("icd10"):
            logger.warning("ICD10 table not found. Skipping clinical dictionary rebuild.")
            return

        if not check_icd10_loaded(session):
            logger.warning("ICD10 not loaded yet. Skipping clinical dictionary rebuild.")
            return

        existing_codes = {
            normalize_icd10_official(str(code))
            for code in session.execute(sa.text("SELECT code FROM icd10")).scalars().all()
            if code
        }

        op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

        op.drop_table("clinical_dictionary", if_exists=True)
        op.create_table(
            "clinical_dictionary",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("term", sa.Text(), nullable=False),
            sa.Column("icd10_code", sa.String(length=10), sa.ForeignKey("icd10.code"), nullable=False),
            sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        )
        op.create_index("ix_clinical_dictionary_term", "clinical_dictionary", ["term"], unique=False)
        op.create_index("ix_clinical_dictionary_icd10_code", "clinical_dictionary", ["icd10_code"], unique=False)
        op.create_index(
            "ux_clinical_dictionary_term_icd10_code",
            "clinical_dictionary",
            ["term", "icd10_code"],
            unique=True,
        )
        op.create_index(
            "idx_clinical_dictionary_trgm",
            "clinical_dictionary",
            ["term"],
            unique=False,
            postgresql_using="gin",
            postgresql_ops={"term": "gin_trgm_ops"},
        )

        seed_terms = [
            ("diabetes", "E11.9", 10),
            ("dm2", "E11.9", 9),
            ("diabetes tipo 2", "E11.9", 10),
            ("diabetes mellitus tipo 2", "E11.9", 10),
            ("diabetes gestacional", "O24.4", 9),
            ("neuropatia diabetica", "E11.4", 8),
            ("pie diabetico", "E11.5", 8),
            ("retinopatia diabetica", "E11.3", 8),
            ("nefropatia diabetica", "E11.2", 8),
            ("hipertension", "I10", 10),
            ("hta", "I10", 9),
            ("infarto", "I21", 9),
            ("insuficiencia cardiaca", "I50", 9),
        ]

        valid_rows = []
        for term, code, priority in seed_terms:
            resolved_code = _resolve_billable_code(code, existing_codes)
            if not resolved_code:
                logger.warning("Skipping clinical_dictionary term '%s': ICD10 code '%s' not found", term, code)
                continue
            valid_rows.append(
                {
                    "id": uuid4(),
                    "term": term.strip().lower(),
                    "icd10_code": resolved_code,
                    "priority": int(priority),
                    "created_at": datetime.now(timezone.utc),
                }
            )

        skipped = len(seed_terms) - len(valid_rows)
        if skipped:
            logger.warning("Skipped %s clinical dictionary rows due to missing ICD10 codes", skipped)

        if not valid_rows:
            logger.warning("No valid clinical dictionary rows to insert after ICD10 validation")
            return

        table = sa.table(
            "clinical_dictionary",
            sa.column("id", postgresql.UUID(as_uuid=True)),
            sa.column("term", sa.Text()),
            sa.column("icd10_code", sa.String(length=10)),
            sa.column("priority", sa.Integer()),
            sa.column("created_at", sa.DateTime(timezone=True)),
        )
        op.bulk_insert(table, valid_rows)
    finally:
        session.close()


def downgrade() -> None:
    op.drop_table("clinical_dictionary", if_exists=True)
