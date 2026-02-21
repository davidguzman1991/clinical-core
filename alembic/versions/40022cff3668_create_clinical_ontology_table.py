"""create clinical_ontology table

Revision ID: 40022cff3668
Revises: 20260217_0001
Create Date: 2026-02-21 13:25:48.921224

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '40022cff3668'
down_revision = '20260217_0001'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "clinical_ontology",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("term", sa.String(length=120), nullable=False),
        sa.Column("normalized_term", sa.String(length=120), nullable=False),
        sa.Column("system", sa.String(length=80), nullable=True),
        sa.Column("organ", sa.String(length=80), nullable=True),
        sa.Column("functional_group", sa.String(length=80), nullable=True),
        sa.Column("related_prefix", sa.String(length=5), nullable=True),
        sa.Column("weight", sa.Float(), nullable=False, server_default=sa.text("0.10")),
    )

    op.create_index(
        "ix_clinical_ontology_normalized_term",
        "clinical_ontology",
        ["normalized_term"],
    )


def downgrade():
    op.drop_index("ix_clinical_ontology_normalized_term", table_name="clinical_ontology")
    op.drop_table("clinical_ontology")
