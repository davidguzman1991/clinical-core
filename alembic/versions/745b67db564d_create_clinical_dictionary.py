"""create clinical dictionary

Revision ID: 745b67db564d
Revises: 20260213_0002
Create Date: 2026-02-15 11:28:43.934367

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from app.db.models import Base


# revision identifiers, used by Alembic.
revision = '745b67db564d'
down_revision = '20260213_0002'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()

    # Early-development reset: drop legacy/UUID schemas instead of attempting unsafe casts.
    op.drop_table("clinical_dictionary", if_exists=True)
    op.drop_table("clinical_search_logs", if_exists=True)

    tables = []
    for name in ("clinical_dictionary", "clinical_search_logs"):
        table = Base.metadata.tables.get(name)
        if table is not None:
            tables.append(table)

    Base.metadata.create_all(bind=bind, tables=tables)


def downgrade() -> None:
    op.drop_table("clinical_search_logs", if_exists=True)
    op.drop_table("clinical_dictionary", if_exists=True)
