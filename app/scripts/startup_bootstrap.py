from __future__ import annotations

import logging

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.scripts.load_icd10 import load_icd10
from app.scripts.seed_clinical_dictionary import seed_clinical_dictionary
from app.services.icd10_state import check_icd10_loaded

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _clinical_dictionary_needs_rebuild(db: Session) -> bool:
    bind = db.get_bind()
    inspector = inspect(bind)
    if not inspector.has_table("clinical_dictionary"):
        return True

    columns = {c["name"] for c in inspector.get_columns("clinical_dictionary")}
    required = {"id", "term", "icd10_code", "priority", "created_at"}
    return not required.issubset(columns)


def _rebuild_clinical_dictionary(db: Session) -> None:
    logger.info("Rebuilding clinical_dictionary table to expected schema")
    db.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    db.execute(text("DROP TABLE IF EXISTS clinical_dictionary"))
    db.execute(
        text(
            """
            CREATE TABLE clinical_dictionary (
                id UUID PRIMARY KEY,
                term TEXT NOT NULL,
                icd10_code VARCHAR(10) NOT NULL REFERENCES icd10(code),
                priority INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    )
    db.execute(text("CREATE INDEX ix_clinical_dictionary_term ON clinical_dictionary(term)"))
    db.execute(text("CREATE INDEX ix_clinical_dictionary_icd10_code ON clinical_dictionary(icd10_code)"))
    db.execute(
        text(
            """
            CREATE UNIQUE INDEX ux_clinical_dictionary_term_icd10_code
            ON clinical_dictionary(term, icd10_code)
            """
        )
    )
    db.execute(
        text(
            """
            CREATE INDEX idx_clinical_dictionary_trgm
            ON clinical_dictionary USING gin (term gin_trgm_ops)
            """
        )
    )


def bootstrap() -> None:
    _configure_logging()

    # Always load ICD-10 first; dictionary rebuild/seed depends on FK integrity.
    load_icd10()

    db: Session = SessionLocal()
    schema_ready = False
    try:
        if not check_icd10_loaded(db):
            logger.warning("ICD10 not loaded yet. Skipping clinical dictionary rebuild.")
            return

        if _clinical_dictionary_needs_rebuild(db):
            with db.begin():
                _rebuild_clinical_dictionary(db)
        else:
            logger.info("clinical_dictionary schema already up to date")
        schema_ready = True
    except Exception:
        db.rollback()
        logger.exception("Startup bootstrap failed while ensuring clinical_dictionary schema")
    finally:
        db.close()

    # Seed terms after schema is ready and ICD10 is loaded.
    if schema_ready:
        seed_clinical_dictionary()


def main() -> None:
    try:
        bootstrap()
    except Exception:
        # Never crash startup process due to bootstrap tasks.
        logger.exception("Startup bootstrap terminated with unexpected error")


if __name__ == "__main__":
    main()
