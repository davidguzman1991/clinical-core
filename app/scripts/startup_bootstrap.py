from __future__ import annotations

import logging
import os

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.scripts.load_icd10 import load_icd10
from app.scripts.seed_clinical_dictionary import seed_clinical_dictionary
from app.services.icd10_state import check_icd10_loaded

logger = logging.getLogger(__name__)
DEFAULT_EXTENDED_SEARCH_TEXT_MIN_COVERAGE = 0.85


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


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


def _icd10_extended_stats(db: Session) -> dict[str, float] | None:
    bind = db.get_bind()
    inspector = inspect(bind)
    if not inspector.has_table("icd10_extended"):
        return None

    row = db.execute(
        text(
            """
            SELECT
                COUNT(*)::float AS total,
                SUM(CASE WHEN COALESCE(BTRIM(search_text), '') = '' THEN 1 ELSE 0 END)::float AS empty_search_text,
                SUM(CASE WHEN COALESCE(BTRIM(description_normalized), '') = '' THEN 1 ELSE 0 END)::float AS empty_description_normalized
            FROM icd10_extended
            """
        )
    ).mappings().first()

    total = float((row or {}).get("total") or 0.0)
    empty_search_text = float((row or {}).get("empty_search_text") or 0.0)
    empty_description_normalized = float((row or {}).get("empty_description_normalized") or 0.0)

    coverage_search_text = (0.0 if total <= 0 else ((total - empty_search_text) / total) * 100.0)
    coverage_description_normalized = (
        0.0 if total <= 0 else ((total - empty_description_normalized) / total) * 100.0
    )

    return {
        "total": total,
        "empty_search_text": empty_search_text,
        "empty_description_normalized": empty_description_normalized,
        "coverage_search_text": coverage_search_text,
        "coverage_description_normalized": coverage_description_normalized,
    }


def _ensure_icd10_extended_enriched() -> None:
    min_coverage = _env_float("ICD10_EXTENDED_MIN_SEARCH_TEXT_COVERAGE", DEFAULT_EXTENDED_SEARCH_TEXT_MIN_COVERAGE)

    db: Session = SessionLocal()
    try:
        before = _icd10_extended_stats(db)
        if before is None:
            logger.warning("icd10_extended table not found. Skipping search_text enrichment.")
            return

        logger.info(
            "icd10_extended enrichment precheck total=%s empty_search_text=%s empty_description_normalized=%s "
            "coverage_search_text=%.2f%% coverage_description_normalized=%.2f%%",
            int(before["total"]),
            int(before["empty_search_text"]),
            int(before["empty_description_normalized"]),
            before["coverage_search_text"],
            before["coverage_description_normalized"],
        )

        if before["total"] <= 0:
            logger.info("icd10_extended has no rows. Skipping search_text enrichment.")
            return

        current_coverage_ratio = before["coverage_search_text"] / 100.0
        should_enrich = (before["empty_search_text"] > 0) or (current_coverage_ratio < min_coverage)
        if not should_enrich:
            logger.info(
                "icd10_extended search_text coverage is sufficient (%.2f%% >= %.2f%%). Skipping enrichment.",
                before["coverage_search_text"],
                min_coverage * 100.0,
            )
            return

        # Late import avoids any startup cost/path issues if enrichment is not needed.
        from scripts.enrich_icd10_extended_search_text import enrich_search_text

        logger.info(
            "icd10_extended search_text enrichment triggered reason=low_or_empty_search_text "
            "coverage_search_text=%.2f%% threshold=%.2f%%",
            before["coverage_search_text"],
            min_coverage * 100.0,
        )
        enrich_search_text(csv_path="app/data/clinical_dictionary_clean.csv", batch_size=500, dry_run=False)

        after = _icd10_extended_stats(db)
        if after is None:
            logger.warning("icd10_extended table unavailable after enrichment attempt.")
            return

        logger.info(
            "icd10_extended enrichment postcheck total=%s empty_search_text=%s empty_description_normalized=%s "
            "coverage_search_text=%.2f%% coverage_description_normalized=%.2f%%",
            int(after["total"]),
            int(after["empty_search_text"]),
            int(after["empty_description_normalized"]),
            after["coverage_search_text"],
            after["coverage_description_normalized"],
        )
    except Exception:
        logger.exception("Failed to ensure icd10_extended search_text enrichment")
    finally:
        db.close()


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

    # Ensure search semantics are materialized in icd10_extended before serving.
    if schema_ready:
        _ensure_icd10_extended_enriched()

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
