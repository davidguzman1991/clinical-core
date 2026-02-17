from __future__ import annotations

import argparse
import logging
import re

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.clinical.suggestions.service import normalize_query
from app.db.session import SessionLocal
from app.models.clinical_dictionary import ClinicalDictionary
from app.models.icd10 import ICD10
from app.services.icd10_state import check_icd10_loaded

logger = logging.getLogger(__name__)
_ICD_CODE_RE = re.compile(r"[^A-Z0-9]")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def normalize_icd10_official(code: str) -> str:
    compact = _ICD_CODE_RE.sub("", (code or "").strip().upper())
    if len(compact) > 3 and compact[0].isalpha() and compact[1:3].isdigit():
        return f"{compact[:3]}.{compact[3:]}"
    return compact


def _seed_terms() -> list[dict[str, object]]:
    # Basic high-impact terms for ICD-10 autocomplete.
    terms: list[dict[str, object]] = [
        {"term": "diabetes", "icd10_code": "E11", "priority": 10},
        {"term": "dm2", "icd10_code": "E11", "priority": 9},
        {"term": "diabetes tipo 2", "icd10_code": "E11", "priority": 10},
        {"term": "diabetes mellitus tipo 2", "icd10_code": "E11", "priority": 10},
        {"term": "diabetes gestacional", "icd10_code": "O24.4", "priority": 9},
        {"term": "neuropatia diabetica", "icd10_code": "E11.4", "priority": 8},
        {"term": "pie diabetico", "icd10_code": "E11.5", "priority": 8},
        {"term": "retinopatia diabetica", "icd10_code": "E11.3", "priority": 8},
        {"term": "nefropatia diabetica", "icd10_code": "E11.2", "priority": 8},
        {"term": "hipertension", "icd10_code": "I10", "priority": 10},
        {"term": "hta", "icd10_code": "I10", "priority": 9},
        {"term": "infarto", "icd10_code": "I21", "priority": 9},
        {"term": "insuficiencia cardiaca", "icd10_code": "I50", "priority": 9},
    ]

    out: list[dict[str, object]] = []
    for t in terms:
        term = str(t["term"]).strip()
        icd10_code = normalize_icd10_official(str(t["icd10_code"]))
        if not term:
            continue
        if not icd10_code:
            continue
        out.append(
            {
                "term": normalize_query(term),
                "icd10_code": icd10_code,
                "priority": int(t.get("priority") or 1),
            }
        )

    # Ensure uniqueness by (term, icd10_code) within the seed set.
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, object]] = []
    for row in out:
        key = (str(row["term"]), str(row["icd10_code"]))
        if not all(key) or key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


def seed_clinical_dictionary(batch_size: int = 200) -> None:
    _configure_logging()

    db: Session = SessionLocal()
    try:
        if not check_icd10_loaded(db):
            logger.warning("ICD10 not loaded yet. Skipping clinical dictionary seed.")
            return

        existing_icd10_codes = {
            normalize_icd10_official(str(code))
            for code in db.execute(select(ICD10.code)).scalars().all()
            if code
        }

        seed_rows = _seed_terms()
        if not seed_rows:
            logger.info("No seed rows generated")
            return

        inserted_total = 0
        skipped_missing_code_total = 0

        for start in range(0, len(seed_rows), batch_size):
            chunk = seed_rows[start : start + batch_size]
            try:
                objects: list[ClinicalDictionary] = []
                for r in chunk:
                    term = str(r["term"])
                    code = str(r["icd10_code"])
                    if code not in existing_icd10_codes:
                        skipped_missing_code_total += 1
                        logger.warning("Skipping term '%s': ICD10 code '%s' not found", term, code)
                        continue
                    exists = (
                        db.query(func.count())
                        .select_from(ClinicalDictionary)
                        .filter(
                            ClinicalDictionary.term == term,
                            ClinicalDictionary.icd10_code == code,
                        )
                        .scalar()
                    )
                    if exists:
                        continue
                    objects.append(
                        ClinicalDictionary(
                            term=term,
                            icd10_code=code,
                            priority=int(r.get("priority") or 1),
                        )
                    )

                if objects:
                    db.add_all(objects)
                    db.commit()

                inserted_total += len(objects)
                logger.info("Seed progress: inserted=%s", inserted_total)
                if skipped_missing_code_total:
                    logger.info("Seed progress: skipped_missing_code=%s", skipped_missing_code_total)

            except SQLAlchemyError:
                db.rollback()
                logger.exception("Seed batch failed; aborting seed")
                return

        logger.info(
            "Seed completed. inserted=%s skipped_missing_code=%s",
            inserted_total,
            skipped_missing_code_total,
        )

    except Exception:
        db.rollback()
        logger.exception("Seed failed")
        return
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed clinical_dictionary with high-value initial terms")
    parser.add_argument("--batch-size", type=int, default=200)
    args = parser.parse_args()

    seed_clinical_dictionary(batch_size=args.batch_size)


if __name__ == "__main__":
    main()
