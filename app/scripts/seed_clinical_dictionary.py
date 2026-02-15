from __future__ import annotations

import argparse
import logging

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.clinical.suggestions.service import normalize_query
from app.db.session import SessionLocal
from app.models.clinical_dictionary import ClinicalDictionary

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _seed_terms() -> list[dict[str, object]]:
    # 40-60 high-value clinical terms (Spanish) across targeted domains.
    # term_normalized MUST be generated using normalize_query().
    terms: list[dict[str, object]] = [
        # Endocrinology / Diabetes
        {"term": "diabetes", "specialty": "endocrino", "suggested_icd": "E11", "priority": 10},
        {"term": "diabetes tipo 2", "specialty": "endocrino", "suggested_icd": "E11", "priority": 10},
        {"term": "diabetes tipo 1", "specialty": "endocrino", "suggested_icd": "E10", "priority": 9},
        {"term": "prediabetes", "specialty": "endocrino", "suggested_icd": "R73", "priority": 8},
        {"term": "hiperglucemia", "specialty": "endocrino", "suggested_icd": "R73", "priority": 9},
        {"term": "hipoglucemia", "specialty": "endocrino", "suggested_icd": "E16", "priority": 9},
        {"term": "cetoacidosis diabetica", "specialty": "endocrino", "suggested_icd": "E10", "priority": 8},
        {"term": "estado hiperosmolar", "specialty": "endocrino", "suggested_icd": "E11", "priority": 7},
        {"term": "resistencia a la insulina", "specialty": "endocrino", "suggested_icd": "E88", "priority": 7},
        {"term": "neuropatia diabetica", "specialty": "endocrino", "suggested_icd": "E114", "priority": 7},
        {"term": "nefropatia diabetica", "specialty": "endocrino", "suggested_icd": "E112", "priority": 7},
        {"term": "retinopatia diabetica", "specialty": "endocrino", "suggested_icd": "E113", "priority": 7},
        {"term": "pie diabetico", "specialty": "endocrino", "suggested_icd": "E115", "priority": 8},
        {"term": "ulcera diabetica", "specialty": "endocrino", "suggested_icd": "L97", "priority": 6},
        {"term": "obesidad", "specialty": "endocrino", "suggested_icd": "E66", "priority": 9},
        {"term": "sobrepeso", "specialty": "endocrino", "suggested_icd": "E66", "priority": 7},
        {"term": "sindrome metabolico", "specialty": "endocrino", "suggested_icd": "E88", "priority": 8},
        {"term": "dislipidemia", "specialty": "endocrino", "suggested_icd": "E78", "priority": 8},
        {"term": "hipertrigliceridemia", "specialty": "endocrino", "suggested_icd": "E78", "priority": 7},
        {"term": "higado graso", "specialty": "endocrino", "suggested_icd": "K76", "priority": 6},

        # Cardiovascular
        {"term": "hipertension", "specialty": "cardiologia", "suggested_icd": "I10", "priority": 10},
        {"term": "hipertension arterial", "specialty": "cardiologia", "suggested_icd": "I10", "priority": 10},
        {"term": "crisis hipertensiva", "specialty": "cardiologia", "suggested_icd": "I16", "priority": 8},
        {"term": "dolor toracico", "specialty": "cardiologia", "suggested_icd": "R07", "priority": 9},
        {"term": "angina", "specialty": "cardiologia", "suggested_icd": "I20", "priority": 8},
        {"term": "infarto agudo de miocardio", "specialty": "cardiologia", "suggested_icd": "I21", "priority": 8},
        {"term": "insuficiencia cardiaca", "specialty": "cardiologia", "suggested_icd": "I50", "priority": 9},
        {"term": "fibrilacion auricular", "specialty": "cardiologia", "suggested_icd": "I48", "priority": 7},
        {"term": "taquicardia", "specialty": "cardiologia", "suggested_icd": "R00", "priority": 6},
        {"term": "bradicardia", "specialty": "cardiologia", "suggested_icd": "R00", "priority": 5},
        {"term": "disnea", "specialty": "cardiologia", "suggested_icd": "R06", "priority": 8},
        {"term": "edema", "specialty": "cardiologia", "suggested_icd": "R60", "priority": 6},

        # Renal / cardiometabolic
        {"term": "enfermedad renal cronica", "specialty": "medicina interna", "suggested_icd": "N18", "priority": 8},
        {"term": "proteinuria", "specialty": "medicina interna", "suggested_icd": "R80", "priority": 6},
        {"term": "insuficiencia renal aguda", "specialty": "medicina interna", "suggested_icd": "N17", "priority": 7},

        # Infectious
        {"term": "fiebre", "specialty": "infecciosas", "suggested_icd": "R50", "priority": 9},
        {"term": "sepsis", "specialty": "infecciosas", "suggested_icd": "A41", "priority": 9},
        {"term": "choque septico", "specialty": "infecciosas", "suggested_icd": "R57", "priority": 7},
        {"term": "neumonia", "specialty": "infecciosas", "suggested_icd": "J18", "priority": 8},
        {"term": "bronquitis", "specialty": "infecciosas", "suggested_icd": "J20", "priority": 5},
        {"term": "infeccion urinaria", "specialty": "infecciosas", "suggested_icd": "N39", "priority": 8},
        {"term": "pielonefritis", "specialty": "infecciosas", "suggested_icd": "N10", "priority": 7},
        {"term": "gastroenteritis", "specialty": "infecciosas", "suggested_icd": "A09", "priority": 6},
        {"term": "celulitis", "specialty": "infecciosas", "suggested_icd": "L03", "priority": 6},
        {"term": "absceso", "specialty": "infecciosas", "suggested_icd": "L02", "priority": 5},

        # Respiratory / general urgent
        {"term": "tos", "specialty": "medicina general", "suggested_icd": "R05", "priority": 5},
        {"term": "dolor abdominal", "specialty": "medicina general", "suggested_icd": "R10", "priority": 6},
        {"term": "cefalea", "specialty": "medicina general", "suggested_icd": "R51", "priority": 5},
        {"term": "vomito", "specialty": "medicina general", "suggested_icd": "R11", "priority": 5},
        {"term": "diarrea", "specialty": "medicina general", "suggested_icd": "R19", "priority": 5},
    ]

    # Materialize normalized_term using the project's canonical normalizer.
    out: list[dict[str, object]] = []
    for t in terms:
        term = str(t["term"]).strip()
        if not term:
            continue
        out.append(
            {
                "term_raw": term,
                "term_normalized": normalize_query(term),
                "category": str(t.get("specialty") or "general").strip().lower(),
                "suggested_icd": (str(t.get("suggested_icd")).strip().upper() if t.get("suggested_icd") else None),
                "priority": int(t.get("priority") or 0),
            }
        )

    # Ensure uniqueness by term_normalized within the seed set.
    seen: set[str] = set()
    deduped: list[dict[str, object]] = []
    for row in out:
        key = str(row["term_normalized"])
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


def seed_clinical_dictionary(batch_size: int = 200) -> None:
    _configure_logging()

    db: Session = SessionLocal()
    try:
        # Idempotent: skip if table already has rows.
        if db.query(ClinicalDictionary).first():
            logger.info("clinical_dictionary already has data; skipping seed")
            return

        bind = db.get_bind()
        inspector = inspect(bind)
        cols = {c["name"] for c in inspector.get_columns("clinical_dictionary")}
        has_priority = "priority" in cols

        seed_rows = _seed_terms()
        if not seed_rows:
            logger.info("No seed rows generated")
            return

        inserted_total = 0

        for start in range(0, len(seed_rows), batch_size):
            chunk = seed_rows[start : start + batch_size]
            try:
                objects: list[ClinicalDictionary] = []
                for r in chunk:
                    obj = ClinicalDictionary(
                        term_raw=str(r["term_raw"]),
                        term_normalized=str(r["term_normalized"]),
                        category=str(r.get("category") or "general"),
                        suggested_icd=r.get("suggested_icd"),
                    )

                    # Optional: only set if DB has such column.
                    if has_priority:
                        setattr(obj, "priority", int(r.get("priority") or 0))

                    objects.append(obj)

                db.add_all(objects)
                db.commit()

                inserted_total += len(objects)
                logger.info("Seed progress: inserted=%s", inserted_total)

            except SQLAlchemyError:
                db.rollback()
                logger.exception("Seed batch failed; aborting seed")
                return

        logger.info("Seed completed. inserted=%s", inserted_total)

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
