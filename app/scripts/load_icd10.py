from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.icd10 import ICD10

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _default_csv_path() -> Path:
    # Resolve relative to the repository/package, not the current working directory.
    # Works on Railway where the working dir may vary.
    return Path(__file__).resolve().parents[1] / "data" / "icd10_clean.csv"


def load_icd10(csv_path: str | None = None, batch_size: int = 5000) -> None:
    _configure_logging()

    path = Path(csv_path) if csv_path else _default_csv_path()
    if not path.exists():
        raise FileNotFoundError(f"ICD10 CSV file not found: {path}")

    db: Session = SessionLocal()
    try:
        # Idempotent: only load if table is empty.
        if db.query(ICD10).first():
            logger.info("ICD10 already loaded")
            return

        logger.info("Starting ICD10 load from %s", path.as_posix())

        inserted_total = 0
        batch: list[ICD10] = []

        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = (row.get("code") or "").strip()
                description = (row.get("description") or "").strip()
                if not code or not description:
                    continue

                batch.append(ICD10(code=code, description=description))

                if len(batch) >= batch_size:
                    try:
                        db.add_all(batch)
                        db.commit()
                        inserted_total += len(batch)
                        logger.info("Inserted %s ICD10 rows", inserted_total)
                        batch.clear()
                    except SQLAlchemyError:
                        db.rollback()
                        logger.exception("Failed inserting ICD10 batch")
                        return

        if batch:
            try:
                db.add_all(batch)
                db.commit()
                inserted_total += len(batch)
            except SQLAlchemyError:
                db.rollback()
                logger.exception("Failed inserting final ICD10 batch")
                return

        logger.info("ICD10 loaded successfully. Inserted rows=%s", inserted_total)

    except Exception:
        db.rollback()
        logger.exception("ICD10 load failed")
        return
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ICD-10 codes into PostgreSQL")
    parser.add_argument(
        "--csv",
        default=None,
        help="Path to icd10_clean.csv (defaults to app/data/icd10_clean.csv)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Batch size for bulk inserts",
    )
    args = parser.parse_args()

    load_icd10(csv_path=args.csv, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
