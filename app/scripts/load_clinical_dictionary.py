from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re
import unicodedata
from uuid import uuid4

import pandas as pd
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.clinical_dictionary import ClinicalDictionary

logger = logging.getLogger(__name__)
_ICD_CODE_RE = re.compile(r"[^A-Z0-9]")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


_MULTISPACE_RE = re.compile(r"\s+")


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalize_text(value: object) -> str:
    text = str(value) if value is not None else ""
    text = text.strip().lower()
    text = _strip_accents(text)
    text = _MULTISPACE_RE.sub(" ", text)
    return text


def _pick_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def normalize_icd10_official(value: object) -> str:
    text = str(value) if value is not None else ""
    compact = _ICD_CODE_RE.sub("", text.strip().upper())
    if len(compact) > 3 and compact[0].isalpha() and compact[1:3].isdigit():
        return f"{compact[:3]}.{compact[3:]}"
    return compact


def load_dictionary(
    csv_path: str = "app/data/clinical_dictionary_clean.csv",
    batch_size: int = 5000,
) -> None:
    """Seed clinical_dictionary from a CSV file.

    Safety/performance:
    - Uses pandas for parsing.
    - Inserts in batches with bulk_insert_mappings.
    - Skips duplicates using (term, icd10_code) per-batch DB check.
    - Uses transactions (commit per batch) and rolls back on errors.
    """

    _configure_logging()

    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(path, dtype=str, encoding="utf-8")

    col_term = _pick_column(df, ("term", "term_raw", "term_normalized"))
    col_icd10 = _pick_column(df, ("icd10_code", "suggested_icd"))
    col_priority = _pick_column(df, ("priority",))

    if not col_term:
        raise ValueError("CSV must include one of: term, term_raw, term_normalized")
    if not col_icd10:
        raise ValueError("CSV must include one of: icd10_code, suggested_icd")

    term_series = df[col_term].fillna("").map(_normalize_text)
    icd10_series = df[col_icd10].fillna("").map(normalize_icd10_official)
    priority_series = (
        pd.to_numeric(df[col_priority], errors="coerce").fillna(1).astype(int)
        if col_priority
        else pd.Series([1] * len(df), dtype=int)
    )

    normalized_df = pd.DataFrame(
        {
            "term": term_series,
            "icd10_code": icd10_series,
            "priority": priority_series,
        }
    )

    normalized_df = normalized_df[
        (normalized_df["term"] != "")
        & (normalized_df["icd10_code"] != "")
    ]
    normalized_df = normalized_df.drop_duplicates(subset=["term", "icd10_code"], keep="first")

    total = len(normalized_df)
    if total == 0:
        logger.info("No rows to load (empty after normalization)")
        return

    inserted_total = 0
    skipped_total = 0

    db: Session = SessionLocal()
    try:
        logger.info("Loading clinical dictionary from %s (%s rows)", path.as_posix(), total)

        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            chunk = normalized_df.iloc[start:end]

            terms = chunk["term"].tolist()
            codes = chunk["icd10_code"].tolist()
            if not terms or not codes:
                continue

            try:
                with db.begin():
                    existing_pairs = set(
                        db.execute(
                            select(ClinicalDictionary.term, ClinicalDictionary.icd10_code).where(
                                ClinicalDictionary.term.in_(terms),
                                ClinicalDictionary.icd10_code.in_(codes),
                            )
                        )
                        .all()
                    )

                    mappings: list[dict[str, object]] = []
                    for row in chunk.itertuples(index=False):
                        term = getattr(row, "term")
                        icd10_code = getattr(row, "icd10_code")
                        priority = int(getattr(row, "priority") or 1)

                        if (term, icd10_code) in existing_pairs:
                            skipped_total += 1
                            continue

                        mappings.append(
                            {
                                "id": str(uuid4()),
                                "term": term,
                                "icd10_code": icd10_code,
                                "priority": priority,
                            }
                        )

                    if mappings:
                        db.bulk_insert_mappings(ClinicalDictionary, mappings)
                        inserted_total += len(mappings)

                logger.info(
                    "Progress: %s/%s rows processed | inserted=%s skipped=%s",
                    end,
                    total,
                    inserted_total,
                    skipped_total,
                )

            except SQLAlchemyError:
                logger.exception("Failed loading batch [%s:%s]", start, end)
                raise

        logger.info("Done. inserted=%s skipped=%s", inserted_total, skipped_total)

    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load clinical dictionary into PostgreSQL")
    parser.add_argument(
        "--csv",
        default="app/data/clinical_dictionary_clean.csv",
        help="Path to clinical_dictionary_clean.csv",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Batch size for bulk inserts",
    )
    args = parser.parse_args()

    load_dictionary(csv_path=args.csv, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
