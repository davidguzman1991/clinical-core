from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re
import unicodedata

import pandas as pd
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.clinical_dictionary import ClinicalDictionary

logger = logging.getLogger(__name__)


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


def load_dictionary(
    csv_path: str = "app/data/clinical_dictionary_clean.csv",
    batch_size: int = 5000,
) -> None:
    """Seed clinical_dictionary from a CSV file.

    Safety/performance:
    - Uses pandas for parsing.
    - Inserts in batches with bulk_insert_mappings.
    - Skips duplicates using term_normalized (per-batch DB check).
    - Uses transactions (commit per batch) and rolls back on errors.
    """

    _configure_logging()

    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(path, dtype=str, encoding="utf-8")

    # Support both formats:
    # - Old: term_raw, term_normalized, category
    # - New: term, specialty, suggested_icd
    col_term_raw = _pick_column(df, ("term_raw", "term"))
    col_term_norm = _pick_column(df, ("term_normalized",))
    col_category = _pick_column(df, ("category", "specialty"))
    col_suggested_icd = _pick_column(df, ("suggested_icd",))

    if not col_term_raw:
        raise ValueError("CSV must include 'term_raw' or 'term'")

    term_raw_series = df[col_term_raw].fillna("").map(_normalize_text)
    if col_term_norm:
        term_norm_series = df[col_term_norm].fillna("").map(_normalize_text)
    else:
        term_norm_series = term_raw_series

    if col_category:
        category_series = df[col_category].fillna("").map(_normalize_text)
    else:
        category_series = pd.Series(["general"] * len(df))

    category_series = category_series.replace("", "general")

    if col_suggested_icd:
        suggested_icd_series = df[col_suggested_icd].fillna("").map(_normalize_text)
        suggested_icd_series = suggested_icd_series.replace("", None)
    else:
        suggested_icd_series = pd.Series([None] * len(df))

    normalized_df = pd.DataFrame(
        {
            "term_raw": term_raw_series,
            "term_normalized": term_norm_series,
            "category": category_series,
            "suggested_icd": suggested_icd_series,
        }
    )

    normalized_df = normalized_df[(normalized_df["term_raw"] != "") & (normalized_df["term_normalized"] != "")]
    normalized_df = normalized_df.drop_duplicates(subset=["term_normalized"], keep="first")

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

            norms = chunk["term_normalized"].tolist()
            if not norms:
                continue

            try:
                with db.begin():
                    existing = set(
                        db.execute(
                            select(ClinicalDictionary.term_normalized).where(
                                ClinicalDictionary.term_normalized.in_(norms)
                            )
                        )
                        .scalars()
                        .all()
                    )

                    mappings: list[dict[str, object]] = []
                    for row in chunk.itertuples(index=False):
                        term_raw = getattr(row, "term_raw")
                        term_normalized = getattr(row, "term_normalized")
                        category = getattr(row, "category") or "general"
                        suggested_icd = getattr(row, "suggested_icd")

                        if term_normalized in existing:
                            skipped_total += 1
                            continue

                        mappings.append(
                            {
                                "term_raw": term_raw,
                                "term_normalized": term_normalized,
                                "category": category,
                                "suggested_icd": suggested_icd,
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
