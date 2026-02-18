from __future__ import annotations

import argparse
import csv
import logging
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

from sqlalchemy import Column, MetaData, String, Table, Text, bindparam, func, select, update
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

_MULTISPACE_RE = re.compile(r"\s+")
_ICD_CODE_RE = re.compile(r"[^A-Z0-9]")


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalize_term(value: object) -> str:
    text = str(value) if value is not None else ""
    text = text.strip().lower()
    text = _strip_accents(text)
    return _MULTISPACE_RE.sub(" ", text)


def _normalize_icd10_official(value: object) -> str:
    text = str(value) if value is not None else ""
    compact = _ICD_CODE_RE.sub("", text.strip().upper())
    if len(compact) > 3 and compact[0].isalpha() and compact[1:3].isdigit():
        return f"{compact[:3]}.{compact[3:]}"
    return compact


def _compact_code(value: str) -> str:
    return _ICD_CODE_RE.sub("", (value or "").upper())


def _pick_column(headers: list[str], candidates: tuple[str, ...]) -> str | None:
    lower = {h.lower(): h for h in headers}
    for candidate in candidates:
        if candidate in lower:
            return lower[candidate]
    return None


def _parse_existing_search_text(value: str) -> set[str]:
    if not value:
        return set()
    terms: set[str] = set()
    for raw in value.split("|"):
        term = _normalize_term(raw)
        if term:
            terms.add(term)
    return terms


def _load_dictionary_terms(csv_path: Path) -> dict[str, set[str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        term_col = _pick_column(headers, ("term", "term_raw", "term_normalized"))
        code_col = _pick_column(headers, ("icd10_code", "suggested_icd"))
        if not term_col or not code_col:
            raise ValueError("CSV must include term + (icd10_code or suggested_icd)")

        by_code: dict[str, set[str]] = defaultdict(set)
        for row in reader:
            term = _normalize_term(row.get(term_col, ""))
            icd = _normalize_icd10_official(row.get(code_col, ""))
            compact = _compact_code(icd)
            if term and compact:
                by_code[compact].add(term)

    return by_code


def enrich_search_text(
    csv_path: str = "app/data/clinical_dictionary_clean.csv",
    *,
    batch_size: int = 500,
    dry_run: bool = False,
) -> None:
    source = Path(csv_path)
    if not source.exists():
        raise FileNotFoundError(f"Dictionary CSV not found: {source}")

    terms_by_code = _load_dictionary_terms(source)
    if not terms_by_code:
        logger.info("No terms found in dictionary CSV")
        return

    metadata = MetaData()
    table = Table(
        "icd10_extended",
        metadata,
        Column("code", String, primary_key=True),
        Column("search_text", Text),
        extend_existing=True,
    )
    code_compact_expr = func.replace(func.replace(func.upper(table.c.code), ".", ""), " ", "")
    update_stmt = (
        update(table)
        .where(code_compact_expr == bindparam("b_code_compact"))
        .values(search_text=bindparam("b_search_text"))
    )

    compacts = list(terms_by_code.keys())
    updated_rows = 0
    missing_codes = 0

    db: Session = SessionLocal()
    try:
        logger.info("enrich_icd10_extended_search_text started codes=%s batch_size=%s", len(compacts), batch_size)

        for start in range(0, len(compacts), batch_size):
            chunk_codes = compacts[start : start + batch_size]
            chunk_terms = {code: terms_by_code[code] for code in chunk_codes}

            rows = db.execute(
                select(
                    code_compact_expr.label("code_compact"),
                    func.coalesce(table.c.search_text, "").label("search_text"),
                ).where(code_compact_expr.in_(chunk_codes))
            ).all()

            found = {row.code_compact for row in rows}
            missing_codes += len(set(chunk_codes) - found)

            updates: list[dict[str, str]] = []
            for row in rows:
                merged = _parse_existing_search_text(row.search_text)
                merged.update(chunk_terms.get(row.code_compact, set()))
                if not merged:
                    continue
                merged_text = " | ".join(sorted(merged))
                if merged_text != (row.search_text or ""):
                    updates.append(
                        {
                            "b_code_compact": row.code_compact,
                            "b_search_text": merged_text,
                        }
                    )

            if updates:
                if not dry_run:
                    db.execute(update_stmt, updates)
                    db.commit()
                updated_rows += len(updates)

            logger.info(
                "progress=%s/%s updated=%s missing_codes=%s",
                min(start + batch_size, len(compacts)),
                len(compacts),
                updated_rows,
                missing_codes,
            )

        logger.info(
            "enrich_icd10_extended_search_text done updated=%s missing_codes=%s dry_run=%s",
            updated_rows,
            missing_codes,
            dry_run,
        )
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich icd10_extended.search_text from clinical dictionary CSV")
    parser.add_argument("--csv", default="app/data/clinical_dictionary_clean.csv")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    enrich_search_text(csv_path=args.csv, batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
