from __future__ import annotations

import argparse
import logging
import re
import unicodedata
from dataclasses import dataclass

from sqlalchemy import Column, MetaData, String, Table, Text, bindparam, inspect, select, text, update
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

CODE_COMPACT_RE = re.compile(r"[^A-Z0-9]")
TOKEN_RE = re.compile(r"[a-z0-9]+")
MULTISPACE_RE = re.compile(r"\s+")
TAG_SPLIT_RE = re.compile(r"[|,;]")

DIGESTIVE_DESC_TERMS = {
    "estomago",
    "duodeno",
    "colon",
    "recto",
    "esofago",
    "intestino",
    "gastrointestinal",
}

BASE_SEARCH_TERMS = {
    "digestivo",
    "gastrointestinal",
    "tracto digestivo",
}

LOWER_BLEED_TERMS = {"recto", "colon", "ano"}
UPPER_BLEED_TERMS = {"duodeno", "estomago", "esofago"}

GI_TAG = "gastrointestinal"


@dataclass
class EnrichmentStats:
    scanned: int = 0
    eligible: int = 0
    updated_search_text: int = 0
    updated_tags: int = 0
    updated_rows: int = 0


def _strip_accents(value: str) -> str:
    nfkd = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _normalize_text(value: str) -> str:
    lowered = _strip_accents((value or "").lower())
    return MULTISPACE_RE.sub(" ", lowered).strip()


def _normalize_term(value: str) -> str:
    return _normalize_text(value)


def _compact_code(value: str) -> str:
    return CODE_COMPACT_RE.sub("", (value or "").upper().strip())


def _is_k00_k93(code: str) -> bool:
    compact = _compact_code(code)
    if not compact.startswith("K") or len(compact) < 3:
        return False
    prefix = compact[1:3]
    if not prefix.isdigit():
        return False
    major = int(prefix)
    return 0 <= major <= 93


def _word_tokens(value: str) -> set[str]:
    return set(TOKEN_RE.findall(_normalize_text(value)))


def _parse_search_terms(value: str) -> set[str]:
    if not value:
        return set()
    terms: set[str] = set()
    for raw in value.split("|"):
        term = _normalize_term(raw)
        if term:
            terms.add(term)
    return terms


def _parse_tag_terms(value: str) -> set[str]:
    if not value:
        return set()
    terms: set[str] = set()
    for raw in TAG_SPLIT_RE.split(value):
        term = _normalize_term(raw)
        if term:
            terms.add(term)
    return terms


def _join_terms(terms: set[str]) -> str:
    return " | ".join(sorted(terms))


def _description_matches_digestive(description: str) -> bool:
    tokens = _word_tokens(description)
    return bool(tokens & DIGESTIVE_DESC_TERMS)


def enrich_digestive_semantics(*, dry_run: bool = False) -> EnrichmentStats:
    metadata = MetaData()
    table = Table(
        "icd10_extended",
        metadata,
        Column("code", String, primary_key=True),
        Column("description", Text),
        Column("search_text", Text),
        Column("tags", Text),
        extend_existing=True,
    )

    stats = EnrichmentStats()
    updates: list[dict[str, str]] = []

    db: Session = SessionLocal()
    try:
        inspector = inspect(db.bind)
        if not inspector.has_table("icd10_extended"):
            logger.warning("Table icd10_extended not found. Skipping enrichment.")
            return stats

        rows = db.execute(
            select(
                table.c.code,
                table.c.description,
                table.c.search_text,
                table.c.tags,
            )
        ).all()
        stats.scanned = len(rows)

        for row in rows:
            code = row.code or ""
            description = row.description or ""

            is_chapter_xi = _is_k00_k93(code)
            has_digestive_desc = _description_matches_digestive(description)
            if not (is_chapter_xi or has_digestive_desc):
                continue

            stats.eligible += 1
            desc_tokens = _word_tokens(description)

            search_terms = _parse_search_terms(row.search_text or "")
            original_search = _join_terms(search_terms)

            search_terms.update(BASE_SEARCH_TERMS)
            if desc_tokens & LOWER_BLEED_TERMS:
                search_terms.add("digestiva baja")
            if desc_tokens & UPPER_BLEED_TERMS:
                search_terms.add("digestiva alta")

            new_search = _join_terms(search_terms)

            tag_terms = _parse_tag_terms(row.tags or "")
            original_tags = _join_terms(tag_terms)
            tag_terms.add(GI_TAG)
            new_tags = _join_terms(tag_terms)

            search_changed = new_search != original_search
            tags_changed = new_tags != original_tags
            if not (search_changed or tags_changed):
                continue

            if search_changed:
                stats.updated_search_text += 1
            if tags_changed:
                stats.updated_tags += 1
            stats.updated_rows += 1

            updates.append(
                {
                    "b_code": code,
                    "b_search_text": new_search,
                    "b_tags": new_tags,
                }
            )

        if updates and not dry_run:
            stmt = (
                update(table)
                .where(table.c.code == bindparam("b_code"))
                .values(
                    search_text=bindparam("b_search_text"),
                    tags=bindparam("b_tags"),
                )
            )
            db.execute(stmt, updates)
            db.commit()

        logger.info(
            "digestive_semantics_enrichment scanned=%s eligible=%s updated_rows=%s "
            "updated_search_text=%s updated_tags=%s dry_run=%s",
            stats.scanned,
            stats.eligible,
            stats.updated_rows,
            stats.updated_search_text,
            stats.updated_tags,
            dry_run,
        )

        validation_rows = db.execute(
            text(
                "SELECT code FROM icd10_extended "
                "WHERE LOWER(COALESCE(search_text, '')) LIKE '%digestiva%' "
                "LIMIT 10"
            )
        ).all()
        logger.info("validation digestiva sample codes=%s", [r[0] for r in validation_rows])

        return stats
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich icd10_extended digestive anatomy semantics")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    enrich_digestive_semantics(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
