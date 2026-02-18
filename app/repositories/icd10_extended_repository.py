"""Repository for the icd10_extended table.

This repository provides async access to the optimised icd10_extended table
which includes:
- search_text / description_normalized  (pre-processed for trigram search)
- trigram GIN index
- priority column
- tags column (comma-separated or JSONB — queried via text containment)

All methods are async and designed for use with ``AsyncSession``.
No other repositories are modified by this module.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, Text, func, literal, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.search_config import search_tuning

logger = logging.getLogger(__name__)

ICD_CODE_QUERY_RE = re.compile(r"^[A-Za-z]\d[0-9A-Za-z.]*$")

# ---------------------------------------------------------------------------
# Data transfer objects
# ---------------------------------------------------------------------------


@dataclass
class ExtendedICD10Candidate:
    """A single candidate row coming from icd10_extended."""

    code: str
    description: str
    description_normalized: str
    similarity: float = 0.0
    priority: int = 0
    tags: str = ""
    exact_code_match: bool = False
    prefix_match: bool = False
    description_match: bool = False


@dataclass
class ExtendedICD10Detail:
    """Full detail for a single code lookup."""

    code: str
    description: str
    description_normalized: str
    search_text: str
    priority: int = 0
    tags: str = ""


# ---------------------------------------------------------------------------
# Table reflection helper (cached per engine)
# ---------------------------------------------------------------------------

_table_cache: dict[int, Table] = {}


def _get_icd10_extended_table(bind_key: int, metadata: MetaData) -> Table:
    """Return a reflected or manually-defined Table object for icd10_extended.

    We define columns explicitly to avoid a synchronous ``autoload_with`` call
    inside an async context.  The column set matches the known DDL of the
    icd10_extended table.
    """
    if bind_key in _table_cache:
        return _table_cache[bind_key]

    table = Table(
        "icd10_extended",
        metadata,
        Column("code", String, primary_key=True),
        Column("description", Text, nullable=False),
        Column("description_normalized", Text, nullable=True),
        Column("search_text", Text, nullable=True),
        Column("priority", Integer, nullable=True, server_default=text("0")),
        Column("tags", Text, nullable=True),
        extend_existing=True,
    )
    _table_cache[bind_key] = table
    return table


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class ICD10ExtendedRepository:
    """Async repository for icd10_extended with trigram similarity support."""

    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self._metadata = MetaData()

    @property
    def _table(self) -> Table:
        engine = self._db.get_bind()
        return _get_icd10_extended_table(id(engine), self._metadata)

    # ------------------------------------------------------------------
    # search_candidates
    # ------------------------------------------------------------------

    async def search_candidates(
        self,
        query: str,
        limit: int = search_tuning.default_limit,
        *,
        tags_filter: Optional[Sequence[str]] = None,
        query_is_code: bool = False,
    ) -> List[ExtendedICD10Candidate]:
        """Search icd10_extended using trigram similarity + ILIKE + priority.

        Parameters
        ----------
        query:
            Already-normalised search string (lowercase, accent-stripped).
        limit:
            Maximum number of candidates to return.
        tags_filter:
            Optional list of tags; only rows whose ``tags`` column contains
            **any** of the given tags will receive a tag-match bonus.

        Returns
        -------
        List of ``ExtendedICD10Candidate`` ordered by composite score desc.
        """
        logger.warning(
            "icd10_extended.search_candidates raw_query=%r query_len=%s limit=%s tags_filter=%r query_is_code=%s",
            query,
            len(query or ""),
            limit,
            tags_filter,
            query_is_code,
        )
        if not query:
            logger.warning("icd10_extended.search_candidates query is empty; returning []")
            return []

        compact_query = (query or "").strip().replace(" ", "")
        query_is_code = query_is_code or bool(ICD_CODE_QUERY_RE.match(compact_query))

        t = self._table
        threshold = search_tuning.similarity_threshold

        code_col = func.coalesce(t.c.code, "")
        code_compact = func.replace(func.replace(func.upper(code_col), ".", ""), " ", "")
        desc_norm = func.coalesce(t.c.description_normalized, "")
        search_txt = func.coalesce(t.c.search_text, "")
        priority_col = func.coalesce(t.c.priority, 0)

        code_upper = func.upper(compact_query)
        compact_code_query = compact_query.replace(".", "").upper()

        # --- match expressions ---------------------------------------------------
        exact_code = (code_compact == compact_code_query) if query_is_code else (func.upper(code_col) == code_upper)
        prefix_code = code_compact.like(f"{compact_code_query}%") if query_is_code else code_col.ilike(f"{compact_query}%")
        desc_match = or_(
            desc_norm.ilike(f"%{query}%"),
            search_txt.ilike(f"%{query}%"),
        )

        # --- trigram similarity (requires pg_trgm) --------------------------------
        use_similarity = (len(query) >= 3) and (not query_is_code)
        logger.warning(
            "icd10_extended.search_candidates query_type=%s similarity_threshold=%.3f use_similarity=%s",
            "code" if query_is_code else "natural_language",
            threshold,
            use_similarity,
        )
        if use_similarity:
            sim_score = func.greatest(
                func.similarity(desc_norm, query),
                func.similarity(search_txt, query),
            )
        else:
            sim_score = literal(0.0)

        sim_filter = (sim_score > threshold) if use_similarity else literal(False)

        # --- composite ordering score ---------------------------------------------
        # priority_boost:  higher priority rows surface first
        # sim_score:       trigram similarity for fuzzy matches
        # exact / prefix:  strong boosts for code-level matches
        score = (
            literal(3.0) * func.cast(exact_code, Float)
            + literal(2.0) * func.cast(prefix_code, Float)
            + literal(1.5) * func.cast(desc_match, Float)
            + sim_score
            + literal(0.1) * func.cast(priority_col, Float)
        ).label("_rank_score")

        stmt = (
            select(
                t.c.code,
                t.c.description,
                desc_norm.label("description_normalized"),
                sim_score.label("similarity"),
                priority_col.label("priority"),
                func.coalesce(t.c.tags, "").label("tags"),
                exact_code.label("exact_code_match"),
                prefix_code.label("prefix_match"),
                desc_match.label("description_match"),
            )
            .where(or_(exact_code, prefix_code) if query_is_code else or_(exact_code, prefix_code, desc_match, sim_filter))
            .order_by(score.desc(), t.c.code.asc())
            .limit(limit)
        )

        # Optional tag containment filter (bonus, not exclusion)
        # Tags are stored as comma-separated text; we use ILIKE for portability.
        if tags_filter:
            tag_conditions = [func.coalesce(t.c.tags, "").ilike(f"%{tag}%") for tag in tags_filter]
            stmt = stmt.where(or_(*tag_conditions))

        try:
            result = await self._db.execute(stmt)
            rows = result.all()
        except Exception:
            # Safe fallback: if similarity / expression compilation fails,
            # return a strict exact/prefix code search instead of raising 500.
            logger.exception(
                "icd10_extended.search_candidates primary query failed; falling back to exact/prefix code search"
            )
            fallback_stmt = (
                select(
                    t.c.code,
                    t.c.description,
                    desc_norm.label("description_normalized"),
                    literal(0.0).label("similarity"),
                    priority_col.label("priority"),
                    func.coalesce(t.c.tags, "").label("tags"),
                    exact_code.label("exact_code_match"),
                    prefix_code.label("prefix_match"),
                    literal(False).label("description_match"),
                )
                .where(or_(exact_code, prefix_code))
                .order_by(exact_code.desc(), prefix_code.desc(), priority_col.desc(), t.c.code.asc())
                .limit(limit)
            )
            try:
                fallback_result = await self._db.execute(fallback_stmt)
                rows = fallback_result.all()
            except Exception:
                logger.exception("icd10_extended.search_candidates fallback query failed; returning []")
                return []
        logger.warning(
            "icd10_extended.search_candidates rows=%s query=%r",
            len(rows),
            query,
        )

        if not rows:
            # Diagnostics to determine if threshold/similarity is too restrictive.
            pre_similarity_stmt = (
                select(func.count())
                .select_from(t)
                .where(or_(exact_code, prefix_code, desc_match))
            )
            pre_similarity_count = (await self._db.execute(pre_similarity_stmt)).scalar() or 0

            top_similarity_stmt = (
                select(
                    t.c.code,
                    t.c.description,
                    sim_score.label("sim"),
                )
                .order_by(sim_score.desc(), t.c.code.asc())
                .limit(3)
            )
            top_similarity_rows = (await self._db.execute(top_similarity_stmt)).all()
            logger.warning(
                "icd10_extended.search_candidates diagnostics query=%r pre_similarity_count=%s top_similarity=%s",
                query,
                pre_similarity_count,
                [
                    (r.code, round(float(r.sim or 0.0), 4))
                    for r in top_similarity_rows
                ],
            )

        return [
            ExtendedICD10Candidate(
                code=r.code,
                description=r.description,
                description_normalized=r.description_normalized or "",
                similarity=float(r.similarity or 0.0),
                priority=int(r.priority or 0),
                tags=r.tags or "",
                exact_code_match=bool(r.exact_code_match),
                prefix_match=bool(r.prefix_match),
                description_match=bool(r.description_match),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # lookup_code
    # ------------------------------------------------------------------

    async def lookup_code(self, code: str) -> Optional[ExtendedICD10Detail]:
        """Return a single icd10_extended row by exact code match."""
        if not code:
            return None

        t = self._table
        stmt = select(
            t.c.code,
            t.c.description,
            func.coalesce(t.c.description_normalized, "").label("description_normalized"),
            func.coalesce(t.c.search_text, "").label("search_text"),
            func.coalesce(t.c.priority, 0).label("priority"),
            func.coalesce(t.c.tags, "").label("tags"),
        ).where(func.upper(t.c.code) == func.upper(code))

        result = await self._db.execute(stmt)
        row = result.first()
        if row is None:
            return None

        return ExtendedICD10Detail(
            code=row.code,
            description=row.description,
            description_normalized=row.description_normalized,
            search_text=row.search_text,
            priority=int(row.priority or 0),
            tags=row.tags or "",
        )

    # ------------------------------------------------------------------
    # expand_root_to_billable
    # ------------------------------------------------------------------

    async def expand_root_to_billable(
        self,
        code: str,
        *,
        limit: int = 20,
    ) -> List[ExtendedICD10Detail]:
        """Given a root/parent ICD-10 code (e.g. ``J18``), return billable
        (more specific) children that share the same prefix.

        This is useful when a clinician selects a category-level code and the
        system needs to suggest valid billable alternatives.
        """
        if not code:
            return []

        root = code.strip().upper().replace(".", "")
        if len(root) < 3:
            return []

        t = self._table
        # Match codes that start with the root prefix and are longer (more specific)
        code_compact = func.replace(func.upper(t.c.code), ".", "")
        stmt = (
            select(
                t.c.code,
                t.c.description,
                func.coalesce(t.c.description_normalized, "").label("description_normalized"),
                func.coalesce(t.c.search_text, "").label("search_text"),
                func.coalesce(t.c.priority, 0).label("priority"),
                func.coalesce(t.c.tags, "").label("tags"),
            )
            .where(
                code_compact.like(f"{root}%"),
                func.length(code_compact) > len(root),
            )
            .order_by(func.coalesce(t.c.priority, 0).desc(), t.c.code.asc())
            .limit(limit)
        )

        result = await self._db.execute(stmt)
        rows = result.all()

        return [
            ExtendedICD10Detail(
                code=r.code,
                description=r.description,
                description_normalized=r.description_normalized,
                search_text=r.search_text,
                priority=int(r.priority or 0),
                tags=r.tags or "",
            )
            for r in rows
        ]
