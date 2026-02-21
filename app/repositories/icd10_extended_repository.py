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
import os
import re
from dataclasses import dataclass
from typing import List, Optional, Sequence

from sqlalchemy import (
    and_,
    Column,
    Float,
    MetaData,
    String,
    Table,
    Text,
    bindparam,
    case,
    func,
    literal,
    or_,
    select,
    text,
)
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
    priority: float = 0.0
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
    priority: float = 0.0
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
        Column("priority", Text, nullable=True),
        Column("priority_score", Float, nullable=True, server_default=text("0")),
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

    @staticmethod
    def _bool_as_float(expr):
        return case((expr, literal(1.0)), else_=literal(0.0))

    def _log_stmt_debug(self, stmt, params: dict) -> None:
        try:
            bind = self._db.get_bind()
            compiled = stmt.compile(dialect=bind.dialect, compile_kwargs={"literal_binds": False})
            logger.warning("icd10_extended.search_candidates sql=%s params=%s", str(compiled), params)
        except Exception:
            logger.exception("icd10_extended.search_candidates failed to compile debug SQL")

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
        force_no_similarity: bool = False,
        min_hits: Optional[int] = None,
        anatomical_term: Optional[str] = None,
    ) -> List[ExtendedICD10Candidate]:
        """Search icd10_extended using hybrid trigram similarity + clinical boosts.

        Parameters
        ----------
        query:
            Already-normalised search string (lowercase, accent-stripped).
        limit:
            Maximum number of candidates to return.
        tags_filter:
            Optional list of tags. NOTE: current behavior is EXCLUSIONARY:
            if provided, results must match at least one tag.
            (If you want "bonus but not exclusion", we can refactor ranking later.)

        Returns
        -------
        List of ``ExtendedICD10Candidate`` ordered by composite score desc.
        """
        # Hybrid clinical ranking engine v1
        logger.warning(
            "icd10_extended.search_candidates raw_query=%r query_len=%s limit=%s tags_filter=%r query_is_code=%s force_no_similarity=%s",
            query,
            len(query or ""),
            limit,
            tags_filter,
            query_is_code,
            force_no_similarity,
        )
        if not query:
            logger.warning("icd10_extended.search_candidates query is empty; returning []")
            return []

        compact_query = (query or "").strip().replace(" ", "")
        query_is_code = query_is_code or bool(ICD_CODE_QUERY_RE.match(compact_query))
        
        t = self._table
        
        # Hybrid clinical ranking engine v1.1 – exact word boost added
        similarity_score = func.similarity(func.coalesce(t.c.search_text, ""), bindparam("query"))
 
        description_boost = case(
            (t.c.description.ilike(bindparam("query") + "%"), literal(0.3)),
            else_=literal(0.0),
        )
 
        parent_code_boost = case(
            (t.c.code.op("~")(literal("^[A-Z][0-9]{2}$")), literal(0.2)),
            else_=literal(0.0),
        )
 
        exact_word_boost = case(
            (
                func.coalesce(t.c.search_text, "").op("~*")(
                    func.concat(literal(r"\m"), bindparam("query"), literal(r"\M"))
                ),
                literal(0.25),
            ),
            else_=literal(0.0),
        )
        anatomical_term_param = bindparam("anatomical_term")
        anatomical_boost = case(
            (
                and_(
                    anatomical_term_param.is_not(None),
                    func.coalesce(t.c.search_text, "").ilike(
                        func.concat(literal("%"), anatomical_term_param, literal("%"))
                    ),
                ),
                literal(0.7),
            ),
            else_=literal(0.0),
        )
 
        # Hybrid clinical ranking engine v2.2 – dual automatic mode (code + similarity)
        hybrid_score = (
            similarity_score * literal(0.4)
            + description_boost
            + parent_code_boost
            + exact_word_boost
        ).label("score")

        if query_is_code:
            stmt = (
                select(
                    t.c.code,
                    t.c.description,
                    t.c.description_normalized,
                    literal(1.0).label("similarity"),
                    func.coalesce(t.c.priority_score, literal(0.0)).label("priority_score"),
                    func.coalesce(t.c.priority, "").label("priority_label"),
                    t.c.tags,
                    literal(False).label("exact_code_match"),
                    literal(False).label("prefix_match"),
                    literal(False).label("description_match"),
                )
                .where(
                    or_(
                        t.c.code.ilike(bindparam("query") + "%"),
                        func.replace(t.c.code, ".", "").ilike(bindparam("query") + "%"),
                    )
                )
                .order_by(t.c.code.asc())
                .limit(limit)
            )
        else:
            # Hybrid clinical ranking engine v2.3 – multi-token clinical intent scoring
            normalized_query = (query or "").strip()
            tokens = [tok for tok in normalized_query.split() if tok]

            if len(tokens) >= 2:
                primary_token = tokens[0]
                token_similarity_sum = sum(
                    (
                        func.similarity(func.coalesce(t.c.search_text, ""), literal(tok))
                        for tok in tokens
                    ),
                    literal(0.0),
                )
                prefix_boost = case(
                    (t.c.description.ilike(literal(primary_token) + "%"), literal(0.5)),
                    (t.c.description.ilike("%" + literal(primary_token) + "%"), literal(0.3)),
                    else_=literal(0.0),
                )
                all_tokens_boost = case(
                    (
                        and_(
                            *[
                                func.coalesce(t.c.search_text, "").ilike(f"%{tok}%")
                                for tok in tokens
                            ]
                        ),
                        literal(0.4),
                    ),
                    else_=literal(0.0),
                )
                # Refinement clínico: boost proporcional por cobertura de tokens en search_text.
                token_match_ratio_boost = literal(0.0)

                if tokens:
                    matched_tokens_score = literal(0.0)

                    for tok in tokens:
                        matched_tokens_score = matched_tokens_score + case(
                            (
                                func.coalesce(t.c.search_text, "").ilike(literal(f"%{tok}%")),
                                literal(1.0),
                            ),
                            else_=literal(0.0),
                        )

                    token_ratio = matched_tokens_score / literal(float(len(tokens)))
                    token_match_ratio_boost = token_ratio * literal(1.0)
                branch_similarity = (
                    (similarity_score * literal(0.30))
                    + (token_similarity_sum * literal(0.45))
                    + prefix_boost
                    + parent_code_boost
                    + all_tokens_boost
                    + token_match_ratio_boost
                    + anatomical_boost
                )
            else:
                branch_similarity = hybrid_score + anatomical_boost

            # Base query with hybrid scoring
            stmt = (
                select(
                    t.c.code,
                    t.c.description,
                    func.coalesce(t.c.description_normalized, "").label("description_normalized"),
                    branch_similarity.label("similarity"),  # Keep as similarity for response compatibility
                    func.coalesce(t.c.priority_score, literal(0.0)).label("priority_score"),
                    func.coalesce(t.c.priority, "").label("priority_label"),
                    func.coalesce(t.c.tags, "").label("tags"),
                    literal(False).label("exact_code_match"),
                    literal(False).label("prefix_match"),
                    literal(False).label("description_match"),
                )
                .where(
                    func.similarity(
                        func.coalesce(t.c.search_text, ""),
                        bindparam("query"),
                    )
                    > literal(0.08)
                )
                .order_by(text("similarity DESC"))
                .limit(limit)
            )
        
        # Optional tag filter (CURRENT behavior: exclusion)
        if tags_filter:
            tag_conditions = [func.coalesce(t.c.tags, "").ilike(f"%{tag}%") for tag in tags_filter]
            stmt = stmt.where(or_(*tag_conditions))
        
        params = {
            "query": query,
            "anatomical_term": anatomical_term,
        }
        
        self._log_stmt_debug(stmt, params)
        
        try:
            result = await self._db.execute(stmt, params)
            rows = result.all()
        except Exception as e:
            logger.exception("ICD10 extended search failed: %s", e)
            return []
        
        logger.warning("icd10_extended.search_candidates rows=%s query=%r", len(rows), query)
        
        return [
            ExtendedICD10Candidate(
                code=r.code,
                description=r.description,
                description_normalized=r.description_normalized or "",
                similarity=float(r.similarity or 0.0),
                priority=float(r.priority_score or 0.0),
                tags=r.tags or "",
                exact_code_match=bool(r.exact_code_match),
                prefix_match=bool(r.prefix_match),
                description_match=bool(r.description_match),
            )
            for r in rows
        ]

    async def search_by_exact_term(self, term: str, limit: int = 1) -> List[ExtendedICD10Candidate]:
        if not term:
            return []

        t = self._table
        stmt = (
            select(
                t.c.code,
                t.c.description,
                func.coalesce(t.c.description_normalized, "").label("description_normalized"),
                literal(0.0).label("similarity"),
                func.coalesce(t.c.priority_score, literal(0.0)).label("priority_score"),
                func.coalesce(t.c.tags, "").label("tags"),
                literal(False).label("exact_code_match"),
                literal(False).label("prefix_match"),
                literal(False).label("description_match"),
            )
            .where(func.coalesce(t.c.description_normalized, "").ilike(f"%{term}%"))
            .order_by(func.coalesce(t.c.priority_score, literal(0.0)).desc(), t.c.code.asc())
            .limit(limit)
        )

        try:
            result = await self._db.execute(stmt)
            rows = result.all()
        except Exception:
            logger.exception("icd10_extended.search_by_exact_term failed term=%r", term)
            return []

        return [
            ExtendedICD10Candidate(
                code=r.code,
                description=r.description,
                description_normalized=r.description_normalized or "",
                similarity=float(r.similarity or 0.0),
                priority=float(r.priority_score or 0.0),
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
            func.coalesce(t.c.priority_score, literal(0.0)).label("priority_score"),
            func.coalesce(t.c.priority, "").label("priority_label"),
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
            priority=float(row.priority_score or 0.0),
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
        """
        if not code:
            return []

        root = code.strip().upper().replace(".", "")
        if len(root) < 3:
            return []

        t = self._table
        code_compact = func.replace(func.upper(t.c.code), ".", "")
        stmt = (
            select(
                t.c.code,
                t.c.description,
                func.coalesce(t.c.description_normalized, "").label("description_normalized"),
                func.coalesce(t.c.search_text, "").label("search_text"),
                func.coalesce(t.c.priority_score, literal(0.0)).label("priority_score"),
                func.coalesce(t.c.priority, "").label("priority_label"),
                func.coalesce(t.c.tags, "").label("tags"),
            )
            .where(
                code_compact.like(f"{root}%"),
                func.length(code_compact) > len(root),
            )
            .order_by(func.coalesce(t.c.priority_score, literal(0.0)).desc(), t.c.code.asc())
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
                priority=float(r.priority_score or 0.0),
                tags=r.tags or "",
            )
            for r in rows
        ]
