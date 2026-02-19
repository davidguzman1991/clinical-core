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
    Integer,
    MetaData,
    String,
    Table,
    Text,
    bindparam,
    case,
    cast,
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

    @staticmethod
    def _bool_as_float(expr):
        return case((expr, literal(1.0)), else_=literal(0.0))

    @staticmethod
    def _priority_as_float(col):
        priority_text = func.lower(func.trim(func.coalesce(cast(col, Text), literal(""))))
        numeric_pattern = r"^[0-9]+(\.[0-9]+)?$"
        return case(
            (priority_text == literal(""), literal(0.0)),
            (priority_text == literal("high"), literal(1.0)),
            (priority_text == literal("medium"), literal(0.6)),
            (priority_text == literal("low"), literal(0.2)),
            (priority_text.op("~")(numeric_pattern), cast(priority_text, Float)),
            else_=literal(0.0),
        )

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
    ) -> List[ExtendedICD10Candidate]:
        """Search icd10_extended using trigram similarity + ILIKE + priority.

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
        if query_is_code:
            tokens = []
        else:
            raw_tokens = [t for t in (query or "").split() if t]
            incomplete_last_token = (
                bool(query)
                and (not query.endswith(" "))
                and bool(raw_tokens)
                and (len(raw_tokens[-1]) < 4)
            )
            scoring_tokens = raw_tokens[:-1] if incomplete_last_token else raw_tokens
            tokens = [t for t in scoring_tokens if len(t) >= 4][:5]
        token_count = len(tokens)

        t = self._table
        try:
            threshold = float(search_tuning.similarity_threshold or 0.2)
        except (TypeError, ValueError):
            threshold = 0.2

        code_col = func.coalesce(t.c.code, "")
        code_compact = func.replace(func.replace(func.upper(code_col), ".", ""), " ", "")
        desc_norm = func.coalesce(t.c.description_normalized, "")
        search_txt = func.coalesce(t.c.search_text, "")
        priority_col = self._priority_as_float(t.c.priority)

        compact_code_query = compact_query.replace(".", "").upper()

        # --- match expressions ---------------------------------------------------
        # Bind-safe: in natural language, do NOT require code binds.
        exact_code = code_compact == bindparam("compact_code_query") if query_is_code else literal(False)
        prefix_code = code_compact.like(bindparam("compact_prefix_query")) if query_is_code else literal(False)

        # Substring match (fast + predictable)
        desc_match = or_(
            desc_norm.ilike(bindparam("desc_query")),
            search_txt.ilike(bindparam("desc_query")),
        )

        # --- trigram similarity (requires pg_trgm) --------------------------------
        use_similarity = (len(query) >= 3) and (not query_is_code) and (not force_no_similarity)
        logger.warning(
            "icd10_extended.search_candidates query_type=%s similarity_used=%s similarity_threshold=%.3f",
            "code" if query_is_code else "natural_language",
            use_similarity,
            threshold,
        )

        if use_similarity:
            sim_score = func.greatest(
                func.similarity(desc_norm, bindparam("query")),
                func.similarity(search_txt, bindparam("query")),
            )
            # ✅ KEY FIX:
            # Allow candidates into WHERE via similarity threshold even if no substring match.
            sim_match = sim_score >= bindparam("similarity_threshold")
        else:
            sim_score = literal(0.0)
            sim_match = literal(False)

        token_match_exprs = []
        token_param_names: list[str] = []
        for i, token in enumerate(tokens):
            token_param = f"token_query_{i}"
            token_param_names.append(token_param)
            token_match_exprs.append(
                or_(
                    desc_norm.ilike(bindparam(token_param)),
                    search_txt.ilike(bindparam(token_param)),
                )
            )

        if token_match_exprs:
            token_hit_count = sum(
                (case((token_expr, literal(1.0)), else_=literal(0.0)) for token_expr in token_match_exprs),
                literal(0.0),
            ).label("token_hit_count")
            if token_count >= 2:
                default_min_hits = 2
            elif token_count == 1:
                default_min_hits = 1
            else:
                default_min_hits = 0
            if token_count >= 2 and min_hits is not None:
                effective_min_hits = max(1, min(int(min_hits), token_count))
            else:
                effective_min_hits = default_min_hits
            token_gate_match = and_(
                literal(effective_min_hits > 0),
                token_hit_count >= literal(effective_min_hits),
            )
            token_any_match = or_(*token_match_exprs)
        else:
            token_hit_count = literal(0.0).label("token_hit_count")
            default_min_hits = 0
            effective_min_hits = 0
            token_gate_match = literal(False)
            token_any_match = literal(False)

        if os.getenv("SEARCH_DEBUG") == "1":
            logger.warning(
                "icd10_extended.search_candidates token_debug query=%r tokens=%s min_hits=%s query_is_code=%s similarity_used=%s",
                query,
                tokens,
                effective_min_hits,
                query_is_code,
                use_similarity,
            )

        code_score = (
            literal(3.0) * self._bool_as_float(exact_code)
            + literal(2.0) * self._bool_as_float(prefix_code)
            + literal(0.1) * priority_col
        ).label("_rank_score")

        text_score = (
            literal(3.0) * self._bool_as_float(exact_code)
            + literal(2.0) * self._bool_as_float(prefix_code)
            + literal(1.5) * self._bool_as_float(desc_match)
            + literal(0.8) * token_hit_count
            + sim_score
            + literal(0.1) * priority_col
        ).label("_rank_score")

        base_select = select(
            t.c.code,
            t.c.description,
            desc_norm.label("description_normalized"),
            sim_score.label("similarity"),
            priority_col.label("priority"),
            func.coalesce(t.c.tags, "").label("tags"),
            exact_code.label("exact_code_match"),
            prefix_code.label("prefix_match"),
            desc_match.label("description_match"),
            token_hit_count,
        )

        if query_is_code:
            search_filter = or_(exact_code, prefix_code)
            stmt = (
                base_select.where(search_filter)
                .order_by(code_score.desc(), t.c.code.asc())
                .limit(limit)
            )
        else:
            if token_count >= 2:
                token_hits_ok = token_hit_count >= literal(effective_min_hits)
                sim_gate = and_(sim_match, token_hits_ok)
                search_filter = or_(desc_match, token_hits_ok, sim_gate)
            elif token_count == 1:
                search_filter = or_(desc_match, sim_match)
            else:
                # Keep current behavior for empty tokenization.
                search_filter = or_(desc_match, sim_match)
            stmt = (
                base_select.where(search_filter)
                .order_by(text_score.desc(), t.c.code.asc())
                .limit(limit)
            )

        # Optional tag filter (CURRENT behavior: exclusion)
        if tags_filter:
            tag_conditions = [func.coalesce(t.c.tags, "").ilike(f"%{tag}%") for tag in tags_filter]
            stmt = stmt.where(or_(*tag_conditions))

        params = {
            "query": query,
            "compact_query": compact_query,
            "compact_code_query": compact_code_query,
            "prefix_query": f"{compact_query}%",
            "compact_prefix_query": f"{compact_code_query}%",
            "desc_query": f"%{query}%",
            "similarity_threshold": threshold,
        }
        for i, token in enumerate(tokens):
            params[f"token_query_{i}"] = f"%{token}%"

        expected_binds = {"desc_query"}
        if query_is_code:
            expected_binds.update({"compact_code_query", "compact_prefix_query"})
        if use_similarity:
            expected_binds.update({"query", "similarity_threshold"})
        expected_binds.update(token_param_names)

        missing_binds = expected_binds - set(params.keys())
        if missing_binds:
            logger.warning(
                "icd10_extended.search_candidates missing bind params: %s; query=%r",
                sorted(missing_binds),
                query,
            )

        self._log_stmt_debug(stmt, params)

        try:
            result = await self._db.execute(stmt, params)
            rows = result.all()
        except Exception:
            try:
                await self._db.rollback()
            except Exception:
                logger.exception("icd10_extended.search_candidates rollback failed")

            logger.exception("ICD10 extended search failed, switching to fallback")

            fallback_stmt = (
                select(
                    t.c.code,
                    t.c.description,
                    desc_norm.label("description_normalized"),
                    literal(0.0).label("similarity"),
                    priority_col.label("priority"),
                    func.coalesce(t.c.tags, "").label("tags"),
                    (code_compact == bindparam("compact_code_query")).label("exact_code_match"),
                    code_compact.like(bindparam("compact_prefix_query")).label("prefix_match"),
                    literal(False).label("description_match"),
                    literal(0.0).label("token_hit_count"),
                )
                .where(
                    or_(
                        code_compact == bindparam("compact_code_query"),
                        code_compact.like(bindparam("compact_prefix_query")),
                    )
                )
                .order_by(
                    (code_compact == bindparam("compact_code_query")).desc(),
                    code_compact.like(bindparam("compact_prefix_query")).desc(),
                    priority_col.desc(),
                    t.c.code.asc(),
                )
                .limit(limit)
            )
            try:
                fallback_result = await self._db.execute(fallback_stmt, params)
                rows = fallback_result.all()
            except Exception:
                try:
                    await self._db.rollback()
                except Exception:
                    logger.exception("icd10_extended.search_candidates fallback rollback failed")
                logger.exception("icd10_extended.search_candidates fallback query failed; returning []")
                return []

        logger.warning("icd10_extended.search_candidates rows=%s query=%r", len(rows), query)
        if os.getenv("SEARCH_DEBUG") == "1" and token_count >= 2:
            debug_top = [
                (
                    r.code,
                    round(float(r.similarity or 0.0), 4),
                    int(float(getattr(r, "token_hit_count", 0.0) or 0.0)),
                )
                for r in rows[:3]
            ]
            logger.warning(
                "icd10_extended.search_candidates top3_debug query=%r token_count=%s min_hits=%s top3=%s",
                query,
                token_count,
                effective_min_hits,
                debug_top,
            )

        if not rows:
            # Diagnostics: count rows that match pre-filters (substring/code/sim)
            pre_filters = search_filter

            pre_similarity_stmt = select(func.count()).select_from(t).where(pre_filters)
            pre_similarity_count = (await self._db.execute(pre_similarity_stmt, params)).scalar() or 0

            top_similarity_stmt = (
                select(t.c.code, t.c.description, sim_score.label("sim"))
                .order_by(sim_score.desc(), t.c.code.asc())
                .limit(3)
            )
            top_similarity_rows = (await self._db.execute(top_similarity_stmt, params)).all()
            logger.warning(
                "icd10_extended.search_candidates diagnostics query=%r pre_similarity_count=%s top_similarity=%s",
                query,
                pre_similarity_count,
                [(r.code, round(float(r.sim or 0.0), 4)) for r in top_similarity_rows],
            )

        return [
            ExtendedICD10Candidate(
                code=r.code,
                description=r.description,
                description_normalized=r.description_normalized or "",
                similarity=float(r.similarity or 0.0),
                priority=float(r.priority or 0.0),
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
            self._priority_as_float(t.c.priority).label("priority"),
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
            priority=float(row.priority or 0.0),
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
                self._priority_as_float(t.c.priority).label("priority"),
                func.coalesce(t.c.tags, "").label("tags"),
            )
            .where(
                code_compact.like(f"{root}%"),
                func.length(code_compact) > len(root),
            )
            .order_by(self._priority_as_float(t.c.priority).desc(), t.c.code.asc())
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
                priority=float(r.priority or 0.0),
                tags=r.tags or "",
            )
            for r in rows
        ]
