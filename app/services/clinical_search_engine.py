"""Clinical Search Engine — unified orchestrator.

This module implements the central search pipeline for Clinical Core:

1. **Normalize** the incoming query (accent-strip, lowercase, whitespace collapse).
2. **Detect intent** via simple keyword heuristics (infection, cardiometabolic …).
3. **Retrieve** candidates from ``icd10_extended`` (primary) or ``icd10`` (fallback).
4. **Rank** results using configurable weights, intent alignment, and priority boost.
5. **Return** structured ``ClinicalSearchResult`` objects.

The engine is async and designed to be called from FastAPI async endpoints.
It does **not** modify search logs, suggestions, dictionary, or learning modules.
"""

from __future__ import annotations

import logging
import os
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.search_config import (
    ClinicalIntentConfig,
    RankingWeights,
    SearchFeatureFlags,
    SearchTuning,
    clinical_intent_config,
    ranking_weights,
    search_feature_flags,
    search_tuning,
)
from app.repositories.icd10_extended_repository import (
    ExtendedICD10Candidate,
    ICD10ExtendedRepository,
)

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"[a-z0-9.]+")
ICD_CODE_RE = re.compile(r"^[A-Za-z]\d{2,4}(\.\d{0,2})?$")
STOPWORDS_ES = {
    "de",
    "la",
    "del",
    "el",
    "los",
    "las",
    "y",
    "en",
    "con",
    "por",
    "para",
    "al",
    "un",
    "una",
    "unos",
    "unas",
    "a",
    "o",
    "u",
    "que",
    "se",
    "su",
}


# ---------------------------------------------------------------------------
# Result schema
# ---------------------------------------------------------------------------

@dataclass
class MatchFeatures:
    """Bit-flags and continuous signals that explain *why* a result matched."""

    exact_code: bool = False
    prefix_code: bool = False
    description_match: bool = False
    trigram_similarity: float = 0.0
    priority: int = 0
    intent_aligned: bool = False
    tag_matched: bool = False


@dataclass
class ClinicalSearchResult:
    """Structured result returned by the engine."""

    code: str
    label: str
    score: float
    source: str  # "icd10_extended" | "icd10"
    match_features: MatchFeatures = field(default_factory=MatchFeatures)
    explanation: str = ""


# ---------------------------------------------------------------------------
# Structured search event (for logging without perf impact)
# ---------------------------------------------------------------------------

@dataclass
class SearchEvent:
    """Lightweight event emitted after every search for structured logging."""

    query_raw: str
    query_normalized: str
    intent: Optional[str]
    source: str
    candidate_count: int
    result_count: int
    duration_ms: float
    top_code: Optional[str] = None
    top_score: Optional[float] = None


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class ClinicalSearchEngine:
    """Async orchestrator that unifies clinical ICD-10 search."""

    def __init__(
        self,
        db: AsyncSession,
        *,
        flags: SearchFeatureFlags = search_feature_flags,
        weights: RankingWeights = ranking_weights,
        tuning: SearchTuning = search_tuning,
        intent_config: ClinicalIntentConfig = clinical_intent_config,
    ) -> None:
        self._db = db
        self._flags = flags
        self._weights = weights
        self._tuning = tuning
        self._intent_config = intent_config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def search(
        self,
        raw_query: str,
        *,
        limit: int | None = None,
        tags_filter: Optional[Sequence[str]] = None,
    ) -> List[ClinicalSearchResult]:
        """Execute the full search pipeline and return ranked results."""
        t0 = time.perf_counter()
        effective_limit = min(limit or self._tuning.default_limit, self._tuning.max_limit)
        candidate_limit = effective_limit * self._tuning.candidate_multiplier

        try:
            logger.warning(
                "clinical_search_engine.search raw_query=%r effective_limit=%s candidate_limit=%s "
                "similarity_threshold=%.3f default_limit=%s",
                raw_query,
                effective_limit,
                candidate_limit,
                self._tuning.similarity_threshold,
                self._tuning.default_limit,
            )

            raw_direct = (raw_query or "").strip()
            is_code_query = self._is_code_query(raw_direct)

            # 1. Normalize only for natural language queries
            if is_code_query:
                query_for_repo = self._normalize_code_query(raw_direct)
                normalized = query_for_repo
                intent = None
            else:
                normalized = self._normalize_query(raw_query)
                if not normalized:
                    logger.warning("clinical_search_engine.search normalized query is empty; returning []")
                    return []
                query_for_repo = normalized
                intent = self._detect_intent(normalized) if self._flags.enable_intent_detection else None

            use_similarity = (not is_code_query) and len(query_for_repo) >= 3
            logger.warning(
                "clinical_search_engine.search query_type=%s similarity_used=%s normalized_query=%r raw_stripped=%r",
                "code" if is_code_query else "natural_language",
                use_similarity,
                query_for_repo,
                raw_direct,
            )

            # 2. Retrieve candidates from icd10_extended
            repo = ICD10ExtendedRepository(self._db)
            candidates: list[ExtendedICD10Candidate] = []
            variant_used = query_for_repo
            retry_plan_triggered = False

            search_attempts: list[tuple[str, Optional[int], str]] = [(query_for_repo, None, "base")]
            if not is_code_query:
                variants = self._expand_query_variants(query_for_repo)
                for variant in variants:
                    if variant != query_for_repo:
                        search_attempts.append((variant, None, "expanded"))
            if (not is_code_query) and len(query_for_repo.split()) >= 2:
                search_attempts.append((query_for_repo, 1, "relaxed_min_hits"))

            for attempt_query, attempt_min_hits, attempt_kind in search_attempts:
                candidates = await repo.search_candidates(
                    attempt_query,
                    limit=candidate_limit,
                    tags_filter=tags_filter,
                    query_is_code=is_code_query,
                    min_hits=attempt_min_hits,
                )
                if os.getenv("SEARCH_DEBUG") == "1":
                    logger.warning(
                        "clinical_search_engine.search attempt kind=%s query=%r min_hits=%s rows=%s",
                        attempt_kind,
                        attempt_query,
                        attempt_min_hits,
                        len(candidates),
                    )
                if candidates:
                    variant_used = attempt_query
                    retry_plan_triggered = attempt_kind != "base"
                    break

            logger.warning(
                "clinical_search_engine.search candidates=%s query=%r query_type=%s retry_plan_triggered=%s variant_used=%r",
                len(candidates),
                query_for_repo,
                "code" if is_code_query else "natural_language",
                retry_plan_triggered,
                variant_used,
            )

            source = "icd10_extended"

            # 3. Rank
            ranked = self._rank(candidates, query_for_repo, intent=intent)

            # 4. Trim
            results = ranked[:effective_limit]

            # 4.1 Post-ranking visual grouping: parent ICD (3-char) followed by children (XXX.*)
            try:
                extended_results = results
                grouped_results: list[ClinicalSearchResult] = []
                parent_map: dict[str, list[ClinicalSearchResult]] = {}
                parent_detected = False

                for result in extended_results:
                    code = (result.code or "").strip().upper()
                    if len(code) == 3:
                        parent_detected = True
                    if len(code) > 4 and code.startswith(code[:3] + "."):
                        parent_map.setdefault(code[:3], []).append(result)

                if parent_detected:
                    added_ids: set[int] = set()
                    for result in extended_results:
                        rid = id(result)
                        code = (result.code or "").strip().upper()

                        if len(code) == 3:
                            if rid not in added_ids:
                                grouped_results.append(result)
                                added_ids.add(rid)
                            for child in parent_map.get(code, []):
                                child_id = id(child)
                                if child_id not in added_ids:
                                    grouped_results.append(child)
                                    added_ids.add(child_id)
                        else:
                            if rid not in added_ids:
                                grouped_results.append(result)
                                added_ids.add(rid)

                    if len(grouped_results) == len(extended_results):
                        results = grouped_results
            except Exception:
                pass

            # 5. Structured logging (fire-and-forget style, never raises)
            duration_ms = (time.perf_counter() - t0) * 1000
            self._emit_search_event(
                SearchEvent(
                    query_raw=raw_query,
                    query_normalized=normalized,
                    intent=intent,
                    source=source,
                    candidate_count=len(candidates),
                    result_count=len(results),
                    duration_ms=round(duration_ms, 2),
                    top_code=results[0].code if results else None,
                    top_score=results[0].score if results else None,
                )
            )
            logger.warning(
                "clinical_search_engine.search result_count=%s similarity_used=%s retry_plan_triggered=%s",
                len(results),
                use_similarity,
                retry_plan_triggered,
            )
            logger.warning(
                "clinical_search_engine.search extended_results=%s query=%r variant_used=%r",
                len(results),
                query_for_repo,
                variant_used,
            )

            return results
        except Exception:
            try:
                await self._db.rollback()
            except Exception:
                logger.exception("clinical_search_engine.search rollback failed")
            logger.exception("ICD10 extended search failed, switching to fallback")
            logger.warning(
                "clinical_search_engine.search fallback_to_legacy=1 cause=exception query=%r",
                raw_query,
            )
            logger.exception("clinical_search_engine.search failed; returning []")
            return []

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_query(value: str) -> str:
        """Accent-strip, lowercase, tokenize, and remove common ES stopwords.

        Examples (manual):
        - "neumonía de la" -> "neumonia"
        - "dolor de cabeza" -> "dolor cabeza"
        - "insuficiencia cardiaca" -> "insuficiencia cardiaca"
        """
        text = value.strip().lower()
        nfkd = unicodedata.normalize("NFKD", text)
        stripped = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
        tokens = _TOKEN_RE.findall(stripped)
        normalized_before_stopwords = " ".join(tokens)
        filtered_tokens = [token for token in tokens if token not in STOPWORDS_ES]

        # Safety fallback: if filtering removes everything, keep original tokens.
        normalized_after_stopwords = " ".join(filtered_tokens or tokens)

        if os.getenv("SEARCH_DEBUG") == "1":
            logger.info(
                "clinical_search_engine.normalize raw_query=%r normalized_before_stopwords=%r normalized_after_stopwords=%r",
                value,
                normalized_before_stopwords,
                normalized_after_stopwords,
            )

        return normalized_after_stopwords

    def _detect_intent(self, normalized_query: str) -> Optional[str]:
        """Return the first matching clinical intent or ``None``."""
        tokens = set(normalized_query.split())
        best_intent: Optional[str] = None
        best_hits = 0

        for intent, keywords in self._intent_config.intent_keywords.items():
            hits = sum(1 for kw in keywords if kw in normalized_query or kw in tokens)
            if hits > best_hits:
                best_hits = hits
                best_intent = intent

        return best_intent if best_hits > 0 else None

    @staticmethod
    def _is_code_query(value: str) -> bool:
        compact = (value or "").strip().replace(" ", "")
        return bool(ICD_CODE_RE.match(compact)) or bool(re.match(r"^[A-Za-z]\d", compact))

    @staticmethod
    def _normalize_code_query(value: str) -> str:
        # Code queries keep their token shape; only trim and uppercase.
        return (value or "").strip().upper().replace(" ", "")

    @staticmethod
    def _expand_query_variants(normalized_query: str) -> list[str]:
        """Create clinically useful variants for natural-language retries.

        Input is expected to be normalized (lowercase, accent-stripped).
        """
        base_tokens = [t for t in _TOKEN_RE.findall((normalized_query or "").strip().lower()) if t]
        filtered_tokens = [t for t in base_tokens if t not in STOPWORDS_ES]
        collapsed = " ".join(filtered_tokens or base_tokens)

        variants: list[str] = []
        seen: set[str] = set()

        def add_variant(value: str) -> None:
            candidate = " ".join(value.split()).strip()
            if candidate and candidate not in seen:
                seen.add(candidate)
                variants.append(candidate)

        has_dolor = any(t == "dolor" for t in filtered_tokens)
        has_head_prefix = any(t.startswith("cabe") for t in filtered_tokens)
        if has_dolor and has_head_prefix:
            add_variant("cefalea")
            add_variant("migraña")

        add_variant(collapsed)
        return variants

    def _rank(
        self,
        candidates: List[ExtendedICD10Candidate],
        query: str,
        *,
        intent: Optional[str],
    ) -> List[ClinicalSearchResult]:
        """Score and sort candidates using configurable weights."""
        w = self._weights
        scored: List[ClinicalSearchResult] = []

        is_code_query = bool(ICD_CODE_RE.match(query.replace(" ", "")))

        for c in candidates:
            score = 0.0

            # Exact code match
            if c.exact_code_match:
                score += w.exact_match

            # Prefix match
            if c.prefix_match:
                score += w.prefix_match

            # Description match
            if c.description_match:
                score += w.description_match

            # Trigram similarity (continuous)
            score += c.similarity * w.similarity * 100  # normalize to comparable range

            # Priority boost from icd10_extended
            score += c.priority * w.priority_boost

            # Intent alignment
            intent_aligned = False
            if intent and c.tags:
                tags_lower = c.tags.lower()
                if intent in tags_lower:
                    intent_aligned = True
                    score += w.intent_bonus

            # Tag match bonus (any overlap with tags)
            tag_matched = bool(c.tags)
            if tag_matched:
                score += w.tag_match

            features = MatchFeatures(
                exact_code=c.exact_code_match,
                prefix_code=c.prefix_match,
                description_match=c.description_match,
                trigram_similarity=round(c.similarity, 4),
                priority=c.priority,
                intent_aligned=intent_aligned,
                tag_matched=tag_matched,
            )

            explanation_parts: list[str] = []
            if c.exact_code_match:
                explanation_parts.append("exact code")
            if c.prefix_match:
                explanation_parts.append("prefix")
            if c.description_match:
                explanation_parts.append("description")
            if c.similarity > 0:
                explanation_parts.append(f"similarity={c.similarity:.2f}")
            if c.priority > 0:
                explanation_parts.append(f"priority={c.priority}")
            if intent_aligned:
                explanation_parts.append(f"intent={intent}")

            scored.append(
                ClinicalSearchResult(
                    code=c.code,
                    label=c.description,
                    score=round(score, 4),
                    source="icd10_extended",
                    match_features=features,
                    explanation=", ".join(explanation_parts) if explanation_parts else "fuzzy",
                )
            )

        scored.sort(key=lambda r: (-r.score, r.code))
        return scored

    # ------------------------------------------------------------------
    # Structured logging
    # ------------------------------------------------------------------

    def _emit_search_event(self, event: SearchEvent) -> None:
        """Emit a structured log line.  Non-blocking, never raises."""
        if not self._flags.enable_search_logging:
            return

        try:
            logger.info(
                "search_event query=%r normalized=%r intent=%s source=%s "
                "candidates=%d results=%d duration_ms=%.2f top_code=%s top_score=%s",
                event.query_raw,
                event.query_normalized,
                event.intent or "-",
                event.source,
                event.candidate_count,
                event.result_count,
                event.duration_ms,
                event.top_code or "-",
                f"{event.top_score:.4f}" if event.top_score is not None else "-",
            )
        except Exception:  # pragma: no cover — logging must never crash the pipeline
            pass
