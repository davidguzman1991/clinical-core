from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from app.repositories.search_repository import ClinicalSearchRepository, ICD10Candidate, UsageStats
from app.services.scoring_engine import CandidateSignals, ClinicalScoringEngine
from app.services.search_normalization import normalize_text, tokenize_normalized

logger = logging.getLogger(__name__)


@dataclass
class RankedICD10Result:
    code: str
    description: str
    score: float
    match_type: str


class ClinicalSearchService:
    def __init__(
        self,
        repository: ClinicalSearchRepository,
        scoring_engine: Optional[ClinicalScoringEngine] = None,
    ) -> None:
        self.repository = repository
        self.scoring_engine = scoring_engine or ClinicalScoringEngine()

    def search(
        self,
        query: str,
        *,
        limit: int,
        user_id: Optional[str] = None,
        specialty: Optional[str] = None,
    ) -> list[RankedICD10Result]:
        original_query = query.strip()
        normalized_query = normalize_text(original_query)
        if not normalized_query:
            return []

        tokens = tokenize_normalized(normalized_query)

        dictionary_exact = self.repository.find_dictionary_exact(normalized_query)
        exact_suggested_codes = sorted(
            {
                row.suggested_icd
                for row in dictionary_exact
                if row.suggested_icd
            }
        )

        dictionary_synonyms = self.repository.find_dictionary_synonyms(
            normalized_query,
            tokens=tokens,
            suggested_icds=exact_suggested_codes,
            limit=max(20, limit * 2),
        )

        synonym_terms: list[str] = []
        synonym_codes: list[str] = []
        for row in dictionary_synonyms:
            if row.term_normalized and row.term_normalized not in synonym_terms:
                synonym_terms.append(row.term_normalized)
            if row.suggested_icd and row.suggested_icd not in synonym_codes:
                synonym_codes.append(row.suggested_icd)

        hybrid_candidates = self.repository.search_icd10_hybrid(
            normalized_query,
            expanded_terms=synonym_terms,
            limit=max(limit * 4, 40),
        )
        mapped_candidates = self.repository.get_icd10_by_codes(synonym_codes)
        merged_candidates = self._merge_candidates(hybrid_candidates + mapped_candidates)

        usage = self.repository.get_usage_stats(
            [c.code for c in merged_candidates],
            user_id=user_id,
        )

        ranked = self._rank_candidates(merged_candidates, usage)
        top_ranked = ranked[:limit]

        # Search events should not claim a clinician-selected ICD code.
        # Explicit selection is captured by POST /icd10/select.
        selected_icd = None
        selected_term = normalized_query
        try:
            self.repository.insert_search_log(
                original_query=original_query,
                normalized_query=normalized_query,
                selected_icd=selected_icd,
                selected_term=selected_term,
                user_id=user_id,
                specialty=specialty,
            )
        except Exception:
            logger.exception("Failed to persist clinical search log")
            self.repository.db.rollback()

        return top_ranked

    @staticmethod
    def _merge_candidates(candidates: list[ICD10Candidate]) -> list[ICD10Candidate]:
        by_code: dict[str, ICD10Candidate] = {}
        for candidate in candidates:
            existing = by_code.get(candidate.code)
            if not existing:
                by_code[candidate.code] = candidate
                continue

            existing.exact_match = existing.exact_match or candidate.exact_match
            existing.synonym_match = existing.synonym_match or candidate.synonym_match
            existing.fuzzy_similarity = max(existing.fuzzy_similarity, candidate.fuzzy_similarity)

        return list(by_code.values())

    def _rank_candidates(
        self,
        candidates: list[ICD10Candidate],
        usage: dict[str, UsageStats],
    ) -> list[RankedICD10Result]:
        ranked: list[RankedICD10Result] = []
        for candidate in candidates:
            stats = usage.get(candidate.code)
            global_frequency = int(getattr(stats, "global_frequency", 0))
            user_frequency = int(getattr(stats, "user_frequency", 0))
            score = self.scoring_engine.score(
                CandidateSignals(
                    exact_match=candidate.exact_match,
                    synonym_match=candidate.synonym_match,
                    fuzzy_similarity=candidate.fuzzy_similarity,
                    global_frequency=global_frequency,
                    user_frequency=user_frequency,
                )
            )
            match_type = self._match_type(candidate)
            ranked.append(
                RankedICD10Result(
                    code=candidate.code,
                    description=candidate.description,
                    score=round(score, 4),
                    match_type=match_type,
                )
            )

        ranked.sort(key=lambda x: (-x.score, x.code))
        return ranked

    @staticmethod
    def _match_type(candidate: ICD10Candidate) -> str:
        if candidate.exact_match:
            return "exact"
        if candidate.synonym_match:
            return "synonym"
        if candidate.fuzzy_similarity > 0:
            return "fuzzy"
        return "direct"
