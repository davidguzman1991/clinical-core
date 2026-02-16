from __future__ import annotations

from dataclasses import dataclass
import re


ICD_CODE_QUERY_RE = re.compile(r"^[A-Z][0-9]{2,4}$")


@dataclass
class CandidateSignals:
    code: str
    description: str
    exact_match: bool
    prefix_match: bool
    dictionary_mapped: bool
    description_match: bool
    fuzzy_similarity: float
    global_frequency: int
    user_frequency: int


class ClinicalScoringEngine:
    """Deterministic scoring for hybrid clinical search ranking."""

    @staticmethod
    def normalize_icd_code(value: str) -> str:
        """Normalize ICD code-like strings for stable comparisons."""
        return (value or "").replace(".", "").replace(" ", "").upper()

    @classmethod
    def is_icd_code_query(cls, query: str) -> bool:
        normalized = cls.normalize_icd_code(query)
        return bool(ICD_CODE_QUERY_RE.match(normalized))

    def score(self, s: CandidateSignals, *, query: str) -> float:
        if self.is_icd_code_query(query):
            return self._score_code_query(s)
        return self._score_description_query(s)

    def _score_code_query(self, s: CandidateSignals) -> float:
        """
        Clinical priority when the query looks like an ICD code.

        Priority order:
        1) exact code match
        2) prefix code match
        3) dictionary mapped ICD
        4) description match
        5) fuzzy similarity
        6) usage frequency
        """
        exact_boost = 100.0 if s.exact_match else 0.0
        prefix_boost = 50.0 if s.prefix_match else 0.0
        dictionary_boost = 35.0 if s.dictionary_mapped else 0.0
        description_boost = 20.0 if s.description_match else 0.0
        fuzzy_component = max(0.0, s.fuzzy_similarity) * 20.0
        frequency_component = min(s.global_frequency, 200) * 0.15
        previous_usage_component = min(s.user_frequency, 100) * 0.4

        return (
            exact_boost
            + prefix_boost
            + dictionary_boost
            + description_boost
            + fuzzy_component
            + frequency_component
            + previous_usage_component
        )

    def _score_description_query(self, s: CandidateSignals) -> float:
        """Default ranking behavior for non-code free-text queries."""
        exact_boost = 100.0 if s.exact_match else 0.0
        dictionary_boost = 35.0 if s.dictionary_mapped else 0.0
        description_boost = 15.0 if s.description_match else 0.0
        fuzzy_component = max(0.0, s.fuzzy_similarity) * 20.0
        frequency_component = min(s.global_frequency, 200) * 0.15
        previous_usage_component = min(s.user_frequency, 100) * 0.4

        return (
            exact_boost
            + dictionary_boost
            + description_boost
            + fuzzy_component
            + frequency_component
            + previous_usage_component
        )
