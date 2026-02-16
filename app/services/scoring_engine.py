from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CandidateSignals:
    exact_match: bool
    synonym_match: bool
    fuzzy_similarity: float
    global_frequency: int
    user_frequency: int


class ClinicalScoringEngine:
    """Deterministic scoring for hybrid clinical search ranking."""

    def score(self, s: CandidateSignals) -> float:
        exact_boost = 100.0 if s.exact_match else 0.0
        synonym_boost = 35.0 if s.synonym_match else 0.0
        fuzzy_component = max(0.0, s.fuzzy_similarity) * 20.0
        frequency_component = min(s.global_frequency, 200) * 0.15
        previous_usage_component = min(s.user_frequency, 100) * 0.4

        return (
            exact_boost
            + synonym_boost
            + fuzzy_component
            + frequency_component
            + previous_usage_component
        )
