"""Deterministic clinical phenotype classifier.

This module provides a lightweight, side-effect-free utility to classify
clinical phenomena from short free-text queries using keyword matching.
It is intentionally isolated and does not integrate with other modules yet.
"""

from typing import Optional


PHENOMENON_MAP: dict[str, list[str]] = {
    "neuropathic": [
        "ardor",
        "hormigueo",
        "entumecimiento",
        "calambre",
        "neuropatia",
    ],
    "motor": [
        "debilidad",
        "paralisis",
        "rigidez",
        "espasmo",
        "temblor",
    ],
    "inflammatory": [
        "inflamacion",
        "hinchazon",
        "enrojecimiento",
        "fiebre",
    ],
    "traumatic": [
        "golpe",
        "herida",
        "fractura",
        "esguince",
        "contusion",
    ],
    "vascular": [
        "edema",
        "isquemia",
        "trombosis",
        "hipertension",
        "hemorragia",
    ],
    "constitutional": [
        "fatiga",
        "astenia",
        "decaimiento",
        "perdida",
        "malestar",
    ],
}


def _tokenize(query: str) -> list[str]:
    """Normalize to lowercase and split into simple word tokens."""
    normalized = (query or "").lower().strip()
    punctuation = ".,;:!?()[]{}\"'`´“”‘’/\\|-_"
    for ch in punctuation:
        normalized = normalized.replace(ch, " ")
    return [token for token in normalized.split() if token]


def classify_phenotype(query: str) -> dict[str, Optional[str] | list[str]]:
    """Classify the most likely clinical phenotype by keyword overlap.

    Returns:
        {
            "phenotype": str | None,
            "matched_terms": list[str]
        }
    """
    tokens = _tokenize(query)
    if not tokens:
        return {"phenotype": None, "matched_terms": []}

    token_set = set(tokens)
    best_phenotype: Optional[str] = None
    best_matches: list[str] = []

    for phenotype, keywords in PHENOMENON_MAP.items():
        matches = [term for term in keywords if term in token_set]
        if len(matches) > len(best_matches):
            best_phenotype = phenotype
            best_matches = matches

    if not best_matches:
        return {"phenotype": None, "matched_terms": []}

    return {"phenotype": best_phenotype, "matched_terms": best_matches}
