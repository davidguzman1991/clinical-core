"""Search configuration for Clinical Core.

Centralizes feature flags, ranking weights, and tuning parameters for the
clinical search engine.  All values are loaded from environment variables
with sensible defaults so the system works out-of-the-box.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SearchFeatureFlags:
    """Runtime feature flags for the search subsystem."""

    use_extended_icd10: bool = field(
        default_factory=lambda: _env_bool("USE_EXTENDED_ICD10", default=True),
    )
    enable_intent_detection: bool = field(
        default_factory=lambda: _env_bool("SEARCH_ENABLE_INTENT_DETECTION", default=True),
    )
    enable_search_logging: bool = field(
        default_factory=lambda: _env_bool("SEARCH_ENABLE_LOGGING", default=True),
    )
    debug_search: bool = field(
        default_factory=lambda: _env_bool("SEARCH_DEBUG", default=False),
    )


# ---------------------------------------------------------------------------
# Ranking weights
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RankingWeights:
    """Tunable weights used by the clinical search engine scoring pipeline."""

    # Trigram / fuzzy similarity weight
    similarity: float = field(
        default_factory=lambda: _env_float("RANK_W_SIMILARITY", 0.30),
    )
    # Exact code or description match
    exact_match: float = field(
        default_factory=lambda: _env_float("RANK_W_EXACT_MATCH", 100.0),
    )
    # Prefix match on code
    prefix_match: float = field(
        default_factory=lambda: _env_float("RANK_W_PREFIX_MATCH", 50.0),
    )
    # Description substring match
    description_match: float = field(
        default_factory=lambda: _env_float("RANK_W_DESCRIPTION_MATCH", 20.0),
    )
    # Priority field from icd10_extended
    priority_boost: float = field(
        default_factory=lambda: _env_float("RANK_W_PRIORITY_BOOST", 10.0),
    )
    # Clinical intent alignment bonus
    intent_bonus: float = field(
        default_factory=lambda: _env_float("RANK_W_INTENT_BONUS", 15.0),
    )
    # Tag match bonus
    tag_match: float = field(
        default_factory=lambda: _env_float("RANK_W_TAG_MATCH", 5.0),
    )


# ---------------------------------------------------------------------------
# Search tuning
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SearchTuning:
    """Operational limits and thresholds."""

    similarity_threshold: float = field(
        default_factory=lambda: _env_float("SEARCH_SIMILARITY_THRESHOLD", 0.20),
    )
    default_limit: int = field(
        default_factory=lambda: _env_int("SEARCH_DEFAULT_LIMIT", 10),
    )
    max_limit: int = field(
        default_factory=lambda: _env_int("SEARCH_MAX_LIMIT", 50),
    )
    candidate_multiplier: int = field(
        default_factory=lambda: _env_int("SEARCH_CANDIDATE_MULTIPLIER", 4),
    )


# ---------------------------------------------------------------------------
# Clinical intent mapping
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClinicalIntentConfig:
    """Simple keyword-based intent detection rules.

    Each key is an intent label; the value is a list of trigger keywords
    (Spanish, lowercased, accent-stripped).
    """

    intent_keywords: Dict[str, list[str]] = field(default_factory=lambda: {
        "infection": [
            "infeccion", "bacteria", "virus", "viral", "bacteriana", "sepsis",
            "neumonia", "bronquitis", "celulitis", "absceso", "meningitis",
            "pielonefritis", "tuberculosis", "hepatitis", "vih", "sida",
            "covid", "influenza", "dengue", "malaria",
        ],
        "cardiometabolic": [
            "diabetes", "hipertension", "hiperlipidemia", "obesidad",
            "infarto", "insuficiencia cardiaca", "arritmia", "aterosclerosis",
            "cardiopatia", "angina", "dislipidemia", "sindrome metabolico",
            "fibrilacion", "taquicardia",
        ],
        "respiratory": [
            "asma", "epoc", "bronquitis", "neumonia", "disnea", "tos",
            "rinitis", "sinusitis", "faringitis", "laringitis",
            "insuficiencia respiratoria", "embolia pulmonar",
        ],
        "gastrointestinal": [
            "gastritis", "colitis", "diarrea", "estreñimiento", "reflujo",
            "ulcera", "hepatitis", "cirrosis", "pancreatitis", "apendicitis",
            "hernia", "colon irritable",
        ],
        "musculoskeletal": [
            "artritis", "artrosis", "lumbalgia", "cervicalgia", "fractura",
            "tendinitis", "osteoporosis", "dorsalgia", "esguince", "luxacion",
            "mialgia", "fibromialgia",
        ],
        "neurological": [
            "cefalea", "migraña", "epilepsia", "neuropatia", "parkinson",
            "alzheimer", "vertigo", "mareo", "convulsion", "ictus", "acv",
            "esclerosis",
        ],
        "mental_health": [
            "ansiedad", "depresion", "insomnio", "estres", "panico",
            "trastorno bipolar", "esquizofrenia", "psicosis",
        ],
        "oncology": [
            "cancer", "tumor", "neoplasia", "metastasis", "linfoma",
            "leucemia", "melanoma", "carcinoma", "sarcoma",
        ],
        "renal": [
            "insuficiencia renal", "nefritis", "nefrolitiasis", "dialisis",
            "proteinuria", "hematuria", "glomerulonefritis",
        ],
        "endocrine": [
            "hipotiroidismo", "hipertiroidismo", "tiroides", "cushing",
            "addison", "acromegalia", "prolactinoma",
        ],
    })


# ---------------------------------------------------------------------------
# Singleton instances (importable)
# ---------------------------------------------------------------------------

search_feature_flags = SearchFeatureFlags()
ranking_weights = RankingWeights()
search_tuning = SearchTuning()
clinical_intent_config = ClinicalIntentConfig()
