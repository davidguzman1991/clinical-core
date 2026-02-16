from __future__ import annotations

import re
import unicodedata

_TOKEN_RE = re.compile(r"[a-z0-9\.]+")


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_text(value: str) -> str:
    lowered = strip_accents(value.strip().lower())
    tokens = _TOKEN_RE.findall(lowered)
    return " ".join(tokens)


def tokenize_normalized(value: str) -> list[str]:
    if not value:
        return []
    return value.split()
