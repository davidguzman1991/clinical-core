from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from sqlalchemy import case, func, literal, or_, select
from sqlalchemy.orm import Session

from app.models.clinical_dictionary import ClinicalDictionary
from app.models.clinical_search_log import ClinicalSearchLog
from app.models.icd10 import ICD10


@dataclass
class DictionaryMatch:
    term_raw: str
    term_normalized: str
    suggested_icd: str | None


@dataclass
class ICD10Candidate:
    code: str
    description: str
    exact_match: bool
    prefix_match: bool
    description_match: bool
    fuzzy_similarity: float
    synonym_match: bool = False


@dataclass
class UsageStats:
    global_frequency: int
    user_frequency: int


class ClinicalSearchRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    @property
    def is_postgres(self) -> bool:
        dialect = getattr(getattr(self.db, "bind", None), "dialect", None)
        return getattr(dialect, "name", "") == "postgresql"

    def find_dictionary_exact(self, normalized_query: str) -> list[DictionaryMatch]:
        term_raw = func.lower(func.coalesce(ClinicalDictionary.term_raw, ""))
        term_normalized = func.lower(func.coalesce(ClinicalDictionary.term_normalized, ""))

        stmt = (
            select(
                ClinicalDictionary.term_raw,
                ClinicalDictionary.term_normalized,
                ClinicalDictionary.suggested_icd,
            )
            .where(
                or_(
                    term_raw == normalized_query,
                    term_normalized == normalized_query,
                )
            )
            .limit(25)
        )

        rows = self.db.execute(stmt).all()
        return [
            DictionaryMatch(
                term_raw=r.term_raw,
                term_normalized=r.term_normalized,
                suggested_icd=r.suggested_icd,
            )
            for r in rows
        ]

    def find_dictionary_synonyms(
        self,
        normalized_query: str,
        *,
        tokens: Sequence[str],
        suggested_icds: Sequence[str],
        limit: int = 30,
    ) -> list[DictionaryMatch]:
        token_conditions = [
            ClinicalDictionary.term_normalized.ilike(f"%{token}%")
            for token in tokens
            if token
        ]

        similarity_score = (
            func.similarity(ClinicalDictionary.term_normalized, normalized_query)
            if self.is_postgres and len(normalized_query) >= 3
            else literal(0.0)
        )

        conditions = []
        if token_conditions:
            conditions.append(or_(*token_conditions))
        if suggested_icds:
            conditions.append(ClinicalDictionary.suggested_icd.in_(suggested_icds))
        if self.is_postgres and len(normalized_query) >= 3:
            conditions.append(similarity_score > 0.25)

        if not conditions:
            return []

        preferred = (
            case(
                (ClinicalDictionary.suggested_icd.in_(suggested_icds), literal(0)),
                else_=literal(1),
            )
            if suggested_icds
            else literal(1)
        )

        stmt = (
            select(
                ClinicalDictionary.term_raw,
                ClinicalDictionary.term_normalized,
                ClinicalDictionary.suggested_icd,
            )
            .where(
                ClinicalDictionary.term_normalized != "",
                or_(*conditions),
            )
            .order_by(preferred.asc(), similarity_score.desc(), ClinicalDictionary.term_normalized.asc())
            .limit(limit)
        )

        rows = self.db.execute(stmt).all()
        return [
            DictionaryMatch(
                term_raw=r.term_raw,
                term_normalized=r.term_normalized,
                suggested_icd=r.suggested_icd,
            )
            for r in rows
        ]

    def search_icd10_hybrid(
        self,
        normalized_query: str,
        *,
        expanded_terms: Sequence[str],
        normalized_code_query: str | None = None,
        limit: int,
    ) -> list[ICD10Candidate]:
        normalized_code_query = (normalized_code_query or "").strip().lower()
        if not normalized_query and not normalized_code_query:
            return []

        terms: list[str] = []
        for term in (normalized_query, normalized_code_query, *expanded_terms):
            if term and term not in terms:
                terms.append(term)
        terms = terms[:8]

        code_l = func.lower(func.coalesce(ICD10.code, ""))
        code_compact = func.replace(func.replace(code_l, ".", ""), " ", "")
        desc_l = func.lower(ICD10.description)
        search_l = func.lower(func.coalesce(ICD10.search_terms, ""))

        code_exact_match_parts = []
        if normalized_query:
            code_exact_match_parts.append(code_l == normalized_query)
        if normalized_code_query:
            code_exact_match_parts.extend(
                [
                    code_l == normalized_code_query,
                    code_compact == normalized_code_query,
                ]
            )
        code_exact_match = or_(*code_exact_match_parts) if code_exact_match_parts else literal(False)
        exact_match = or_(code_exact_match, desc_l == normalized_query, search_l == normalized_query)

        code_prefix_parts = []
        if normalized_query:
            code_prefix_parts.append(code_l.ilike(f"{normalized_query}%"))
        if normalized_code_query:
            code_prefix_parts.extend(
                [
                    code_l.ilike(f"{normalized_code_query}%"),
                    code_compact.ilike(f"{normalized_code_query}%"),
                ]
            )
        code_prefix_match = or_(*code_prefix_parts) if code_prefix_parts else literal(False)
        prefix_match = or_(
            code_prefix_match,
            desc_l.ilike(f"{normalized_query}%"),
            search_l.ilike(f"{normalized_query}%"),
        )

        description_match = or_(
            desc_l.ilike(f"%{normalized_query}%"),
            search_l.ilike(f"%{normalized_query}%"),
        )

        substring_match = or_(
            *[
                or_(
                    code_l.ilike(f"%{term}%"),
                    code_compact.ilike(f"%{term}%"),
                    desc_l.ilike(f"%{term}%"),
                    search_l.ilike(f"%{term}%"),
                )
                for term in terms
            ]
        )

        sim_components = []
        if self.is_postgres:
            for term in terms:
                if len(term) < 3:
                    continue
                sim_components.extend([func.similarity(desc_l, term), func.similarity(search_l, term)])

        if not sim_components:
            fuzzy_similarity = literal(0.0)
            fuzzy_filter = literal(False)
        elif len(sim_components) == 1:
            fuzzy_similarity = sim_components[0]
            fuzzy_filter = fuzzy_similarity > 0.22
        else:
            fuzzy_similarity = func.greatest(*sim_components)
            fuzzy_filter = fuzzy_similarity > 0.22

        stmt = (
            select(
                ICD10.code,
                ICD10.description,
                code_exact_match.label("exact_match"),
                code_prefix_match.label("prefix_match"),
                description_match.label("description_match"),
                fuzzy_similarity.label("fuzzy_similarity"),
            )
            .where(or_(exact_match, prefix_match, substring_match, fuzzy_filter))
            .order_by(
                exact_match.desc(),
                prefix_match.desc(),
                substring_match.desc(),
                fuzzy_similarity.desc(),
                ICD10.code.asc(),
            )
            .limit(limit)
        )

        rows = self.db.execute(stmt).all()
        return [
            ICD10Candidate(
                code=r.code,
                description=r.description,
                exact_match=bool(r.exact_match),
                prefix_match=bool(r.prefix_match),
                description_match=bool(r.description_match),
                fuzzy_similarity=float(r.fuzzy_similarity or 0.0),
                synonym_match=False,
            )
            for r in rows
        ]

    def get_icd10_by_codes(self, codes: Sequence[str]) -> list[ICD10Candidate]:
        if not codes:
            return []

        stmt = select(ICD10.code, ICD10.description).where(ICD10.code.in_(codes))
        rows = self.db.execute(stmt).all()
        return [
            ICD10Candidate(
                code=r.code,
                description=r.description,
                exact_match=False,
                prefix_match=False,
                description_match=False,
                fuzzy_similarity=0.0,
                synonym_match=True,
            )
            for r in rows
        ]

    def get_usage_stats(self, codes: Sequence[str], *, user_id: Optional[str]) -> dict[str, UsageStats]:
        if not codes:
            return {}

        user_frequency = (
            func.count().filter(ClinicalSearchLog.user_id == user_id).label("user_frequency")
            if user_id is not None
            else literal(0).label("user_frequency")
        )

        stmt = (
            select(
                ClinicalSearchLog.selected_icd.label("selected_icd"),
                func.count().label("global_frequency"),
                user_frequency,
            )
            .where(ClinicalSearchLog.selected_icd.in_(codes))
            .group_by(ClinicalSearchLog.selected_icd)
        )

        rows = self.db.execute(stmt).all()
        return {
            r.selected_icd: UsageStats(
                global_frequency=int(r.global_frequency or 0),
                user_frequency=int(r.user_frequency or 0),
            )
            for r in rows
            if r.selected_icd
        }

    def insert_search_log(
        self,
        *,
        original_query: str,
        normalized_query: str,
        selected_icd: str | None,
        selected_term: str,
        user_id: Optional[str] = None,
        specialty: Optional[str] = None,
    ) -> None:
        self.db.add(
            ClinicalSearchLog(
                user_id=user_id,
                query=original_query,
                normalized_query=normalized_query,
                selected_term=selected_term,
                selected_icd=selected_icd,
                specialty=specialty,
            )
        )
        self.db.commit()
