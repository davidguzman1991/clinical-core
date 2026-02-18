from __future__ import annotations

import asyncio
import logging

from app.db.async_session import AsyncSessionLocal
from app.services.clinical_search_engine import ClinicalSearchEngine

logger = logging.getLogger(__name__)


async def _run_check(query: str, *, limit: int = 10) -> None:
    async with AsyncSessionLocal() as db:
        engine = ClinicalSearchEngine(db)
        results = await engine.search(query, limit=limit)

        print(f"query={query!r} result_count={len(results)}")
        if results:
            top = results[0]
            print(f"top_code={top.code} top_source={top.source} top_score={top.score}")

        assert results, f"No results for query={query!r}"
        assert all(r.source == "icd10_extended" for r in results), (
            f"Non-extended source found for query={query!r}: {[r.source for r in results]}"
        )


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    # Case 1: natural-language path (similarity_used True expected when len >= 3)
    await _run_check("Dolor de cabeza", limit=10)

    # Case 2: ICD code path
    await _run_check("E118", limit=10)

    print("verify_extended_search: OK")


if __name__ == "__main__":
    asyncio.run(main())
