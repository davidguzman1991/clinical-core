"""Clinical Core FastAPI application.

This service is intended to be a lightweight, reusable clinical engine that can be
shared across multiple medical products (e.g., Receta Fácil, Web Diabetes, CALMA).

Only clinical-domain modules should live here (ICD-10 today; SNOMED/LOINC/drug
interactions/AI reasoning in the future). Product-specific concerns such as users,
authentication, prescriptions, and frontend logic must not be added to this repo.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.clinical.icd10.router import router as icd10_router
from app.routers import clinical_icd10_search, clinical_search, intelligent_search, search_learning, search_suggestions


def create_app() -> FastAPI:
    app = FastAPI(
        title="Clinical Core",
        version="0.1.0",
        description="Reusable clinical engine APIs (ICD-10, future clinical modules).",
    )

    # TEMPORAL: usar ["*"] si CORS persiste
    origins = [
        "http://localhost:3000",
        "https://clinical-core-cie10.vercel.app",
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(icd10_router, prefix="/icd10", tags=["ICD-10"])
    app.include_router(clinical_icd10_search.router)
    app.include_router(search_learning.router)
    app.include_router(search_suggestions.router)
    app.include_router(intelligent_search.router)
    app.include_router(clinical_search.router)

    return app


app = create_app()
