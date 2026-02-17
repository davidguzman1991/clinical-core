"""Async SQLAlchemy engine and session management."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings


def _to_async_uri(uri: str) -> str:
    if uri.startswith("postgresql+asyncpg://"):
        return uri
    if uri.startswith("postgresql+psycopg2://"):
        return uri.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
    if uri.startswith("postgresql://"):
        return uri.replace("postgresql://", "postgresql+asyncpg://", 1)
    if uri.startswith("postgres://"):
        return uri.replace("postgres://", "postgresql+asyncpg://", 1)
    return uri


async_engine = create_async_engine(
    _to_async_uri(settings.sqlalchemy_database_uri),
    pool_pre_ping=True,
    pool_recycle=1800,
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as db:
        yield db
