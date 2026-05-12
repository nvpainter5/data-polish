"""Database engine + session management for v3 persistence.

Strategy:
  - Use SQLAlchemy 2.0 ORM
  - Postgres in production (DATABASE_URL=postgresql://...)
  - SQLite at data/datapolish.db locally if DATABASE_URL is blank
    (so developers don't need Supabase set up just to run tests)

`get_session()` is a FastAPI dependency. `Base` is the declarative base
that `models.py` extends.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Generator

# Defense-in-depth: ensure .env is loaded even if db.py is imported in
# isolation (e.g. by tests or a script that doesn't go through api/__init__).
try:
    from dotenv import load_dotenv

    load_dotenv(
        Path(__file__).resolve().parent.parent / ".env", override=False
    )
except ImportError:
    pass

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_PATH = PROJECT_ROOT / "data" / "datapolish.db"


def _database_url() -> str:
    """Return the active database URL.

    Order of precedence:
      1. DATABASE_URL env var (production: Supabase / Postgres)
      2. SQLite fallback at data/datapolish.db (local dev)
    """
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url
    DEFAULT_SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{DEFAULT_SQLITE_PATH}"


DATABASE_URL = _database_url()

# SQLite needs `check_same_thread=False` when used with FastAPI threads.
_engine_kwargs: dict = {}
if DATABASE_URL.startswith("sqlite"):
    _engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, pool_pre_ping=True, **_engine_kwargs)

SessionLocal = sessionmaker(
    bind=engine, autoflush=False, autocommit=False, expire_on_commit=False
)


class Base(DeclarativeBase):
    """Single declarative base for every SQLAlchemy model in the project."""


def get_session() -> Generator[Session, None, None]:
    """FastAPI dependency. Yields a session and closes it on request end."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def is_postgres() -> bool:
    return DATABASE_URL.startswith("postgres")
