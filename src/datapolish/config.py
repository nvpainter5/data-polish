"""Centralized config — loads environment variables once, fails loudly if missing.

Why a separate module instead of `os.getenv` everywhere:
- Single place to add new settings.
- Single place to validate them at import time, so we get one clear error
  early rather than a confusing 401 from the API later.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load `.env` from the project root the moment this module is imported.
# `override=False` means real environment variables (e.g. on AWS Lambda
# in Phase 3b) take precedence over `.env` — this matters in production.
load_dotenv(override=False)


@dataclass(frozen=True)
class Settings:
    groq_api_key: str
    llm_provider: str = "groq"
    llm_model: str = "llama-3.3-70b-versatile"


def load_settings() -> Settings:
    """Build a Settings object, raising a clear error if anything is missing."""
    groq_api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not groq_api_key:
        raise RuntimeError(
            "GROQ_API_KEY is not set. Copy .env.example to .env and paste your "
            "key from https://console.groq.com."
        )

    return Settings(
        groq_api_key=groq_api_key,
        llm_provider=os.getenv("LLM_PROVIDER", "groq"),
        llm_model=os.getenv("LLM_MODEL", "llama-3.3-70b-versatile"),
    )


# Module-level singleton — import this from anywhere with:
#     from datapolish.config import settings
settings = load_settings()
