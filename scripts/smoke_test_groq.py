"""Smoke test — confirms our Groq setup works end-to-end.

Run this after installing requirements and setting up `.env`. If it prints
a sensible reply, we're ready to start building real pipeline steps.

Usage:
    python scripts/smoke_test_groq.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add `src/` to the import path so `from datapolish import ...` works
# without us having to `pip install -e .` yet.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from datapolish.llm_client import LLMClient  # noqa: E402


def main() -> None:
    print("Connecting to Groq...")
    client = LLMClient()

    print(f"Provider: {client.provider}")
    print(f"Model:    {client.model}")
    print()

    messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful assistant for a data engineer who is "
                "learning AI. Keep replies short and friendly."
            ),
        },
        {
            "role": "user",
            "content": (
                "In exactly one sentence, introduce yourself as my new AI "
                "data-cleaning assistant. Mention one specific thing you "
                "could help me with on messy data."
            ),
        },
    ]

    print("Sending test prompt...")
    reply = client.chat(messages, max_tokens=120)

    print()
    print("Model replied:")
    print("-" * 60)
    print(reply)
    print("-" * 60)
    print()
    print("Groq is working. Ready to build the pipeline.")


if __name__ == "__main__":
    main()
