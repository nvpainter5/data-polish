"""Smoke tests that don't require network or API keys.

Real LLM-calling tests live elsewhere and are skipped by default in CI.
"""

from __future__ import annotations


def test_package_importable() -> None:
    """If this fails, the project layout / pyproject is broken."""
    import datapolish

    assert datapolish.__version__
