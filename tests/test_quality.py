"""Tests for the quality score and outlier detection.

The LLM-calling `generate_suggestions` is not exercised here — that's a
network call best left to manual smoke testing. We do confirm it returns
empty (not crashes) when called without a working client.
"""

from __future__ import annotations

import pandas as pd

from datapolish.profile import profile_dataset
from datapolish.quality import (
    ISSUE_PENALTY_PER_COLUMN,
    QualityScore,
    Suggestions,
    compute_quality_score,
    generate_suggestions,
)


def _profile_for(df: pd.DataFrame):
    return profile_dataset(df, source_path="memory")


def test_clean_dataset_scores_high():
    df = pd.DataFrame(
        {
            "id": list(range(20)),
            "category": ["a"] * 10 + ["b"] * 10,
        }
    )
    score = compute_quality_score(_profile_for(df))
    assert score.score == 100
    assert score.issue_count == 0


def test_dirty_dataset_loses_points():
    df = pd.DataFrame(
        {
            "complaint_type": [
                "Noise",
                "HEAT/HOT WATER",
                "Blocked Driveway",
                "PAVEMENT CONDITION",
            ]
            * 10,
            "addr": [
                "  123 Main St",
                "456 Oak  Ave",
                "789 Pine Rd ",
                "MAIN STREET  ",
            ]
            * 10,
        }
    )
    score = compute_quality_score(_profile_for(df))
    assert score.score < 100
    assert score.issue_count > 0
    types = {i.type for i in score.issues}
    # We expect at least some of these flags to fire on this dirty data.
    assert types & {
        "mixed_casing",
        "whitespace_padding",
        "double_spaces",
    }


def test_score_never_below_zero():
    """Construct an artificial profile with absurdly many issues to confirm
    the floor at 0."""
    df = pd.DataFrame(
        {f"col_{i}": ["A", "b", "C", "d"] * 5 for i in range(50)}
    )
    score = compute_quality_score(_profile_for(df))
    assert 0 <= score.score <= 100


def test_score_drops_with_issues():
    """Adding a problematic column should lower the dataset score."""
    df_clean = pd.DataFrame({"row": list(range(20))})
    # Use varied values so we trigger ONLY whitespace_padding (not also
    # constant_column).
    df_dirty = pd.DataFrame(
        {
            "row": list(range(20)),
            "name": [" alice ", " bob ", " carol ", " dave "] * 5,
        }
    )
    s_clean = compute_quality_score(_profile_for(df_clean))
    s_dirty = compute_quality_score(_profile_for(df_dirty))
    assert s_clean.score == 100
    assert s_dirty.score < s_clean.score
    # The dirty column fires whitespace_padding (1 issue) -> column score
    # 75. Average with the clean `row` column (100): (100 + 75) / 2 = 87/88.
    assert 80 <= s_dirty.score <= 95


def test_per_column_penalty_caps_at_zero():
    """A column with 4+ issues should land at 0 (penalty floor)."""
    # 50% null + mixed casing (upper + title) + leading/trailing whitespace
    # + double spaces -> 4 distinct issue flags on one column.
    df = pd.DataFrame(
        {
            "messy": [
                "  Alpha Thing  ",  # leading + trailing ws, title case
                "BETA  THING",       # all upper, double space
                None,
                None,
            ]
            * 10  # 40 rows, 20 null -> 50% null -> high_nulls fires
        }
    )
    score = compute_quality_score(_profile_for(df))
    # Single column, 4 issues -> column score 0 -> dataset score 0.
    assert score.score == 0
    assert score.issue_count >= 4


def test_numeric_outlier_detection():
    """A long tail of normal values plus a few extreme ones should fire IQR."""
    df = pd.DataFrame({"value": list(range(100)) + [10_000, 20_000]})
    profile = _profile_for(df)
    col = next(c for c in profile.columns if c.name == "value")
    assert col.outliers is not None
    assert col.outliers.numeric_iqr_outliers >= 2


def test_rare_categorical_detection():
    """A single odd value among 1000 common ones should be flagged rare."""
    df = pd.DataFrame({"status": ["ok"] * 1000 + ["weird_typo"]})
    profile = _profile_for(df)
    col = next(c for c in profile.columns if c.name == "status")
    assert col.outliers is not None
    assert col.outliers.rare_categorical_count >= 1
    assert "weird_typo" in col.outliers.rare_categorical_examples


def test_quality_score_is_pydantic_serializable():
    df = pd.DataFrame({"x": [1, 2, 3]})
    score = compute_quality_score(_profile_for(df))
    blob = score.model_dump()
    assert "score" in blob
    assert "issues" in blob


def test_constant_column_is_flagged():
    """A column with one unique value across many rows should be flagged."""
    df = pd.DataFrame(
        {
            "real_data": list(range(50)),
            "dead_column": ["only_value"] * 50,
        }
    )
    score = compute_quality_score(_profile_for(df))
    assert any(
        i.type == "constant_column" and i.column == "dead_column"
        for i in score.issues
    )


def test_suspected_key_duplicates_flagged():
    """A column named *_id with HIGH but imperfect uniqueness fires.

    The check is for the 'this column looks like a primary key with a few
    stray duplicates' case (>50% but <100% unique). Heavily-duplicated
    columns are likely foreign keys, not stray-dupe primary keys, and
    don't fire.
    """
    df = pd.DataFrame(
        {
            # 95 distinct values + 5 duplicates -> 95% unique: looks like
            # a primary key with a few accidental duplicates.
            "customer_id": list(range(95)) + [1, 2, 3, 4, 5],
            "row_count": list(range(100)),
        }
    )
    score = compute_quality_score(_profile_for(df))
    assert any(
        i.type == "suspected_key_duplicates" and i.column == "customer_id"
        for i in score.issues
    )


def test_perfectly_unique_id_not_flagged():
    """A column named *_id with 100% uniqueness is fine — no flag."""
    df = pd.DataFrame({"customer_id": list(range(100))})
    score = compute_quality_score(_profile_for(df))
    types = {i.type for i in score.issues}
    assert "suspected_key_duplicates" not in types


def test_camelcase_id_column_flagged():
    """CamelCase/PascalCase ID columns also fire the duplicate-key check."""
    # 95 unique + 5 duplicates = 95% unique
    df = pd.DataFrame(
        {
            "CMSCustomerID": list(range(95)) + [1, 2, 3, 4, 5],
            "filler": list(range(100)),
        }
    )
    score = compute_quality_score(_profile_for(df))
    assert any(
        i.type == "suspected_key_duplicates" and i.column == "CMSCustomerID"
        for i in score.issues
    )


def test_word_ending_in_id_not_flagged():
    """A non-ID word that happens to end in 'id' (like 'Mid') should NOT
    be flagged as a key column."""
    from datapolish.quality import _looks_like_id_column

    assert not _looks_like_id_column("Mid")
    assert not _looks_like_id_column("rapid")
    assert _looks_like_id_column("CustomerID")
    assert _looks_like_id_column("customer_id")
    assert _looks_like_id_column("OrderKey")


def test_suggestions_returns_empty_on_failure(monkeypatch):
    """If the LLM client can't be constructed (e.g. no API key set), we
    return an empty Suggestions rather than crashing the pipeline."""

    # Force LLMClient() to fail by clearing the env var that
    # `datapolish.config` reads on construction.
    monkeypatch.setenv("GROQ_API_KEY", "")

    # Also have to stub the cached `settings` module — config.py reads
    # the env at import time. Easiest path: monkey-patch propose to fail.
    from datapolish.apply import ApplyAudit

    audit = ApplyAudit(
        started_at="t",
        finished_at="t",
        input_rows=1,
        output_rows=1,
        input_columns=1,
        output_columns=1,
    )
    quality = QualityScore(score=100, issue_count=0, issues=[])

    class _BoomClient:
        def chat(self, *a, **kw):
            raise RuntimeError("simulated network failure")

    result = generate_suggestions(audit, quality, client=_BoomClient())
    assert isinstance(result, Suggestions)
    assert result.suggestions == []
