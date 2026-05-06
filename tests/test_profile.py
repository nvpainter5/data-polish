"""Unit tests for the profiler.

These tests use small in-memory DataFrames so they run in milliseconds
and don't require the real dataset to be present.
"""

from __future__ import annotations

import pandas as pd

from datapolish.profile import (
    profile_column,
    profile_dataset,
)


def test_basic_dataset_profile() -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "BOB", "  charlie", None, "Alice"],
            "age": [25, 30, None, 45, 50],
            "created_date": [
                "2026-01-01",
                "2026-02-01",
                "2026-03-01",
                None,
                "not-a-date",
            ],
        }
    )

    profile = profile_dataset(df, source_path="memory")

    assert profile.row_count == 5
    assert profile.column_count == 4

    cols = {c.name: c for c in profile.columns}
    assert cols["name"].null_count == 1
    assert cols["age"].null_count == 1
    assert cols["age"].numeric_stats is not None
    assert cols["age"].numeric_stats.min == 25
    assert cols["age"].numeric_stats.max == 50


def test_string_casing_detected() -> None:
    """Mixed casing should be visible in the string stats."""
    s = pd.Series(["Alice", "BOB", "carol", "DAVE", "Eve"])
    col = profile_column("name", s)
    assert col.string_stats is not None
    assert col.string_stats.count_all_upper == 2  # BOB, DAVE
    assert col.string_stats.count_title_case >= 2  # Alice, Eve


def test_whitespace_detected() -> None:
    s = pd.Series(["alice", " bob", "carol ", "dave  eve"])
    col = profile_column("name", s)
    assert col.string_stats is not None
    assert col.string_stats.has_leading_whitespace == 1
    assert col.string_stats.has_trailing_whitespace == 1
    assert col.string_stats.has_double_spaces == 1


def test_datetime_parse_failures() -> None:
    s = pd.Series(["2026-01-01", "2026-02-01", "garbage", None])
    col = profile_column("created_date", s)
    assert col.datetime_stats is not None
    # 'garbage' is non-null but unparseable -> 1 parse failure
    assert col.datetime_stats.parse_failures == 1


def test_high_cardinality_skips_top_values() -> None:
    """Columns with many distinct values shouldn't get a top_values list."""
    s = pd.Series([f"id_{i}" for i in range(200)])
    col = profile_column("uid", s)
    assert col.unique_count == 200
    assert col.top_values is None


def test_low_cardinality_has_top_values() -> None:
    s = pd.Series(["a", "b", "a", "c", "a", "b"] * 10)
    col = profile_column("category", s)
    assert col.top_values is not None
    # Most frequent value first.
    assert col.top_values[0].value == "a"
    assert col.top_values[0].count == 30
