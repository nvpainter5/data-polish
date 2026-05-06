"""Unit tests for the apply step — safety gates and per-op behavior."""

from __future__ import annotations

import pandas as pd

from datapolish.apply import (
    _count_changed_rows,
    apply_plan,
    validate_cleaned,
)
from datapolish.cleaning import CleaningPlan, CleaningRule
from datapolish.profile import (
    ColumnProfile,
    DatasetProfile,
    StringStats,
    profile_dataset,
)


def _profile_for(df: pd.DataFrame) -> DatasetProfile:
    return profile_dataset(df, source_path="memory")


def test_set_case_applied_when_mixed_casing_present() -> None:
    """Use realistic complaint-style values so the short-code guard
    (max_length <= 6) doesn't accidentally fire."""
    df = pd.DataFrame(
        {
            "complaint_type": [
                "Noise - Residential",
                "HEAT/HOT WATER",
                "Blocked Driveway",
                "ILLEGAL PARKING",
                "Street Condition",
                "PAVEMENT CONDITION",
            ]
            * 10
        }
    )
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="complaint_type",
                operation="set_case",
                parameters={"case": "title"},
                confidence="high",
                reasoning="mixed casing",
            )
        ],
    )

    cleaned, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 1
    # Originally all-uppercase values should now be title case.
    assert "Heat/Hot Water" in cleaned["complaint_type"].values
    assert "Illegal Parking" in cleaned["complaint_type"].values


def test_set_case_skipped_when_only_one_casing_present() -> None:
    """The agency / NYPD safety case — column is uniformly upper."""
    df = pd.DataFrame({"agency": ["NYPD", "HPD", "DOT", "DSNY"] * 25})
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="agency",
                operation="set_case",
                parameters={"case": "title"},
                confidence="high",
                reasoning="model misread the column",
            )
        ],
    )

    cleaned, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 0
    assert audit.skipped_count == 1
    # The column was untouched.
    assert all(v.isupper() for v in cleaned["agency"])
    # The skip reason mentions the gate behavior.
    assert "consistent" in audit.entries[0].reason


def test_medium_confidence_rule_skipped() -> None:
    df = pd.DataFrame({"x": ["Alpha", "BETA"]})
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="x",
                operation="set_case",
                parameters={"case": "title"},
                confidence="medium",
                reasoning="not sure",
            )
        ],
    )

    _, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 0
    assert audit.skipped_count == 1
    assert "confidence=medium" in audit.entries[0].reason


def test_collapse_internal_whitespace_applied() -> None:
    df = pd.DataFrame({"addr": ["123  Main St", "456 Oak  Ave", "ok value"]})
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="addr",
                operation="collapse_internal_whitespace",
                parameters={},
                confidence="high",
                reasoning="double spaces",
            )
        ],
    )

    cleaned, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 1
    assert "  " not in cleaned["addr"].iloc[0]
    assert "  " not in cleaned["addr"].iloc[1]


def test_collapse_skipped_when_no_double_spaces() -> None:
    df = pd.DataFrame({"clean_col": ["one", "two", "three"]})
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="clean_col",
                operation="collapse_internal_whitespace",
                parameters={},
                confidence="high",
                reasoning="false alarm",
            )
        ],
    )

    _, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 0
    assert audit.skipped_count == 1
    assert "double-spaces" in audit.entries[0].reason


def test_drop_column_never_auto_applied() -> None:
    df = pd.DataFrame({"keep": [1, 2], "drop": [3, 4]})
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="drop",
                operation="drop_column",
                parameters={},
                confidence="high",
                reasoning="user wants this gone",
            )
        ],
    )

    cleaned, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 0
    assert audit.skipped_count == 1
    assert "drop" in cleaned.columns  # column survived


def test_mark_for_review_never_applied() -> None:
    df = pd.DataFrame({"x": ["a", "b"]})
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="x",
                operation="mark_for_review",
                parameters={"note": "something"},
                confidence="high",
                reasoning="reviewer attention",
            )
        ],
    )

    _, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 0
    assert audit.skipped_count == 1


def test_rows_changed_count_correct() -> None:
    """Verify that only actually-modified rows count."""
    before = pd.Series(["a", "B", "c", "D"])
    after = pd.Series(["a", "b", "c", "d"])
    assert _count_changed_rows(before, after) == 2


def test_validate_cleaned_passes_on_good_input() -> None:
    df = pd.DataFrame({"unique_key": [1, 2, 3], "complaint_type": ["a", "b", "c"]})
    failures = validate_cleaned(
        df,
        df,
        required_columns=["unique_key", "complaint_type"],
        unique_key_column="unique_key",
    )
    assert failures == []


def test_validate_cleaned_catches_missing_column() -> None:
    original = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    cleaned = pd.DataFrame({"a": [1, 2]})  # b missing
    failures = validate_cleaned(
        cleaned, original, required_columns=["a", "b"]
    )
    assert any(f.check == "required_columns_present" for f in failures)


def test_trim_whitespace_applied() -> None:
    df = pd.DataFrame(
        {"name": [" alice", "bob ", "  carol  ", "dave"]}
    )
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="name",
                operation="trim_whitespace",
                parameters={},
                confidence="high",
                reasoning="leading/trailing whitespace",
            )
        ],
    )

    cleaned, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 1
    assert list(cleaned["name"]) == ["alice", "bob", "carol", "dave"]


def test_replace_value_map_applied() -> None:
    df = pd.DataFrame(
        {"status": ["Closed", "Open", "Closed", "Resolved", "Open"]}
    )
    profile = _profile_for(df)
    plan = CleaningPlan(
        summary="ok",
        rules=[
            CleaningRule(
                column="status",
                operation="replace_value_map",
                parameters={"mapping": {"Resolved": "Closed"}},
                confidence="high",
                reasoning="Resolved is a synonym for Closed in this schema",
            )
        ],
    )

    cleaned, audit = apply_plan(df, plan, profile)

    assert audit.applied_count == 1
    assert "Resolved" not in cleaned["status"].values
    assert (cleaned["status"] == "Closed").sum() == 3
