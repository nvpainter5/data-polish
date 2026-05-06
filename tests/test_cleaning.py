"""Tests for the cleaning module — schema validation and prompt construction.

LLM-calling tests live elsewhere (would be a slower integration suite).
These tests run in milliseconds and don't require network or API keys.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from datapolish.cleaning import (
    CleaningPlan,
    build_user_prompt,
)
from datapolish.profile import ColumnProfile, DatasetProfile


def _tiny_profile() -> DatasetProfile:
    return DatasetProfile(
        source_path="memory",
        row_count=10,
        column_count=1,
        columns=[
            ColumnProfile(
                name="x",
                dtype="int64",
                null_count=0,
                null_pct=0.0,
                unique_count=10,
                sample_values=["1", "2", "3"],
            )
        ],
    )


def test_valid_plan_validates() -> None:
    raw = {
        "summary": "Mixed casing in two columns; otherwise clean.",
        "rules": [
            {
                "column": "complaint_type",
                "operation": "set_case",
                "parameters": {"case": "title"},
                "confidence": "high",
                "reasoning": "10k rows in upper case among 50k total.",
            },
            {
                "column": "agency_name",
                "operation": "mark_for_review",
                "parameters": {"note": "denormalized with agency"},
                "confidence": "medium",
                "reasoning": "Same distribution as agency suggests redundancy.",
            },
        ],
    }
    plan = CleaningPlan.model_validate(raw)
    assert len(plan.rules) == 2
    assert plan.rules[0].operation == "set_case"


def test_invalid_operation_rejected() -> None:
    raw = {
        "summary": "Bad op",
        "rules": [
            {
                "column": "x",
                "operation": "delete_everything",  # not allowed
                "parameters": {},
                "confidence": "high",
                "reasoning": "irrelevant",
            }
        ],
    }
    with pytest.raises(ValidationError):
        CleaningPlan.model_validate(raw)


def test_invalid_confidence_rejected() -> None:
    raw = {
        "summary": "Bad conf",
        "rules": [
            {
                "column": "x",
                "operation": "set_case",
                "parameters": {"case": "title"},
                "confidence": "very_high",  # not allowed
                "reasoning": "irrelevant",
            }
        ],
    }
    with pytest.raises(ValidationError):
        CleaningPlan.model_validate(raw)


def test_missing_reasoning_rejected() -> None:
    raw = {
        "summary": "Missing required field",
        "rules": [
            {
                "column": "x",
                "operation": "trim_whitespace",
                "parameters": {},
                "confidence": "high",
                # no reasoning
            }
        ],
    }
    with pytest.raises(ValidationError):
        CleaningPlan.model_validate(raw)


def test_user_prompt_embeds_profile() -> None:
    profile = _tiny_profile()
    prompt = build_user_prompt(profile)
    assert "<profile>" in prompt
    assert "</profile>" in prompt
    # Slim payload uses compact JSON.
    assert '"name":"x"' in prompt


def test_high_null_columns_filtered() -> None:
    """Columns with >95% null should not appear in the slim payload."""
    from datapolish.profile import to_cleaning_payload

    profile = DatasetProfile(
        source_path="memory",
        row_count=1000,
        column_count=2,
        columns=[
            ColumnProfile(
                name="keep_me",
                dtype="str",
                null_count=0,
                null_pct=0.0,
                unique_count=10,
                sample_values=["a", "b"],
            ),
            ColumnProfile(
                name="drop_me",
                dtype="str",
                null_count=999,
                null_pct=99.9,
                unique_count=1,
                sample_values=["x"],
            ),
        ],
    )
    payload = to_cleaning_payload(profile)
    column_names = [c["name"] for c in payload["columns"]]
    assert "keep_me" in column_names
    assert "drop_me" not in column_names
    assert payload["columns_skipped_for_high_null"] == ["drop_me"]


def test_empty_rule_list_is_valid() -> None:
    """If the LLM thinks the dataset is already clean, an empty plan is OK."""
    raw = {"summary": "No issues found.", "rules": []}
    plan = CleaningPlan.model_validate(raw)
    assert plan.rules == []
