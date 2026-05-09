"""Tests for the cleaning module — schema validation and prompt construction.

LLM-calling tests live elsewhere (would be a slower integration suite).
These tests run in milliseconds and don't require network or API keys.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from datapolish.cleaning import (
    CleaningPlan,
    CleaningRule,
    build_user_prompt,
    derive_short_label,
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


def test_custom_instructions_appear_in_user_prompt() -> None:
    profile = _tiny_profile()
    prompt = build_user_prompt(profile, custom_instructions="be conservative on dates")
    assert "user_steering" in prompt
    assert "be conservative on dates" in prompt


def test_custom_instructions_are_capped_and_stripped() -> None:
    profile = _tiny_profile()
    long_text = "  abc " * 200  # > 500 chars after expansion
    prompt = build_user_prompt(profile, custom_instructions=long_text)
    # The user_steering block exists but is bounded.
    assert "user_steering" in prompt
    # Should not be longer than the cap plus a small overhead.
    block = prompt.split("<user_steering>", 1)[1].split("</user_steering>", 1)[0]
    assert len(block.strip()) <= 510


def test_no_user_steering_block_when_instructions_empty() -> None:
    profile = _tiny_profile()
    prompt = build_user_prompt(profile, custom_instructions=None)
    assert "user_steering" not in prompt
    prompt2 = build_user_prompt(profile, custom_instructions="   ")
    assert "user_steering" not in prompt2


def _rule(operation: str, column: str = "x", **params) -> CleaningRule:
    return CleaningRule(
        column=column,
        operation=operation,
        parameters=params or {},
        confidence="high",
        reasoning="t",
    )


def test_derive_short_label_set_case() -> None:
    label = derive_short_label(_rule("set_case", "complaint_type", case="title"))
    assert label.startswith("Title-case")
    assert "complaint_type" in label


def test_derive_short_label_other_ops() -> None:
    assert derive_short_label(_rule("trim_whitespace", "name")).startswith("Trim")
    assert derive_short_label(
        _rule("collapse_internal_whitespace", "addr")
    ).startswith("Collapse")
    assert derive_short_label(_rule("mark_for_review", "agency_name")).startswith(
        "Review"
    )


def test_derive_short_label_capped_to_40() -> None:
    label = derive_short_label(_rule("set_case", "x" * 100, case="title"))
    assert len(label) <= 40


def test_short_label_back_filled_when_missing() -> None:
    """If the LLM omits short_label, it gets filled in deterministically.
    Verified at the schema level — short_label has a default of \"\"."""
    rule = CleaningRule(
        column="complaint_type",
        operation="set_case",
        parameters={"case": "title"},
        confidence="high",
        reasoning="t",
    )
    assert rule.short_label == ""  # default before back-fill
    rule.short_label = derive_short_label(rule)
    assert rule.short_label  # non-empty after back-fill
