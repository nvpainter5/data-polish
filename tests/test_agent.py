"""Tests for the agent's tool implementations.

We don't test the LLM loop here (that requires real API calls and is
flaky / costly). We test the tool dispatch and individual tool behaviors
deterministically.
"""

from __future__ import annotations

import pandas as pd

from datapolish.agent import (
    AgentState,
    _tool_apply_rule,
    _tool_compare_before_after,
    _tool_finish,
    _tool_get_column_profile,
    _tool_get_dataset_overview,
)
from datapolish.profile import profile_dataset


def _make_state(df: pd.DataFrame) -> AgentState:
    profile = profile_dataset(df, source_path="memory")
    return AgentState(df=df.copy(), profile=profile)


def test_overview_lists_all_columns() -> None:
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    state = _make_state(df)
    result = _tool_get_dataset_overview(state, {})
    assert result["row_count"] == 3
    assert result["column_count"] == 2
    names = [c["name"] for c in result["columns"]]
    assert "a" in names and "b" in names
    # New: issue_summary is now part of the overview.
    assert "issue_summary" in result


def test_overview_flags_mixed_casing_columns() -> None:
    """Overview should pre-compute which columns have casing issues."""
    df = pd.DataFrame(
        {
            "complaint_type": [
                "Noise - Residential",
                "HEAT/HOT WATER",
                "Blocked Driveway",
                "ILLEGAL PARKING",
            ]
            * 5,
            "agency": ["NYPD", "HPD", "DOT"] * 6 + ["DSNY", "DPR"],
        }
    )
    state = _make_state(df)
    result = _tool_get_dataset_overview(state, {})
    flagged = result["issue_summary"]["mixed_casing"]
    # complaint_type has mixed casing AND isn't a short-code column -> flagged.
    assert "complaint_type" in flagged
    # agency is all upper short-codes -> NOT flagged (matches safety gate).
    assert "agency" not in flagged


def test_overview_flags_double_spaces() -> None:
    df = pd.DataFrame(
        {
            "addr": ["123  Main St", "456 Oak  Ave"] * 5,
            "clean": ["one", "two"] * 5,
        }
    )
    state = _make_state(df)
    result = _tool_get_dataset_overview(state, {})
    assert "addr" in result["issue_summary"]["double_spaces"]
    assert "clean" not in result["issue_summary"]["double_spaces"]


def test_get_column_profile_returns_slim_view() -> None:
    df = pd.DataFrame(
        {"complaint_type": ["Noise", "HEAT/HOT WATER", "Other"] * 5}
    )
    state = _make_state(df)
    result = _tool_get_column_profile(state, {"column": "complaint_type"})
    assert result["name"] == "complaint_type"
    # Should include string_stats since it's a string column.
    assert "string_stats" in result
    assert "complaint_type" in state.inspected_columns


def test_get_column_profile_handles_missing_column() -> None:
    df = pd.DataFrame({"a": [1]})
    state = _make_state(df)
    result = _tool_get_column_profile(state, {"column": "does_not_exist"})
    assert "error" in result


def test_apply_rule_succeeds_on_valid_mixed_casing() -> None:
    df = pd.DataFrame(
        {
            "complaint_type": [
                "Noise - Residential",
                "HEAT/HOT WATER",
                "Blocked Driveway",
                "ILLEGAL PARKING",
                "Street Condition",
            ]
            * 10
        }
    )
    state = _make_state(df)
    result = _tool_apply_rule(
        state,
        {
            "column": "complaint_type",
            "operation": "set_case",
            "parameters": {"case": "title"},
            "reasoning": "mixed casing detected",
        },
    )
    assert result["status"] == "applied"
    assert result["rows_changed"] > 0
    assert any(e.status == "applied" for e in state.audit_entries)


def test_apply_rule_rejected_on_short_code_column() -> None:
    """The agency / NYPD safety case — gate must refuse."""
    df = pd.DataFrame({"agency": ["NYPD", "HPD", "DOT", "DSNY"] * 25})
    state = _make_state(df)
    result = _tool_apply_rule(
        state,
        {
            "column": "agency",
            "operation": "set_case",
            "parameters": {"case": "title"},
            "reasoning": "model thinks this needs casing",
        },
    )
    assert result["status"] == "rejected"
    assert "consistent" in result["reason"] or "abbreviation" in result["reason"]
    # Original data untouched.
    assert all(v.isupper() for v in state.df["agency"])


def test_apply_rule_mark_for_review() -> None:
    df = pd.DataFrame({"x": ["a", "b", "c"]})
    state = _make_state(df)
    result = _tool_apply_rule(
        state,
        {
            "column": "x",
            "operation": "mark_for_review",
            "parameters": {"note": "human attention needed"},
            "reasoning": "uncertain",
        },
    )
    assert result["status"] == "marked_for_review"
    assert any(e.status == "skipped" for e in state.audit_entries)


def test_compare_before_after_returns_samples() -> None:
    df = pd.DataFrame({"x": ["a", "b", "c", "d"]})
    state = _make_state(df)
    result = _tool_compare_before_after(state, {"column": "x", "n_samples": 3})
    assert result["column"] == "x"
    assert len(result["current_samples"]) == 3


def test_finish_marks_state_done() -> None:
    df = pd.DataFrame({"x": [1]})
    state = _make_state(df)
    result = _tool_finish(state, {"summary": "all done"})
    assert result["acknowledged"] is True
    assert state.finished is True
    assert state.final_summary == "all done"
