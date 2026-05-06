"""Apply a CleaningPlan to a DataFrame with deterministic safety gates.

Architecture: the LLM proposes; deterministic code disposes.

For every rule in the plan we run two layers of defense:
  1. A confidence gate — only `high` confidence rules auto-apply. `medium`
     and `low` are skipped and logged for human review.
  2. A per-operation safety gate — re-checks the actual column profile to
     make sure the rule's preconditions hold (e.g., set_case requires both
     casing variants to actually be present). This catches LLM false
     positives even when they pass the confidence gate.

Every rule produces an audit entry with status (`applied` / `skipped` /
`failed`), the reason if skipped, and the count of rows changed.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field

from .cleaning import CleaningPlan, CleaningRule
from .profile import ColumnProfile, DatasetProfile

RuleStatus = Literal["applied", "skipped", "failed"]


class RuleAuditEntry(BaseModel):
    rule: CleaningRule
    status: RuleStatus
    reason: str = ""
    rows_changed: int = 0


class ApplyAudit(BaseModel):
    started_at: str
    finished_at: str
    input_rows: int
    output_rows: int
    input_columns: int
    output_columns: int
    entries: list[RuleAuditEntry] = Field(default_factory=list)

    @property
    def applied_count(self) -> int:
        return sum(1 for e in self.entries if e.status == "applied")

    @property
    def skipped_count(self) -> int:
        return sum(1 for e in self.entries if e.status == "skipped")

    @property
    def failed_count(self) -> int:
        return sum(1 for e in self.entries if e.status == "failed")


# --------------------------------------------------------------------------- #
# Per-operation safety gates.
# Each gate returns (ok: bool, reason_if_not_ok: str).
# --------------------------------------------------------------------------- #


def _gate_set_case(rule: CleaningRule, col: ColumnProfile | None) -> tuple[bool, str]:
    if col is None:
        return False, "column not found in profile"
    if col.string_stats is None:
        return False, "column has no string_stats — set_case requires a string column"

    ss = col.string_stats
    if not (ss.count_all_upper > 0 and ss.count_title_case > 0):
        return False, (
            f"column is internally consistent "
            f"(count_all_upper={ss.count_all_upper}, "
            f"count_title_case={ss.count_title_case}); "
            "set_case would corrupt deliberate casing"
        )

    # Short-code / abbreviation guard.
    if ss.max_length <= 6 and col.unique_count < 50:
        return False, (
            f"column looks like a short-code abbreviation "
            f"(max_length={ss.max_length}, unique_count={col.unique_count})"
        )

    case = (rule.parameters or {}).get("case")
    if case not in ("upper", "lower", "title"):
        return False, f"unknown case parameter: {case!r}"

    return True, ""


def _gate_trim_whitespace(
    rule: CleaningRule, col: ColumnProfile | None
) -> tuple[bool, str]:
    if col is None:
        return False, "column not found in profile"
    if col.string_stats is None:
        return False, "column has no string_stats"
    ss = col.string_stats
    if ss.has_leading_whitespace == 0 and ss.has_trailing_whitespace == 0:
        return False, "no leading/trailing whitespace detected in profile"
    return True, ""


def _gate_collapse_internal_whitespace(
    rule: CleaningRule, col: ColumnProfile | None
) -> tuple[bool, str]:
    if col is None:
        return False, "column not found in profile"
    if col.string_stats is None:
        return False, "column has no string_stats"
    if col.string_stats.has_double_spaces == 0:
        return False, "no double-spaces detected in profile"
    return True, ""


def _gate_replace_value_map(
    rule: CleaningRule, col: ColumnProfile | None
) -> tuple[bool, str]:
    if col is None:
        return False, "column not found in profile"
    mapping = (rule.parameters or {}).get("mapping")
    if not isinstance(mapping, dict) or not mapping:
        return False, "missing or empty 'mapping' parameter"
    return True, ""


def _gate_drop_column(
    rule: CleaningRule, col: ColumnProfile | None
) -> tuple[bool, str]:
    # Drops are dangerous. Don't auto-apply.
    return False, "drop_column is never auto-applied; review manually"


def _gate_mark_for_review(
    rule: CleaningRule, col: ColumnProfile | None
) -> tuple[bool, str]:
    # Always skipped — by definition.
    return False, "mark_for_review rules are never applied"


# --------------------------------------------------------------------------- #
# Per-operation apply functions.
# Each takes a Series, returns a new Series.
# --------------------------------------------------------------------------- #


def _apply_set_case(s: pd.Series, params: dict) -> pd.Series:
    case = params["case"]
    if case == "upper":
        return s.str.upper()
    if case == "lower":
        return s.str.lower()
    if case == "title":
        return s.str.title()
    raise ValueError(f"Unknown case: {case!r}")


def _apply_trim_whitespace(s: pd.Series, params: dict) -> pd.Series:
    return s.str.strip()


def _apply_collapse_internal_whitespace(
    s: pd.Series, params: dict
) -> pd.Series:
    return s.str.replace(r"\s+", " ", regex=True)


def _apply_replace_value_map(s: pd.Series, params: dict) -> pd.Series:
    return s.replace(params["mapping"])


# --------------------------------------------------------------------------- #
# Top-level orchestration.
# --------------------------------------------------------------------------- #


GATES = {
    "set_case": _gate_set_case,
    "trim_whitespace": _gate_trim_whitespace,
    "collapse_internal_whitespace": _gate_collapse_internal_whitespace,
    "replace_value_map": _gate_replace_value_map,
    "drop_column": _gate_drop_column,
    "mark_for_review": _gate_mark_for_review,
}

APPLIERS = {
    "set_case": _apply_set_case,
    "trim_whitespace": _apply_trim_whitespace,
    "collapse_internal_whitespace": _apply_collapse_internal_whitespace,
    "replace_value_map": _apply_replace_value_map,
    # drop_column handled at the DataFrame level, not per-series.
    # mark_for_review is never applied.
}


def _count_changed_rows(before: pd.Series, after: pd.Series) -> int:
    """Count positions where the value actually changed, ignoring NaN==NaN."""
    both_na = before.isna() & after.isna()
    return int(((before != after) & ~both_na).sum())


def apply_plan(
    df: pd.DataFrame,
    plan: CleaningPlan,
    profile: DatasetProfile,
) -> tuple[pd.DataFrame, ApplyAudit]:
    """Apply each rule with safety gates; return cleaned df + audit log."""
    started_at = datetime.now().isoformat(timespec="seconds")
    out = df.copy()

    profile_by_column = {c.name: c for c in profile.columns}
    entries: list[RuleAuditEntry] = []

    for rule in plan.rules:
        col_profile = profile_by_column.get(rule.column)

        # Confidence gate — only auto-apply high-confidence rules.
        if rule.confidence != "high":
            entries.append(
                RuleAuditEntry(
                    rule=rule,
                    status="skipped",
                    reason=(
                        f"confidence={rule.confidence}; only `high` "
                        "rules auto-apply"
                    ),
                )
            )
            continue

        # Per-operation safety gate.
        gate = GATES.get(rule.operation)
        if gate is None:
            entries.append(
                RuleAuditEntry(
                    rule=rule,
                    status="failed",
                    reason=f"no gate defined for operation {rule.operation!r}",
                )
            )
            continue

        ok, gate_reason = gate(rule, col_profile)
        if not ok:
            entries.append(
                RuleAuditEntry(rule=rule, status="skipped", reason=gate_reason)
            )
            continue

        # Apply.
        try:
            if rule.operation == "drop_column":
                # Should be unreachable — gate refuses — but guard anyway.
                continue

            applier = APPLIERS[rule.operation]
            if rule.column not in out.columns:
                entries.append(
                    RuleAuditEntry(
                        rule=rule,
                        status="failed",
                        reason="column not found in dataframe",
                    )
                )
                continue

            before = out[rule.column]
            after = applier(before, rule.parameters or {})
            out[rule.column] = after

            rows_changed = _count_changed_rows(before, after)
            entries.append(
                RuleAuditEntry(
                    rule=rule, status="applied", rows_changed=rows_changed
                )
            )
        except Exception as exc:  # noqa: BLE001
            entries.append(
                RuleAuditEntry(
                    rule=rule, status="failed", reason=f"{type(exc).__name__}: {exc}"
                )
            )

    audit = ApplyAudit(
        started_at=started_at,
        finished_at=datetime.now().isoformat(timespec="seconds"),
        input_rows=len(df),
        output_rows=len(out),
        input_columns=len(df.columns),
        output_columns=len(out.columns),
        entries=entries,
    )
    return out, audit


# --------------------------------------------------------------------------- #
# Lightweight post-apply validation — Phase 1 sanity checks only.
# --------------------------------------------------------------------------- #


class ValidationFailure(BaseModel):
    check: str
    detail: str


def validate_cleaned(
    cleaned: pd.DataFrame,
    original: pd.DataFrame,
    *,
    required_columns: list[str] | None = None,
    unique_key_column: str | None = None,
) -> list[ValidationFailure]:
    """Run sanity checks on the cleaned dataframe. Returns a (possibly empty)
    list of failures.

    These checks are intentionally minimal for Phase 1. Phase 2 may layer
    in great_expectations or schema-driven validation.
    """
    failures: list[ValidationFailure] = []

    if len(cleaned) != len(original):
        failures.append(
            ValidationFailure(
                check="row_count_preserved",
                detail=(
                    f"input has {len(original):,} rows but output has "
                    f"{len(cleaned):,}"
                ),
            )
        )

    for col in required_columns or []:
        if col not in cleaned.columns:
            failures.append(
                ValidationFailure(
                    check="required_columns_present",
                    detail=f"required column missing: {col!r}",
                )
            )

    if unique_key_column and unique_key_column in cleaned.columns:
        dup_count = cleaned[unique_key_column].duplicated().sum()
        if dup_count > 0:
            failures.append(
                ValidationFailure(
                    check="unique_key_unique",
                    detail=(
                        f"{dup_count:,} duplicate values in "
                        f"{unique_key_column!r}"
                    ),
                )
            )

    return failures


# --------------------------------------------------------------------------- #
# Persistence helpers.
# --------------------------------------------------------------------------- #


def save_audit(audit: ApplyAudit, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(audit.model_dump_json(indent=2))
