"""Data quality score + LLM-driven follow-up suggestions.

Two related concerns kept in one module:

1. `compute_quality_score(profile)` — deterministic 0-100 number summarizing
   how clean a dataset looks. Computed from the profile alone, no AI.
   Computed on BOTH raw and cleaned profiles so the UI can show a delta.

   The score covers issues that fall into two buckets:
     - Auto-fixable (mixed casing, whitespace, etc.) — what the pipeline cleans
     - Flag-only (constant columns, suspected duplicate keys) — needs human review
   A score of 100 doesn't mean the data is semantically perfect; it means
   none of the deterministic checks fired.

2. `generate_suggestions(audit, quality)` — small LLM call that proposes
   3-5 actionable next steps given what was already cleaned and what's
   left. Failures are non-fatal — pipelines run even if Groq is down.
"""

from __future__ import annotations

import json
import re

from pydantic import BaseModel, Field, ValidationError

from .apply import ApplyAudit
from .llm_client import LLMClient
from .profile import DatasetProfile

# --------------------------------------------------------------------------- #
# Quality score (deterministic).
# --------------------------------------------------------------------------- #


class QualityIssue(BaseModel):
    type: str
    column: str
    detail: str = ""


class QualityScore(BaseModel):
    score: int  # 0-100, higher is cleaner
    issue_count: int
    issues: list[QualityIssue] = Field(default_factory=list)


# Per-column penalty for each detected issue. Calibrated so a column with
# 4+ distinct issues lands at 0; clean columns stay at 100. The dataset
# score is the average over non-conditional columns (null_pct < 95).
ISSUE_PENALTY_PER_COLUMN = 25
CONDITIONAL_NULL_THRESHOLD = 95.0


def _looks_like_id_column(name: str) -> bool:
    """Detect ID/key column names across naming conventions.

    Matches:
        - Exact: "id", "ID", "key", "Key", "KEY"
        - snake_case: "customer_id", "user_key"
        - camelCase / PascalCase: "CustomerID", "customerId", "OrderKey"
    Rejects words that happen to end in "id" lowercase without an uppercase
    boundary (e.g. "Mid", "rapid").
    """
    if name in ("id", "ID", "key", "Key", "KEY"):
        return True
    if re.search(r"_(?:id|key)$", name, re.IGNORECASE):
        return True
    # Lowercase letter immediately before an uppercase ID/Key suffix —
    # the camelCase / PascalCase boundary marker.
    if re.search(r"[a-z](?:Id|ID|Key|KEY)$", name):
        return True
    return False


def compute_quality_score(profile: DatasetProfile) -> QualityScore:
    """Walk the profile, flag known issues, return a scored summary.

    Scoring strategy:
      1. For each column, collect a list of distinct issues.
      2. column_score = max(0, 100 - 25 * issue_count_for_that_column).
      3. dataset_score = average of column_scores across non-conditional
         columns (null_pct < 95). Conditional fields like
         taxi_company_borough are excluded — they're sparse by design,
         not by neglect.

    This gives a number that lives in the interpretable 50-100 band on
    real-world datasets and visibly moves when the pipeline cleans things
    the metric counts as issues.
    """
    issues_by_column: dict[str, list[QualityIssue]] = {}

    def _flag(col_name: str, type_: str, detail: str = "") -> None:
        issues_by_column.setdefault(col_name, []).append(
            QualityIssue(type=type_, column=col_name, detail=detail)
        )

    for col in profile.columns:
        if 30 < col.null_pct < 95:
            _flag(col.name, "high_nulls", f"{col.null_pct:.1f}% null")

        if col.string_stats:
            ss = col.string_stats

            # Mixed casing — skip clear short-code columns (NYPD/HPD/DOT).
            mixed = ss.count_all_upper > 0 and ss.count_title_case > 0
            short_code = ss.max_length <= 6 and col.unique_count < 50
            if mixed and not short_code:
                _flag(
                    col.name,
                    "mixed_casing",
                    f"upper={ss.count_all_upper:,}, "
                    f"title={ss.count_title_case:,}",
                )

            if (
                ss.has_leading_whitespace > 0
                or ss.has_trailing_whitespace > 0
            ):
                _flag(
                    col.name,
                    "whitespace_padding",
                    f"lead={ss.has_leading_whitespace}, "
                    f"trail={ss.has_trailing_whitespace}",
                )

            if ss.has_double_spaces > 0:
                _flag(
                    col.name,
                    "double_spaces",
                    f"{ss.has_double_spaces:,} rows",
                )

        if col.outliers:
            if col.outliers.numeric_iqr_outliers > 0:
                _flag(
                    col.name,
                    "numeric_outliers",
                    f"{col.outliers.numeric_iqr_outliers:,} IQR outliers",
                )
            if col.outliers.rare_categorical_count > 0:
                _flag(
                    col.name,
                    "rare_categories",
                    f"{col.outliers.rare_categorical_count:,} rare values",
                )

        # --- Flag-only checks (not auto-fixable, but worth surfacing) ---

        # Constant column: 1 unique value over many rows. Either dead
        # legacy data or the load failed. Either way the column carries
        # no information.
        if (
            col.unique_count == 1
            and profile.row_count > 1
            and col.null_pct < 95
        ):
            _flag(
                col.name,
                "constant_column",
                f"only 1 unique value across {profile.row_count:,} rows",
            )

        # Suspected primary/foreign key with duplicates. Heuristic:
        # column name looks like a key AND has high but imperfect
        # uniqueness (50–99%). Handles both snake_case and camelCase:
        #     - exact:      id, ID, key, Key, KEY
        #     - snake_case: customer_id, my_key
        #     - camelCase:  CustomerID, customerId, OrderKey
        if (
            _looks_like_id_column(col.name)
            and col.unique_count > 0
            and profile.row_count > 0
        ):
            uniqueness = col.unique_count / profile.row_count
            if 0.5 < uniqueness < 1.0:
                _flag(
                    col.name,
                    "suspected_key_duplicates",
                    f"{col.unique_count:,} unique vs {profile.row_count:,} "
                    f"rows ({uniqueness * 100:.1f}% unique)",
                )

    # Per-column scores, averaged over non-conditional columns.
    column_scores: list[int] = []
    for col in profile.columns:
        if col.null_pct >= CONDITIONAL_NULL_THRESHOLD:
            continue  # conditional fields are excluded
        col_issue_count = len(issues_by_column.get(col.name, []))
        col_score = max(0, 100 - ISSUE_PENALTY_PER_COLUMN * col_issue_count)
        column_scores.append(col_score)

    if column_scores:
        dataset_score = int(round(sum(column_scores) / len(column_scores)))
    else:
        dataset_score = 100  # nothing to score = nothing to fix

    all_issues = [
        issue for col_issues in issues_by_column.values() for issue in col_issues
    ]

    return QualityScore(
        score=dataset_score,
        issue_count=len(all_issues),
        issues=all_issues,
    )


# --------------------------------------------------------------------------- #
# Suggestions (LLM-driven, optional).
# --------------------------------------------------------------------------- #


class Suggestions(BaseModel):
    suggestions: list[str] = Field(default_factory=list)


SUGGESTIONS_SYSTEM_PROMPT = """\
You are a data quality consultant. Given a summary of cleaning that was just
performed and the issues that remain, propose 3 to 5 concrete next steps the
user could take to further improve their dataset.

Each suggestion must be:
- Actionable and specific (not "review your data" — name a column or pattern)
- About something NOT already cleaned by the audit
- One short sentence, plain English

Return ONLY a JSON object matching this shape, no markdown, no prose:
{"suggestions": ["...", "...", ...]}
"""


def generate_suggestions(
    audit: ApplyAudit,
    quality: QualityScore,
    *,
    client: LLMClient | None = None,
    max_remaining_issues: int = 20,
) -> Suggestions:
    """Ask the LLM for follow-up suggestions. Returns empty Suggestions on
    failure — never raises (suggestions are nice-to-have, not critical)."""
    try:
        client = client or LLMClient()
    except Exception:  # noqa: BLE001
        return Suggestions()

    applied = [
        f"- {e.rule.short_label or e.rule.operation} on "
        f"{e.rule.column}: {e.rows_changed:,} rows changed"
        for e in audit.entries
        if e.status == "applied"
    ]
    skipped = [
        f"- {e.rule.short_label or e.rule.operation} on "
        f"{e.rule.column} (skipped: {e.reason[:80]})"
        for e in audit.entries
        if e.status == "skipped"
    ]
    remaining = [
        f"- {i.column}: {i.type} ({i.detail})"
        for i in quality.issues[:max_remaining_issues]
    ]

    user_prompt = (
        f"APPLIED ({len(applied)} rules):\n"
        + ("\n".join(applied) if applied else "(none)")
        + f"\n\nSKIPPED ({len(skipped)} rules):\n"
        + ("\n".join(skipped) if skipped else "(none)")
        + "\n\nREMAINING QUALITY ISSUES (sample):\n"
        + ("\n".join(remaining) if remaining else "(none)")
        + f"\n\nQuality score after cleaning: {quality.score}/100"
    )

    try:
        raw = client.chat(
            [
                {"role": "system", "content": SUGGESTIONS_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=600,
            response_format={"type": "json_object"},
        )
        data = json.loads(raw)
        return Suggestions.model_validate(data)
    except (json.JSONDecodeError, ValidationError, Exception):  # noqa: BLE001
        return Suggestions()
