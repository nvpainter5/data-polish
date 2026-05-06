"""LLM-driven cleaning rule proposer.

This module turns a `DatasetProfile` into a `CleaningPlan` by asking an LLM
to audit the profile and propose deterministic, executable rules.

Design notes:
- The LLM never sees raw rows. The profile JSON (~5-15 KB) carries enough
  signal to spot quality issues, and is small enough to process in a single
  call cheaply and quickly.
- The LLM's reply is parsed into a typed `CleaningPlan` (pydantic). If the
  model hallucinates an operation outside the allowed set, validation fails
  loud — we never apply rules we don't recognize.
- The system prompt encodes the *spec*. It is the single source of truth
  for what good cleaning looks like in this pipeline. Iterating on this
  prompt is the dominant form of AI engineering work for this project.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from .llm_client import LLMClient
from .profile import DatasetProfile, estimate_tokens, to_cleaning_payload

# --------------------------------------------------------------------------- #
# Output schema — enforced on the LLM's reply.
# --------------------------------------------------------------------------- #

Operation = Literal[
    "set_case",                       # title / upper / lower
    "trim_whitespace",                # strip leading/trailing whitespace
    "collapse_internal_whitespace",   # collapse double-spaces inside the string
    "replace_value_map",              # apply a small dict of replacements
    "drop_column",                    # remove the column entirely
    "mark_for_review",                # flag as needing human attention
]

Confidence = Literal["high", "medium", "low"]


class CleaningRule(BaseModel):
    column: str
    operation: Operation
    parameters: dict[str, Any] = Field(default_factory=dict)
    confidence: Confidence
    reasoning: str


class CleaningPlan(BaseModel):
    summary: str
    rules: list[CleaningRule]


# --------------------------------------------------------------------------- #
# Prompt — the spec the LLM must follow.
# --------------------------------------------------------------------------- #

SYSTEM_PROMPT = """\
You are a meticulous data quality auditor for a tabular data cleaning pipeline.

INPUT
You will receive a JSON profile of a dataset. Each column entry contains:
  - name, dtype, null_count, null_pct, unique_count, sample_values
  - type-specific stats: string_stats, numeric_stats, datetime_stats
  - top_values (only present for low-cardinality columns)

OUTPUT
Return a SINGLE JSON object matching this exact shape (no markdown, no prose
before or after):

{
  "summary": "1-2 sentence summary of the main quality issues found.",
  "rules": [
    {
      "column": "<column name>",
      "operation": "<one of the operations listed below>",
      "parameters": { ... operation-specific args, possibly empty ... },
      "confidence": "<high | medium | low>",
      "reasoning": "<one sentence explaining why this rule applies>"
    }
  ]
}

ALLOWED OPERATIONS AND THEIR PARAMETERS
  - set_case
      parameters: {"case": "upper" | "lower" | "title"}
  - trim_whitespace
      parameters: {}
  - collapse_internal_whitespace
      parameters: {}
  - replace_value_map
      parameters: {"mapping": {"old_value": "new_value_or_null", ...}}
  - drop_column
      parameters: {}
  - mark_for_review
      parameters: {"note": "describe what concerns you"}

MANDATORY HEURISTICS — DO NOT VIOLATE

1. ONLY propose rules you are confident in. When uncertain, use mark_for_review.

2. DO NOT propose drop_column for sparse columns. High-null columns are usually
   CONDITIONAL FIELDS that apply only to certain row types (e.g.,
   taxi_company_borough only matters for taxi complaints). Dropping them loses
   information.

3. CASING RULES — common source of false positives. Read carefully.

   3a. "Mixed casing" requires BOTH count_all_upper > 0 AND count_title_case > 0.
       If only one of those is greater than zero, the column is internally
       consistent. DO NOT propose set_case in that case.

   3b. When mixed casing IS present, propose set_case to "title".

   3c. Short-code / abbreviation columns are intentionally uppercase. Signals:
       max_length <= 6 AND unique_count < 50 AND sample_values look like
       all-caps codes. DO NOT propose any casing change for these.

   3d. POSITIVE EXAMPLE — propose set_case to "title":
       name="complaint_type", max_length=39, unique_count=158,
       sample_values=["Noise - Residential", "Consumer Complaint"],
       count_all_upper=9996, count_title_case=39116.
       (Both upper and title present; values are descriptive phrases.)

   3e. NEGATIVE EXAMPLE — propose NO rule:
       name="agency", max_length=5, unique_count=14,
       sample_values=["NYPD", "HPD", "DOT"],
       count_all_upper=50000, count_title_case=0.
       (Only one casing present; values are deliberate uppercase abbreviations.
       Mutating these would corrupt the data.)

4. WHITESPACE RULES:
   - has_leading_whitespace > 0 OR has_trailing_whitespace > 0
       -> trim_whitespace.
   - has_double_spaces > 0 -> collapse_internal_whitespace.

5. RULES ARE INDEPENDENT. A column can have BOTH a casing issue AND a
   whitespace issue. Apply both rules. Do not skip whitespace just because
   you already proposed casing on the same column.

6. BE EXHAUSTIVE.
   - Audit every column in the payload. If 5 columns share the same casing
     issue, propose 5 rules — not 2 or 3 because they "look similar."
   - Pairs of columns with apparently identical distributions are
     denormalization (e.g., agency vs agency_name; borough vs park_borough).
     Use mark_for_review on the suspected duplicate. DO NOT auto-drop.

7. CONSERVATISM ON FREE-TEXT. Columns with high unique_count or mean_length > 30
   are free text. Casing changes can break meaning there. Prefer mark_for_review
   for casing on those columns. Whitespace fixes are still safe.

8. STYLE: One rule per (column, issue) pair. Don't stack redundant rules.

Return ONLY the JSON object. No markdown fences, no explanatory text.
"""


def build_user_prompt(profile: DatasetProfile) -> str:
    """Wrap a slim, task-specific view of the profile in a user message.

    We deliberately use the slim payload (not the full profile) to fit
    comfortably under free-tier rate limits and to focus the model's
    attention on cleaning-relevant signal.
    """
    payload = to_cleaning_payload(profile)
    # Compact JSON — no indentation, no separating whitespace.
    payload_json = json.dumps(payload, separators=(",", ":"))
    return (
        "Audit the following dataset profile and propose cleaning rules per "
        "the specification. Columns with >95% nulls have been filtered out "
        "as conditional fields (not cleaning targets); their names are "
        "listed under columns_skipped_for_high_null.\n\n"
        f"<profile>\n{payload_json}\n</profile>"
    )


# --------------------------------------------------------------------------- #
# Driver.
# --------------------------------------------------------------------------- #


def propose_cleaning_rules(
    profile: DatasetProfile,
    *,
    client: LLMClient | None = None,
) -> CleaningPlan:
    """Send the profile to the LLM and parse a CleaningPlan from the reply.

    Raises:
        RuntimeError: if the LLM returns invalid JSON or its JSON does not
            match the CleaningPlan schema. Both error messages include the
            offending payload for debugging.
    """
    client = client or LLMClient()

    user_prompt = build_user_prompt(profile)
    estimated = estimate_tokens(SYSTEM_PROMPT) + estimate_tokens(user_prompt)
    print(f"  Estimated prompt size: ~{estimated:,} tokens")

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    raw = client.chat(
        messages,
        temperature=0.0,
        max_tokens=4000,
        # JSON mode forces the model to return parseable JSON. Schema
        # enforcement is still ours — done by pydantic on the next line.
        response_format={"type": "json_object"},
    )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "LLM returned non-JSON output. First 500 chars:\n"
            f"{raw[:500]}"
        ) from exc

    try:
        return CleaningPlan.model_validate(data)
    except ValidationError as exc:
        raise RuntimeError(
            "LLM JSON did not match the CleaningPlan schema:\n"
            f"{exc}\n\nReceived:\n{json.dumps(data, indent=2)[:1000]}"
        ) from exc


def save_plan(plan: CleaningPlan, output_path: Path) -> None:
    """Serialize a CleaningPlan to JSON on disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(plan.model_dump_json(indent=2))
