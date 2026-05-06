"""Tool-using autonomous data quality auditor (Phase 2).

Where Phase 1 produced a complete cleaning plan in one LLM call and applied
it as a static script, Phase 2 turns the LLM into an agent: it's given typed
tools and decides what to do next, iteratively, based on what it discovers
about the data.

The same safety gates from `apply.py` still protect the data — when the
agent calls `apply_rule`, the gates re-validate exactly as before. Phase 1's
defensive infrastructure becomes Phase 2's tooling.

Tools the agent has:
  - get_dataset_overview() : list of all columns with basic stats
  - get_column_profile(column) : drill into one column
  - apply_rule(column, op, params, reasoning) : execute (subject to gates)
  - compare_before_after(column) : verify a change
  - finish(summary) : terminate the loop

The agent's behavior is controlled by AGENT_SYSTEM_PROMPT — adjust there.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from .apply import (
    APPLIERS,
    GATES,
    RuleAuditEntry,
    _count_changed_rows,
)
from .cleaning import CleaningRule
from .llm_client import LLMClient
from .profile import (
    ColumnProfile,
    DatasetProfile,
    to_cleaning_payload,
)


# --------------------------------------------------------------------------- #
# Agent state — what the agent operates on through its tools.
# --------------------------------------------------------------------------- #


@dataclass
class AgentState:
    df: pd.DataFrame
    profile: DatasetProfile
    profile_by_column: dict[str, ColumnProfile] = field(default_factory=dict)
    audit_entries: list[RuleAuditEntry] = field(default_factory=list)
    inspected_columns: set[str] = field(default_factory=set)
    iterations: int = 0
    finished: bool = False
    final_summary: str = ""

    def __post_init__(self) -> None:
        if not self.profile_by_column:
            self.profile_by_column = {c.name: c for c in self.profile.columns}


# --------------------------------------------------------------------------- #
# Tool schemas — what the LLM sees.
# --------------------------------------------------------------------------- #


TOOLS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_dataset_overview",
            "description": (
                "List every column with its dtype, null %, and unique count. "
                "Call this FIRST to orient yourself before drilling into "
                "specific columns."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_column_profile",
            "description": (
                "Get the full profile for one column: casing patterns, "
                "whitespace stats, top values, sample values, etc. Call "
                "this on columns you suspect have quality issues."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "column": {
                        "type": "string",
                        "description": "Name of the column to profile.",
                    }
                },
                "required": ["column"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "apply_rule",
            "description": (
                "Apply a cleaning rule to a column. The rule passes through "
                "deterministic safety gates — if a gate refuses, the tool "
                "returns a rejection with the reason. DO NOT retry the same "
                "rule after a rejection; either pick a different operation "
                "or use mark_for_review."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "operation": {
                        "type": "string",
                        "enum": [
                            "set_case",
                            "trim_whitespace",
                            "collapse_internal_whitespace",
                            "replace_value_map",
                            "mark_for_review",
                        ],
                    },
                    "parameters": {
                        "type": "object",
                        "description": (
                            "Operation-specific parameters. "
                            "set_case: {\"case\": \"title\"|\"upper\"|\"lower\"}. "
                            "trim_whitespace: {}. "
                            "collapse_internal_whitespace: {}. "
                            "replace_value_map: {\"mapping\": {...}}. "
                            "mark_for_review: {\"note\": \"...\"}."
                        ),
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "One sentence: why this rule applies.",
                    },
                },
                "required": ["column", "operation", "reasoning"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_before_after",
            "description": (
                "Show current sample values for a column. Use after "
                "applying a rule to verify the change looks right."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "n_samples": {"type": "integer", "default": 5},
                },
                "required": ["column"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "finish",
            "description": (
                "Call when you have completed the audit. Provide a brief "
                "summary of what you applied, skipped, and flagged."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                },
                "required": ["summary"],
            },
        },
    },
]


# --------------------------------------------------------------------------- #
# Tool implementations.
# --------------------------------------------------------------------------- #


def _tool_get_dataset_overview(state: AgentState, args: dict) -> dict:
    """Enriched overview — for each column, includes pre-computed issue hints
    so the agent doesn't have to drill into every column to discover problems.

    The agent still decides what to DO with these hints; we just save it the
    token cost of rediscovering things the deterministic profiler already
    knows.
    """
    cols: list[dict] = []
    mixed_casing: list[str] = []
    has_double_spaces: list[str] = []
    has_padding_whitespace: list[str] = []
    high_null_skip: list[str] = []
    possible_denorm_pairs: list[list[str]] = []

    for c in state.profile.columns:
        entry = {
            "name": c.name,
            "dtype": c.dtype,
            "null_pct": c.null_pct,
            "unique_count": c.unique_count,
        }

        if c.null_pct > 95:
            high_null_skip.append(c.name)
        elif c.string_stats:
            ss = c.string_stats
            entry["max_length"] = ss.max_length
            issues: list[str] = []
            if ss.count_all_upper > 0 and ss.count_title_case > 0:
                # Skip the short-code case to match the safety gate's logic.
                if not (ss.max_length <= 6 and c.unique_count < 50):
                    issues.append("mixed_casing")
                    mixed_casing.append(c.name)
            if ss.has_double_spaces > 0:
                issues.append("double_spaces")
                has_double_spaces.append(c.name)
            if (
                ss.has_leading_whitespace > 0
                or ss.has_trailing_whitespace > 0
            ):
                issues.append("whitespace_padding")
                has_padding_whitespace.append(c.name)
            if issues:
                entry["issues"] = issues

        cols.append(entry)

    # Detect possible denormalization: columns sharing identical
    # (unique_count, dtype) and similar top-value distributions.
    seen: dict[tuple, list[str]] = {}
    for c in state.profile.columns:
        if c.top_values and c.unique_count <= 50:
            key = (
                c.unique_count,
                tuple((tv.value, tv.count) for tv in c.top_values[:5]),
            )
            seen.setdefault(key, []).append(c.name)
    for names in seen.values():
        if len(names) > 1:
            possible_denorm_pairs.append(names)

    return {
        "row_count": state.profile.row_count,
        "column_count": state.profile.column_count,
        "columns": cols,
        "issue_summary": {
            "mixed_casing": mixed_casing,
            "double_spaces": has_double_spaces,
            "whitespace_padding": has_padding_whitespace,
            "high_null_to_skip": high_null_skip,
            "possible_denormalization_pairs": possible_denorm_pairs,
        },
    }


def _tool_get_column_profile(state: AgentState, args: dict) -> dict:
    column = args["column"]
    col = state.profile_by_column.get(column)
    if col is None:
        return {"error": f"column not found: {column!r}"}

    state.inspected_columns.add(column)

    # Build a single-column slim profile (same shape the LLM saw in Phase 1).
    payload = to_cleaning_payload(
        DatasetProfile(
            source_path=state.profile.source_path,
            row_count=state.profile.row_count,
            column_count=1,
            columns=[col],
        ),
        skip_high_null_threshold=100.0,  # don't filter — agent wants to see it
    )
    if payload["columns"]:
        return payload["columns"][0]
    return {"name": column, "null_pct": col.null_pct}


def _tool_apply_rule(state: AgentState, args: dict) -> dict:
    column = args["column"]
    operation = args["operation"]
    parameters = args.get("parameters") or {}
    reasoning = args["reasoning"]

    rule = CleaningRule(
        column=column,
        operation=operation,
        parameters=parameters,
        confidence="high",  # agent is committing — high confidence by definition
        reasoning=reasoning,
    )

    col_profile = state.profile_by_column.get(column)

    # mark_for_review is the agent's deliberate "needs human attention" signal.
    # It must be handled BEFORE the safety gates — otherwise the gate (which
    # exists for Phase 1's auto-apply policy) would refuse it as "never applied"
    # and we'd report a misleading "rejected" status.
    if operation == "mark_for_review":
        state.audit_entries.append(
            RuleAuditEntry(
                rule=rule,
                status="skipped",
                reason="marked for human review",
            )
        )
        return {
            "status": "marked_for_review",
            "note": parameters.get("note", ""),
        }

    gate = GATES.get(operation)
    if gate is None:
        return {
            "status": "rejected",
            "reason": f"no gate defined for operation {operation!r}",
        }

    ok, gate_reason = gate(rule, col_profile)
    if not ok:
        state.audit_entries.append(
            RuleAuditEntry(rule=rule, status="skipped", reason=gate_reason)
        )
        return {"status": "rejected", "reason": gate_reason}

    if column not in state.df.columns:
        return {"status": "rejected", "reason": "column not found in dataframe"}

    try:
        applier = APPLIERS[operation]
        before = state.df[column]
        after = applier(before, parameters)
        state.df[column] = after
        rows_changed = _count_changed_rows(before, after)
        state.audit_entries.append(
            RuleAuditEntry(
                rule=rule, status="applied", rows_changed=rows_changed
            )
        )
        return {"status": "applied", "rows_changed": rows_changed}
    except Exception as exc:  # noqa: BLE001
        state.audit_entries.append(
            RuleAuditEntry(
                rule=rule,
                status="failed",
                reason=f"{type(exc).__name__}: {exc}",
            )
        )
        return {"status": "failed", "reason": str(exc)}


def _tool_compare_before_after(state: AgentState, args: dict) -> dict:
    column = args["column"]
    n = int(args.get("n_samples", 5))

    if column not in state.df.columns:
        return {"error": f"column not in dataframe: {column!r}"}

    samples = state.df[column].dropna().head(n).tolist()
    return {
        "column": column,
        "current_samples": [str(s) for s in samples],
        "note": (
            "These reflect the column AFTER any applied rules. Trust the "
            "rows_changed count from apply_rule for verification."
        ),
    }


def _tool_finish(state: AgentState, args: dict) -> dict:
    state.finished = True
    state.final_summary = args.get("summary", "")
    return {"acknowledged": True}


TOOL_DISPATCH = {
    "get_dataset_overview": _tool_get_dataset_overview,
    "get_column_profile": _tool_get_column_profile,
    "apply_rule": _tool_apply_rule,
    "compare_before_after": _tool_compare_before_after,
    "finish": _tool_finish,
}


# --------------------------------------------------------------------------- #
# System prompt — drives the agent's behavior.
# --------------------------------------------------------------------------- #


AGENT_SYSTEM_PROMPT = """\
You are an autonomous data quality auditor. Examine a tabular dataset
through tool calls, apply safe cleaning rules across ALL columns that need
them, and use mark_for_review when uncertain.

WORKFLOW
1. Call get_dataset_overview FIRST. The result includes an `issue_summary`
   block listing columns flagged with mixed_casing, double_spaces,
   whitespace_padding, and possible denormalization pairs. THIS IS YOUR
   ROADMAP — work through it.

2. For each flagged column, you may optionally call get_column_profile to
   see details (sample values, top values), but it is NOT required if the
   issue is clear from the overview hints.

3. Apply rules in batches by issue type:
   - Apply set_case (case=title) to EVERY column listed under mixed_casing.
   - Apply collapse_internal_whitespace to EVERY column listed under
     double_spaces.
   - Apply trim_whitespace to EVERY column listed under whitespace_padding.
   - For each pair under possible_denormalization_pairs, mark_for_review
     ONE of the columns with a note explaining the redundancy.

4. Be thorough — do NOT stop after the first few obvious columns. Address
   every column the overview flags.

5. When everything reasonable is addressed, call finish with a brief summary.

CONSTRAINTS
- Skip columns listed under high_null_to_skip — those are conditional fields.
- Trust the safety gates. If apply_rule rejects, do NOT retry the same rule
  on the same column — pick a different operation or move on.
- Don't waste turns on get_column_profile when the overview already told you
  what's wrong. Use it only when you need sample values or top values.
- Keep tool-call count under 30 total.

CASING REMINDER
- The overview's mixed_casing list already filters out short-code columns
  (max_length <= 6 with all-caps codes like NYPD/HPD/DOT). Trust it.
- Free-text columns (high unique_count, long mean_length) — be cautious
  with set_case; prefer mark_for_review for those.

Always include reasoning when calling apply_rule.
"""


# --------------------------------------------------------------------------- #
# Agent loop.
# --------------------------------------------------------------------------- #


@dataclass
class AgentTrace:
    """Trace of the agent's run — debugging and portfolio artifact."""

    iterations: int = 0
    tool_calls: list[dict] = field(default_factory=list)
    final_summary: str = ""


def run_agent(
    df: pd.DataFrame,
    profile: DatasetProfile,
    *,
    client: LLMClient | None = None,
    max_iterations: int = 25,
    verbose: bool = True,
) -> tuple[pd.DataFrame, AgentState, AgentTrace]:
    """Run the agent loop until it calls finish or hits max_iterations.

    Returns the cleaned dataframe, the final state, and a trace of all
    tool calls made.
    """
    client = client or LLMClient()
    state = AgentState(df=df.copy(), profile=profile)
    trace = AgentTrace()

    messages: list[dict] = [
        {"role": "system", "content": AGENT_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "Audit and clean this dataset. Begin by calling "
                "get_dataset_overview."
            ),
        },
    ]

    for iteration in range(max_iterations):
        if state.finished:
            break

        state.iterations = iteration + 1
        trace.iterations = iteration + 1

        if verbose:
            print(f"\n--- iteration {iteration + 1} ---", flush=True)

        response = client.chat_with_tools(messages, TOOLS, max_tokens=1500)

        # Append assistant turn (with any tool_calls) to the conversation.
        assistant_msg: dict[str, Any] = {
            "role": "assistant",
            "content": response.text or "",
        }
        if response.tool_calls:
            assistant_msg["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.name,
                        "arguments": json.dumps(tc.arguments),
                    },
                }
                for tc in response.tool_calls
            ]
        messages.append(assistant_msg)

        if not response.tool_calls:
            if verbose:
                print("  (no tool calls — agent ended turn)")
            break

        for tc in response.tool_calls:
            handler = TOOL_DISPATCH.get(tc.name)
            if handler is None:
                result = {"error": f"unknown tool: {tc.name!r}"}
            else:
                try:
                    result = handler(state, tc.arguments)
                except Exception as exc:  # noqa: BLE001
                    result = {"error": f"{type(exc).__name__}: {exc}"}

            trace.tool_calls.append(
                {
                    "iteration": iteration + 1,
                    "tool": tc.name,
                    "arguments": tc.arguments,
                    "result": result,
                }
            )

            if verbose:
                args_short = json.dumps(tc.arguments)[:80]
                result_short = json.dumps(result)[:140]
                print(f"  -> {tc.name}({args_short})")
                print(f"     {result_short}")

            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(result),
                }
            )

    trace.final_summary = state.final_summary
    return state.df, state, trace
