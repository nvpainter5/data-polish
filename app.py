"""Streamlit dashboard for Data Polish — visualizes past pipeline runs.

Run with:
    streamlit run app.py

This is a *visualizer*, not an executor. It loads the latest profile, plan,
audit, and agent trace from reports/ and shows them across five tabs.
The pipeline scripts (scripts/profile_dataset.py, scripts/propose_cleaning.py,
scripts/apply_cleaning.py, scripts/run_agent.py) are still how you produce
those artifacts.

Designed to be screenshot-friendly for portfolio/demo use.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
REPORTS = PROJECT_ROOT / "reports"
RAW = PROJECT_ROOT / "data" / "raw"
CLEANED = PROJECT_ROOT / "data" / "cleaned"

st.set_page_config(
    page_title="Data Polish",
    layout="wide",
    initial_sidebar_state="expanded",
)


# --------------------------------------------------------------------------- #
# Loaders (cached so reruns are snappy).
# --------------------------------------------------------------------------- #


@st.cache_data
def load_json(path: str) -> dict:
    return json.loads(Path(path).read_text())


@st.cache_data
def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


@st.cache_data
def load_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


def list_sorted(pattern: str) -> list[Path]:
    return sorted(REPORTS.glob(pattern))


# --------------------------------------------------------------------------- #
# Header.
# --------------------------------------------------------------------------- #

st.title("Data Polish")
st.markdown(
    "**An AI-augmented data engineering pipeline.** Messy CSV in → "
    "deterministic profile → LLM proposes cleaning rules → safety gates "
    "re-validate each rule → cleaned parquet out, with a full audit log."
)
st.markdown(
    "[GitHub](https://github.com/nvpainter5/DataPolish) "
    "· Built on NYC 311 Service Requests "
    "· Llama 3.3 70B via Groq · Phase 1 + Phase 2 complete"
)


# --------------------------------------------------------------------------- #
# Sidebar — pick which run to inspect.
# --------------------------------------------------------------------------- #

with st.sidebar:
    st.header("Run selection")

    profiles = list_sorted("profile_*.json")
    plans = list_sorted("cleaning_plan_*.json")
    audits = list_sorted("cleaning_audit_*.json")
    traces = list_sorted("agent_trace_*.json")

    if not profiles:
        st.error(
            "No profile found in reports/. "
            "Run `python scripts/profile_dataset.py` first."
        )
        st.stop()

    profile_path = st.selectbox(
        "Profile",
        profiles,
        index=len(profiles) - 1,
        format_func=lambda p: p.name,
    )

    plan_path = (
        st.selectbox(
            "Cleaning plan",
            plans,
            index=len(plans) - 1,
            format_func=lambda p: p.name,
        )
        if plans
        else None
    )

    audit_path = (
        st.selectbox(
            "Cleaning audit",
            audits,
            index=len(audits) - 1,
            format_func=lambda p: p.name,
        )
        if audits
        else None
    )

    trace_path = (
        st.selectbox(
            "Agent trace (Phase 2)",
            [None] + traces,
            index=len(traces) if traces else 0,
            format_func=lambda p: "(none)" if p is None else p.name,
        )
        if traces
        else None
    )

    st.divider()
    st.markdown(
        "### About\n"
        "Data Polish is a portfolio project demonstrating production AI "
        "engineering patterns: profile-first prompting, structured outputs "
        "as a contract, and LLM-proposes-deterministic-code-disposes."
    )


# --------------------------------------------------------------------------- #
# Load the selected artifacts.
# --------------------------------------------------------------------------- #

profile = load_json(str(profile_path))
plan = load_json(str(plan_path)) if plan_path else None
audit = load_json(str(audit_path)) if audit_path else None
trace = load_json(str(trace_path)) if trace_path else None


# --------------------------------------------------------------------------- #
# Tabs.
# --------------------------------------------------------------------------- #

tab_overview, tab_profile, tab_plan, tab_audit, tab_compare = st.tabs(
    [
        "Overview",
        "Profile",
        "Cleaning plan (LLM)",
        "Audit (code)",
        "Before / After",
    ]
)

# ---- OVERVIEW ----
with tab_overview:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Rows", f"{profile['row_count']:,}")
    col2.metric("Columns", profile["column_count"])
    col3.metric(
        "Rules proposed",
        len(plan["rules"]) if plan else 0,
    )
    if audit:
        applied = sum(
            1 for e in audit["entries"] if e["status"] == "applied"
        )
        col4.metric("Rules applied", applied)
    else:
        col4.metric("Rules applied", "—")

    if plan:
        st.subheader("LLM summary (Phase 1)")
        st.info(plan.get("summary", "(no summary)"))

    if trace:
        st.subheader("Agent run (Phase 2)")
        c1, c2, c3 = st.columns(3)
        c1.metric("Iterations", trace["iterations"])
        c2.metric("Tool calls", len(trace["tool_calls"]))
        c3.metric(
            "Final action",
            (
                "finish"
                if trace["tool_calls"]
                and trace["tool_calls"][-1]["tool"] == "finish"
                else "max iterations"
            ),
        )
        if trace.get("final_summary"):
            st.success(trace["final_summary"])

# ---- PROFILE ----
with tab_profile:
    st.subheader("Per-column profile")
    rows = []
    for c in profile["columns"]:
        rows.append(
            {
                "Column": c["name"],
                "Dtype": c["dtype"],
                "Null %": c["null_pct"],
                "Unique": c["unique_count"],
                "Has string_stats": bool(c.get("string_stats")),
                "Has numeric_stats": bool(c.get("numeric_stats")),
                "Has datetime_stats": bool(c.get("datetime_stats")),
            }
        )
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.caption(
        f"Profiled at {profile.get('profiled_at', 'unknown')}. "
        f"Source: {profile.get('source_path', 'unknown')}."
    )

# ---- PLAN ----
with tab_plan:
    if not plan:
        st.warning(
            "No cleaning plan loaded. Run `python scripts/propose_cleaning.py`."
        )
    else:
        st.subheader("LLM-proposed cleaning rules")
        rule_rows = []
        for r in plan["rules"]:
            rule_rows.append(
                {
                    "Column": r["column"],
                    "Operation": r["operation"],
                    "Confidence": r["confidence"],
                    "Parameters": json.dumps(r.get("parameters", {})),
                    "Reasoning": r["reasoning"],
                }
            )
        df = pd.DataFrame(rule_rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(
            "These are PROPOSED rules. The audit step (next tab) shows "
            "which ones the safety gates actually let through."
        )

# ---- AUDIT ----
with tab_audit:
    if not audit:
        st.warning(
            "No audit loaded. Run `python scripts/apply_cleaning.py`."
        )
    else:
        applied = [e for e in audit["entries"] if e["status"] == "applied"]
        skipped = [e for e in audit["entries"] if e["status"] == "skipped"]
        failed = [e for e in audit["entries"] if e["status"] == "failed"]

        c1, c2, c3 = st.columns(3)
        c1.metric("Applied", len(applied))
        c2.metric("Skipped", len(skipped))
        c3.metric("Failed", len(failed))

        st.subheader("Applied rules")
        if applied:
            df = pd.DataFrame(
                [
                    {
                        "Column": e["rule"]["column"],
                        "Operation": e["rule"]["operation"],
                        "Rows changed": e.get("rows_changed", 0),
                    }
                    for e in applied
                ]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.write("(none)")

        st.subheader("Skipped rules — why")
        if skipped:
            df = pd.DataFrame(
                [
                    {
                        "Column": e["rule"]["column"],
                        "Operation": e["rule"]["operation"],
                        "Confidence": e["rule"]["confidence"],
                        "Reason": e.get("reason", ""),
                    }
                    for e in skipped
                ]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.write("(none)")

        if failed:
            st.subheader("Failed rules")
            df = pd.DataFrame(
                [
                    {
                        "Column": e["rule"]["column"],
                        "Operation": e["rule"]["operation"],
                        "Reason": e.get("reason", ""),
                    }
                    for e in failed
                ]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)

# ---- BEFORE / AFTER ----
with tab_compare:
    raw_path = RAW / "nyc_311_sample.csv"
    cleaned_paths = sorted(CLEANED.glob("nyc_311_*cleaned.parquet"))

    if not raw_path.exists() or not cleaned_paths:
        st.warning(
            "Raw CSV or cleaned parquet missing. Run the full pipeline first "
            "(see README)."
        )
    else:
        raw = load_csv(str(raw_path))

        cleaned_choice = st.selectbox(
            "Cleaned dataset",
            cleaned_paths,
            index=len(cleaned_paths) - 1,
            format_func=lambda p: p.name,
        )
        cleaned = load_parquet(str(cleaned_choice))

        # Pick a column that actually changed.
        changed_columns: list[str] = []
        for col in raw.columns:
            if col in cleaned.columns:
                neq = (
                    (raw[col].astype(str) != cleaned[col].astype(str))
                    & ~(raw[col].isna() & cleaned[col].isna())
                )
                if neq.any():
                    changed_columns.append(col)

        if not changed_columns:
            st.info("No columns differ between raw and cleaned.")
        else:
            chosen = st.selectbox("Column to inspect", changed_columns)
            n_samples = st.slider("How many sample rows", 5, 50, 12)

            mask = (
                (raw[chosen].astype(str) != cleaned[chosen].astype(str))
                & ~(raw[chosen].isna() & cleaned[chosen].isna())
            )
            sample_idx = raw[mask].head(n_samples).index

            compare_df = pd.DataFrame(
                {
                    "Before": raw.loc[sample_idx, chosen].astype(str).values,
                    "After": cleaned.loc[sample_idx, chosen].astype(str).values,
                }
            )
            st.dataframe(
                compare_df, use_container_width=True, hide_index=True
            )

            total_changed = int(mask.sum())
            st.caption(
                f"Showing {len(sample_idx)} of {total_changed:,} rows where "
                f"`{chosen}` was modified by the pipeline."
            )
