"""Page 3 — Inspect a completed job."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from api_client import (  # noqa: E402
    before_after,
    get_audit,
    get_job,
    get_plan,
    get_profile,
    get_quality,
    get_suggestions,
)
from auth_helpers import require_auth  # noqa: E402

st.set_page_config(page_title="Results — Data Polish", layout="wide")

require_auth()

st.title("Results")

if "job_id" not in st.session_state:
    st.warning("No job in this session. Go to **Upload** first.")
    st.stop()

job_id = st.session_state["job_id"]

try:
    job = get_job(job_id)
except Exception as exc:  # noqa: BLE001
    st.error(f"Couldn't fetch job: {exc}")
    st.stop()

if job["status"] != "done":
    st.warning(
        f"Job status is `{job['status']}`. Run the pipeline on the **Run** "
        "page first."
    )
    st.stop()

# --------------------------------------------------------------------------- #
# Header metrics from the job summary.
# --------------------------------------------------------------------------- #

s = job.get("summary", {})
c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{s.get('rows_in', 0):,}")
c2.metric("Columns", s.get("columns", 0))
c3.metric("Rules applied", s.get("rules_applied", 0))
c4.metric("Rules skipped", s.get("rules_skipped", 0))

# --------------------------------------------------------------------------- #
# Quality score — before / after / delta. The delta is the visual hero of
# this page: did the cleaning actually move the needle?
# --------------------------------------------------------------------------- #

try:
    quality = get_quality(job_id)
except Exception:  # noqa: BLE001
    quality = None

if quality:
    qc1, qc2, qc3, qc4 = st.columns(4)
    before = quality.get("before", {}).get("score", 0)
    after = quality.get("after", {}).get("score", 0)
    delta = quality.get("delta", after - before)
    issues_before = quality.get("before", {}).get("issue_count", 0)
    issues_after = quality.get("after", {}).get("issue_count", 0)

    qc1.metric("Quality score (raw)", f"{before}/100")
    qc2.metric(
        "Quality score (cleaned)",
        f"{after}/100",
        delta=f"{delta:+d}",
    )
    qc3.metric("Issues found (raw)", issues_before)
    qc4.metric(
        "Issues remaining",
        issues_after,
        delta=f"{issues_after - issues_before:+d}",
        delta_color="inverse",  # fewer remaining issues is good
    )

    st.caption(
        "Score covers format-level checks. 100 doesn't mean semantically "
        "perfect — business rules and referential integrity aren't checked. "
        "See Suggestions below for follow-ups."
    )

# --------------------------------------------------------------------------- #
# Suggestions box — "what more can be done" from a second LLM call.
# --------------------------------------------------------------------------- #

try:
    suggestions = get_suggestions(job_id).get("suggestions", [])
except Exception:  # noqa: BLE001
    suggestions = []

if suggestions:
    with st.expander(
        f"Suggestions: {len(suggestions)} next steps",
        expanded=True,
    ):
        for sug in suggestions:
            st.markdown(f"- {sug}")

# --------------------------------------------------------------------------- #
# Tabs.
# --------------------------------------------------------------------------- #

tab_audit, tab_compare, tab_plan, tab_profile, tab_quality = st.tabs(
    [
        "Audit (code)",
        "Before / After",
        "Cleaning plan (LLM)",
        "Profile",
        "Quality issues",
    ]
)

# ---- AUDIT ----
with tab_audit:
    audit = get_audit(job_id)
    applied = [e for e in audit["entries"] if e["status"] == "applied"]
    skipped = [e for e in audit["entries"] if e["status"] == "skipped"]
    failed = [e for e in audit["entries"] if e["status"] == "failed"]

    st.markdown(
        f"**{len(applied)} applied · {len(skipped)} skipped · "
        f"{len(failed)} failed**"
    )

    def _label(entry: dict) -> str:
        """Pull the short label, with deterministic fallback."""
        rule = entry["rule"]
        return rule.get("short_label") or f"{rule['operation']} · {rule['column']}"

    if applied:
        st.subheader("Applied")
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "What": _label(e),
                        "Column": e["rule"]["column"],
                        "Rows changed": e.get("rows_changed", 0),
                    }
                    for e in applied
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
        with st.expander("Why each rule applied (full reasoning)"):
            for e in applied:
                st.markdown(
                    f"**{_label(e)}** &nbsp;·&nbsp; "
                    f"`{e['rule']['operation']}` on `{e['rule']['column']}`"
                )
                st.caption(e["rule"]["reasoning"])

    if skipped:
        st.subheader("Skipped — why")
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "What": _label(e),
                        "Column": e["rule"]["column"],
                        "Confidence": e["rule"]["confidence"],
                        "Reason": e.get("reason", ""),
                    }
                    for e in skipped
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    if failed:
        st.subheader("Failed")
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "What": _label(e),
                        "Column": e["rule"]["column"],
                        "Reason": e.get("reason", ""),
                    }
                    for e in failed
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

# ---- BEFORE / AFTER ----
with tab_compare:
    audit = get_audit(job_id)
    changed_cols = sorted(
        {
            e["rule"]["column"]
            for e in audit["entries"]
            if e["status"] == "applied"
        }
    )

    if not changed_cols:
        st.info("No columns were modified by the pipeline.")
    else:
        col = st.selectbox("Column", changed_cols)
        n_samples = st.slider("How many sample rows", 5, 50, 12)

        try:
            result = before_after(job_id, col, n_samples)
        except Exception as exc:  # noqa: BLE001
            st.error(f"{type(exc).__name__}: {exc}")
        else:
            st.markdown(
                f"**{result['total_changed']:,}** rows were modified in "
                f"`{col}`. Showing the first {len(result['samples'])}."
            )
            st.dataframe(
                pd.DataFrame(result["samples"]),
                use_container_width=True,
                hide_index=True,
            )

# ---- CLEANING PLAN ----
with tab_plan:
    plan = get_plan(job_id)
    st.info(plan.get("summary", "(no LLM summary)"))
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "What": r.get("short_label")
                    or f"{r['operation']} · {r['column']}",
                    "Column": r["column"],
                    "Operation": r["operation"],
                    "Confidence": r["confidence"],
                    "Parameters": json.dumps(r.get("parameters", {})),
                }
                for r in plan["rules"]
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )
    with st.expander("Full reasoning per rule"):
        for r in plan["rules"]:
            label = r.get("short_label") or f"{r['operation']} · {r['column']}"
            st.markdown(f"**{label}** &nbsp;·&nbsp; `{r['confidence']}`")
            st.caption(r["reasoning"])

# ---- PROFILE ----
with tab_profile:
    profile = get_profile(job_id)
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Column": c["name"],
                    "Dtype": c["dtype"],
                    "Null %": c["null_pct"],
                    "Unique": c["unique_count"],
                }
                for c in profile["columns"]
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )

# ---- QUALITY ISSUES ----
with tab_quality:
    if not quality:
        st.info("No quality report for this run.")
    else:
        before_data = quality.get("before", {})
        after_data = quality.get("after", {})

        sub_before, sub_after = st.tabs(
            [
                f"Raw — {before_data.get('issue_count', 0)} issues",
                f"Cleaned — {after_data.get('issue_count', 0)} issues",
            ]
        )

        def _issues_df(issues_list: list[dict]) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "Column": i["column"],
                        "Issue": i["type"],
                        "Detail": i.get("detail", ""),
                    }
                    for i in issues_list
                ]
            )

        with sub_before:
            issues = before_data.get("issues", [])
            if not issues:
                st.success("No quality issues detected in raw data.")
            else:
                st.dataframe(
                    _issues_df(issues),
                    use_container_width=True,
                    hide_index=True,
                )

        with sub_after:
            issues = after_data.get("issues", [])
            if not issues:
                st.success(
                    "No quality issues detected after cleaning. Done."
                )
            else:
                st.caption(
                    "Issues that remain after the pipeline ran. "
                    "Most are things the safety gates skipped or that "
                    "fall outside the current rule set."
                )
                st.dataframe(
                    _issues_df(issues),
                    use_container_width=True,
                    hide_index=True,
                )
