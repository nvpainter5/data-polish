"""Page 2 — Run the pipeline on the current job."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st  # noqa: E402

from api_client import get_job, run_job  # noqa: E402
from auth_helpers import require_auth  # noqa: E402

st.set_page_config(page_title="Run — Data Polish", layout="wide")

require_auth()

st.title("Run")

# --------------------------------------------------------------------------- #
# Need a job in session to run anything.
# --------------------------------------------------------------------------- #

if "job_id" not in st.session_state:
    st.warning("No job in this session. Go to **Upload** first.")
    st.stop()

job_id = st.session_state["job_id"]

try:
    job = get_job(job_id)
except Exception as exc:  # noqa: BLE001
    st.error(
        f"Couldn't fetch job from API: {type(exc).__name__}: {exc}. "
        "Try uploading again."
    )
    st.stop()

st.markdown(
    f"**Job:** `{job_id}`  ·  **Status:** `{job['status']}`  ·  "
    f"**File:** `{job.get('input_filename') or '(none)'}`"
)

# --------------------------------------------------------------------------- #
# Branch on status — we only let the user run when status == 'uploaded'.
# --------------------------------------------------------------------------- #

if job["status"] == "created":
    st.warning("Upload a file first — Run is gated until a CSV is uploaded.")
    st.stop()

if job["status"] == "running":
    st.info(
        "Pipeline is running. (v2.0 runs synchronously — refresh once it "
        "completes.)"
    )
    st.stop()

if job["status"] == "done":
    st.success("Already complete. Go to **Results** to see what happened.")
    st.json(job["summary"])
    st.stop()

if job["status"] == "failed":
    st.error(f"Job failed: {job.get('error_message') or 'unknown error'}")
    st.caption("Re-upload the file and try again.")
    st.stop()

# --------------------------------------------------------------------------- #
# status == 'uploaded' — show the run form.
# --------------------------------------------------------------------------- #

custom = st.text_area(
    "Custom instructions (optional)",
    height=120,
    max_chars=500,
    placeholder="e.g. be conservative on date columns, focus on case standardization",
    help="Free-text steering added to the LLM system prompt.",
)

st.markdown("---")

if st.button("Run pipeline", type="primary"):
    with st.spinner(
        "Running pipeline... (~30 seconds — profile, propose, apply, validate)"
    ):
        try:
            result = run_job(job_id, custom or None)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Run failed: {type(exc).__name__}: {exc}")
            st.stop()

    if result["status"] == "done":
        # Persist to session_state and rerun so the View Results button
        # lives at the top level and its click actually navigates.
        st.session_state["run_complete"] = True
        st.session_state["run_result"] = result
        st.rerun()
    else:
        st.error(f"Job ended in `{result['status']}`.")
        if result.get("error_message"):
            st.code(result["error_message"])

# --------------------------------------------------------------------------- #
# Post-run state — renders the "View results" CTA + summary.
# Lives at top level so the button click works.
# --------------------------------------------------------------------------- #

if st.session_state.get("run_complete"):
    result = st.session_state.get("run_result", {})
    s = result.get("summary", {})

    st.success(
        f"Done. {s.get('rules_applied', 0)} applied / "
        f"{s.get('rules_skipped', 0)} skipped / "
        f"{s.get('rules_failed', 0)} failed across "
        f"{s.get('columns', 0)} columns."
    )

    if st.button("View results →", type="primary"):
        st.session_state["run_complete"] = False
        st.switch_page("pages/3_Results.py")

    with st.expander("Run summary"):
        st.json(s)
