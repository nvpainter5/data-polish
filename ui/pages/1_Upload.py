"""Page 1 — Upload a CSV and create a job."""

from __future__ import annotations

import sys
from pathlib import Path

# pages/X.py needs ui/ on sys.path to import api_client.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st  # noqa: E402

import pandas as pd  # noqa: E402

from api_client import (  # noqa: E402
    create_job,
    get_job,
    get_preview,
    upload_csv,
    upload_from_s3,
)
from auth_helpers import require_auth  # noqa: E402

st.set_page_config(page_title="Upload — DataPolish", layout="wide")

require_auth()  # gate the page

st.title("Upload")
st.caption(
    "Pick a CSV from your computer or read one directly from your S3 bucket."
)

# --------------------------------------------------------------------------- #
# Show current job if one is already in progress.
# --------------------------------------------------------------------------- #

if "job_id" in st.session_state:
    try:
        job = get_job(st.session_state["job_id"])
        st.info(
            f"You already have a job in this session: "
            f"`{job['job_id']}` (status: `{job['status']}`). "
            "Uploading a new file below will start a fresh job."
        )
    except Exception:  # noqa: BLE001
        # Stale session — backend doesn't know the job (probably restarted).
        st.session_state.pop("job_id", None)

# --------------------------------------------------------------------------- #
# File picker.
# --------------------------------------------------------------------------- #

tab_local, tab_s3 = st.tabs(["Local file", "From S3"])

# --------------------------------------------------------------------------- #
# Tab 1 — local file picker (existing flow).
# --------------------------------------------------------------------------- #

with tab_local:
    uploaded = st.file_uploader(
        "Choose a tabular data file",
        type=["csv", "tsv", "txt", "json", "parquet"],
        accept_multiple_files=False,
        help=(
            "CSV, TSV, pipe-delimited, JSON (records or lines), parquet — "
            "delimiter is auto-detected. Up to ~100 MB. Larger files: use "
            "the From S3 tab."
        ),
    )

    if uploaded is not None:
        size_mb = uploaded.size / (1024 * 1024)
        st.success(f"Selected: **{uploaded.name}** ({size_mb:.2f} MB)")

        if st.button("Create job + upload", type="primary", key="local_submit"):
            progress = st.progress(0, text="Creating job...")
            try:
                job = create_job()
                st.session_state["job_id"] = job["job_id"]
                progress.progress(40, text=f"Uploading {uploaded.name}...")

                updated = upload_csv(
                    job["job_id"], uploaded.getvalue(), uploaded.name
                )
                progress.progress(100, text="Upload complete.")
            except Exception as exc:  # noqa: BLE001
                progress.empty()
                st.error(f"{type(exc).__name__}: {exc}")
                st.stop()

            progress.empty()
            st.session_state["upload_complete"] = True
            st.session_state["last_upload"] = updated
            st.rerun()

# --------------------------------------------------------------------------- #
# Tab 2 — read from S3.
# --------------------------------------------------------------------------- #

with tab_s3:
    st.caption(
        "Read a CSV directly from your S3 bucket. Credentials are sent "
        "to the API for this single call only — they are NOT persisted."
    )

    s3_bucket = st.text_input("Bucket name", placeholder="my-data-bucket")
    s3_key = st.text_input(
        "Object key (path inside the bucket)",
        placeholder="data/incoming.csv",
        help="Any extension works (.csv, .txt, .tsv) as long as the content is CSV-formatted.",
    )

    with st.expander(
        "AWS credentials (required when running locally — see help)",
        expanded=True,
    ):
        st.caption(
            "When the API server runs on your laptop it has no AWS "
            "permissions of its own, so you must paste read-access "
            "credentials here. When the API server eventually runs on "
            "AWS infrastructure (EC2 / ECS / App Runner) with an IAM "
            "role attached, this section becomes optional — boto3 picks "
            "up credentials from the instance role automatically."
        )
        col1, col2 = st.columns(2)
        with col1:
            s3_access_key = st.text_input(
                "Access key ID", type="password", key="s3_ak"
            )
        with col2:
            s3_secret = st.text_input(
                "Secret access key", type="password", key="s3_sk"
            )
        s3_region = st.text_input(
            "Region (optional)", placeholder="us-east-1", key="s3_region"
        )

    can_submit = bool(s3_bucket and s3_key)
    if st.button(
        "Create job + import from S3",
        type="primary",
        key="s3_submit",
        disabled=not can_submit,
    ):
        progress = st.progress(0, text="Creating job...")
        try:
            job = create_job()
            st.session_state["job_id"] = job["job_id"]
            progress.progress(
                40, text=f"Pulling s3://{s3_bucket}/{s3_key} ..."
            )
            updated = upload_from_s3(
                job["job_id"],
                s3_bucket,
                s3_key,
                access_key_id=s3_access_key or None,
                secret_access_key=s3_secret or None,
                region=s3_region or None,
            )
            progress.progress(100, text="Import complete.")
        except Exception as exc:  # noqa: BLE001
            progress.empty()
            st.error(f"{type(exc).__name__}: {exc}")
            st.stop()

        progress.empty()
        st.session_state["upload_complete"] = True
        st.session_state["last_upload"] = updated
        st.rerun()

# --------------------------------------------------------------------------- #
# Post-upload state — renders the "Next" CTA + job details.
# Lives at top level so the button click works.
# --------------------------------------------------------------------------- #

if st.session_state.get("upload_complete"):
    last = st.session_state.get("last_upload", {})
    job_id_for_preview = last.get("job_id")
    st.success(f"Uploaded. Job ID: `{job_id_for_preview}`.")

    # ---- Preview + delimiter override ------------------------------------
    st.subheader("Verify how your file was parsed")
    st.caption(
        "Look at the columns and sample rows below. If columns are smushed "
        "together, the auto-detected delimiter is wrong — pick the right "
        "one from the dropdown."
    )

    DELIMITER_OPTIONS = {
        "Auto-detect (re-run)": None,
        "Comma  ,": ",",
        "Pipe  |": "|",
        "Tab  \\t": "\t",
        "Semicolon  ;": ";",
        "Colon  :": ":",
        "Custom...": "__custom__",
    }

    # Try a preview with the currently-stored delimiter (no override yet).
    try:
        preview = get_preview(job_id_for_preview)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Couldn't preview: {exc}")
        st.stop()

    current_delim = preview.get("delimiter", ",")
    st.markdown(
        f"**Detected delimiter:** `{current_delim!r}`  "
        f"·  **Columns:** `{len(preview['columns'])}`"
    )
    st.dataframe(
        pd.DataFrame(preview["sample_rows"]),
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Wrong delimiter? Override below"):
        choice_label = st.selectbox(
            "Delimiter",
            list(DELIMITER_OPTIONS.keys()),
            index=0,
        )
        choice_value = DELIMITER_OPTIONS[choice_label]

        if choice_value == "__custom__":
            choice_value = st.text_input(
                "Custom delimiter (single character)", max_chars=2
            )

        if st.button("Re-parse with this delimiter"):
            try:
                preview = get_preview(
                    job_id_for_preview, delimiter=choice_value
                )
                st.success(
                    f"Re-parsed with delimiter `{preview['delimiter']!r}`. "
                    "Reloading…"
                )
                st.rerun()
            except Exception as exc:  # noqa: BLE001
                st.error(f"Re-parse failed: {exc}")

    # ---- Proceed to Run --------------------------------------------------
    st.divider()
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Looks right — Next: Run →", type="primary"):
            st.session_state["upload_complete"] = False
            st.switch_page("pages/2_Run.py")
    with col2:
        st.caption(
            "Confirm the columns above look right, then proceed."
        )

    with st.expander("Raw job details"):
        st.json(last)
