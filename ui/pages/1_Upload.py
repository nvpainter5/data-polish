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
    upload_from_azure,
    upload_from_gcs,
    upload_from_s3,
)
from auth_helpers import require_auth  # noqa: E402

st.set_page_config(page_title="Upload — Data Polish", layout="wide")

require_auth()  # gate the page

# Single source of truth for the upload size cap — surfaced wherever the
# user might bump into it.
MAX_UPLOAD_MB = 250
MAX_S3_OBJECT_MB = 500

st.title("Upload")
st.caption(
    f"Local upload up to **{MAX_UPLOAD_MB} MB**, or read from S3 up to "
    f"**{MAX_S3_OBJECT_MB} MB**. Larger datasets (GB-scale streaming) "
    "are on the v4 roadmap."
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

tab_local, tab_s3, tab_gcs, tab_azure = st.tabs(
    ["Local file", "AWS S3", "Google Cloud", "Azure Blob"]
)

# --------------------------------------------------------------------------- #
# Tab 1 — local file picker (existing flow).
# --------------------------------------------------------------------------- #

with tab_local:
    uploaded = st.file_uploader(
        f"Choose a file (max {MAX_UPLOAD_MB} MB)",
        type=["csv", "tsv", "txt", "json", "parquet"],
        accept_multiple_files=False,
        help="Supported: CSV, TSV, pipe-delimited, JSON, parquet. Delimiter auto-detected.",
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
        f"Read a file from S3. Max object size: **{MAX_S3_OBJECT_MB} MB**. "
        "Credentials are sent once and not stored."
    )

    s3_bucket = st.text_input("Bucket name", placeholder="my-data-bucket")
    s3_key = st.text_input(
        "Object key",
        placeholder="data/incoming.csv",
    )

    with st.expander("AWS credentials", expanded=True):
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
            "Region", placeholder="us-east-1", key="s3_region"
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
# Tab 3 — Google Cloud Storage.
# --------------------------------------------------------------------------- #

with tab_gcs:
    st.caption(
        f"Read from a GCS bucket. Max object size: **{MAX_S3_OBJECT_MB} MB**. "
        "Credentials are not stored."
    )

    g_bucket = st.text_input("Bucket name", key="g_bucket")
    g_blob = st.text_input(
        "Blob name (path)", key="g_blob", placeholder="data/incoming.csv"
    )

    with st.expander("Service account JSON", expanded=True):
        st.caption(
            "Paste the full service-account JSON keyfile content. "
            "Leave blank only if the API server has its own GCP credentials."
        )
        g_sa = st.text_area(
            "Service account JSON",
            key="g_sa",
            height=160,
            placeholder='{"type": "service_account", ...}',
            label_visibility="collapsed",
        )

    if st.button(
        "Create job + import from GCS",
        type="primary",
        key="g_submit",
        disabled=not (g_bucket and g_blob),
    ):
        progress = st.progress(0, text="Creating job...")
        try:
            job = create_job()
            st.session_state["job_id"] = job["job_id"]
            progress.progress(
                40, text=f"Pulling gs://{g_bucket}/{g_blob} ..."
            )
            updated = upload_from_gcs(
                job["job_id"],
                g_bucket,
                g_blob,
                service_account_json=g_sa or None,
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
# Tab 4 — Azure Blob Storage.
# --------------------------------------------------------------------------- #

with tab_azure:
    st.caption(
        f"Read from an Azure Blob container. Max object size: "
        f"**{MAX_S3_OBJECT_MB} MB**. Credentials are not stored."
    )

    az_account = st.text_input(
        "Storage account name", key="az_account", placeholder="mystorage"
    )
    az_container = st.text_input("Container", key="az_container")
    az_blob = st.text_input("Blob name", key="az_blob")

    with st.expander("Auth (pick one)", expanded=True):
        az_auth_method = st.radio(
            "Auth method",
            ["Connection string", "Account key", "SAS token"],
            key="az_auth_method",
            horizontal=True,
        )
        az_conn = az_key = az_sas = None
        if az_auth_method == "Connection string":
            az_conn = st.text_input(
                "Connection string", type="password", key="az_conn"
            )
        elif az_auth_method == "Account key":
            az_key = st.text_input(
                "Account key", type="password", key="az_key"
            )
        else:
            az_sas = st.text_input(
                "SAS token", type="password", key="az_sas"
            )

    az_ready = bool(
        az_account and az_container and az_blob
        and (az_conn or az_key or az_sas)
    )
    if st.button(
        "Create job + import from Azure",
        type="primary",
        key="az_submit",
        disabled=not az_ready,
    ):
        progress = st.progress(0, text="Creating job...")
        try:
            job = create_job()
            st.session_state["job_id"] = job["job_id"]
            progress.progress(
                40,
                text=f"Pulling azure://{az_account}/{az_container}/{az_blob} ...",
            )
            updated = upload_from_azure(
                job["job_id"],
                az_account,
                az_container,
                az_blob,
                connection_string=az_conn or None,
                account_key=az_key or None,
                sas_token=az_sas or None,
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
    st.caption("If columns look wrong, override the delimiter below.")

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
    if st.button("Looks right — Next: Run →", type="primary"):
        st.session_state["upload_complete"] = False
        st.switch_page("pages/2_Run.py")

    with st.expander("Raw job details"):
        st.json(last)
