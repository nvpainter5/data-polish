"""Thin httpx wrapper around the DataPolish FastAPI backend.

Only the UI imports this. Keeping a single client module means changing
the API base URL or adding auth headers (v2.4) is a one-file edit.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import streamlit as st

API_BASE = os.environ.get("DATAPOLISH_API_BASE", "http://localhost:8000")


def _auth_headers() -> dict[str, str]:
    """Auth header sent on every API call.

    For v2.4 MVP we use a simple X-User-ID header populated from the
    Streamlit session. v2.6 will replace this with a JWT signed by a
    shared secret once we deploy off-localhost.
    """
    username = st.session_state.get("username")
    return {"X-User-ID": username} if username else {}


def _get(path: str, **kwargs: Any) -> dict:
    headers = {**_auth_headers(), **kwargs.pop("headers", {})}
    r = httpx.get(
        f"{API_BASE}{path}", timeout=30, headers=headers, **kwargs
    )
    r.raise_for_status()
    return r.json()


def _post(path: str, **kwargs: Any) -> dict:
    timeout = kwargs.pop("timeout", 60)
    headers = {**_auth_headers(), **kwargs.pop("headers", {})}
    r = httpx.post(
        f"{API_BASE}{path}", timeout=timeout, headers=headers, **kwargs
    )
    r.raise_for_status()
    return r.json()


# --------------------------------------------------------------------------- #
# Endpoints — one wrapper per FastAPI route.
# --------------------------------------------------------------------------- #


def healthz() -> dict:
    return _get("/healthz")


def create_job() -> dict:
    return _post("/jobs")


def get_job(job_id: str) -> dict:
    return _get(f"/jobs/{job_id}")


def upload_csv(job_id: str, file_bytes: bytes, filename: str) -> dict:
    return _post(
        f"/jobs/{job_id}/upload",
        files={"file": (filename, file_bytes, "text/csv")},
    )


def upload_from_s3(
    job_id: str,
    bucket: str,
    key: str,
    *,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    region: str | None = None,
) -> dict:
    payload = {
        "bucket": bucket,
        "key": key,
        "access_key_id": access_key_id or None,
        "secret_access_key": secret_access_key or None,
        "region": region or None,
    }
    return _post(f"/jobs/{job_id}/upload-s3", json=payload)


def run_job(
    job_id: str, custom_instructions: str | None = None
) -> dict:
    data = {}
    if custom_instructions:
        data["custom_instructions"] = custom_instructions
    # Pipeline can take a while — Lambda was 20s, local Groq similar.
    return _post(f"/jobs/{job_id}/run", data=data, timeout=600)


def get_profile(job_id: str) -> dict:
    return _get(f"/jobs/{job_id}/profile")


def get_plan(job_id: str) -> dict:
    return _get(f"/jobs/{job_id}/plan")


def get_audit(job_id: str) -> dict:
    return _get(f"/jobs/{job_id}/audit")


def get_preview(job_id: str, delimiter: str | None = None) -> dict:
    """Fetch a small preview of the uploaded source. Optionally override
    the delimiter — the API persists the override on the job."""
    params = {"delimiter": delimiter} if delimiter else {}
    return _get(f"/jobs/{job_id}/preview", params=params)


def get_quality(job_id: str) -> dict:
    return _get(f"/jobs/{job_id}/quality")


def get_suggestions(job_id: str) -> dict:
    return _get(f"/jobs/{job_id}/suggestions")


def before_after(job_id: str, column: str, n_samples: int = 12) -> dict:
    return _get(
        f"/jobs/{job_id}/before-after",
        params={"column": column, "n_samples": n_samples},
    )
