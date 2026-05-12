"""Thin httpx wrapper around the Data Polish FastAPI backend.

Only the UI imports this. Keeping a single client module means changing
the API base URL or adding auth headers (v2.4) is a one-file edit.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import streamlit as st

API_BASE = os.environ.get("DATAPOLISH_API_BASE", "http://localhost:8000")


class APIError(Exception):
    """Friendly error carrying the API's `detail` message instead of the
    raw httpx status-line, so the UI can show users something useful."""

    def __init__(self, detail: str, status_code: int | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


def _raise_for_status(r: httpx.Response) -> None:
    """Convert HTTP errors into APIError with the server's detail message."""
    if r.is_success:
        return
    try:
        detail = r.json().get("detail") or r.text
    except Exception:  # noqa: BLE001
        detail = r.text or f"HTTP {r.status_code}"
    raise APIError(detail, status_code=r.status_code)


def _auth_headers() -> dict[str, str]:
    """Send the signed JWT as Authorization: Bearer ...

    The API decodes it, verifies the signature + expiry, and looks up
    the user. The old X-User-ID fallback was removed in v3.7 — if there's
    no access_token in session state, the request goes unauthenticated
    and the API will 401.
    """
    token = st.session_state.get("access_token")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


def register(username: str, email: str, name: str, password: str) -> dict:
    return _post(
        "/auth/register",
        json={
            "username": username,
            "email": email,
            "name": name,
            "password": password,
        },
    )


def login(username_or_email: str, password: str) -> dict:
    return _post(
        "/auth/login",
        json={
            "username_or_email": username_or_email,
            "password": password,
        },
    )


def me() -> dict:
    return _get("/auth/me")


def magic_request(email: str) -> dict:
    return _post("/auth/magic/request", json={"email": email})


def magic_verify(email: str, code: str) -> dict:
    return _post("/auth/magic/verify", json={"email": email, "code": code})


def get_my_activity() -> list[dict]:
    """Return the current user's recent audit events."""
    # _get expects a dict but the endpoint returns a list. Use httpx
    # directly to handle the list response.
    headers = _auth_headers()
    r = httpx.get(
        f"{API_BASE}/users/me/activity", headers=headers, timeout=30
    )
    _raise_for_status(r)
    return r.json()


def _get(path: str, **kwargs: Any) -> dict:
    headers = {**_auth_headers(), **kwargs.pop("headers", {})}
    r = httpx.get(
        f"{API_BASE}{path}", timeout=30, headers=headers, **kwargs
    )
    _raise_for_status(r)
    return r.json()


def _post(path: str, **kwargs: Any) -> dict:
    timeout = kwargs.pop("timeout", 60)
    headers = {**_auth_headers(), **kwargs.pop("headers", {})}
    r = httpx.post(
        f"{API_BASE}{path}", timeout=timeout, headers=headers, **kwargs
    )
    _raise_for_status(r)
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


def upload_from_gcs(
    job_id: str,
    bucket: str,
    blob_name: str,
    *,
    service_account_json: str | None = None,
) -> dict:
    payload = {
        "bucket": bucket,
        "blob_name": blob_name,
        "service_account_json": service_account_json or None,
    }
    return _post(f"/jobs/{job_id}/upload-gcs", json=payload)


def upload_from_azure(
    job_id: str,
    account_name: str,
    container: str,
    blob_name: str,
    *,
    connection_string: str | None = None,
    account_key: str | None = None,
    sas_token: str | None = None,
) -> dict:
    payload = {
        "account_name": account_name,
        "container": container,
        "blob_name": blob_name,
        "connection_string": connection_string or None,
        "account_key": account_key or None,
        "sas_token": sas_token or None,
    }
    return _post(f"/jobs/{job_id}/upload-azure", json=payload)


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
