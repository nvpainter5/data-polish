"""Smoke tests for the FastAPI backend.

Rewritten for v3.7. The tests now:

  - Use a per-test temporary SQLite database (via DATABASE_URL env var
    set before api.main is imported) so they're hermetic.
  - Insert real test users via api.user_store and mint real JWTs via
    api.auth — the X-User-ID legacy header was removed in v3.7.
  - Mock the LLM-driven `run_pipeline` call; we're testing the HTTP
    contract, not the cleaning engine (that's covered in test_apply.py).
"""

from __future__ import annotations

import io
import os
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# api/ isn't a package on PYTHONPATH by default; make it importable.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture
def fresh_api(tmp_path, monkeypatch):
    """Boot the API against a temp SQLite DB and temp storage root.

    Setting DATABASE_URL + JWT_SECRET BEFORE importing api.main is the
    important bit — api/db.py reads these at import time.
    """
    monkeypatch.setenv(
        "DATABASE_URL", f"sqlite:///{tmp_path / 'test.db'}"
    )
    monkeypatch.setenv("JWT_SECRET", "test-secret-do-not-use-in-prod")

    # Drop any cached api.* modules so the fresh env vars take effect.
    for mod in list(sys.modules):
        if mod == "api" or mod.startswith("api."):
            sys.modules.pop(mod)

    from api import main as main_module  # noqa: E402
    from api.storage import LocalStorage  # noqa: E402

    fresh_storage = LocalStorage(tmp_path / "jobs")
    monkeypatch.setattr(main_module, "storage", fresh_storage)
    return main_module


def _register_and_login(main_module, username: str = "alice") -> tuple[TestClient, dict]:
    """Spin up a TestClient and create a fresh test user, returning the
    client + bearer-token-bearing headers."""
    client = TestClient(main_module.app)
    email = f"{username}@example.com"
    r = client.post(
        "/auth/register",
        json={
            "username": username,
            "email": email,
            "name": username.title(),
            "password": "correct-horse-battery-staple",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    token = body["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    return client, headers


@pytest.fixture
def client(fresh_api):
    """Convenience: API client pre-authed as `testuser`."""
    test_client, headers = _register_and_login(fresh_api, "testuser")
    test_client.headers.update(headers)
    return test_client


# --------------------------------------------------------------------------- #
# Health
# --------------------------------------------------------------------------- #


def test_healthz_returns_200_with_db_ok(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["service"] == "datapolish-api"
    assert body["checks"]["database"] == "ok"


# --------------------------------------------------------------------------- #
# Jobs — happy paths
# --------------------------------------------------------------------------- #


def test_create_and_get_job(client):
    r = client.post("/jobs")
    assert r.status_code == 200
    job = r.json()
    assert job["status"] == "created"

    r2 = client.get(f"/jobs/{job['job_id']}")
    assert r2.status_code == 200
    assert r2.json()["job_id"] == job["job_id"]


def test_upload_then_run_calls_pipeline(client):
    summary = {
        "rules_proposed": 3,
        "rules_applied": 2,
        "rules_skipped": 1,
        "rules_failed": 0,
        "rows_in": 5,
        "rows_out": 5,
        "columns": 2,
    }

    df = pd.DataFrame(
        {"complaint_type": ["Noise", "HEAT", "Other"], "id": [1, 2, 3]}
    )
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    r = client.post("/jobs")
    job_id = r.json()["job_id"]

    r = client.post(
        f"/jobs/{job_id}/upload",
        files={"file": ("incoming.csv", io.BytesIO(csv_bytes), "text/csv")},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "uploaded"
    assert r.json()["input_filename"] == "incoming.csv"

    with patch("api.main.run_pipeline", return_value=summary) as mock_run:
        r = client.post(
            f"/jobs/{job_id}/run",
            data={"custom_instructions": "be conservative on addresses"},
        )

    assert r.status_code == 200
    assert r.json()["status"] == "done"
    assert r.json()["summary"] == summary
    mock_run.assert_called_once()


def test_run_before_upload_409(client):
    r = client.post("/jobs")
    job_id = r.json()["job_id"]

    r = client.post(f"/jobs/{job_id}/run")
    assert r.status_code == 409


def test_upload_rejects_unsupported_extension(client):
    r = client.post("/jobs")
    job_id = r.json()["job_id"]

    r = client.post(
        f"/jobs/{job_id}/upload",
        files={"file": ("nope.pdf", io.BytesIO(b"hello"), "application/pdf")},
    )
    assert r.status_code == 400


def test_upload_accepts_pipe_delimited_txt(client):
    r = client.post("/jobs")
    job_id = r.json()["job_id"]

    csv_bytes = b"a|b|c\n1|2|3\n4|5|6\n"
    r = client.post(
        f"/jobs/{job_id}/upload",
        files={"file": ("data.txt", io.BytesIO(csv_bytes), "text/plain")},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "uploaded"
    assert body["delimiter"] == "|"


# --------------------------------------------------------------------------- #
# Auth
# --------------------------------------------------------------------------- #


def test_missing_bearer_token_returns_401(fresh_api):
    """Without an Authorization header the API refuses every job route."""
    bare = TestClient(fresh_api.app)
    r = bare.post("/jobs")
    assert r.status_code == 401


def test_invalid_bearer_token_returns_401(fresh_api):
    bare = TestClient(fresh_api.app)
    r = bare.post(
        "/jobs", headers={"Authorization": "Bearer not-a-real-token"}
    )
    assert r.status_code == 401


def test_legacy_x_user_id_header_no_longer_works(fresh_api):
    """v3.7 removed the X-User-ID fallback. Sending one alone == 401."""
    bare = TestClient(fresh_api.app)
    r = bare.post("/jobs", headers={"X-User-ID": "anyone"})
    assert r.status_code == 401


def test_cross_user_access_returns_403(fresh_api):
    """User A creates a job; user B asks for it; expects 403."""
    alice_client, alice_headers = _register_and_login(fresh_api, "alice")
    bob_client, bob_headers = _register_and_login(fresh_api, "bob")

    r = alice_client.post("/jobs", headers=alice_headers)
    job_id = r.json()["job_id"]

    r2 = bob_client.get(f"/jobs/{job_id}", headers=bob_headers)
    assert r2.status_code == 403


def test_list_my_jobs_only_returns_caller_jobs(fresh_api):
    alice_client, alice_headers = _register_and_login(fresh_api, "alice")
    bob_client, bob_headers = _register_and_login(fresh_api, "bob")

    alice_client.post("/jobs", headers=alice_headers)
    alice_client.post("/jobs", headers=alice_headers)
    bob_client.post("/jobs", headers=bob_headers)

    r = alice_client.get("/users/me/jobs", headers=alice_headers)
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 2
    assert all(j["status"] == "created" for j in body)


# --------------------------------------------------------------------------- #
# Cloud storage source (S3 path)
# --------------------------------------------------------------------------- #


def test_upload_from_s3_endpoint(client, monkeypatch):
    """User-supplied S3 source becomes the raw.csv for the job."""
    from api import main as main_module

    csv_bytes = b"complaint_type\nNoise\nHEAT/HOT WATER\n"
    monkeypatch.setattr(
        main_module.cloud_storage,
        "download_csv_from_s3",
        lambda *args, **kwargs: csv_bytes,
    )

    r = client.post("/jobs")
    job_id = r.json()["job_id"]

    r = client.post(
        f"/jobs/{job_id}/upload-s3",
        json={
            "bucket": "test-bucket",
            "key": "data/incoming.csv",
            "access_key_id": "AKIAFAKE",
            "secret_access_key": "FAKE",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "uploaded"
    assert body["input_filename"] == "s3://test-bucket/data/incoming.csv"


def test_upload_from_s3_propagates_failure(client, monkeypatch):
    """Bad bucket / missing key surfaces as 400 with a useful message."""
    from api import main as main_module

    def _fail(*args, **kwargs):
        raise RuntimeError("Couldn't read s3://nope/missing.csv: NoSuchKey")

    monkeypatch.setattr(
        main_module.cloud_storage, "download_csv_from_s3", _fail
    )

    r = client.post("/jobs")
    job_id = r.json()["job_id"]

    r = client.post(
        f"/jobs/{job_id}/upload-s3",
        json={"bucket": "nope", "key": "missing.csv"},
    )
    assert r.status_code == 400
    assert "NoSuchKey" in r.json()["detail"]
