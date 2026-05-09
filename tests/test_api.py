"""Smoke tests for the FastAPI backend.

We use FastAPI's TestClient + a temporary storage root so tests don't
collide with the real data/jobs/ directory. The pipeline endpoint is
covered indirectly via the job state machine; the actual cleaning
behavior is already tested in test_apply.py and friends.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# api/ isn't a package on PYTHONPATH by default; make it importable.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def client(tmp_path, monkeypatch):
    # Point storage at a temp directory before importing api.main so the
    # module-level singletons get a clean root.
    from api import main as main_module
    from api.jobs import JobStore
    from api.storage import LocalStorage

    fresh_storage = LocalStorage(tmp_path / "jobs")
    monkeypatch.setattr(main_module, "storage", fresh_storage)
    monkeypatch.setattr(main_module, "job_store", JobStore(fresh_storage))

    test_client = TestClient(main_module.app)
    # Default auth header for all test requests. Auth-specific tests
    # override this per-call.
    test_client.headers.update({"X-User-ID": "testuser"})
    return test_client


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["service"] == "datapolish-api"


def test_create_and_get_job(client):
    r = client.post("/jobs")
    assert r.status_code == 200
    job = r.json()
    assert job["status"] == "created"

    r2 = client.get(f"/jobs/{job['job_id']}")
    assert r2.status_code == 200
    assert r2.json()["job_id"] == job["job_id"]


def test_upload_then_run_calls_pipeline(client):
    # Mock the pipeline so we don't need a real LLM call in unit tests.
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
    """Tabular extensions are accepted; binaries / images / PDFs are not."""
    r = client.post("/jobs")
    job_id = r.json()["job_id"]

    r = client.post(
        f"/jobs/{job_id}/upload",
        files={"file": ("nope.pdf", io.BytesIO(b"hello"), "application/pdf")},
    )
    assert r.status_code == 400


def test_upload_accepts_pipe_delimited_txt(client):
    """A .txt file with pipe-delimited content should be accepted and
    auto-detect '|' as the delimiter."""
    r = client.post("/jobs")
    job_id = r.json()["job_id"]

    csv_bytes = b"a|b|c\n1|2|3\n4|5|6\n"
    r = client.post(
        f"/jobs/{job_id}/upload",
        files={
            "file": ("data.txt", io.BytesIO(csv_bytes), "text/plain")
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "uploaded"
    assert body["delimiter"] == "|"


def test_missing_user_header_returns_401(client):
    """Without X-User-ID the API refuses every job route."""
    r = client.post("/jobs", headers={"X-User-ID": ""})
    assert r.status_code == 401


def test_cross_user_access_returns_403(client):
    """User A creates a job; user B asks for it; expects 403."""
    r = client.post("/jobs", headers={"X-User-ID": "alice"})
    job_id = r.json()["job_id"]

    r2 = client.get(f"/jobs/{job_id}", headers={"X-User-ID": "bob"})
    assert r2.status_code == 403


def test_list_my_jobs(client):
    """The /users/me/jobs endpoint returns only the caller's jobs."""
    client.post("/jobs", headers={"X-User-ID": "alice"})
    client.post("/jobs", headers={"X-User-ID": "alice"})
    client.post("/jobs", headers={"X-User-ID": "bob"})

    r = client.get("/users/me/jobs", headers={"X-User-ID": "alice"})
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 2
    assert all(j["status"] == "created" for j in body)


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
