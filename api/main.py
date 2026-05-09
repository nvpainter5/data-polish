"""FastAPI app for DataPolish v2.

Run locally:
    uvicorn api.main:app --reload --port 8000

OpenAPI docs at http://localhost:8000/docs once running.

Endpoints:
    POST   /jobs                       create a new job
    GET    /jobs                       list jobs (current user, eventually)
    GET    /jobs/{id}                  get job status + summary
    POST   /jobs/{id}/upload           multipart file upload (raw CSV)
    POST   /jobs/{id}/run              run the pipeline synchronously
    GET    /jobs/{id}/profile          profile JSON
    GET    /jobs/{id}/plan             cleaning plan JSON
    GET    /jobs/{id}/audit            audit JSON
    GET    /jobs/{id}/before-after     side-by-side rows for a column
    GET    /healthz                    liveness check
"""

from __future__ import annotations

import json
import os
import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from . import cloud_storage  # noqa: E402
from .jobs import JobStore  # noqa: E402
from .pipeline_runner import detect_delimiter, run_pipeline  # noqa: E402
from .storage import LocalStorage  # noqa: E402

# --------------------------------------------------------------------------- #
# App + dependencies (single-tenant in v2.0; per-user scoping arrives in 2.4).
# --------------------------------------------------------------------------- #

JOBS_ROOT = PROJECT_ROOT / "data" / "jobs"
storage = LocalStorage(JOBS_ROOT)
job_store = JobStore(storage)

app = FastAPI(
    title="DataPolish API",
    version="2.0.0-dev",
    description="Backend for the DataPolish multi-user web app.",
)

# CORS: dev server runs on :8501, deployed Streamlit Cloud lives elsewhere.
# Configure via ALLOWED_ORIGINS env var (comma-separated). Leave the local
# dev origins on by default so local development keeps working.
_default_origins = "http://localhost:8501,http://127.0.0.1:8501"
ALLOWED_ORIGINS = [
    o.strip()
    for o in os.environ.get("ALLOWED_ORIGINS", _default_origins).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------- #
# Auth — every job-related endpoint requires X-User-ID. Per-user authorization
# is enforced by `_authorize_job` (404/403 cleanly distinguished from 401).
# --------------------------------------------------------------------------- #


def require_user_id(x_user_id: str | None = Header(default=None)) -> str:
    if not x_user_id:
        raise HTTPException(401, "X-User-ID header required")
    return x_user_id


def _authorize_job(job_id: str, user_id: str):
    job = job_store.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job.user_id and job.user_id != user_id:
        raise HTTPException(403, "this job does not belong to you")
    return job


# --------------------------------------------------------------------------- #
# Schemas (only what the UI needs; the heavy types stay inside `datapolish`).
# --------------------------------------------------------------------------- #


class JobOut(BaseModel):
    job_id: str
    status: str
    created_at: str
    updated_at: str
    input_filename: str | None = None
    error_message: str | None = None
    summary: dict
    delimiter: str | None = None


def _job_out(job) -> JobOut:
    return JobOut(
        job_id=job.job_id,
        status=job.status,
        created_at=job.created_at,
        updated_at=job.updated_at,
        input_filename=job.input_filename,
        error_message=job.error_message,
        summary=job.summary,
        delimiter=job.delimiter,
    )


class PreviewResponse(BaseModel):
    delimiter: str
    columns: list[str]
    sample_rows: list[dict[str, str]]


def _build_preview(
    raw_bytes: bytes, delimiter: str, max_rows: int = 5
) -> PreviewResponse:
    """Parse the first few rows with the given delimiter, return a small JSON
    sample. All cell values are stringified so the response is always JSON-safe.
    """
    df = pd.read_csv(
        BytesIO(raw_bytes),
        sep=delimiter,
        nrows=max_rows,
        low_memory=False,
        dtype=str,  # everything as string for JSON safety
        keep_default_na=False,
    )
    return PreviewResponse(
        delimiter=delimiter,
        columns=[str(c) for c in df.columns],
        sample_rows=[
            {str(k): str(v) for k, v in row.items()}
            for row in df.head(max_rows).to_dict(orient="records")
        ],
    )


# --------------------------------------------------------------------------- #
# Routes.
# --------------------------------------------------------------------------- #


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "service": "datapolish-api", "version": app.version}


@app.post("/jobs", response_model=JobOut)
def create_job(user_id: str = Depends(require_user_id)) -> JobOut:
    job = job_store.create(user_id=user_id)
    return _job_out(job)


@app.get("/users/me/jobs", response_model=list[JobOut])
def list_my_jobs(user_id: str = Depends(require_user_id)) -> list[JobOut]:
    return [_job_out(j) for j in job_store.list_for_user(user_id)]


@app.get("/jobs/{job_id}", response_model=JobOut)
def get_job(
    job_id: str, user_id: str = Depends(require_user_id)
) -> JobOut:
    job = _authorize_job(job_id, user_id)
    return _job_out(job)


@app.post("/jobs/{job_id}/upload", response_model=JobOut)
def upload_csv(
    job_id: str,
    file: UploadFile = File(...),
    user_id: str = Depends(require_user_id),
) -> JobOut:
    job = _authorize_job(job_id, user_id)
    if job.status not in ("created", "uploaded"):
        raise HTTPException(
            409, f"cannot upload while job is {job.status!r}"
        )
    if not file.filename:
        raise HTTPException(400, "missing filename")
    # We accept any tabular-data extension. The actual parse happens at
    # run time via pipeline_runner._smart_read_dataframe, which
    # auto-detects format. Files that aren't parseable will fail loudly
    # at the Run step with a clear pandas error.
    allowed = (".csv", ".tsv", ".txt", ".json", ".parquet")
    if not file.filename.lower().endswith(allowed):
        raise HTTPException(
            400,
            f"unsupported file extension. Allowed: {', '.join(allowed)}",
        )

    storage.write_stream(job_id, "raw.csv", file.file)
    # Auto-detect delimiter immediately so the UI can show a preview.
    raw = storage.read_bytes(job_id, "raw.csv")
    delim = detect_delimiter(raw)
    job = job_store.update(
        job_id,
        status="uploaded",
        input_filename=file.filename,
        delimiter=delim,
    )
    return _job_out(job)


# --------------------------------------------------------------------------- #
# Cloud-storage source (v2.5). User points at a CSV in their S3 bucket and
# we pull it down into the job's storage. Credentials are not persisted —
# they live only in the request.
# --------------------------------------------------------------------------- #


class S3SourceRequest(BaseModel):
    bucket: str
    key: str
    access_key_id: str | None = None
    secret_access_key: str | None = None
    region: str | None = None


@app.post("/jobs/{job_id}/upload-s3", response_model=JobOut)
def upload_from_s3(
    job_id: str,
    body: S3SourceRequest,
    user_id: str = Depends(require_user_id),
) -> JobOut:
    job = _authorize_job(job_id, user_id)
    if job.status not in ("created", "uploaded"):
        raise HTTPException(
            409, f"cannot upload while job is {job.status!r}"
        )

    try:
        data = cloud_storage.download_csv_from_s3(
            body.bucket,
            body.key,
            access_key_id=body.access_key_id,
            secret_access_key=body.secret_access_key,
            region=body.region,
        )
    except RuntimeError as exc:
        raise HTTPException(400, str(exc))

    storage.write_bytes(job_id, "raw.csv", data)
    delim = detect_delimiter(data)
    job = job_store.update(
        job_id,
        status="uploaded",
        input_filename=f"s3://{body.bucket}/{body.key}",
        delimiter=delim,
    )
    return _job_out(job)


@app.get("/jobs/{job_id}/preview", response_model=PreviewResponse)
def preview_job_source(
    job_id: str,
    delimiter: str | None = None,
    user_id: str = Depends(require_user_id),
) -> PreviewResponse:
    """Return a small sample-rows preview for the job's uploaded source.

    If `delimiter` is omitted, uses whatever was detected/stored on the job.
    If passed, re-parses with the override AND persists it on the job so
    the run step uses the chosen value.
    """
    job = _authorize_job(job_id, user_id)
    if not storage.exists(job_id, "raw.csv"):
        raise HTTPException(404, "no source uploaded yet")

    raw = storage.read_bytes(job_id, "raw.csv")
    chosen = delimiter or job.delimiter or detect_delimiter(raw)

    try:
        preview = _build_preview(raw, chosen)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            400, f"couldn't parse with delimiter {chosen!r}: {exc}"
        )

    # Persist the (possibly new) delimiter on the job so the run uses it.
    if chosen != job.delimiter:
        job_store.update(job_id, delimiter=chosen)

    return preview


@app.post("/jobs/{job_id}/run", response_model=JobOut)
def run_job(
    job_id: str,
    custom_instructions: str | None = Form(default=None),
    user_id: str = Depends(require_user_id),
) -> JobOut:
    job = _authorize_job(job_id, user_id)
    if job.status != "uploaded":
        raise HTTPException(
            409, f"cannot run while job is {job.status!r}"
        )

    job_store.update(
        job_id,
        status="running",
        custom_instructions=custom_instructions,
    )
    try:
        summary = run_pipeline(
            job_id,
            storage,
            custom_instructions=custom_instructions,
            delimiter=job.delimiter,
        )
    except Exception as exc:  # noqa: BLE001
        job = job_store.update(
            job_id, status="failed", error_message=str(exc)
        )
        return _job_out(job)

    job = job_store.update(job_id, status="done", summary=summary)
    return _job_out(job)


def _serve_job_json(job_id: str, user_id: str, name: str) -> JSONResponse:
    _authorize_job(job_id, user_id)
    if not storage.exists(job_id, name):
        raise HTTPException(404, f"{name} not yet available for this job")
    return JSONResponse(json.loads(storage.read_bytes(job_id, name)))


@app.get("/jobs/{job_id}/profile")
def get_profile(
    job_id: str, user_id: str = Depends(require_user_id)
) -> JSONResponse:
    return _serve_job_json(job_id, user_id, "profile.json")


@app.get("/jobs/{job_id}/plan")
def get_plan(
    job_id: str, user_id: str = Depends(require_user_id)
) -> JSONResponse:
    return _serve_job_json(job_id, user_id, "plan.json")


@app.get("/jobs/{job_id}/audit")
def get_audit(
    job_id: str, user_id: str = Depends(require_user_id)
) -> JSONResponse:
    return _serve_job_json(job_id, user_id, "audit.json")


@app.get("/jobs/{job_id}/quality")
def get_quality(
    job_id: str, user_id: str = Depends(require_user_id)
) -> JSONResponse:
    return _serve_job_json(job_id, user_id, "quality.json")


@app.get("/jobs/{job_id}/suggestions")
def get_suggestions(
    job_id: str, user_id: str = Depends(require_user_id)
) -> JSONResponse:
    return _serve_job_json(job_id, user_id, "suggestions.json")


@app.get("/jobs/{job_id}/before-after")
def before_after(
    job_id: str,
    column: str,
    n_samples: int = 12,
    user_id: str = Depends(require_user_id),
) -> dict:
    _authorize_job(job_id, user_id)
    if not storage.exists(job_id, "raw.csv") or not storage.exists(
        job_id, "cleaned.parquet"
    ):
        raise HTTPException(404, "job has no cleaned output yet")

    raw = pd.read_csv(BytesIO(storage.read_bytes(job_id, "raw.csv")), low_memory=False)
    cleaned = pd.read_parquet(BytesIO(storage.read_bytes(job_id, "cleaned.parquet")))

    if column not in raw.columns or column not in cleaned.columns:
        raise HTTPException(400, f"column {column!r} not found")

    mask = (raw[column].astype(str) != cleaned[column].astype(str)) & ~(
        raw[column].isna() & cleaned[column].isna()
    )
    idx = raw[mask].head(n_samples).index

    return {
        "column": column,
        "total_changed": int(mask.sum()),
        "samples": [
            {"before": str(raw.loc[i, column]), "after": str(cleaned.loc[i, column])}
            for i in idx
        ],
    }
