"""FastAPI app for Data Polish v2.

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
import logging
import os
import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    Header,
    HTTPException,
    Request,
    UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

logger = logging.getLogger(__name__)

from . import (  # noqa: E402
    audit,
    auth,
    cloud_storage,
    jobs as jobs_repo,
    magic_link,
    user_store,
)
from .db import Base, engine, get_session  # noqa: E402
from .models import Job, User  # noqa: E402
from .pipeline_runner import detect_delimiter, run_pipeline  # noqa: E402
from .storage import LocalStorage  # noqa: E402

# --------------------------------------------------------------------------- #
# App + dependencies (single-tenant in v2.0; per-user scoping arrives in 2.4).
# --------------------------------------------------------------------------- #

JOBS_ROOT = PROJECT_ROOT / "data" / "jobs"
storage = LocalStorage(JOBS_ROOT)

# Bootstrap the database schema. For v3.0 we use create_all (idempotent —
# creates tables only if they don't exist). Alembic migrations come in
# when the schema starts to evolve.
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Data Polish API",
    version="3.7.0",
    description="Backend for the Data Polish multi-user web app.",
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
# Auth — every job-related endpoint requires a Bearer JWT. Per-user
# authorization is enforced by `_authorize_job` (404/403 cleanly
# distinguished from 401).
# --------------------------------------------------------------------------- #


def require_user(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_session),
) -> User:
    """Auth dependency.

    Expects `Authorization: Bearer <jwt>`. The legacy X-User-ID header
    that v3.0 supported is gone — the UI has been on JWT since v3.1 and
    keeping a header-based escape hatch around in prod is an obvious
    foot-gun.

    Returns the User row from Postgres.
    """
    if not authorization:
        raise HTTPException(401, "Authentication required")

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(401, "Authentication required")

    user_id = auth.user_id_from_token(token)
    if user_id is None:
        raise HTTPException(401, "Invalid or expired token")

    user = user_store.get_user(db, user_id)
    if not user:
        raise HTTPException(401, "Unknown user")
    return user


def _authorize_job(db: Session, job_id: str, user: User) -> Job:
    job = jobs_repo.get(db, job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job.user_id != user.id:
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


def _job_out(job: Job) -> JobOut:
    return JobOut(
        job_id=job.id,
        status=job.status,
        created_at=job.created_at.isoformat() if job.created_at else "",
        updated_at=job.updated_at.isoformat() if job.updated_at else "",
        input_filename=job.input_filename,
        error_message=job.error_message,
        summary=job.summary or {},
        delimiter=job.delimiter,
    )


# --------------------------------------------------------------------------- #
# Auth schemas — registration / login return an AuthResponse carrying a
# short-lived JWT the UI then sends as `Authorization: Bearer ...` on
# every protected call. user_id must exist in the users table.
# --------------------------------------------------------------------------- #


class RegisterRequest(BaseModel):
    username: str
    email: str
    name: str | None = None
    password: str


class LoginRequest(BaseModel):
    username_or_email: str
    password: str


class UserOut(BaseModel):
    id: str
    username: str
    email: str
    name: str


class AuthResponse(BaseModel):
    """Returned by /auth/register and /auth/login. Client stores the
    `access_token` and sends it as `Authorization: Bearer ...` on every
    subsequent request."""

    user: UserOut
    access_token: str
    token_type: str = "bearer"


def _user_out(user: User) -> UserOut:
    return UserOut(
        id=user.id, username=user.username, email=user.email, name=user.name
    )


def _auth_response(user: User) -> AuthResponse:
    return AuthResponse(user=_user_out(user), access_token=auth.mint_jwt(user.id))


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
def healthz(db: Session = Depends(get_session)) -> JSONResponse:
    """Readiness probe.

    Returns 200 with `{"ok": true, ...}` only when the API process is up
    AND it can reach the database. Render's health check hits this every
    minute; failing it triggers a redeploy attempt.

    The DB check is a cheap `SELECT 1` — runs under 5 ms on Supabase
    when the pool is warm. We never raise here; we always respond with
    JSON so monitoring can parse the body.
    """
    db_ok = True
    db_error: str | None = None
    try:
        db.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        db_ok = False
        db_error = f"{type(exc).__name__}: {exc}"
        logger.error("Healthcheck DB probe failed: %s", db_error)

    payload = {
        "ok": db_ok,
        "service": "datapolish-api",
        "version": app.version,
        "env": os.environ.get("DATAPOLISH_ENV", "development"),
        "checks": {
            "database": "ok" if db_ok else "down",
            "sentry": "enabled" if os.environ.get("SENTRY_DSN") else "disabled",
            "email_provider": "resend" if os.environ.get("RESEND_API_KEY") else "dev-console",
        },
    }
    if db_error:
        payload["error"] = db_error

    # 503 when not ready so orchestrators do the right thing.
    return JSONResponse(payload, status_code=200 if db_ok else 503)


@app.post("/auth/register", response_model=AuthResponse)
def auth_register(
    body: RegisterRequest,
    request: Request,
    db: Session = Depends(get_session),
) -> AuthResponse:
    try:
        user = user_store.register_user(
            db,
            username=body.username,
            email=body.email,
            name=body.name or body.username,
            password=body.password,
        )
    except user_store.UserStoreError as exc:
        audit.log(
            db,
            "register_failed",
            request=request,
            metadata={"username": body.username, "email": body.email, "reason": str(exc)},
        )
        raise HTTPException(400, str(exc))

    audit.log(db, "register", user_id=user.id, request=request)
    return _auth_response(user)


@app.post("/auth/login", response_model=AuthResponse)
def auth_login(
    body: LoginRequest,
    request: Request,
    db: Session = Depends(get_session),
) -> AuthResponse:
    user = user_store.authenticate(
        db,
        username_or_email=body.username_or_email,
        password=body.password,
    )
    if not user:
        audit.log(
            db,
            "login_failed",
            request=request,
            metadata={"identifier": body.username_or_email},
        )
        raise HTTPException(401, "Invalid credentials")

    audit.log(db, "login_success", user_id=user.id, request=request)
    return _auth_response(user)


@app.get("/auth/me", response_model=UserOut)
def auth_me(user: User = Depends(require_user)) -> UserOut:
    return _user_out(user)


# --------------------------------------------------------------------------- #
# Magic-link auth (v3.2) — emailed one-time code, no password needed.
# --------------------------------------------------------------------------- #


class MagicLinkRequestBody(BaseModel):
    email: str


class MagicLinkVerifyBody(BaseModel):
    email: str
    code: str


@app.post("/auth/magic/request")
def auth_magic_request(
    body: MagicLinkRequestBody,
    request: Request,
    db: Session = Depends(get_session),
) -> dict:
    try:
        magic_link.request_magic_link(db, body.email)
    except magic_link.MagicLinkError as exc:
        audit.log(
            db,
            "magic_link_request_blocked",
            request=request,
            metadata={"email": body.email, "reason": str(exc)},
        )
        if "Too many" in str(exc):
            raise HTTPException(429, str(exc))
        raise HTTPException(400, str(exc))

    audit.log(
        db,
        "magic_link_requested",
        request=request,
        metadata={"email": body.email},
    )
    return {"sent": True}


@app.post("/auth/magic/verify", response_model=AuthResponse)
def auth_magic_verify(
    body: MagicLinkVerifyBody,
    request: Request,
    db: Session = Depends(get_session),
) -> AuthResponse:
    user = magic_link.verify_magic_link(db, body.email, body.code)
    if not user:
        audit.log(
            db,
            "magic_link_verify_failed",
            request=request,
            metadata={"email": body.email},
        )
        raise HTTPException(401, "Invalid or expired code.")

    audit.log(db, "magic_link_login", user_id=user.id, request=request)
    return _auth_response(user)


@app.get("/users/me/activity")
def list_my_activity(
    user: User = Depends(require_user), db: Session = Depends(get_session)
) -> list[dict]:
    """Recent audit events for the current user."""
    events = audit.recent_for_user(db, user.id, limit=50)
    return [
        {
            "event_type": e.event_type,
            "ip": e.ip,
            "user_agent": e.user_agent,
            "metadata": e.metadata_json,
            "created_at": e.created_at.isoformat() if e.created_at else "",
        }
        for e in events
    ]


@app.post("/jobs", response_model=JobOut)
def create_job(
    request: Request,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JobOut:
    job = jobs_repo.create(db, user_id=user.id)
    audit.log(
        db,
        "job_created",
        user_id=user.id,
        request=request,
        metadata={"job_id": job.id},
    )
    return _job_out(job)


@app.get("/users/me/jobs", response_model=list[JobOut])
def list_my_jobs(
    user: User = Depends(require_user), db: Session = Depends(get_session)
) -> list[JobOut]:
    return [_job_out(j) for j in jobs_repo.list_for_user(db, user.id)]


@app.get("/jobs/{job_id}", response_model=JobOut)
def get_job(
    job_id: str,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JobOut:
    job = _authorize_job(db, job_id, user)
    return _job_out(job)


@app.post("/jobs/{job_id}/upload", response_model=JobOut)
def upload_csv(
    job_id: str,
    file: UploadFile = File(...),
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JobOut:
    job = _authorize_job(db, job_id, user)
    if job.status not in ("created", "uploaded"):
        raise HTTPException(
            409, f"cannot upload while job is {job.status!r}"
        )
    if not file.filename:
        raise HTTPException(400, "missing filename")
    allowed = (".csv", ".tsv", ".txt", ".json", ".parquet")
    if not file.filename.lower().endswith(allowed):
        raise HTTPException(
            400,
            f"unsupported file extension. Allowed: {', '.join(allowed)}",
        )

    storage.write_stream(job_id, "raw.csv", file.file)
    raw = storage.read_bytes(job_id, "raw.csv")
    delim = detect_delimiter(raw)
    job = jobs_repo.update(
        db,
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
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JobOut:
    job = _authorize_job(db, job_id, user)
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
    job = jobs_repo.update(
        db,
        job_id,
        status="uploaded",
        input_filename=f"s3://{body.bucket}/{body.key}",
        delimiter=delim,
    )
    return _job_out(job)


# --------------------------------------------------------------------------- #
# Google Cloud Storage source (v3.5)
# --------------------------------------------------------------------------- #


class GCSSourceRequest(BaseModel):
    bucket: str
    blob_name: str
    service_account_json: str | None = None


@app.post("/jobs/{job_id}/upload-gcs", response_model=JobOut)
def upload_from_gcs(
    job_id: str,
    body: GCSSourceRequest,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JobOut:
    job = _authorize_job(db, job_id, user)
    if job.status not in ("created", "uploaded"):
        raise HTTPException(
            409, f"cannot upload while job is {job.status!r}"
        )

    try:
        data = cloud_storage.download_csv_from_gcs(
            body.bucket,
            body.blob_name,
            service_account_json=body.service_account_json,
        )
    except RuntimeError as exc:
        raise HTTPException(400, str(exc))

    storage.write_bytes(job_id, "raw.csv", data)
    delim = detect_delimiter(data)
    job = jobs_repo.update(
        db,
        job_id,
        status="uploaded",
        input_filename=f"gs://{body.bucket}/{body.blob_name}",
        delimiter=delim,
    )
    return _job_out(job)


# --------------------------------------------------------------------------- #
# Azure Blob source (v3.5)
# --------------------------------------------------------------------------- #


class AzureSourceRequest(BaseModel):
    account_name: str
    container: str
    blob_name: str
    connection_string: str | None = None
    account_key: str | None = None
    sas_token: str | None = None


@app.post("/jobs/{job_id}/upload-azure", response_model=JobOut)
def upload_from_azure(
    job_id: str,
    body: AzureSourceRequest,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JobOut:
    job = _authorize_job(db, job_id, user)
    if job.status not in ("created", "uploaded"):
        raise HTTPException(
            409, f"cannot upload while job is {job.status!r}"
        )

    try:
        data = cloud_storage.download_csv_from_azure(
            body.account_name,
            body.container,
            body.blob_name,
            connection_string=body.connection_string,
            account_key=body.account_key,
            sas_token=body.sas_token,
        )
    except RuntimeError as exc:
        raise HTTPException(400, str(exc))

    storage.write_bytes(job_id, "raw.csv", data)
    delim = detect_delimiter(data)
    job = jobs_repo.update(
        db,
        job_id,
        status="uploaded",
        input_filename=(
            f"azure://{body.account_name}/{body.container}/{body.blob_name}"
        ),
        delimiter=delim,
    )
    return _job_out(job)


@app.get("/jobs/{job_id}/preview", response_model=PreviewResponse)
def preview_job_source(
    job_id: str,
    delimiter: str | None = None,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> PreviewResponse:
    """Return a small sample-rows preview for the job's uploaded source."""
    job = _authorize_job(db, job_id, user)
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

    if chosen != job.delimiter:
        jobs_repo.update(db, job_id, delimiter=chosen)

    return preview


@app.post("/jobs/{job_id}/run", response_model=JobOut)
def run_job(
    job_id: str,
    custom_instructions: str | None = Form(default=None),
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JobOut:
    job = _authorize_job(db, job_id, user)
    if job.status != "uploaded":
        raise HTTPException(
            409, f"cannot run while job is {job.status!r}"
        )

    jobs_repo.update(
        db,
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
        # Surface dataset-size errors with a user-friendly message that
        # still includes the technical detail in metadata.
        from .pipeline_runner import DatasetTooLargeError

        if isinstance(exc, DatasetTooLargeError):
            error_message = (
                "Dataset is too large for this tier. "
                "Try a smaller sample or use S3 with a smaller key."
            )
        else:
            error_message = str(exc)

        job = jobs_repo.update(
            db, job_id, status="failed", error_message=error_message
        )
        return _job_out(job)

    job = jobs_repo.update(db, job_id, status="done", summary=summary)
    return _job_out(job)


def _serve_job_json(
    db: Session, job_id: str, user: User, name: str
) -> JSONResponse:
    _authorize_job(db, job_id, user)
    if not storage.exists(job_id, name):
        raise HTTPException(404, f"{name} not yet available for this job")
    return JSONResponse(json.loads(storage.read_bytes(job_id, name)))


@app.get("/jobs/{job_id}/profile")
def get_profile(
    job_id: str,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JSONResponse:
    return _serve_job_json(db, job_id, user, "profile.json")


@app.get("/jobs/{job_id}/plan")
def get_plan(
    job_id: str,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JSONResponse:
    return _serve_job_json(db, job_id, user, "plan.json")


@app.get("/jobs/{job_id}/audit")
def get_audit(
    job_id: str,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JSONResponse:
    return _serve_job_json(db, job_id, user, "audit.json")


@app.get("/jobs/{job_id}/quality")
def get_quality(
    job_id: str,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JSONResponse:
    return _serve_job_json(db, job_id, user, "quality.json")


@app.get("/jobs/{job_id}/suggestions")
def get_suggestions(
    job_id: str,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> JSONResponse:
    return _serve_job_json(db, job_id, user, "suggestions.json")


@app.get("/jobs/{job_id}/before-after")
def before_after(
    job_id: str,
    column: str,
    n_samples: int = 12,
    user: User = Depends(require_user),
    db: Session = Depends(get_session),
) -> dict:
    _authorize_job(db, job_id, user)
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
