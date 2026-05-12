"""Job state, now Postgres-backed (v3.0).

Replaces the v2 in-memory-plus-status.json design. Same public methods
(`create`, `get`, `update`, `list_for_user`) so callers in main.py barely
change. Persistence is now durable, concurrent-safe, and queryable.
"""

from __future__ import annotations

import uuid
from typing import Any, Literal

from sqlalchemy.orm import Session

from .models import Job

JobStatus = Literal["created", "uploaded", "running", "done", "failed"]


def _new_job_id() -> str:
    return uuid.uuid4().hex[:12]


def create(db: Session, *, user_id: str) -> Job:
    job = Job(id=_new_job_id(), user_id=user_id, status="created")
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def get(db: Session, job_id: str) -> Job | None:
    return db.query(Job).filter(Job.id == job_id).first()


def update(db: Session, job_id: str, **changes: Any) -> Job:
    job = get(db, job_id)
    if job is None:
        raise KeyError(f"Unknown job: {job_id}")
    for k, v in changes.items():
        setattr(job, k, v)
    db.commit()
    db.refresh(job)
    return job


def list_for_user(db: Session, user_id: str) -> list[Job]:
    return (
        db.query(Job)
        .filter(Job.user_id == user_id)
        .order_by(Job.created_at.desc())
        .all()
    )
