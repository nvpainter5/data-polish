"""Job state machine for v2.

A Job represents one pipeline run for one user. State transitions:

    created  -> uploaded  -> running  -> done
                                      -> failed

Job state lives both in memory (fast lookups) and on disk (`status.json`
inside the job directory) so a server restart doesn't lose state.

For v2.0 we keep this simple — synchronous job execution, in-process state.
v2.1+ can swap the in-memory dict for Redis/Postgres without changing the
public interface.
"""

from __future__ import annotations

import json
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Literal

from .storage import StorageBackend

JobStatus = Literal["created", "uploaded", "running", "done", "failed"]


@dataclass
class Job:
    job_id: str
    status: JobStatus
    created_at: str
    updated_at: str
    user_id: str | None = None  # populated once auth lands (v2.4)
    input_filename: str | None = None
    custom_instructions: str | None = None
    error_message: str | None = None
    summary: dict = field(default_factory=dict)
    # Column delimiter as detected (or overridden by user) at upload time.
    # Populated by /preview; used by pipeline_runner.run_pipeline.
    delimiter: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class JobStore:
    """In-memory + on-disk job registry.

    Thread-safe; backed by `<storage>/<job_id>/status.json` so a process
    restart can rehydrate the dict from disk on demand.
    """

    STATUS_FILE = "status.json"

    def __init__(self, storage: StorageBackend) -> None:
        self.storage = storage
        self._jobs: dict[str, Job] = {}
        self._lock = threading.RLock()

    def _now(self) -> str:
        return datetime.utcnow().isoformat(timespec="seconds") + "Z"

    def _persist(self, job: Job) -> None:
        self.storage.write_bytes(
            job.job_id,
            self.STATUS_FILE,
            json.dumps(job.to_dict(), indent=2).encode("utf-8"),
        )

    def _hydrate(self, job_id: str) -> Job | None:
        if not self.storage.exists(job_id, self.STATUS_FILE):
            return None
        raw = self.storage.read_bytes(job_id, self.STATUS_FILE)
        data = json.loads(raw)
        # Tolerate older status.json files written before new fields existed:
        # filter to only the keys Job actually accepts.
        valid_fields = {f.name for f in Job.__dataclass_fields__.values()}
        return Job(**{k: v for k, v in data.items() if k in valid_fields})

    def create(self, user_id: str | None = None) -> Job:
        with self._lock:
            now = self._now()
            job = Job(
                job_id=uuid.uuid4().hex[:12],
                status="created",
                created_at=now,
                updated_at=now,
                user_id=user_id,
            )
            self._jobs[job.job_id] = job
            self._persist(job)
            return job

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            if job_id in self._jobs:
                return self._jobs[job_id]
            job = self._hydrate(job_id)
            if job:
                self._jobs[job_id] = job
            return job

    def update(self, job_id: str, **changes) -> Job:
        with self._lock:
            job = self.get(job_id)
            if not job:
                raise KeyError(f"Unknown job: {job_id}")
            for k, v in changes.items():
                setattr(job, k, v)
            job.updated_at = self._now()
            self._persist(job)
            return job

    def list_for_user(self, user_id: str | None) -> list[Job]:
        with self._lock:
            return [
                j for j in self._jobs.values() if j.user_id == user_id
            ]
