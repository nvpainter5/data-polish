"""Storage abstraction for v2.

A `StorageBackend` is the single interface every job uses to read and write
its files. Today: `LocalStorage` writes to `data/jobs/<job_id>/` on the API
server's disk. Phase v2.5: `S3Backend`, `GCSBackend`, etc. plug in here
without changes to the pipeline or the UI.

Per-job layout (regardless of backend):
    raw.csv          uploaded source
    profile.json     deterministic profile
    plan.json        LLM cleaning plan
    audit.json       apply audit (what was applied / skipped / why)
    cleaned.parquet  cleaned dataset
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import IO, BinaryIO


class StorageBackend(ABC):
    """The contract every storage backend implements."""

    @abstractmethod
    def write_bytes(self, job_id: str, name: str, data: bytes) -> None: ...

    @abstractmethod
    def write_stream(self, job_id: str, name: str, stream: IO[bytes]) -> int: ...

    @abstractmethod
    def read_bytes(self, job_id: str, name: str) -> bytes: ...

    @abstractmethod
    def exists(self, job_id: str, name: str) -> bool: ...

    @abstractmethod
    def list(self, job_id: str) -> list[str]: ...

    @abstractmethod
    def path(self, job_id: str, name: str) -> str:
        """Return a backend-specific path string. For LocalStorage this is
        an OS path callers can hand to pandas; for S3 it's an s3:// URI."""


class LocalStorage(StorageBackend):
    """Files live under `<root>/<job_id>/<name>` on the API server's disk."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _job_dir(self, job_id: str) -> Path:
        d = self.root / job_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def write_bytes(self, job_id: str, name: str, data: bytes) -> None:
        (self._job_dir(job_id) / name).write_bytes(data)

    def write_stream(
        self, job_id: str, name: str, stream: IO[bytes]
    ) -> int:
        target = self._job_dir(job_id) / name
        size = 0
        with target.open("wb") as out:
            while chunk := stream.read(1 << 20):  # 1 MB
                out.write(chunk)
                size += len(chunk)
        return size

    def read_bytes(self, job_id: str, name: str) -> bytes:
        return (self._job_dir(job_id) / name).read_bytes()

    def exists(self, job_id: str, name: str) -> bool:
        return (self.root / job_id / name).exists()

    def list(self, job_id: str) -> list[str]:
        d = self.root / job_id
        if not d.exists():
            return []
        return sorted(p.name for p in d.iterdir() if p.is_file())

    def path(self, job_id: str, name: str) -> str:
        return str(self._job_dir(job_id) / name)
