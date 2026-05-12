"""SQLAlchemy models for v3 persistence.

Three core tables for v3.0:
  - users         — replaces auth_config.yaml
  - jobs          — replaces per-job status.json + JobStore in-memory dict
  - audit_events  — security/lifecycle log; surfaces in v3.3

Sessions / JWT live separately (v3.1 lands a `sessions` table or
stateless JWT scheme).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_id(prefix: str = "") -> str:
    raw = uuid.uuid4().hex[:12]
    return f"{prefix}{raw}" if prefix else raw


# --------------------------------------------------------------------------- #
# Users — replaces auth_config.yaml
# --------------------------------------------------------------------------- #


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(
        String(64), primary_key=True, default=lambda: _new_id("u_")
    )
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    email: Mapped[str] = mapped_column(String(256), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128))

    # bcrypt hash. Never plaintext.
    password_hash: Mapped[str] = mapped_column(String(256))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow
    )

    # When the user last successfully logged in.
    last_login_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # OAuth linking — populated when the user signed in with Google/GitHub
    # (v3.6). Single provider per user for v3.0; extend later if multi-link
    # becomes important.
    oauth_provider: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    oauth_subject: Mapped[str | None] = mapped_column(
        String(128), nullable=True, index=True
    )

    jobs: Mapped[list["Job"]] = relationship(back_populates="user")


# --------------------------------------------------------------------------- #
# Jobs — replaces on-disk status.json
# --------------------------------------------------------------------------- #


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(
        String(64), primary_key=True, default=lambda: _new_id()
    )
    user_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), index=True
    )

    status: Mapped[str] = mapped_column(String(32), index=True)
    input_filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    custom_instructions: Mapped[str | None] = mapped_column(Text, nullable=True)
    delimiter: Mapped[str | None] = mapped_column(String(8), nullable=True)

    summary: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow
    )

    user: Mapped[User] = relationship(back_populates="jobs")


# --------------------------------------------------------------------------- #
# Audit events — security log (v3.3)
# --------------------------------------------------------------------------- #


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    user_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # event_type values (free-form for now; will lock down in v3.3):
    #   login_success / login_failed / register / password_reset_request
    #   password_reset_complete / oauth_sign_in / job_created / s3_connect
    event_type: Mapped[str] = mapped_column(String(64), index=True)

    ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(512), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, index=True
    )

    __table_args__ = (
        # Most common query: "events for user X over recent window."
        Index("ix_audit_user_time", "user_id", "created_at"),
    )


# --------------------------------------------------------------------------- #
# Magic-link tokens — v3.2 passwordless auth via emailed OTP
# --------------------------------------------------------------------------- #


class MagicLinkToken(Base):
    """One-time codes mailed to a user for passwordless sign-in.

    The code itself is bcrypt-hashed before storage (same treatment as
    passwords). `attempts` is bumped on each verify attempt so we can
    rate-limit brute force.
    """

    __tablename__ = "magic_link_tokens"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    email: Mapped[str] = mapped_column(String(256), index=True)
    code_hash: Mapped[str] = mapped_column(String(256))
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True
    )
    used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, index=True
    )
