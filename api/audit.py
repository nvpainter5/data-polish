"""Audit logging — records security-relevant events to the audit_events table.

Usage:
    from . import audit
    audit.log(db, "login_success", user_id=user.id, request=request, metadata={...})

Failures are intentionally swallowed (a flaky audit log shouldn't break
the actual login). Errors print to stdout for operator debugging.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from sqlalchemy.orm import Session

from .models import AuditEvent

logger = logging.getLogger(__name__)

# Hard cap so a maliciously long User-Agent header can't blow up the row.
USER_AGENT_MAX_LEN = 512


def _request_info(request: Request | None) -> tuple[str | None, str | None]:
    if request is None:
        return None, None
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")
    if ua and len(ua) > USER_AGENT_MAX_LEN:
        ua = ua[:USER_AGENT_MAX_LEN]
    return ip, ua


def log(
    db: Session,
    event_type: str,
    *,
    user_id: str | None = None,
    request: Request | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Insert one row into audit_events. Best-effort — never raises."""
    try:
        ip, ua = _request_info(request)
        event = AuditEvent(
            user_id=user_id,
            event_type=event_type,
            ip=ip,
            user_agent=ua,
            metadata_json=metadata,
        )
        db.add(event)
        db.commit()
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        logger.error(
            "Failed to write audit event %r: %s: %s",
            event_type,
            type(exc).__name__,
            exc,
        )


def recent_for_user(
    db: Session, user_id: str, limit: int = 50
) -> list[AuditEvent]:
    return (
        db.query(AuditEvent)
        .filter(AuditEvent.user_id == user_id)
        .order_by(AuditEvent.created_at.desc())
        .limit(limit)
        .all()
    )
