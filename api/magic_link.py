"""Magic-link (one-time emailed code) authentication.

Flow:
  1. User submits email -> /auth/magic/request
  2. We generate a 6-digit code, bcrypt-hash it, store the hash, send the
     plain code via Resend.
  3. User pastes the code -> /auth/magic/verify
  4. We compare against stored hashes (latest unexpired token wins).
  5. On success we mark the token used and return an AuthResponse (JWT).

Rate limits:
  - Max 3 magic-link requests per email per 15-minute window
  - Max 5 verify attempts per token

To prevent email enumeration we always behave the same way regardless of
whether the email is registered (the email just doesn't get sent if no
user exists).
"""

from __future__ import annotations

import logging
import os
import secrets
from datetime import datetime, timedelta, timezone

import bcrypt
import resend
from sqlalchemy.orm import Session

from .models import MagicLinkToken, User

logger = logging.getLogger(__name__)

CODE_LENGTH = 6
CODE_TTL_MINUTES = 15
MAX_REQUESTS_PER_EMAIL = 3
REQUEST_WINDOW_MINUTES = 15
MAX_ATTEMPTS_PER_TOKEN = 5


def _generate_code() -> str:
    """Cryptographically random 6-digit numeric OTP."""
    return "".join(secrets.choice("0123456789") for _ in range(CODE_LENGTH))


def _hash_code(code: str) -> str:
    return bcrypt.hashpw(code.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_code(code: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(code.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:  # noqa: BLE001
        return False


class MagicLinkError(Exception):
    """Surfaced to API callers as 4xx errors."""


def request_magic_link(db: Session, email: str) -> None:
    """Issue a code and email it. Silent if the email isn't registered
    (anti-enumeration). Raises MagicLinkError on rate limit."""
    email = (email or "").lower().strip()
    if not email:
        raise MagicLinkError("Email is required.")

    cutoff = datetime.now(timezone.utc) - timedelta(
        minutes=REQUEST_WINDOW_MINUTES
    )
    recent_count = (
        db.query(MagicLinkToken)
        .filter(
            MagicLinkToken.email == email,
            MagicLinkToken.created_at >= cutoff,
        )
        .count()
    )
    if recent_count >= MAX_REQUESTS_PER_EMAIL:
        raise MagicLinkError(
            "Too many sign-in requests for this email. "
            f"Try again in {REQUEST_WINDOW_MINUTES} minutes."
        )

    # Always create a token row even if the email isn't registered, so
    # rate limiting is consistent and timing doesn't leak existence.
    code = _generate_code()
    token = MagicLinkToken(
        email=email,
        code_hash=_hash_code(code),
        expires_at=datetime.now(timezone.utc)
        + timedelta(minutes=CODE_TTL_MINUTES),
    )
    db.add(token)
    db.commit()

    # Only actually send the email if a user exists for this address.
    user = db.query(User).filter(User.email == email).first()
    if user:
        _send_code_email(email, code)


def verify_magic_link(db: Session, email: str, code: str) -> User | None:
    """Match the code against the latest unexpired unused token for the
    email. Returns the User on success, None on any failure (invalid,
    expired, too many attempts, no such user)."""
    email = (email or "").lower().strip()
    code = (code or "").strip()
    if not email or not code:
        return None

    token = (
        db.query(MagicLinkToken)
        .filter(
            MagicLinkToken.email == email,
            MagicLinkToken.used_at.is_(None),
            MagicLinkToken.expires_at > datetime.now(timezone.utc),
        )
        .order_by(MagicLinkToken.created_at.desc())
        .first()
    )
    if token is None:
        return None
    if token.attempts >= MAX_ATTEMPTS_PER_TOKEN:
        return None

    token.attempts += 1
    db.commit()

    if not _verify_code(code, token.code_hash):
        return None

    # Mark token used so the same code can't be replayed.
    token.used_at = datetime.now(timezone.utc)
    db.commit()

    return db.query(User).filter(User.email == email).first()


def _is_dev_mode() -> bool:
    return os.environ.get("DEV_MODE", "").strip().lower() in ("1", "true", "yes")


def _send_code_email(email: str, code: str) -> None:
    """Send the OTP via Resend. Behaviors:

    - DEV_MODE=true OR RESEND_API_KEY unset:
        Print the code to stdout so it shows up in the uvicorn console.
        Useful for local testing without Resend properly configured.

    - Otherwise: try Resend. Errors are logged to stdout but not raised —
        we don't want to reveal email-existence via timing or error codes.
    """
    api_key = os.environ.get("RESEND_API_KEY", "").strip()

    if _is_dev_mode() or not api_key:
        # WARNING: this is a secret. Only logged in dev mode / when no
        # Resend key is configured. Never enable DEV_MODE in production.
        logger.warning(
            "DEV_MODE magic-link code for %s: %s (valid %d min)",
            email,
            code,
            CODE_TTL_MINUTES,
        )
        if not api_key:
            return

    resend.api_key = api_key
    from_email = os.environ.get(
        "RESEND_FROM_EMAIL", "onboarding@resend.dev"
    )

    html = f"""
        <div style="font-family: -apple-system, system-ui, sans-serif; max-width: 480px; margin: 0 auto;">
          <h2 style="color: #1a1a1a;">Your Data Polish sign-in code</h2>
          <p>Enter this code in the sign-in page:</p>
          <p style="font-size: 28px; letter-spacing: 6px; font-weight: 700;
                    background: #f4f4f5; padding: 12px 16px; border-radius: 8px;
                    text-align: center; font-family: monospace;">
            {code}
          </p>
          <p style="color: #666;">This code expires in {CODE_TTL_MINUTES} minutes.</p>
          <p style="color: #666;">If you didn't request this, you can safely ignore this email.</p>
          <hr style="border: none; border-top: 1px solid #eee; margin: 24px 0;"/>
          <p style="color: #999; font-size: 12px;">Data Polish</p>
        </div>
    """

    try:
        result = resend.Emails.send(
            {
                "from": from_email,
                "to": [email],
                "subject": "Your Data Polish sign-in code",
                "html": html,
            }
        )
        logger.info(
            "Resend accepted magic-link email for %s (id=%s, from=%s)",
            email,
            result.get("id", "?"),
            from_email,
        )
    except Exception as exc:  # noqa: BLE001
        # Log so on-call can debug, but don't propagate — surfacing the
        # error to the caller would leak whether the email is registered
        # (the rest of the flow is silent for unknown emails).
        logger.error(
            "Resend send FAILED for %s (from=%s): %s: %s",
            email,
            from_email,
            type(exc).__name__,
            exc,
        )
