"""Password hashing + JWT minting/verification.

v3.0: bcrypt directly (the `bcrypt` package), no passlib. passlib 1.7.4
has a compatibility bug with bcrypt 4.x that raises spurious "password
too long" errors. Direct bcrypt is simpler and avoids the issue.

v3.1: JWT sessions via python-jose. Tokens are signed with JWT_SECRET
(env var). Payload is {sub: user_id, exp: int}. 24-hour lifetime.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import bcrypt
from jose import JWTError, jwt

BCRYPT_MAX_BYTES = 72

# JWT config — kept here so callers don't have to know the algorithm.
JWT_ALGORITHM = "HS256"
JWT_DEFAULT_TTL_HOURS = 24


def _jwt_secret() -> str:
    secret = os.environ.get("JWT_SECRET", "").strip()
    if not secret:
        raise RuntimeError(
            "JWT_SECRET not set. Generate one with `python -c "
            "\"import secrets; print(secrets.token_hex(32))\"` and add it "
            "to .env."
        )
    return secret


def _to_bcrypt_bytes(plain: str) -> bytes:
    """UTF-8 encode and truncate to bcrypt's 72-byte input limit."""
    raw = plain.encode("utf-8")
    return raw[:BCRYPT_MAX_BYTES]


def hash_password(plain: str) -> str:
    """Return a bcrypt hash. Always slow on purpose."""
    return bcrypt.hashpw(_to_bcrypt_bytes(plain), bcrypt.gensalt()).decode(
        "utf-8"
    )


def verify_password(plain: str, hashed: str) -> bool:
    """Constant-time bcrypt verification. Returns False on any error."""
    if not plain or not hashed:
        return False
    try:
        return bcrypt.checkpw(
            _to_bcrypt_bytes(plain), hashed.encode("utf-8")
        )
    except Exception:  # noqa: BLE001
        return False


# --------------------------------------------------------------------------- #
# JWT — v3.1 sessions.
# --------------------------------------------------------------------------- #


def mint_jwt(user_id: str, *, ttl_hours: int = JWT_DEFAULT_TTL_HOURS) -> str:
    """Issue a signed JWT for a user. `sub` is the user_id; `exp` enforces TTL."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=ttl_hours)).timestamp()),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm=JWT_ALGORITHM)


def decode_jwt(token: str) -> dict:
    """Verify the signature + expiry. Raises JWTError on any failure."""
    return jwt.decode(token, _jwt_secret(), algorithms=[JWT_ALGORITHM])


def user_id_from_token(token: str) -> str | None:
    """Convenience: return the user_id (sub) or None if the token is invalid."""
    try:
        payload = decode_jwt(token)
    except JWTError:
        return None
    sub = payload.get("sub")
    return sub if isinstance(sub, str) and sub else None
