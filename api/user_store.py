"""User CRUD backed by Postgres (Supabase in production, SQLite locally).

Replaces the old auth_config.yaml file. Everything goes through SQLAlchemy
so concurrency, persistence, and migrations all work properly.
"""

from __future__ import annotations

import re

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .auth import hash_password, verify_password
from .models import User

USERNAME_RE = re.compile(r"^[a-z0-9_.-]{2,32}$")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class UserStoreError(Exception):
    """Validation / conflict errors surfaced to API callers as 400s."""


def _validate_inputs(username: str, email: str, password: str) -> None:
    # username/email already lowercased by caller — validate the canonical form.
    if not USERNAME_RE.match(username or ""):
        raise UserStoreError(
            "Username must be 2-32 chars: letters, digits, or _ . -"
        )
    if not EMAIL_RE.match(email or ""):
        raise UserStoreError("Email looks malformed.")
    if not password or len(password) < 8:
        raise UserStoreError("Password must be at least 8 characters.")


def register_user(
    db: Session,
    *,
    username: str,
    email: str,
    name: str,
    password: str,
) -> User:
    """Create a user. Raises UserStoreError on validation or duplicate.

    Username and email are normalized to lowercase before storage so the
    UI doesn't have to babysit casing.
    """
    canonical_username = (username or "").strip().lower()
    canonical_email = (email or "").strip().lower()
    _validate_inputs(canonical_username, canonical_email, password)

    # Pre-check uniqueness so we can return a specific error message.
    # IntegrityError's string includes the full INSERT SQL listing every
    # column, so substring matching on the exception text is unreliable.
    if db.query(User).filter(User.email == canonical_email).first():
        raise UserStoreError(
            "An account with that email already exists."
        )
    if db.query(User).filter(User.username == canonical_username).first():
        raise UserStoreError("That username is taken.")

    user = User(
        username=canonical_username,
        email=canonical_email,
        name=(name or username or "").strip() or canonical_username,
        password_hash=hash_password(password),
    )
    db.add(user)
    try:
        db.commit()
    except IntegrityError as exc:
        # Race condition fallback: two simultaneous registrations could
        # both pass the pre-check then collide at commit. Re-query to give
        # a specific message.
        db.rollback()
        if db.query(User).filter(User.email == canonical_email).first():
            raise UserStoreError(
                "An account with that email already exists."
            ) from exc
        if db.query(User).filter(
            User.username == canonical_username
        ).first():
            raise UserStoreError("That username is taken.") from exc
        raise UserStoreError("Could not create user.") from exc

    db.refresh(user)
    return user


def authenticate(
    db: Session, *, username_or_email: str, password: str
) -> User | None:
    """Return the User if credentials match, else None."""
    needle = (username_or_email or "").lower().strip()
    if not needle or not password:
        return None

    user = (
        db.query(User)
        .filter((User.username == needle) | (User.email == needle))
        .first()
    )
    if user is None:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


def get_user(db: Session, user_id: str) -> User | None:
    return db.query(User).filter(User.id == user_id).first()


def update_password(db: Session, user: User, new_password: str) -> None:
    if not new_password or len(new_password) < 8:
        raise UserStoreError("Password must be at least 8 characters.")
    user.password_hash = hash_password(new_password)
    db.commit()
