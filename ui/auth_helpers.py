"""Auth helpers — talk to the FastAPI auth endpoints.

State lives entirely in `st.session_state`. We keep four keys:
  - user_id          : DB id (for display + UI logic only; never sent
                       to the API directly since v3.7)
  - username         : login handle (display in sidebar)
  - display_name     : friendly name
  - access_token     : signed JWT, sent as `Authorization: Bearer ...`

When `access_token` is missing we treat the user as logged-out.
"""

from __future__ import annotations

import streamlit as st


SESSION_KEYS = ("user_id", "username", "display_name", "access_token")


def is_authenticated() -> bool:
    """Authenticated means we have BOTH a user_id and a JWT in session."""
    return bool(st.session_state.get("user_id")) and bool(
        st.session_state.get("access_token")
    )


def require_auth() -> str:
    """Page guard. Stops if logged out; returns user_id when logged in."""
    if not is_authenticated():
        st.warning("Please log in via the **Home** page first.")
        st.stop()
    return st.session_state["user_id"]


def set_session(auth_response: dict) -> None:
    """Store the logged-in user + JWT.

    `auth_response` is the AuthResponse payload from /auth/register or
    /auth/login: { user: {id, username, ...}, access_token: "..." }.
    """
    user = auth_response["user"]
    st.session_state["user_id"] = user["id"]
    st.session_state["username"] = user["username"]
    st.session_state["display_name"] = user.get("name") or user["username"]
    st.session_state["access_token"] = auth_response["access_token"]


def clear_session() -> None:
    for k in SESSION_KEYS:
        st.session_state.pop(k, None)
    # Also wipe any in-flight job state so logout = clean slate.
    for k in ("job_id", "upload_complete", "last_upload", "run_complete", "run_result"):
        st.session_state.pop(k, None)
