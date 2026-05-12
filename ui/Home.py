"""Data Polish v3 — landing page with API-backed auth.

Run with:
    streamlit run ui/Home.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make sibling modules importable.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st  # noqa: E402

import api_client as api  # noqa: E402
from auth_helpers import (  # noqa: E402
    clear_session,
    is_authenticated,
    set_session,
)

st.set_page_config(page_title="Data Polish", layout="wide")


# --------------------------------------------------------------------------- #
# Auth gate.
# --------------------------------------------------------------------------- #

if not is_authenticated():
    st.title("Data Polish")
    st.markdown("**AI-augmented data cleaning. Upload, review, export.**")

    tab_login, tab_magic, tab_register = st.tabs(
        ["Log in", "Magic link", "Register"]
    )

    with tab_login:
        with st.form("login_form", clear_on_submit=False):
            identifier = st.text_input(
                "Username or email",
                help="Use either — both work.",
            )
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Log in", type="primary")
            st.caption(
                "Forgot which email you used? Try your username instead. "
                "Or use the **Magic link** tab to sign in with an emailed code."
            )
        if submitted:
            try:
                auth_response = api.login(identifier, password)
            except api.APIError as exc:
                st.error(exc.detail)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Login error: {exc}")
            else:
                set_session(auth_response)
                st.rerun()

    with tab_magic:
        st.caption(
            "Skip the password. We'll email you a 6-digit code that signs you in."
        )

        # Two-step flow: enter email -> paste code. We use session_state
        # to remember which step we're on between reruns.
        magic_email = st.session_state.get("magic_pending_email", "")

        if not magic_email:
            with st.form("magic_request_form", clear_on_submit=False):
                m_email = st.text_input("Email")
                m_submitted = st.form_submit_button(
                    "Email me a sign-in code", type="primary"
                )
            if m_submitted:
                try:
                    api.magic_request(m_email)
                except api.APIError as exc:
                    st.error(exc.detail)
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Couldn't send code: {exc}")
                else:
                    st.session_state["magic_pending_email"] = m_email
                    st.rerun()
        else:
            st.info(
                f"If an account exists for **{magic_email}**, a code is "
                "on its way. Check your inbox (and spam folder)."
            )
            with st.form("magic_verify_form", clear_on_submit=False):
                m_code = st.text_input(
                    "6-digit code", max_chars=6, placeholder="123456"
                )
                m_verify = st.form_submit_button("Sign in", type="primary")
            if m_verify:
                try:
                    auth_response = api.magic_verify(magic_email, m_code)
                except api.APIError as exc:
                    st.error(exc.detail)
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Sign-in failed: {exc}")
                else:
                    st.session_state.pop("magic_pending_email", None)
                    set_session(auth_response)
                    st.rerun()

            if st.button("Use a different email", key="magic_reset"):
                st.session_state.pop("magic_pending_email", None)
                st.rerun()

    with tab_register:
        with st.form("register_form", clear_on_submit=False):
            r_username = st.text_input(
                "Username", help="2-32 chars: letters, digits, _ . -"
            )
            r_email = st.text_input("Email")
            r_name = st.text_input("Display name (optional)")
            r_password = st.text_input(
                "Password", type="password", help="At least 8 characters"
            )
            r_submitted = st.form_submit_button("Create account", type="primary")
        if r_submitted:
            try:
                auth_response = api.register(
                    r_username, r_email, r_name or r_username, r_password
                )
            except api.APIError as exc:
                st.error(exc.detail)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Registration error: {exc}")
            else:
                set_session(auth_response)
                st.success("Account created. Logged in.")
                st.rerun()

    # Trust note under the login form.
    st.divider()
    st.caption(
        "🔒 **Your data is yours.** Passwords are bcrypt-hashed — we never "
        "see or store them in plain text. Connections are HTTPS in production. "
        "Uploaded files are scoped to your account and never shared with "
        "other users."
    )

    st.stop()


# --------------------------------------------------------------------------- #
# Logged-in body.
# --------------------------------------------------------------------------- #

st.title("Data Polish")
st.markdown("**AI-augmented data cleaning. Upload, review, export.**")

with st.sidebar:
    st.markdown(
        f"**Logged in as:** `{st.session_state.get('username')}`  \n"
        f"({st.session_state.get('display_name')})"
    )
    if st.button("Log out", type="secondary"):
        clear_session()
        st.rerun()

    st.divider()

    st.markdown("### API status")
    try:
        info = api.healthz()
        st.success(f"{info['service']} v{info['version']}")
        st.caption(f"`{api.API_BASE}`")
    except Exception as exc:  # noqa: BLE001
        st.error("Backend not reachable")
        st.caption(f"`{api.API_BASE}`")
        st.caption(f"{type(exc).__name__}: {exc}")

    st.divider()
    st.markdown(
        "**Workflow**\n\n"
        "1. **Upload** a file\n"
        "2. **Run** the pipeline\n"
        "3. **Results** — audit, quality score, before/after\n"
    )

st.markdown(
    """
1. **Upload** a CSV, TSV, JSON, or parquet — locally or from S3.
2. **Run** the pipeline. An LLM proposes cleaning rules; safety gates
   validate each one before any data mutates.
3. **Review** the audit, quality score, and LLM suggestions. Download cleaned parquet.
"""
)

if "job_id" in st.session_state:
    st.info(
        f"Active job: `{st.session_state['job_id']}` — continue at "
        "**Run** or **Results**."
    )
