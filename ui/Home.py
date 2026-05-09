"""DataPolish v2 — landing page (login + workflow overview).

Run with:
    streamlit run ui/Home.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make `from api_client import ...` work for sibling pages too.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st  # noqa: E402
import streamlit_authenticator as stauth  # noqa: E402

from api_client import API_BASE, healthz  # noqa: E402
from auth_helpers import load_auth_config, save_auth_config  # noqa: E402

st.set_page_config(page_title="DataPolish", layout="wide")

# --------------------------------------------------------------------------- #
# Auth gate — must run before any other UI or API calls.
# --------------------------------------------------------------------------- #

config = load_auth_config()
# load_auth_config now always returns a dict — it auto-creates an empty
# config in deployed environments so the Register tab works on first visit.

authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
)

# Render Login + Register tabs while not authenticated. After login they
# disappear and the main page renders.
status = st.session_state.get("authentication_status")
if status is not True:
    st.title("DataPolish")
    st.markdown(
        "Log in or register to use the AI-augmented data cleaning pipeline."
    )

    tab_login, tab_register = st.tabs(["Login", "Register"])

    with tab_login:
        authenticator.login(location="main")
        if st.session_state.get("authentication_status") is False:
            st.error("Username or password is incorrect.")
        elif st.session_state.get("authentication_status") is None:
            st.caption(
                "Don't have an account? Use the Register tab."
            )

    with tab_register:
        st.caption(
            "Create an account. Username + password get stored locally "
            "in `auth_config.yaml` (gitignored). Passwords are hashed "
            "with bcrypt — never stored in plain text."
        )
        # streamlit-authenticator's register_user signature has shifted
        # across releases (pre_authorization vs pre_authorized vs none).
        # Probe with a minimal call first; fall back to even simpler
        # signatures if the kwargs aren't accepted.
        try:
            try:
                result = authenticator.register_user(
                    location="main", captcha=False
                )
            except TypeError:
                result = authenticator.register_user(location="main")

            # Newer versions return (email, username, name); older may return
            # something else or None. Treat any non-empty leading value as
            # registration success.
            if result and isinstance(result, tuple) and result[0]:
                save_auth_config(config)
                st.success(
                    "Registered. Switch to the **Login** tab to sign in."
                )
        except Exception as exc:  # noqa: BLE001
            st.error(str(exc))

    st.stop()

# --------------------------------------------------------------------------- #
# Logged in.
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
# Logged in.
# --------------------------------------------------------------------------- #

st.title("DataPolish")
st.markdown(
    "**An AI-augmented data engineering pipeline for messy real-world data.**"
)

# --------------------------------------------------------------------------- #
# API health badge — instantly tells the user if the backend is up.
# --------------------------------------------------------------------------- #

with st.sidebar:
    st.markdown(
        f"**Logged in as:** `{st.session_state.get('username')}`  \n"
        f"({st.session_state.get('name')})"
    )
    authenticator.logout("Logout", "sidebar")

    with st.expander("Reset password"):
        try:
            if authenticator.reset_password(
                st.session_state["username"], location="main"
            ):
                save_auth_config(config)
                st.success("Password updated.")
        except Exception as exc:  # noqa: BLE001
            st.error(str(exc))

    st.divider()

    st.markdown("### API status")
    try:
        info = healthz()
        st.success(f"{info['service']} v{info['version']}")
        st.caption(f"`{API_BASE}`")
    except Exception as exc:  # noqa: BLE001
        st.error("Backend not reachable")
        st.caption(f"`{API_BASE}`")
        st.caption(f"{type(exc).__name__}: {exc}")
        st.caption(
            "Run `uvicorn api.main:app --reload --port 8000` in another "
            "terminal."
        )

    st.divider()
    st.markdown(
        "**Workflow**\n\n"
        "1. **Upload** a CSV\n"
        "2. **Run** the pipeline (optionally with your own instructions)\n"
        "3. **Results** — see what was applied, what was skipped, and why\n"
    )

# --------------------------------------------------------------------------- #
# Body — what is this thing.
# --------------------------------------------------------------------------- #

st.markdown(
    """
Upload a CSV. The pipeline:

1. **Profiles** every column deterministically — dtype, nulls, casing
   patterns, top values, length stats.
2. **Asks an LLM** (Llama 3.3 70B via Groq) to propose cleaning rules in
   a typed JSON schema with a fixed set of operations.
3. **Applies** each rule through deterministic safety gates that
   re-validate the rule against the actual column profile before it
   touches the data.
4. **Returns** clean parquet plus a full audit log — every applied,
   skipped, and rejected rule, with reasons.

The structural decision: the LLM proposes; deterministic code disposes.
That's how this differs from "stuff data into ChatGPT" demos.
"""
)

if "job_id" in st.session_state:
    st.info(
        f"Current job in this session: `{st.session_state['job_id']}`. "
        "Continue at the **Run** or **Results** page."
    )

st.markdown("---")
st.caption(
    "v2.0 — single-user, local upload. Auth, cloud storage connectors, "
    "outlier detection, and custom rule steering coming next."
)
