"""Shared auth utilities for the Streamlit UI.

Three things live here:
  - load_auth_config / save_auth_config — read/write the YAML credentials
    store. `register_user` and `reset_password` mutate the config in place;
    we have to persist those mutations.
  - require_auth — page guard for non-Home pages. Returns username on success.

Behavior in the deployed environment (Streamlit Community Cloud, Render):
the filesystem is ephemeral, so on first run there is no auth_config.yaml.
We auto-create an empty one with a stable cookie key sourced from the
AUTH_COOKIE_KEY env var (set as a Streamlit secret in production). Users
register themselves through the Register tab.
"""

from __future__ import annotations

import os
import secrets

from pathlib import Path

import streamlit as st
import yaml
from yaml.loader import SafeLoader

CONFIG_PATH = Path(__file__).resolve().parent.parent / "auth_config.yaml"


def _bootstrap_empty_config() -> dict:
    """Create an empty auth_config.yaml so users can self-register.

    Cookie key prefers the AUTH_COOKIE_KEY env var so sessions survive
    server restarts in deployed environments. Falls back to a fresh random
    key for local dev.
    """
    key = os.environ.get("AUTH_COOKIE_KEY") or secrets.token_hex(32)
    return {
        "credentials": {"usernames": {}},
        "cookie": {
            "name": "datapolish_auth",
            "key": key,
            "expiry_days": 7,
        },
        "pre-authorized": {"emails": []},
    }


def load_auth_config() -> dict:
    """Load auth_config.yaml. If it doesn't exist (fresh deploy), create
    an empty one so the Register tab is usable on first visit."""
    if not CONFIG_PATH.exists():
        config = _bootstrap_empty_config()
        save_auth_config(config)
        return config
    return yaml.load(CONFIG_PATH.read_text(), Loader=SafeLoader)


def save_auth_config(config: dict) -> None:
    """Persist the in-memory config back to YAML.

    Called after register_user, reset_password, etc. — those streamlit-
    authenticator methods mutate the dict in place but don't write to disk.
    """
    CONFIG_PATH.write_text(yaml.dump(config, default_flow_style=False))


def require_auth() -> str:
    """Page guard. Stops the render if not authenticated; returns username."""
    if not st.session_state.get("authentication_status"):
        st.warning("Please log in via the **Home** page first.")
        st.stop()
    username = st.session_state.get("username")
    if not username:
        st.error("Auth state inconsistent. Log out and log in again.")
        st.stop()
    return username
