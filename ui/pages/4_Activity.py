"""Page 4 — Account activity / security audit log."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from api_client import get_my_activity  # noqa: E402
from auth_helpers import require_auth  # noqa: E402

st.set_page_config(page_title="Activity — Data Polish", layout="wide")

require_auth()

st.title("Activity")
st.caption(
    "Recent security and account events on your Data Polish account. "
    "If you see logins you don't recognize, change your password and log out "
    "of every device."
)

# --------------------------------------------------------------------------- #
# Fetch + render.
# --------------------------------------------------------------------------- #

try:
    events = get_my_activity()
except Exception as exc:  # noqa: BLE001
    st.error(f"Couldn't load activity: {exc}")
    st.stop()

if not events:
    st.info("No activity logged yet. Come back after a few logins.")
    st.stop()


def _pretty_event(t: str) -> str:
    """Map raw event_type strings to human-readable labels."""
    return {
        "login_success": "Signed in (password)",
        "login_failed": "Failed login",
        "register": "Account created",
        "register_failed": "Registration error",
        "magic_link_requested": "Magic-link requested",
        "magic_link_request_blocked": "Magic-link blocked",
        "magic_link_login": "Signed in (magic link)",
        "magic_link_verify_failed": "Magic-link code rejected",
        "job_created": "New cleaning job started",
    }.get(t, t)


rows = []
for e in events:
    rows.append(
        {
            "When": e.get("created_at", "")[:19].replace("T", " "),
            "Event": _pretty_event(e.get("event_type", "")),
            "IP": e.get("ip") or "-",
            "Details": ", ".join(
                f"{k}: {v}"
                for k, v in (e.get("metadata") or {}).items()
                if k not in ("email",)  # don't echo email back
            )
            or "-",
        }
    )

st.dataframe(
    pd.DataFrame(rows), use_container_width=True, hide_index=True
)

st.caption(
    f"Showing the last {len(events)} events. "
    "Older events are retained but not displayed."
)
