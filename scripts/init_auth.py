"""Bootstrap auth_config.yaml with one initial user.

Interactive — no credentials are baked into the source code, which means
forking the public repo doesn't ship default passwords. Run once on a
fresh checkout:

    python scripts/init_auth.py

Re-run with --force to wipe and recreate. Additional users register
themselves via the Streamlit UI's Register tab once this script has
created the first user.
"""

from __future__ import annotations

import argparse
import getpass
import re
import secrets
from pathlib import Path

import bcrypt
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "auth_config.yaml"

USERNAME_RE = re.compile(r"^[a-z0-9_.-]{2,32}$")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
MIN_PASSWORD_LEN = 6


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def _prompt(label: str, validator=None, *, lower: bool = False) -> str:
    while True:
        value = input(label).strip()
        if lower:
            value = value.lower()
        if validator is None or validator(value):
            return value
        print("  Invalid input. Try again.")


def _prompt_password() -> str:
    while True:
        first = getpass.getpass(f"Password (min {MIN_PASSWORD_LEN} chars): ")
        if len(first) < MIN_PASSWORD_LEN:
            print(f"  Password must be at least {MIN_PASSWORD_LEN} characters.")
            continue
        confirm = getpass.getpass("Confirm password: ")
        if first != confirm:
            print("  Passwords don't match. Try again.")
            continue
        return first


def collect_first_user() -> dict[str, str]:
    print("First-time setup — creating the initial user account.\n")
    username = _prompt(
        "Username (2-32 chars, lowercase + digits/_.-): ",
        validator=lambda v: bool(USERNAME_RE.match(v)),
        lower=True,
    )
    name = _prompt(
        f"Display name (default: {username}): ",
        validator=lambda v: True,
    ) or username
    email = _prompt(
        "Email: ",
        validator=lambda v: bool(EMAIL_RE.match(v)),
    )
    password = _prompt_password()
    return {
        "username": username,
        "name": name,
        "email": email,
        "password": password,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite auth_config.yaml if it already exists.",
    )
    args = parser.parse_args()

    if CONFIG_PATH.exists() and not args.force:
        print(f"{CONFIG_PATH} already exists.")
        print(
            "Existing users can register more accounts via the UI's "
            "Register tab. To reset auth from scratch, re-run with --force."
        )
        return

    user = collect_first_user()

    config = {
        "credentials": {
            "usernames": {
                user["username"]: {
                    "name": user["name"],
                    "email": user["email"],
                    "password": hash_password(user["password"]),
                    "logged_in": False,
                    "failed_login_attempts": 0,
                }
            }
        },
        "cookie": {
            "name": "datapolish_auth",
            "key": secrets.token_hex(32),
            "expiry_days": 7,
        },
        "pre-authorized": {"emails": []},
    }

    CONFIG_PATH.write_text(yaml.dump(config, default_flow_style=False))
    print()
    print(f"Wrote {CONFIG_PATH}.")
    print(f"Log in as: {user['username']}")
    print(
        "Additional users can register via the Streamlit UI's "
        "Register tab without re-running this script."
    )


if __name__ == "__main__":
    main()
