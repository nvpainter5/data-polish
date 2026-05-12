"""FastAPI backend for Data Polish v2/v3.

The pure `datapolish` library stays the engine. This package wraps it in
HTTP endpoints and per-job state so a multi-user UI can drive it.

Three things happen at package import time, in this order:

1. `load_dotenv` runs so every module that reads env vars (api/db.py for
   DATABASE_URL, api/auth.py for JWT_SECRET, etc.) sees `.env` values
   before they're consumed.

2. Python `logging` is configured once with a sane format. Modules use
   `logger = logging.getLogger(__name__)` and stop calling print().
   In prod (LOG_LEVEL=INFO, default), only structured events show up;
   set LOG_LEVEL=DEBUG locally for chattier output.

3. Sentry is initialised IF `SENTRY_DSN` is set. Unhandled exceptions
   from FastAPI route handlers get captured automatically; we don't
   need to sprinkle `sentry_sdk.capture_exception` everywhere.
"""

import logging
import os
from pathlib import Path

# --------------------------------------------------------------------------- #
# 1. Load .env
# --------------------------------------------------------------------------- #

try:
    from dotenv import load_dotenv

    _PROJECT_ROOT = Path(__file__).resolve().parent.parent
    load_dotenv(_PROJECT_ROOT / ".env", override=False)
except ImportError:
    # dotenv not installed (e.g. in production where env vars are injected
    # by the platform). That's fine — env vars come from elsewhere.
    pass


# --------------------------------------------------------------------------- #
# 2. Logging
# --------------------------------------------------------------------------- #

_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
# Tame noisy third-party libraries so our own logs aren't drowned out.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# --------------------------------------------------------------------------- #
# 3. Sentry (optional)
# --------------------------------------------------------------------------- #

_SENTRY_DSN = os.environ.get("SENTRY_DSN", "").strip()
if _SENTRY_DSN:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

        sentry_sdk.init(
            dsn=_SENTRY_DSN,
            environment=os.environ.get("DATAPOLISH_ENV", "development"),
            release=os.environ.get("DATAPOLISH_RELEASE"),
            # 10% of transactions get a perf trace. Cheap for the free
            # tier; bump up later if we want richer perf data.
            traces_sample_rate=float(
                os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.1")
            ),
            send_default_pii=False,  # never leak request bodies w/ creds
            integrations=[FastApiIntegration(), SqlalchemyIntegration()],
        )
        logging.getLogger(__name__).info(
            "Sentry initialised (env=%s)",
            os.environ.get("DATAPOLISH_ENV", "development"),
        )
    except ImportError:
        logging.getLogger(__name__).warning(
            "SENTRY_DSN is set but sentry-sdk is not installed — "
            "run `pip install -r requirements.txt`. Skipping init."
        )
