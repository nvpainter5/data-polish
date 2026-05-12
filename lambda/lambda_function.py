"""AWS Lambda handler for Data Polish.

Triggered by s3:ObjectCreated:* on the raw bucket. For each new CSV:
  1. Download the CSV from s3://<RAW_BUCKET>/<key>
  2. Run the Phase 1 pipeline (profile -> propose -> apply -> validate)
  3. Upload cleaned parquet + audit JSON to s3://<CLEANED_BUCKET>/...

Why Phase 1 and not the agent: a single LLM call is predictable in latency
and cost. The agent loop is fine for interactive use but in a serverless
hot path we want bounded execution time. The agent path is still available
as a separate Lambda if we want it (Phase 3c, future work).

Environment variables expected:
  GROQ_API_KEY     — Groq API key (configured by SAM template).
  CLEANED_BUCKET   — destination bucket name.
"""

from __future__ import annotations

import json
import logging
import os
from io import BytesIO
from typing import Any
from urllib.parse import unquote_plus

import boto3
import pandas as pd

from datapolish.apply import apply_plan, save_audit, validate_cleaned
from datapolish.cleaning import propose_cleaning_rules
from datapolish.profile import profile_dataset

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

REQUIRED_COLUMNS = ["unique_key", "created_date", "complaint_type"]
UNIQUE_KEY = "unique_key"

s3 = boto3.client("s3")


def _output_keys(src_key: str) -> dict[str, str]:
    """Derive output key names from the source object key.

    s3://raw/incoming/foo.csv  ->  cleaned/foo_cleaned.parquet
                                    cleaned/foo_audit.json
    """
    base = src_key.rsplit("/", 1)[-1]
    if base.endswith(".csv"):
        base = base[: -len(".csv")]
    return {
        "parquet": f"cleaned/{base}_cleaned.parquet",
        "audit": f"cleaned/{base}_audit.json",
    }


def lambda_handler(event: dict, context: Any) -> dict:
    cleaned_bucket = os.environ["CLEANED_BUCKET"]

    record = event["Records"][0]
    src_bucket = record["s3"]["bucket"]["name"]
    src_key = unquote_plus(record["s3"]["object"]["key"])

    LOGGER.info("Processing s3://%s/%s", src_bucket, src_key)

    # ---- Download ----
    obj = s3.get_object(Bucket=src_bucket, Key=src_key)
    df = pd.read_csv(obj["Body"], low_memory=False)
    LOGGER.info("Loaded %d rows x %d cols", len(df), len(df.columns))

    # ---- Profile ----
    profile = profile_dataset(df, source_path=f"s3://{src_bucket}/{src_key}")
    LOGGER.info("Profiled %d columns", profile.column_count)

    # ---- Propose ----
    plan = propose_cleaning_rules(profile)
    LOGGER.info("LLM proposed %d rules", len(plan.rules))

    # ---- Apply ----
    cleaned, audit = apply_plan(df, plan, profile)
    LOGGER.info(
        "Audit: %d applied / %d skipped / %d failed",
        audit.applied_count,
        audit.skipped_count,
        audit.failed_count,
    )

    # ---- Validate ----
    failures = validate_cleaned(
        cleaned,
        df,
        required_columns=REQUIRED_COLUMNS,
        unique_key_column=UNIQUE_KEY,
    )
    if failures:
        LOGGER.error("Validation failures: %s", failures)
        raise RuntimeError(
            f"Pipeline produced invalid output: {[f.detail for f in failures]}"
        )

    # ---- Upload ----
    keys = _output_keys(src_key)

    parquet_buf = BytesIO()
    cleaned.to_parquet(parquet_buf, index=False)
    parquet_buf.seek(0)
    s3.put_object(
        Bucket=cleaned_bucket,
        Key=keys["parquet"],
        Body=parquet_buf.getvalue(),
        ContentType="application/octet-stream",
    )

    s3.put_object(
        Bucket=cleaned_bucket,
        Key=keys["audit"],
        Body=audit.model_dump_json(indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    LOGGER.info(
        "Uploaded cleaned dataset to s3://%s/%s",
        cleaned_bucket,
        keys["parquet"],
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "input": f"s3://{src_bucket}/{src_key}",
                "cleaned": f"s3://{cleaned_bucket}/{keys['parquet']}",
                "audit": f"s3://{cleaned_bucket}/{keys['audit']}",
                "rules_applied": audit.applied_count,
                "rules_skipped": audit.skipped_count,
                "rules_failed": audit.failed_count,
                "rows_in": audit.input_rows,
                "rows_out": audit.output_rows,
            }
        ),
    }
