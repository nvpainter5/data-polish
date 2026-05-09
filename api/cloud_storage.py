"""Helpers for reading CSV input from cloud object stores.

v2.5: S3 first. The interface is intentionally tiny — one function per
provider that takes a bucket/key + creds and returns raw bytes. The API
endpoint then writes those bytes through the existing StorageBackend, so
the rest of the pipeline doesn't know or care that the source lived in
the cloud.

Adding GCS / Azure is a one-function-per-provider job that follows the
same shape.
"""

from __future__ import annotations

import boto3
from botocore.exceptions import BotoCoreError, ClientError


# Hard cap on object size we'll pull. Prevents a malicious key from
# trying to spool a 100 GB object into Lambda memory.
MAX_S3_OBJECT_BYTES = 200 * 1024 * 1024  # 200 MB


def download_csv_from_s3(
    bucket: str,
    key: str,
    *,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    region: str | None = None,
) -> bytes:
    """Download a single S3 object with user-supplied credentials.

    Returns the raw bytes for the caller to persist. Raises
    `RuntimeError` with a clean message on any failure (auth, missing
    object, network) — the API layer turns these into 4xx responses.

    No extension validation here — CSV data routinely lives in .txt or
    .tsv files. If the bytes aren't CSV-shaped, pandas will raise a
    clear ParserError downstream.
    """
    session_kwargs: dict[str, str] = {}
    if access_key_id and secret_access_key:
        session_kwargs["aws_access_key_id"] = access_key_id
        session_kwargs["aws_secret_access_key"] = secret_access_key
    if region:
        session_kwargs["region_name"] = region

    s3 = boto3.client("s3", **session_kwargs)

    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        raise RuntimeError(
            f"Couldn't read s3://{bucket}/{key}: {exc.response['Error']['Message']}"
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"S3 connection failed: {exc}") from exc

    size = int(head.get("ContentLength", 0))
    if size > MAX_S3_OBJECT_BYTES:
        raise RuntimeError(
            f"Object too large: {size:,} bytes (cap {MAX_S3_OBJECT_BYTES:,})."
        )

    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()
