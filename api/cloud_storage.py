"""Helpers for reading data files from cloud object stores.

One function per provider, each taking provider-specific creds and
returning raw bytes. The API endpoint then writes those bytes through
the existing StorageBackend, so the rest of the pipeline doesn't care
whether the source was local upload, S3, GCS, or Azure.

Providers supported:
  - AWS S3   — boto3
  - GCS      — google-cloud-storage
  - Azure    — azure-storage-blob

All three honor the same size cap (MAX_S3_OBJECT_BYTES). Credentials
are never persisted — they live only in the request body.
"""

from __future__ import annotations

import json

import boto3
from botocore.exceptions import BotoCoreError, ClientError


# Hard cap on object size we'll pull. Prevents a malicious key from
# trying to spool a multi-GB object into the server's memory. Surfaced
# to users as MAX_S3_OBJECT_MB in ui/pages/1_Upload.py — keep in sync.
MAX_S3_OBJECT_BYTES = 500 * 1024 * 1024  # 500 MB


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


# --------------------------------------------------------------------------- #
# Google Cloud Storage
# --------------------------------------------------------------------------- #


def download_csv_from_gcs(
    bucket: str,
    blob_name: str,
    *,
    service_account_json: str | None = None,
) -> bytes:
    """Download an object from GCS.

    `service_account_json` is the full JSON keyfile content pasted as a
    string. If omitted, falls back to application default credentials
    (useful when the API server runs on GCP with attached service account).
    """
    # Imports are lazy so deployments without these connectors don't pay
    # the cold-start cost on every boot.
    from google.cloud import storage
    from google.oauth2 import service_account

    try:
        if service_account_json:
            try:
                creds_info = json.loads(service_account_json)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    "Service account JSON is malformed."
                ) from exc
            credentials = service_account.Credentials.from_service_account_info(
                creds_info
            )
            client = storage.Client(
                credentials=credentials,
                project=creds_info.get("project_id"),
            )
        else:
            client = storage.Client()

        blob = client.bucket(bucket).blob(blob_name)
        if not blob.exists():
            raise RuntimeError(f"Object not found: gs://{bucket}/{blob_name}")

        blob.reload()
        if blob.size and blob.size > MAX_S3_OBJECT_BYTES:
            raise RuntimeError(
                f"Object too large: {blob.size:,} bytes "
                f"(cap {MAX_S3_OBJECT_BYTES:,})."
            )

        return blob.download_as_bytes()
    except RuntimeError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"GCS read failed: {exc}") from exc


# --------------------------------------------------------------------------- #
# Azure Blob Storage
# --------------------------------------------------------------------------- #


def download_csv_from_azure(
    account_name: str,
    container: str,
    blob_name: str,
    *,
    connection_string: str | None = None,
    account_key: str | None = None,
    sas_token: str | None = None,
) -> bytes:
    """Download an object from Azure Blob Storage.

    Auth precedence: connection_string > account_key > sas_token. At
    least one must be provided.
    """
    from azure.storage.blob import BlobServiceClient

    try:
        if connection_string:
            client = BlobServiceClient.from_connection_string(connection_string)
        elif account_key:
            url = f"https://{account_name}.blob.core.windows.net"
            client = BlobServiceClient(account_url=url, credential=account_key)
        elif sas_token:
            url = f"https://{account_name}.blob.core.windows.net"
            client = BlobServiceClient(account_url=url, credential=sas_token)
        else:
            raise RuntimeError(
                "Provide a connection string, account key, or SAS token."
            )

        blob_client = client.get_blob_client(
            container=container, blob=blob_name
        )
        props = blob_client.get_blob_properties()

        if props.size > MAX_S3_OBJECT_BYTES:
            raise RuntimeError(
                f"Object too large: {props.size:,} bytes "
                f"(cap {MAX_S3_OBJECT_BYTES:,})."
            )

        download = blob_client.download_blob()
        return download.readall()
    except RuntimeError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Azure read failed: {exc}") from exc
