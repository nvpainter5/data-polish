"""Lightweight test for the Lambda handler.

We mock S3 (boto3) and the LLM client so the handler runs end-to-end
without AWS credentials or network access. The point is to confirm the
handler's wiring works — the actual pipeline logic is already tested by
the test suite for profile.py / cleaning.py / apply.py.
"""

from __future__ import annotations

import json
import sys
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

# The lambda/ folder isn't a package; add it to sys.path so we can import
# lambda_function directly here.
LAMBDA_DIR = Path(__file__).resolve().parent.parent / "lambda"
sys.path.insert(0, str(LAMBDA_DIR))


def _fake_s3_event(bucket: str, key: str) -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }


def test_lambda_handler_round_trip(monkeypatch) -> None:
    """Upload a tiny CSV via mocked S3, invoke the handler, verify the
    expected put_object calls came back out."""
    monkeypatch.setenv("CLEANED_BUCKET", "test-cleaned-bucket")
    monkeypatch.setenv("GROQ_API_KEY", "gsk_dummy_for_test")

    import lambda_function as lf

    # ---- Mock S3 ----
    mock_s3 = MagicMock()
    df = pd.DataFrame(
        {
            "unique_key": [1, 2, 3],
            "created_date": ["2026-04-01", "2026-04-02", "2026-04-03"],
            "complaint_type": ["Noise", "HEAT/HOT WATER", "Other"],
        }
    )
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    mock_s3.get_object.return_value = {"Body": BytesIO(csv_bytes)}
    monkeypatch.setattr(lf, "s3", mock_s3)

    # ---- Mock the LLM ----
    fake_plan_dict = {
        "summary": "ok",
        "rules": [
            {
                "column": "complaint_type",
                "operation": "set_case",
                "parameters": {"case": "title"},
                "confidence": "high",
                "reasoning": "mixed casing in test fixture",
            }
        ],
    }
    from datapolish.cleaning import CleaningPlan

    fake_plan = CleaningPlan.model_validate(fake_plan_dict)

    with patch(
        "lambda_function.propose_cleaning_rules",
        return_value=fake_plan,
    ):
        result = lf.lambda_handler(
            _fake_s3_event("test-raw-bucket", "incoming.csv"),
            context=None,
        )

    # ---- Assertions ----
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["input"] == "s3://test-raw-bucket/incoming.csv"
    assert body["cleaned"].endswith("incoming_cleaned.parquet")
    assert body["audit"].endswith("incoming_audit.json")

    # The handler should have made exactly two put_object calls.
    assert mock_s3.put_object.call_count == 2
    keys_uploaded = sorted(
        call.kwargs["Key"] for call in mock_s3.put_object.call_args_list
    )
    assert keys_uploaded == [
        "cleaned/incoming_audit.json",
        "cleaned/incoming_cleaned.parquet",
    ]
