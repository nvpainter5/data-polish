"""Apply the latest CleaningPlan to the raw NYC 311 sample.

Reads:
  - data/raw/nyc_311_sample.csv
  - reports/profile_<latest>.json
  - reports/cleaning_plan_<latest>.json

Writes:
  - data/cleaned/nyc_311_cleaned.parquet
  - reports/cleaning_audit_<timestamp>.json

Usage:
    python scripts/apply_cleaning.py
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from datapolish.apply import (  # noqa: E402
    apply_plan,
    save_audit,
    validate_cleaned,
)
from datapolish.cleaning import CleaningPlan  # noqa: E402
from datapolish.profile import DatasetProfile  # noqa: E402


REQUIRED_COLUMNS = ["unique_key", "created_date", "complaint_type"]
UNIQUE_KEY = "unique_key"

DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "nyc_311_sample.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "cleaned" / "nyc_311_cleaned.parquet"


def latest(pattern: str) -> Path:
    matches = sorted((PROJECT_ROOT / "reports").glob(pattern))
    if not matches:
        sys.exit(
            f"No file matching {pattern!r} in reports/. "
            "Run earlier pipeline steps first."
        )
    return matches[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT,
        help="Raw CSV input.",
    )
    parser.add_argument(
        "--profile", type=Path, default=None,
        help="Profile JSON. Defaults to most recent in reports/.",
    )
    parser.add_argument(
        "--plan", type=Path, default=None,
        help="Cleaning plan JSON. Defaults to most recent in reports/.",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help="Cleaned parquet output.",
    )
    args = parser.parse_args()

    profile_path = args.profile or latest("profile_*.json")
    plan_path = args.plan or latest("cleaning_plan_*.json")

    print(f"Loading raw data: {args.input}")
    df = pd.read_csv(args.input, low_memory=False)
    print(f"  {len(df):,} rows x {len(df.columns)} columns")

    print(f"Loading profile: {profile_path}")
    profile = DatasetProfile.model_validate_json(profile_path.read_text())

    print(f"Loading plan:    {plan_path}")
    plan = CleaningPlan.model_validate_json(plan_path.read_text())
    print(f"  {len(plan.rules)} proposed rules")
    print()

    print("Applying plan with safety gates...")
    cleaned, audit = apply_plan(df, plan, profile)
    print()

    # ---- Console summary ----
    print("=" * 70)
    print(
        f"AUDIT: {audit.applied_count} applied / "
        f"{audit.skipped_count} skipped / "
        f"{audit.failed_count} failed"
    )
    print("=" * 70)
    print()

    if audit.applied_count:
        print("APPLIED:")
        for e in audit.entries:
            if e.status == "applied":
                print(
                    f"  [{e.rule.operation}] {e.rule.column}  "
                    f"-> {e.rows_changed:,} rows changed"
                )
        print()

    if audit.skipped_count:
        print("SKIPPED:")
        for e in audit.entries:
            if e.status == "skipped":
                print(
                    f"  [{e.rule.operation}] {e.rule.column}  "
                    f"({e.rule.confidence})"
                )
                print(f"      reason: {e.reason}")
        print()

    if audit.failed_count:
        print("FAILED:")
        for e in audit.entries:
            if e.status == "failed":
                print(f"  [{e.rule.operation}] {e.rule.column}")
                print(f"      reason: {e.reason}")
        print()

    # ---- Validate ----
    print("Validating cleaned dataframe...")
    failures = validate_cleaned(
        cleaned,
        df,
        required_columns=REQUIRED_COLUMNS,
        unique_key_column=UNIQUE_KEY,
    )
    if failures:
        print("VALIDATION FAILURES:")
        for f in failures:
            print(f"  [{f.check}] {f.detail}")
        sys.exit(1)
    print("  All sanity checks passed.")
    print()

    # ---- Persist ----
    args.output.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_parquet(args.output, index=False)
    size_mb = args.output.stat().st_size / 1e6
    print(f"Wrote cleaned dataset: {args.output}  ({size_mb:.1f} MB)")

    audit_path = (
        PROJECT_ROOT
        / "reports"
        / f"cleaning_audit_{datetime.now():%Y%m%d_%H%M%S}.json"
    )
    save_audit(audit, audit_path)
    print(f"Wrote audit:           {audit_path}")


if __name__ == "__main__":
    main()
