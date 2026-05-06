"""Profile our NYC 311 sample and write a JSON report.

Reads:  data/raw/nyc_311_sample.csv
Writes: reports/profile_<timestamp>.json

Also prints a human-readable summary to stdout — high-null columns,
low-cardinality columns with their top values, and string columns
with interesting casing patterns.

Usage:
    python scripts/profile_dataset.py
    python scripts/profile_dataset.py --input data/raw/some_other.csv
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from datapolish.profile import (  # noqa: E402
    DatasetProfile,
    profile_dataset,
    save_profile,
)


DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "nyc_311_sample.csv"


def print_summary(profile: DatasetProfile) -> None:
    """Print human-readable highlights of the profile to stdout."""
    print()
    print("=" * 70)
    print(f"Dataset: {profile.row_count:,} rows x {profile.column_count} columns")
    print(f"Source : {profile.source_path}")
    print(f"At     : {profile.profiled_at}")
    print("=" * 70)

    # ---- High-null columns ----
    high_null = sorted(
        (c for c in profile.columns if c.null_pct > 0),
        key=lambda c: -c.null_pct,
    )[:15]
    if high_null:
        print()
        print("Columns with the most nulls:")
        for c in high_null:
            print(f"  {c.null_pct:>6.2f}%   {c.name}  ({c.dtype})")

    # ---- Low-cardinality columns with top values ----
    cat_cols = [c for c in profile.columns if c.top_values]
    if cat_cols:
        print()
        print(f"Categorical columns ({len(cat_cols)} total) — top values:")
        for c in cat_cols[:10]:
            print(f"\n  {c.name}  [{c.unique_count} unique]")
            for tv in c.top_values[:5]:
                print(f"    {tv.count:>6,}  {tv.value}")

    # ---- String columns with casing or whitespace anomalies ----
    suspicious = []
    for c in profile.columns:
        if not c.string_stats:
            continue
        ss = c.string_stats
        # If the column has BOTH all-upper and title-case variants,
        # something's inconsistent.
        if ss.count_all_upper > 0 and ss.count_title_case > 0:
            suspicious.append((c, "mixed casing"))
        elif ss.has_leading_whitespace or ss.has_trailing_whitespace:
            suspicious.append((c, "whitespace issues"))
        elif ss.has_double_spaces:
            suspicious.append((c, "double spaces"))

    if suspicious:
        print()
        print("String columns with quality smells:")
        for c, reason in suspicious[:15]:
            print(f"  {c.name}  -- {reason}")
            ss = c.string_stats
            print(
                f"    upper={ss.count_all_upper:,}  "
                f"title={ss.count_title_case:,}  "
                f"lead_ws={ss.has_leading_whitespace:,}  "
                f"trail_ws={ss.has_trailing_whitespace:,}  "
                f"dbl_space={ss.has_double_spaces:,}"
            )

    # ---- Datetime parse failures ----
    dt_issues = [
        c for c in profile.columns
        if c.datetime_stats and c.datetime_stats.parse_failures > 0
    ]
    if dt_issues:
        print()
        print("Datetime columns with parse failures:")
        for c in dt_issues:
            print(
                f"  {c.name}  -- "
                f"{c.datetime_stats.parse_failures:,} unparseable"
            )

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"CSV to profile (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON path (default: reports/profile_<timestamp>.json)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        sys.exit(f"ERROR: input file not found: {args.input}")

    print(f"Loading {args.input}...")
    df = pd.read_csv(args.input, low_memory=False)
    print(f"Loaded {len(df):,} rows x {len(df.columns)} columns")

    print("Profiling...")
    profile = profile_dataset(df, source_path=str(args.input))

    output_path = args.output or (
        PROJECT_ROOT
        / "reports"
        / f"profile_{datetime.now():%Y%m%d_%H%M%S}.json"
    )
    save_profile(profile, output_path)
    print(f"Wrote profile to {output_path}")

    print_summary(profile)


if __name__ == "__main__":
    main()
