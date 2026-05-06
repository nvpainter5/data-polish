"""Download a sample of NYC 311 Service Requests from NYC Open Data.

The script is here (rather than a one-off curl command) so the dataset is
*reproducible* — anyone cloning the repo can re-run this and get a sample
of the same shape and date range.

Source : https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9
API doc: https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9

Usage:
    python scripts/download_311_sample.py
    python scripts/download_311_sample.py --rows 100000
    python scripts/download_311_sample.py --start 2026-03-01 --end 2026-03-31

Note: The Socrata API is rate-limited without an app token but our sample
sizes are well within free-tier limits.
"""

from __future__ import annotations

import argparse
import sys
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "nyc_311_sample.csv"

# Socrata returns at most this many rows per request without an app token.
PAGE_SIZE = 10_000


def download(
    target_rows: int,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    """Paginate through the API until we hit `target_rows` or run dry."""
    chunks: list[pd.DataFrame] = []
    offset = 0

    where_clause: str | None = None
    if start_date and end_date:
        where_clause = (
            f"created_date between '{start_date}T00:00:00'"
            f" and '{end_date}T23:59:59'"
        )

    while sum(len(c) for c in chunks) < target_rows:
        remaining = target_rows - sum(len(c) for c in chunks)
        page_size = min(PAGE_SIZE, remaining)

        params: dict[str, str | int] = {
            "$limit": page_size,
            "$offset": offset,
            "$order": "created_date DESC",
        }
        if where_clause:
            params["$where"] = where_clause

        print(
            f"  Fetching rows {offset:,}-{offset + page_size:,}...",
            flush=True,
        )

        # Three retries with exponential backoff in case Socrata hiccups.
        for attempt in range(3):
            try:
                resp = requests.get(API_URL, params=params, timeout=60)
                resp.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt == 2:
                    raise
                wait = 2**attempt
                print(
                    f"  Retry in {wait}s after error: {exc}",
                    file=sys.stderr,
                )
                time.sleep(wait)

        chunk = pd.read_csv(StringIO(resp.text), low_memory=False)
        if len(chunk) == 0:
            print("  API returned no more rows. Stopping.", flush=True)
            break

        chunks.append(chunk)
        offset += page_size

    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Target row count (default: 50,000)",
    )
    parser.add_argument(
        "--start",
        default="2026-04-01",
        help="Start date (YYYY-MM-DD). Default: 2026-04-01.",
    )
    parser.add_argument(
        "--end",
        default="2026-04-30",
        help="End date (YYYY-MM-DD). Default: 2026-04-30.",
    )
    parser.add_argument(
        "--no-date-filter",
        action="store_true",
        help="Ignore --start/--end and just take the latest rows.",
    )
    args = parser.parse_args()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if args.no_date_filter:
        print(f"Downloading latest {args.rows:,} NYC 311 records...")
        df = download(args.rows, None, None)
    else:
        print(
            f"Downloading up to {args.rows:,} NYC 311 records "
            f"from {args.start} to {args.end}..."
        )
        df = download(args.rows, args.start, args.end)

    if df.empty:
        sys.exit(
            "ERROR: no data returned. Try widening the date range or use "
            "--no-date-filter to grab the latest available rows."
        )

    df.to_csv(OUTPUT_PATH, index=False)

    size_mb = OUTPUT_PATH.stat().st_size / 1e6
    print()
    print(f"Saved {len(df):,} rows x {len(df.columns)} cols to:")
    print(f"  {OUTPUT_PATH}")
    print(f"  ({size_mb:.1f} MB)")
    print()
    print("First 8 columns:", list(df.columns)[:8])
    print()

    # Quick sanity peek so we can spot the messiness immediately.
    if "complaint_type" in df.columns:
        top = df["complaint_type"].value_counts().head(5)
        print("Top 5 complaint_type values:")
        for name, count in top.items():
            print(f"  {count:>6,}  {name}")


if __name__ == "__main__":
    main()
