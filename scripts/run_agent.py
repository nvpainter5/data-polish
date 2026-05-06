"""Run the autonomous data quality agent on the latest profile + raw data.

Reads:
  - data/raw/nyc_311_sample.csv
  - reports/profile_<latest>.json

Writes:
  - data/cleaned/nyc_311_agent_cleaned.parquet
  - reports/agent_trace_<timestamp>.json   (full tool-call history)

Usage:
    python scripts/run_agent.py
    python scripts/run_agent.py --max-iterations 30
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

from datapolish.agent import run_agent  # noqa: E402
from datapolish.profile import DatasetProfile  # noqa: E402

DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "nyc_311_sample.csv"
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "data" / "cleaned" / "nyc_311_agent_cleaned.parquet"
)


def latest_profile_path() -> Path:
    profiles = sorted((PROJECT_ROOT / "reports").glob("profile_*.json"))
    if not profiles:
        sys.exit(
            "No profile found in reports/. "
            "Run scripts/profile_dataset.py first."
        )
    return profiles[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--profile", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--max-iterations", type=int, default=25)
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress per-iteration logs"
    )
    args = parser.parse_args()

    profile_path = args.profile or latest_profile_path()

    print(f"Loading profile: {profile_path}")
    profile = DatasetProfile.model_validate_json(profile_path.read_text())

    print(f"Loading raw data: {args.input}")
    df = pd.read_csv(args.input, low_memory=False)
    print(f"  {len(df):,} rows x {len(df.columns)} columns")

    print()
    print("Starting agent loop...")
    cleaned, state, trace = run_agent(
        df,
        profile,
        max_iterations=args.max_iterations,
        verbose=not args.quiet,
    )

    print()
    print("=" * 70)
    print(
        f"AGENT FINISHED — {trace.iterations} iterations, "
        f"{len(trace.tool_calls)} tool calls"
    )
    print("=" * 70)
    print()
    print("FINAL SUMMARY (from agent):")
    print(state.final_summary or "(agent did not call finish)")
    print()

    applied = [e for e in state.audit_entries if e.status == "applied"]
    skipped = [e for e in state.audit_entries if e.status == "skipped"]
    failed = [e for e in state.audit_entries if e.status == "failed"]

    print(
        f"Audit: {len(applied)} applied / {len(skipped)} skipped / "
        f"{len(failed)} failed"
    )
    print()

    for e in applied:
        print(
            f"  [APPLIED] {e.rule.operation:30s} {e.rule.column:30s} "
            f"-> {e.rows_changed:,} rows changed"
        )
    for e in skipped:
        print(f"  [SKIPPED] {e.rule.operation:30s} {e.rule.column:30s}")
        print(f"            reason: {e.reason}")
    for e in failed:
        print(f"  [FAILED ] {e.rule.operation:30s} {e.rule.column:30s}")
        print(f"            reason: {e.reason}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_parquet(args.output, index=False)
    size_mb = args.output.stat().st_size / 1e6
    print(f"\nWrote cleaned dataset: {args.output} ({size_mb:.1f} MB)")

    trace_path = (
        PROJECT_ROOT
        / "reports"
        / f"agent_trace_{datetime.now():%Y%m%d_%H%M%S}.json"
    )
    trace_path.write_text(
        json.dumps(
            {
                "iterations": trace.iterations,
                "tool_calls": trace.tool_calls,
                "final_summary": trace.final_summary,
            },
            indent=2,
            default=str,
        )
    )
    print(f"Wrote agent trace:     {trace_path}")


if __name__ == "__main__":
    main()
