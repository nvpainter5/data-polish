"""Ask the LLM to propose cleaning rules from the latest dataset profile.

Reads:  reports/profile_<latest>.json  (or --profile path)
Writes: reports/cleaning_plan_<timestamp>.json

Prints a human-readable summary grouped by confidence so we can eyeball
the plan before deciding whether to apply it.

Usage:
    python scripts/propose_cleaning.py
    python scripts/propose_cleaning.py --profile reports/profile_20260503_183817.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from datapolish.cleaning import (  # noqa: E402
    propose_cleaning_rules,
    save_plan,
)
from datapolish.profile import DatasetProfile  # noqa: E402


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
    parser.add_argument(
        "--profile",
        type=Path,
        default=None,
        help="Path to a profile JSON. Defaults to the most recent one.",
    )
    args = parser.parse_args()

    profile_path = args.profile or latest_profile_path()
    print(f"Loading profile: {profile_path}")

    profile_data = json.loads(profile_path.read_text())
    profile = DatasetProfile.model_validate(profile_data)
    print(
        f"Profile: {profile.column_count} columns over "
        f"{profile.row_count:,} rows"
    )
    print()

    print("Calling LLM to propose cleaning rules...")
    print("(System prompt: ~150 lines. User prompt: profile JSON.)")
    print()

    plan = propose_cleaning_rules(profile)

    output_path = (
        PROJECT_ROOT
        / "reports"
        / f"cleaning_plan_{datetime.now():%Y%m%d_%H%M%S}.json"
    )
    save_plan(plan, output_path)

    # ---- Console summary ----
    print("=" * 70)
    print("LLM SUMMARY")
    print("=" * 70)
    print(plan.summary)
    print()
    print("=" * 70)
    print(f"PROPOSED {len(plan.rules)} RULES")
    print("=" * 70)
    print()

    for confidence in ("high", "medium", "low"):
        rules = [r for r in plan.rules if r.confidence == confidence]
        if not rules:
            continue

        print(f"--- {confidence.upper()} CONFIDENCE ({len(rules)}) ---")
        for r in rules:
            params = (
                json.dumps(r.parameters, ensure_ascii=False)
                if r.parameters
                else "{}"
            )
            print(f"  [{r.operation}] {r.column}  {params}")
            print(f"      reasoning: {r.reasoning}")
        print()

    print(f"Saved cleaning plan to: {output_path}")


if __name__ == "__main__":
    main()
