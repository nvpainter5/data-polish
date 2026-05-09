"""Glue between API jobs and the `datapolish` pipeline.

Takes a job_id, reads the uploaded data through the storage backend
(format-detected — CSV, TSV, pipe-delimited, JSON), runs profile ->
propose -> apply -> validate, writes profile.json, plan.json, audit.json,
and cleaned.parquet back through the storage backend.

Synchronous for v2.0. v2.1+ can wrap this in a queue/worker without
changing callers.
"""

from __future__ import annotations

import json
import re
from io import BytesIO
from pathlib import Path
import sys

import pandas as pd

# Make `from datapolish ...` work when api/ is the entrypoint.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from datapolish.apply import apply_plan, validate_cleaned  # noqa: E402
from datapolish.cleaning import propose_cleaning_rules  # noqa: E402
from datapolish.profile import profile_dataset  # noqa: E402
from datapolish.quality import (  # noqa: E402
    compute_quality_score,
    generate_suggestions,
)

from .storage import StorageBackend  # noqa: E402


def detect_delimiter(raw_bytes: bytes) -> str:
    """Pick the most likely column separator by counting occurrences in the
    first non-empty line of the file.

    Why not pandas' `sep=None` sniffer: it's based on `csv.Sniffer`, which
    is unreliable for pipe-delimited files in particular. Counting raw
    delimiter occurrences in the header line is dumber but more correct
    on real-world data.

    Returns ',' as the safe fallback if nothing wins.
    """
    sample = raw_bytes[:16384]
    try:
        text = sample.decode("utf-8")
    except UnicodeDecodeError:
        text = sample.decode("utf-8", errors="replace")

    # Find the first non-empty line — usually the header.
    first_line = ""
    for line in text.splitlines():
        if line.strip():
            first_line = line
            break

    if not first_line:
        return ","

    candidates = ["|", "\t", ",", ";", ":"]
    counts = {delim: first_line.count(delim) for delim in candidates}
    best = max(counts, key=counts.get)
    return best if counts[best] >= 1 else ","


def _smart_read_dataframe(
    raw_bytes: bytes, source_hint: str = ""
) -> pd.DataFrame:
    """Best-effort tabular parse of arbitrary uploaded bytes.

    Strategy:
      1. If the source hints at JSON (`.json` extension or content
         starts with `[` / `{`), use pandas.read_json.
      2. If the source hints at parquet, use pandas.read_parquet.
      3. Otherwise detect the delimiter manually (count occurrences in
         the header line) and read as CSV with that separator.

    Raises ValueError with a clear message if nothing parses cleanly.
    """
    head = raw_bytes.lstrip()[:1]
    is_json_extension = bool(re.search(r"\.json($|\?|#)", source_hint, re.I))
    is_parquet_extension = bool(
        re.search(r"\.parquet($|\?|#)", source_hint, re.I)
    )
    looks_like_json = head in (b"{", b"[")

    if is_json_extension or looks_like_json:
        try:
            return pd.read_json(BytesIO(raw_bytes))
        except ValueError:
            # JSON Lines (one object per line) is also common.
            return pd.read_json(BytesIO(raw_bytes), lines=True)

    if is_parquet_extension:
        return pd.read_parquet(BytesIO(raw_bytes))

    delimiter = detect_delimiter(raw_bytes)
    return pd.read_csv(
        BytesIO(raw_bytes), sep=delimiter, low_memory=False
    )


def run_pipeline(
    job_id: str,
    storage: StorageBackend,
    *,
    custom_instructions: str | None = None,
    delimiter: str | None = None,
) -> dict:
    """Run the pipeline for one job. Returns a small summary dict suitable
    for storing on the Job and surfacing to the UI.

    `delimiter` is the user-confirmed column separator from the preview
    step. If omitted, falls back to content sniffing (handles JSON,
    parquet, and auto-delimiter).
    """

    raw_bytes = storage.read_bytes(job_id, "raw.csv")
    if delimiter:
        df = pd.read_csv(
            BytesIO(raw_bytes), sep=delimiter, low_memory=False
        )
    else:
        df = _smart_read_dataframe(raw_bytes)

    profile = profile_dataset(
        df, source_path=storage.path(job_id, "raw.csv")
    )
    storage.write_bytes(
        job_id, "profile.json", profile.model_dump_json(indent=2).encode()
    )

    plan = propose_cleaning_rules(
        profile, custom_instructions=custom_instructions
    )
    storage.write_bytes(
        job_id, "plan.json", plan.model_dump_json(indent=2).encode()
    )

    cleaned, audit = apply_plan(df, plan, profile)

    failures = validate_cleaned(
        cleaned,
        df,
        required_columns=[],
        unique_key_column=None,
    )
    if failures:
        raise RuntimeError(
            "Validation failed: "
            + "; ".join(f.detail for f in failures)
        )

    storage.write_bytes(
        job_id, "audit.json", audit.model_dump_json(indent=2).encode()
    )

    parquet_buf = BytesIO()
    cleaned.to_parquet(parquet_buf, index=False)
    storage.write_bytes(job_id, "cleaned.parquet", parquet_buf.getvalue())

    # ---- Quality score (before + after) -----------------------------------
    quality_before = compute_quality_score(profile)
    cleaned_profile = profile_dataset(
        cleaned, source_path=storage.path(job_id, "cleaned.parquet")
    )
    quality_after = compute_quality_score(cleaned_profile)

    storage.write_bytes(
        job_id,
        "quality.json",
        json.dumps(
            {
                "before": quality_before.model_dump(),
                "after": quality_after.model_dump(),
                "delta": quality_after.score - quality_before.score,
            },
            indent=2,
        ).encode(),
    )

    # ---- Suggestions (LLM, best-effort — won't fail the run) --------------
    suggestions = generate_suggestions(audit, quality_after)
    storage.write_bytes(
        job_id,
        "suggestions.json",
        suggestions.model_dump_json(indent=2).encode(),
    )

    return {
        "rules_proposed": len(plan.rules),
        "rules_applied": audit.applied_count,
        "rules_skipped": audit.skipped_count,
        "rules_failed": audit.failed_count,
        "rows_in": audit.input_rows,
        "rows_out": audit.output_rows,
        "columns": profile.column_count,
        "quality_before": quality_before.score,
        "quality_after": quality_after.score,
        "quality_delta": quality_after.score - quality_before.score,
        "suggestion_count": len(suggestions.suggestions),
    }
