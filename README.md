# DataPolish

> An AI-augmented data engineering pipeline that ingests messy real-world tabular data, profiles it deterministically, lets an LLM propose cleaning rules in a strict typed schema, applies them through deterministic safety gates, and ships a validated clean dataset plus a full audit trail.

Status: **Phase 1 + Phase 2 complete.** Phase 3 (Streamlit demo + AWS Lambda + S3 deployment) up next.

## Why it's built this way

Three design pillars set this apart from a typical "stuff data into ChatGPT" demo:

1. **Profile-first prompting.** The LLM never sees raw rows. It sees a small, structured profile (column dtypes, null %, casing patterns, top values, etc.) — about 5 KB of signal in place of 50 MB of noise. Faster, cheaper, sharper results.
2. **Structured outputs as a contract.** The LLM's reply is parsed into a typed `CleaningPlan` (pydantic). If the model hallucinates an operation we don't recognize, validation fails loud — we never apply rules we don't understand.
3. **The LLM proposes; deterministic code disposes.** Every proposed rule passes through a confidence gate (only `high` confidence rules auto-apply) AND a per-operation safety gate (which re-checks the actual column profile to confirm the rule's preconditions hold). Catches LLM false positives even when the prompt failed to.

## What the pipeline does

```
data/raw/nyc_311_sample.csv
        |
        v   scripts/profile_dataset.py
DatasetProfile  --->  reports/profile_<timestamp>.json   (canonical archive)
        |
        v   scripts/propose_cleaning.py  (slim view -> Groq Llama 3.3 70B)
CleaningPlan   --->  reports/cleaning_plan_<timestamp>.json
        |
        v   scripts/apply_cleaning.py  (confidence gate + safety gate per rule)
cleaned DataFrame, audit log
        |
        v
data/cleaned/nyc_311_cleaned.parquet
reports/cleaning_audit_<timestamp>.json
```

## Setup

Requires Python 3.11+ and a free [Groq API key](https://console.groq.com).

```bash
# Create venv and install
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env and paste your real GROQ_API_KEY
```

## End-to-end run

Four commands, in order:

```bash
# 1. Confirm the LLM connection works
python scripts/smoke_test_groq.py

# 2. Download a reproducible NYC 311 sample (~50k rows from a pinned date range)
python scripts/download_311_sample.py

# 3. Profile the raw dataset (no AI; pure-Python deterministic scan)
python scripts/profile_dataset.py

# 4. Ask the LLM to propose cleaning rules from the profile
python scripts/propose_cleaning.py

# 5. Apply the plan with safety gates; write parquet + audit
python scripts/apply_cleaning.py
```

Each step writes its output into `reports/` (JSON artifacts) or `data/cleaned/` (parquet) and prints a human-readable summary to the terminal.

## Project layout

```
DataPolish/
├── src/datapolish/
│   ├── config.py             # env loading, fail-loud on missing keys
│   ├── llm_client.py         # provider-agnostic LLM wrapper (Groq today)
│   ├── profile.py            # deterministic profiler + slim view for LLM prompts
│   ├── cleaning.py           # LLM-driven cleaning rule proposer
│   └── apply.py              # apply step with confidence + per-op safety gates
├── scripts/                  # runnable CLIs that wire library modules together
├── tests/                    # 26 unit tests covering profile, prompts, gates, apply
├── data/
│   ├── raw/                  # downloaded source (gitignored)
│   └── cleaned/              # pipeline output parquet (gitignored)
├── reports/                  # JSON artifacts: profiles, plans, audits
└── docs/
    └── prompt_iterations.md  # working log of system-prompt changes and effects
```

## Sample run output (truncated)

```
Loading raw data: data/raw/nyc_311_sample.csv
  50,000 rows x 44 columns
Loading profile: reports/profile_20260503_183817.json
Loading plan:    reports/cleaning_plan_20260504_174432.json
  18 proposed rules

Applying plan with safety gates...

======================================================================
AUDIT: 10 applied / 8 skipped / 0 failed
======================================================================

APPLIED:
  [set_case] complaint_type             -> 10,884 rows changed
  [set_case] descriptor                 -> 14,657 rows changed
  [set_case] descriptor_2               -> 13,210 rows changed
  [set_case] location_type              -> 10,044 rows changed
  [collapse_internal_whitespace] incident_address     -> 3,113 rows changed
  [collapse_internal_whitespace] resolution_description -> 10,198 rows changed
  ...

SKIPPED:
  [collapse_internal_whitespace] intersection_street_1  (high)
      reason: no double-spaces detected in profile
  [set_case] borough  (medium)
      reason: confidence=medium; only `high` rules auto-apply
  ...

Validating cleaned dataframe...
  All sanity checks passed.

Wrote cleaned dataset: data/cleaned/nyc_311_cleaned.parquet  (5.0 MB)
Wrote audit:           reports/cleaning_audit_20260505_162032.json
```

The `intersection_street_1` skip is the safety gate earning its keep — the LLM proposed the rule at high confidence, but a deterministic re-check of the column profile found no double-spaces and refused to run.

## Phase 2: the autonomous agent

Phase 2 turns the static Phase 1 pipeline into a tool-using agent. The LLM is given typed tools (`get_dataset_overview`, `get_column_profile`, `apply_rule`, `compare_before_after`, `finish`) and runs an iterative loop where it decides what to do next based on what it observes.

Same safety gates as Phase 1 — when the agent calls `apply_rule`, the gates re-validate the rule's preconditions exactly as before. Phase 1's defensive infrastructure becomes Phase 2's tooling without modification.

Run with:

```bash
python scripts/run_agent.py
```

Architectural takeaway: a thin tool overview (just dtypes and null counts) led the agent to under-explore — it stopped after 3 fixes. Enriching `get_dataset_overview` with pre-computed `issue_summary` hints (which columns have mixed casing, double spaces, denormalization candidates) drove the agent to thorough coverage on the next run. **Hint-rich tools beat agentic discovery.**

## Tests

36 unit tests, all running in under a second. No network or LLM calls in the test suite.

```bash
pytest -v
```

Coverage:
- profiler (numeric / string / datetime stats, casing/whitespace detection, cardinality cutoffs)
- cleaning schema validation (rejects unknown operations, missing fields, bad confidence values)
- slim payload construction (filters high-null columns, compact JSON)
- apply step (confidence gate, per-operation safety gates, applied row counts)
- post-apply validation (row count preservation, required columns, unique key uniqueness)
- agent tools (overview with issue hints, column profile, apply via gates, mark_for_review, finish)

## Provider abstraction

All LLM calls go through `LLMClient` in `src/datapolish/llm_client.py`. Today it wraps Groq. The class accepts `provider` and `model` arguments so swapping to Anthropic or OpenAI for the polished demo run, or to a local Ollama for offline operation, is a one-file change.

## Roadmap

- [x] Phase 0 — Scaffolding
- [x] Phase 1 — Deterministic profiler + LLM-as-tool cleaning + safety-gated apply + validation
- [x] Phase 2 — Tool-using autonomous agent that audits and repairs a fresh dataset
- [ ] Phase 3a — Streamlit demo + write-up
- [ ] Phase 3b — Deploy as AWS Lambda + S3 pipeline

## Source data

NYC 311 Service Requests, via [NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9). The sample is pulled fresh by `scripts/download_311_sample.py` against the public Socrata API; date range and row count are pinned in the script for reproducibility.
