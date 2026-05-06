# Prompt Iteration Log

A running log of every meaningful change to `SYSTEM_PROMPT` in
`src/datapolish/cleaning.py`. One entry per round. Keep it terse — this is a
working log, not documentation.

The log doubles as: (a) a debugging aid when something regresses, and
(b) a portfolio artifact showing concrete evidence of the prompt-engineering
loop.

## Entry template

```
## vN → vN+1  (YYYY-MM-DD)

**Problem:** what was wrong in vN's output
**Fix:**     what was changed in the SYSTEM_PROMPT (be specific — heuristic added,
             example added, rule tightened, etc.)
**Result:**  rule-count delta and the specific behaviors that changed
**Remaining:** honest list of what is still imperfect after this round
**Decision:** keep iterating / stop and add deterministic safety gates / etc.
```

## Failure categories and the prompt fix that usually addresses each

| Failure category | Typical fix |
|---|---|
| False positive (rule proposed that shouldn't be) | Add a NEGATIVE EXAMPLE matching the failing case |
| False negative (rule missed that should have been there) | Add a POSITIVE EXAMPLE plus a "be exhaustive" reminder |
| Wrong reasoning (justification contradicts the data) | Tighten the conditions inside the relevant heuristic |
| Inconsistent application (rule applied to A, skipped on B) | Add an explicit "rules are independent" / "check both X and Y" reminder |

When you've stopped seeing meaningful gains, **stop iterating** and shift
defensive logic into the deterministic apply step instead. Prompts plateau
around 80–85%; the last 15% is what code is for.

---

## v1 → v2  (2026-05-04)

**Problem:**
- False positive on `agency` — model proposed `set_case=title` even though
  the column had `count_all_upper=50000, count_title_case=0` (no actual
  mixed casing, deliberately uppercase agency abbreviations: NYPD, HPD, DOT).
  Applying this rule would have corrupted the data.
- Missed coverage on `descriptor_2`, `location_type`, and `street_name` —
  same patterns as columns the model did flag, but skipped.
- `descriptor` got a casing rule but not the matching whitespace rule even
  though `has_double_spaces > 0` was present.

**Fix:**
- Broke heuristic 3 (casing) into sub-rules 3a–3e with explicit conditions.
- Added a POSITIVE EXAMPLE (3d): `complaint_type` — clear mixed-casing case
  where set_case is correct.
- Added a NEGATIVE EXAMPLE (3e): `agency` (NYPD/HPD/DOT) — column where
  set_case must NOT be proposed.
- Added new rule 5: "RULES ARE INDEPENDENT — a column can have BOTH a casing
  issue AND a whitespace issue."
- Added new rule 6: "BE EXHAUSTIVE — propose a rule for each affected column,
  not just a representative sample."
- Added `borough` vs `park_borough` as a second denormalization example
  alongside `agency` vs `agency_name`.

**Result:**
- Rule count 10 → 18.
- False positive on `agency` ELIMINATED — the negative example worked.
- Coverage gained: `descriptor_2`, `location_type`, and `street_name`
  now correctly flagged.
- `descriptor` now gets BOTH `set_case` AND `collapse_internal_whitespace`.
- Confidence calibration appropriate: mechanical fixes high, ambiguous
  category-vs-text columns medium.

**Remaining:**
- `borough` vs `park_borough` denormalization still NOT caught despite the
  example added. Both got their own `set_case` rule rather than one being
  flagged as a duplicate. Some prompt edits don't take.
- `agency_name` flag survived but with weaker reasoning ("high unique count"
  — factually wrong; it has 14 unique values). The flag itself is still
  useful since the operation is `mark_for_review`, but the justification
  is hallucinated.
- Six "MEDIUM CONFIDENCE" set_case rules were added on mostly-uppercase
  columns (`incident_address`, `street_name`, `city`, `borough`,
  `community_board`, `park_borough`) — debatable whether all of these are
  desirable. Let downstream apply step gate them.

**Decision:**
Stop iterating. We're approaching diminishing returns: gains have shrunk,
some changes don't take, and further prompt-stuffing risks overfitting to
this specific dataset. Move remaining defensive logic into the
deterministic apply step (Task #7), where we re-validate each proposed
rule against the actual column profile before executing it.

---

## v3 — Phase 2 agent (2026-05-05)

This entry covers a different system prompt than v1/v2 — the agent prompt
in `src/datapolish/agent.py` (`AGENT_SYSTEM_PROMPT`), not the static-plan
prompt in `cleaning.py`. Logged here because the lesson is the same shape:
identify the failure, change one thing, re-run.

**Problem:**
First run was conservative. The agent inspected only 3 columns
(`complaint_type`, `incident_address`, `location_type`), applied 3 rules,
and called `finish` — missing 6+ columns Phase 1 had caught (`descriptor`,
`descriptor_2`, `street_name`, `cross_street_*`, `resolution_description`,
the `borough`/`park_borough` denormalization).

Root cause: the `get_dataset_overview` tool returned only name, dtype,
null_pct, unique_count for each column. The agent had no signal about
*which* columns had casing or whitespace issues. It picked the obvious-
by-name candidates and stopped.

**Fix:**
Two changes, made together:
1. Enriched `get_dataset_overview` to compute and return an `issue_summary`
   block: which columns have mixed_casing, double_spaces, whitespace_padding,
   plus pairs of columns with identical distributions (denormalization
   candidates). The hints are pre-computed deterministically from the
   profile — same logic the safety gate uses, so the agent and the gate
   agree on what's flagged.
2. Rewrote `AGENT_SYSTEM_PROMPT` to use `issue_summary` as the workflow's
   spine: "Apply set_case to EVERY column listed under mixed_casing,"
   "Apply collapse_internal_whitespace to EVERY column listed under
   double_spaces," etc.

**Result:**
- Iterations 11 → 4 (parallel tool calls became possible).
- Tool calls 11 → 33 (more thorough coverage in fewer iterations).
- Rules applied 3 → 18.
- `borough`/`park_borough` denormalization caught (Phase 1 had missed
  this twice through prompt iteration).
- Zero false positives. All safety gates accepted.

**Remaining:**
- Agent applied set_case more aggressively than Phase 1 — `BROOKLYN` →
  `Brooklyn`, addresses title-cased. Technically the safety gate accepted
  these (a small number of title-case rows did exist in those columns),
  but it's a more aggressive normalization than Phase 1's. Defensible
  either way; product decision.

**Decision:**
Stop iterating. The lesson generalizes: **hint-rich tools beat agentic
discovery.** A thin tool overview leads to under-exploration; a tool that
pre-computes the analysis and surfaces hints lets the LLM focus on
deciding what to do with the hints. This is the production pattern for
LLM-driven workflows.
