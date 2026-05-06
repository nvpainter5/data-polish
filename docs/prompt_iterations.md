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
