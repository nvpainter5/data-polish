# DataPolish — Project Notes

> Read this first at the start of any new Cowork chat to restore context.

## What this project is
DataPolish is a portfolio project Nirav is building during a job search. He's an experienced data engineer (SQL, pipelines) but new to AI/LLMs. The goal is a single repo that demonstrates AI-augmented data engineering: an LLM-powered pipeline that cleans messy real-world data, plus a tool-using agent on top, plus AWS deployment.

## Working environment
- **Project folder:** `/Users/nvpainter/DataPolish` (mounted in Cowork)
- **Platform:** macOS, Apple Silicon, **8GB RAM** (so we avoid running large local models)
- **Python:** 3.11+ in a `.venv` virtualenv at the project root

## LLM setup
- **Active provider:** Groq (free tier, no credit card)
- **Active model:** `llama-3.3-70b-versatile` — supports tool calling, very fast
- **API key:** lives in `.env` (gitignored) as `GROQ_API_KEY`. Rotate before any public release.
- **Future providers:** Ollama (local fallback, Task #12) and Anthropic API (for the polished Phase 3 demo). All go behind `src/datapolish/llm_client.py`.

## Dataset
- **Choice:** NYC 311 Service Requests (NYC Open Data, public)
- Famous, genuinely messy, has both structured columns and free-text — good for showing both rule-based and LLM-based cleaning.
- We'll work with a sample (~100k rows), not the full dataset.

## Phases
1. **Phase 1 — Pipeline + LLM-as-tool.** [COMPLETE] Profile, propose, apply, validate. Six runnable scripts wired together. 26 unit tests.
2. **Phase 2 — Tool-using agent.** [COMPLETE] 5 tools (overview, column profile, apply, compare, finish) wired through the same safety gates as Phase 1. Agent runs in 4 iterations using parallel tool calls, applies 18 rules, catches the borough/park_borough denormalization Phase 1 missed.
3. **Phase 3a — Polish.** Streamlit demo, architecture diagram, README, LinkedIn post.
4. **Phase 3b — AWS.** Deploy as Lambda triggered by S3 upload (free tier covers this easily).

## Where Phase 1 ended (May 2026)
- Pipeline runs end-to-end on real NYC 311 data: 50k rows, 44 columns, ~10 rules applied, 5 MB cleaned parquet.
- Two-layer safety: confidence gate (only `high` auto-applies) + per-operation gate (re-checks profile preconditions before executing). Demonstrated catching an LLM false positive (`intersection_street_1`) that slipped through both the prompt and the confidence gate.
- Prompt iteration log lives at `docs/prompt_iterations.md` with the v1→v2 entry. Useful both as a debugging aid and a portfolio artifact.
- Tests: 26 passing, < 1 second total runtime, no network or API calls in the suite.

## Where Phase 2 ended (May 2026)
- Agent module in `src/datapolish/agent.py` with 5 tools, system prompt, and loop. Runnable via `scripts/run_agent.py`.
- LLM client gained `chat_with_tools()` returning a `ChatResponse` with optional `tool_calls`. Provider-agnostic interface preserved.
- Same Phase 1 safety gates protect Phase 2's apply path — agent's `apply_rule` tool calls into `GATES` and `APPLIERS` from `apply.py`.
- First run was conservative (only 3 columns fixed) because the overview was too thin. Second run with `issue_summary` hints in the overview drove thorough coverage: 18 rules applied, 1 mark_for_review (borough/park_borough denormalization Phase 1 had missed). 4 iterations using parallel tool calls.
- Tests: 36 passing.

## Division of labor
**Claude (Cowork) handles:**
- Writing all Python code, file scaffolding, prompt design.
- Architecture decisions, explaining what each piece does.
- Debugging from errors Nirav pastes in.

**Nirav handles:**
- Running scripts in the terminal, pasting output back.
- Account setup (Groq, eventually AWS).
- Reviewing code as it's written — don't let Claude move past anything unclear.

## Open questions to revisit
- AWS account status (only matters at Phase 3b).
- Whether to swap to Anthropic API for the Phase 3 demo recording (snappier).
- Exact NYC 311 sample window and how to slice it.

## Suggested kickoff message for any new chat
> "Read PROJECT_NOTES.md in this folder and let's keep going. Last status: [where we left off]."
