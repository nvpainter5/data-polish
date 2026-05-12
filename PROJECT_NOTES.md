# Data Polish — Project Notes

> Read this first at the start of any new Cowork chat to restore context.

## What this project is
Data Polish is a portfolio project Nirav is building during a job search. He's an experienced data engineer (SQL, pipelines) but new to AI/LLMs. The goal is a single repo that demonstrates AI-augmented data engineering: an LLM-powered pipeline that cleans messy real-world data, plus a tool-using agent on top, plus AWS deployment.

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
1. **Phase 1 — Pipeline + LLM-as-tool.** [COMPLETE]
2. **Phase 2 — Tool-using agent.** [COMPLETE]
3. **Phase 3a — Polish.** [COMPLETE] Streamlit dashboard, mermaid diagram, prompt iteration log.
4. **Phase 3b — AWS Lambda + S3 deploy.** [COMPLETE]
5. **v2.0–v2.6 — Multi-user web app.** [COMPLETE] FastAPI + Streamlit, S3 connector, custom instructions, outlier + quality score + suggestions, auth, deployed live.
6. **v3.0–v3.7 — Production foundation.** [COMPLETE] Postgres, JWT,
   magic-link auth, audit log, large-dataset support, GCS+Azure,
   structured logging + Sentry + DB-aware healthcheck. Tasks #20–#27.
   OAuth (v3.6) deferred to Blaze, where Next.js makes it much cleaner.
7. **Blaze — Real product.** [SAVED — DO NOT START] See `docs/blaze_vision.md`. Trigger phrase: **"Start Blaze"**. Begins after Data Polish v3 ships.

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

## Where Phase 3a ended (May 2026)
- `app.py` Streamlit dashboard at the project root with five tabs (Overview / Profile / Plan / Audit / Before-After).
- Mermaid architecture diagram in README — renders natively on GitHub.
- `docs/prompt_iterations.md` v3 entry capturing the agent's "thin overview led to under-exploration; hint-rich tools beat agentic discovery" lesson.
- Run with `streamlit run app.py`.

## Where v3.7 ended (May 2026) — production hardening climax
- Structured Python `logging` everywhere: `print()` removed from
  `api/magic_link.py`, `api/audit.py`. `LOG_LEVEL` env var controls
  verbosity (INFO in prod, DEBUG locally).
- Sentry SDK wired through `api/__init__.py` — fully opt-in via
  `SENTRY_DSN`. FastAPI + SQLAlchemy integrations, 10% trace sample
  rate, PII off. Tagged with `DATAPOLISH_ENV` + `DATAPOLISH_RELEASE`.
- `/healthz` now runs a `SELECT 1` against the DB and reports
  `checks.database`, `checks.sentry`, `checks.email_provider`. Returns
  503 (not 200) when the DB is down so orchestrators react.
- `require_user` dropped the legacy `X-User-ID` fallback. Bearer JWT
  is the only auth path. UI and tests updated to match.
- `app.version` bumped to `3.7.0`.
- New runbook at `docs/operations.md` — health, log filters, common
  incidents, deploy + secret-rotation reference.
- New `docs/resend_domain.md` walks through verifying
  `contact.data-polish.com` so magic-link emails finally deliver to
  real users. Domain registered through Cloudflare Registrar
  (~$10.44/yr, at-cost) — DNS lives on Cloudflare. After DNS records
  land, set `RESEND_FROM_EMAIL=noreply@contact.data-polish.com` and
  unset `DEV_MODE`. (`datapolish.com` was already taken; we use the
  hyphenated variant for the sending domain. Product name stays
  "Data Polish".)
- Tests in `tests/test_api.py` rewritten — they were stale from the v3.0
  refactor. New fixture spins up a temp SQLite DB, registers a real
  user, mints a real JWT.

## Where Phase 3b ended (May 2026)
- `lambda/lambda_function.py` Lambda handler implementing the Phase 1 pipeline: download CSV from S3 → profile → propose → apply → validate → upload cleaned parquet + audit JSON.
- `lambda/Dockerfile` for container-image Lambda (10 GB ceiling, easier with pandas/pyarrow than zip).
- `template.yaml` SAM template declaring the raw bucket, cleaned bucket, Lambda function (PackageType: Image), IAM policies, and S3 ObjectCreated trigger.
- `lambda/requirements-lambda.txt` slimmer runtime requirements (no streamlit, pytest).
- `docs/aws_deployment.md` step-by-step deployment guide.
- `tests/test_lambda.py` test that mocks S3 + the LLM and runs the handler end-to-end with no AWS credentials.
- Code is ready to deploy. Deployment requires AWS account + Docker Desktop + AWS SAM CLI installed; deferred to user when they're ready.

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

## Future product — DO NOT START WITHOUT TRIGGER
A real-product vision called **Blaze** is saved in `docs/blaze_vision.md`.
It's a self-serve AI data workbench (cleaning + analysis + natural-language
Q&A across any cloud) that builds on Data Polish v3 as its foundation.

**Do not pre-emptively begin Blaze.** Wait for Nirav to type
**"Start Blaze"** in a new chat. Until that trigger fires, focus on
finishing Data Polish v3 cleanly.
