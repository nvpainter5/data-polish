# Data Polish — Operations Runbook

The short version of "what to do when something is on fire." Aimed at
the future-Nirav who has to debug a Sunday-night outage with no recent
context. Keep it skimmable.

---

## Architecture, in one breath

- **API:** FastAPI on Render (free tier), `api/main.py`, served by uvicorn.
- **UI:** Streamlit Community Cloud, `app.py` + `ui/pages/*.py`.
- **DB:** Supabase Postgres. `DATABASE_URL` is the connection string.
- **LLM:** Groq (`llama-3.3-70b-versatile`) via `GROQ_API_KEY`.
- **Email:** Resend (`RESEND_API_KEY`) for magic-link OTP delivery.
- **Errors:** Sentry, if `SENTRY_DSN` is set.
- **Object stores:** S3 / GCS / Azure — user supplies creds per request,
  the API never persists them.

Auth is JWT (HS256, signed with `JWT_SECRET`). The UI stores the token
in `st.session_state["access_token"]` and sends it as
`Authorization: Bearer <token>` on every request.

---

## Healthchecks and what they tell you

`GET /healthz` always returns JSON. Render hits it every minute.

```json
{
  "ok": true,
  "service": "datapolish-api",
  "version": "3.7.0",
  "env": "production",
  "checks": {
    "database": "ok",
    "sentry": "enabled",
    "email_provider": "resend"
  }
}
```

Status code:

- **200** — API is up AND DB is reachable.
- **503** — API is up but DB probe failed. Look at the `error` field for
  the SQLAlchemy exception. Usual suspects: Supabase paused (free tier),
  `DATABASE_URL` rotated, Render's outbound networking degraded.

`checks.email_provider` reads `dev-console` when `RESEND_API_KEY` is
unset — if you see that in prod, magic-link codes are being logged
instead of emailed.

---

## Logs

Everything goes through Python `logging`. Format:
`2026-05-11T10:15:32 INFO api.main: ...`.

Tail on Render:

```
$ render logs --service datapolish-api --tail
```

Useful filters:

- `api.magic_link` — magic-link issuance and Resend results.
- `api.audit` — audit-log write failures.
- `api.main` — request-level errors (also captured by Sentry).

`LOG_LEVEL=DEBUG` makes it chattier locally. Don't ship DEBUG to prod —
DEBUG logs at the Groq client level can echo prompt fragments.

---

## Sentry

Set `SENTRY_DSN` from the project's Settings → Client Keys page in
Sentry. Optional knobs:

- `DATAPOLISH_ENV` — tag (e.g. `production`, `staging`). Defaults to
  `development`.
- `DATAPOLISH_RELEASE` — git SHA or version tag. Lets Sentry group
  errors by deploy. On Render you can set this to `$RENDER_GIT_COMMIT`.
- `SENTRY_TRACES_SAMPLE_RATE` — 0.0–1.0. Default 0.1 (10%).

Sentry is **opt-in by env var**, so leaving `SENTRY_DSN` blank disables
it entirely without any code change — handy for local dev.

---

## Common incidents

### "Users say they didn't get the magic-link code"

1. Tail logs and look for `api.magic_link`. You'll see one of:
   - `Resend accepted magic-link email for X@Y (id=...)` — Resend took
     it. Issue is downstream: spam folder, recipient mailbox.
   - `Resend send FAILED for X@Y: ...` — Resend rejected. Read the
     error. The two we've seen:
     - "Domain not verified" → finish `docs/resend_domain.md` setup.
     - "You can only send testing emails to your own email address"
       → `RESEND_FROM_EMAIL` is still `onboarding@resend.dev`. Switch
       to your verified-domain sender (e.g. `noreply@contact.datapolish.com`).
   - `DEV_MODE magic-link code for X@Y: 123456` — `DEV_MODE=true` is
     leaking into prod. Unset it.
2. If everything looks healthy on our side, ask the user to check
   spam — Resend free tier doesn't have great deliverability into
   Gmail/Outlook until the domain is warmed up.

### "Login returns 401 even with a valid password"

- Check the password is < 73 bytes. bcrypt truncates silently above that
  and we cap it in `auth.py:_to_bcrypt_bytes`, but very old hashes from
  pre-cap registrations may need a reset.
- Check `JWT_SECRET` hasn't rotated between when the user logged in and
  when they made the request. Rotating the secret invalidates every
  outstanding token.

### "/healthz returning 503"

- `checks.database == "down"`. Open Supabase, project dashboard.
  - On free tier the DB auto-pauses after a week of inactivity. Hitting
    the dashboard wakes it up.
  - Check `DATABASE_URL` env var on Render still matches Supabase's
    Connection Pooler URI (Settings → Database → Connection pooling).

### "Run job hangs / times out"

- Groq returned a slow response or rate-limited. Check
  `https://status.groq.com`.
- Job size: anything > ~50 MB now uses the chunked-read path in
  `pipeline_runner.py`. If it's failing with `DatasetTooLargeError`,
  the user is over the row-count cap — that's by design.

---

## Deploys

### API (Render)

`render.yaml` is the blueprint. Push to `main` → Render auto-deploys.
Required env vars on the Render service:

```
DATABASE_URL          # Supabase pooler URI
GROQ_API_KEY
JWT_SECRET            # python -c "import secrets; print(secrets.token_hex(32))"
RESEND_API_KEY
RESEND_FROM_EMAIL     # noreply@contact.datapolish.com (post-verification)
SENTRY_DSN            # optional
DATAPOLISH_ENV=production
ALLOWED_ORIGINS=https://data-polish.streamlit.app
```

### UI (Streamlit Cloud)

`app.py` is the entrypoint. Env var on the Streamlit Cloud app:

```
DATAPOLISH_API_BASE=https://data-polish-api.onrender.com
```

`.streamlit/config.toml` caps uploads at 250 MB.

---

## Backups / data recovery

- Postgres: Supabase free tier keeps daily backups for 7 days. Project
  dashboard → Database → Backups.
- Job artifacts (`raw.csv`, `cleaned.parquet`, `audit.json`): currently
  on Render's ephemeral disk. Survive process restarts but not deploys.
  Migrating these to S3 is on the v4 roadmap.

---

## Rotating secrets

| Secret              | What rotates              | Side effect                          |
| ------------------- | ------------------------- | ------------------------------------ |
| `JWT_SECRET`        | Invalidates all sessions  | Users must log in again              |
| `GROQ_API_KEY`      | Switch to new key         | None to users                        |
| `RESEND_API_KEY`    | Switch to new key         | In-flight magic-link sends may fail  |
| `DATABASE_URL`      | New Supabase password     | API restart picks it up              |
| `SENTRY_DSN`        | New project DSN           | Past events stay in old project      |

Rotate `JWT_SECRET` immediately if you suspect any leak. Everyone gets
logged out, but that's much cheaper than an active forgery attack.
