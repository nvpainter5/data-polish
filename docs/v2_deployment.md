# Deploying Data Polish v2

Two free-tier services, one public URL. End-to-end deploy time: ~25 minutes.

## Architecture

```
                              user's browser
                                   |
                                   v
       https://data-polish.streamlit.app   (Streamlit Community Cloud)
                                   |
                                   v   (HTTPS, Bearer JWT)
       https://data-polish-api.onrender.com   (Render Web Service)
                                   |
                  +----------------+----------------+
                  v                                 v
         Groq (Llama 3.3 70B)              ephemeral local disk
                                            (data/jobs/<user>/<id>/)
```

- **Frontend**: Streamlit Community Cloud — free, GitHub-driven, unlimited.
- **Backend**: Render free Web Service — 750 hours/month free, sleeps after 15 minutes idle.

Limitations of the free tier:
- Render sleeps after 15 min idle; first request afterward triggers a ~30 second cold-start.
- Disk is ephemeral on both platforms — uploaded jobs and any auth_config.yaml regenerate on restart. Real persistence would need v3 work (S3 storage backend + a database for auth).

## Pre-flight

- Repo on GitHub, **public** (Streamlit Community Cloud free tier requires public).
- Groq API key (you already have one in `.env`).
- A second clean Groq API key for the deployed service is best practice (rotate locally after; never commit either).

## 1. Backend on Render

1. Sign in to [render.com](https://render.com) (GitHub login is fine).
2. Click **New +** → **Blueprint**.
3. Connect your GitHub account if not already; pick the `data-polish` repo.
4. Render auto-detects `render.yaml` and proposes one service: `data-polish-api`. Click **Apply**.
5. The first build takes ~3 minutes (pip install of pandas/pyarrow/etc). Watch the live logs.
6. When it finishes, find the service in your dashboard. Click **Environment** in the sidebar.
7. **Set `GROQ_API_KEY`** to your `gsk_...` value. Click Save. Render redeploys automatically (~30 seconds).
8. Note your service URL. It looks like `https://data-polish-api-XXXX.onrender.com`. Test it:

    ```bash
    curl https://data-polish-api-XXXX.onrender.com/healthz
    # {"ok":true,"service":"datapolish-api","version":"3.7.0",...}
    ```

## 2. Frontend on Streamlit Community Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io). Sign in with GitHub.
2. Click **New app**.
3. Settings:
   - **Repository**: your data-polish repo
   - **Branch**: `main`
   - **Main file path**: `ui/Home.py`
   - **App URL**: choose something memorable, e.g. `datapolish` → live at `datapolish.streamlit.app`
4. Click **Advanced settings** → **Secrets**. Paste:

    ```toml
    DATAPOLISH_API_BASE = "https://datapolish-api-XXXX.onrender.com"
    AUTH_COOKIE_KEY = "<run: python -c 'import secrets; print(secrets.token_hex(32))'>"
    ```

   Replace the API URL with the one from step 1.8. Click **Save**.

5. Click **Deploy**. First deploy takes ~2 minutes.
6. Once live, your URL is `https://<your-app-name>.streamlit.app`.

## 3. Wire CORS

Now that you know the Streamlit URL, lock down CORS on the backend:

1. Render dashboard → `datapolish-api` → **Environment**.
2. Update `ALLOWED_ORIGINS` to `https://<your-app-name>.streamlit.app`. (Comma-separate if you want multiple.)
3. Save. Render redeploys.

Without this, the browser refuses cross-origin API calls and the UI shows "Backend not reachable" even though the backend is up.

## 4. Smoke test

1. Open `https://<your-app-name>.streamlit.app` in a browser.
2. **Register** a fresh user via the Register tab.
3. **Login** with that user.
4. Sidebar should show: green "datapolish-api v2.0.0-dev" status badge.
5. Upload a small CSV, click Next: Run, run the pipeline, view results.

If the API status badge is red:
- Check Render logs for backend errors.
- Verify `ALLOWED_ORIGINS` matches your Streamlit URL exactly (no trailing slash, exact protocol).
- Verify `DATAPOLISH_API_BASE` in Streamlit secrets matches your Render URL exactly.

## What you can put on LinkedIn now

You have a public URL where anyone (recruiters, friends, strangers) can:
- Register a free account
- Upload their own CSV / TXT / TSV / JSON / parquet
- Read from their own S3 bucket
- See the AI propose cleaning rules
- Watch deterministic safety gates accept or reject each rule
- Download the cleaned output

That's a real, deployable, demoable AI-augmented data engineering product.

## Known follow-ups (v3 territory)

- Replace LocalStorage with an S3Backend so jobs persist across cold starts.
- Replace the YAML auth store with Postgres + JWTs for real multi-tenant.
- Move from Render free tier to a paid tier (or ECS / Fly.io) to eliminate cold starts.
- Async job execution with a worker queue (RQ, Celery) so the UI doesn't block on long pipelines.
