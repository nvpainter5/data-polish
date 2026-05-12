# Wiring up Resend so magic-link emails actually deliver

This is the one thing keeping the production magic-link flow honest.
Until your sending domain is verified at Resend, codes only show up in
the API logs (the `DEV_MODE` path) because `onboarding@resend.dev` only
delivers to your own Resend account email — useless for real users.

Once you've added `contact.data-polish.com` to Resend, this guide
finishes the job: DNS records in Cloudflare → verification → switch
the API over.

---

## Why a subdomain (and not just `data-polish.com`)?

Resend recommends a dedicated subdomain like `contact.<your domain>` or
`mail.<your domain>` so that:

- Reputation issues from transactional mail don't bleed into your apex
  domain (root) if someone marks you as spam.
- You can keep using `data-polish.com` for whatever else (landing page,
  Streamlit Cloud CNAME, etc.) without DNS collisions.

`contact.data-polish.com` is perfect. Keep it.

---

## Step 1 — Open the Resend domain page

1. Log in to <https://resend.com>.
2. Click **Domains** in the sidebar.
3. Click `contact.data-polish.com`.

You'll see a "DNS Records" table with four rows: **MX**, **TXT (SPF)**,
**TXT (DKIM)**, **TXT (DMARC)**. Each row has Name, Type, Priority,
Value. Leave that page open.

---

## Step 2 — Add those records in Cloudflare

The domain is registered through **Cloudflare Registrar**, so DNS is
already on Cloudflare — no nameserver dance needed. In the Cloudflare
dashboard:

1. Click `data-polish.com` from the Domains list.
2. Left sidebar → **DNS → Records**.
3. Click **Add record** four times, once per row that Resend showed
   you. For each:
   - Pick the right **Type** (MX or TXT).
   - **Name**: Cloudflare accepts either the relative prefix
     (e.g. `send.contact`) or the full host
     (e.g. `send.contact.data-polish.com`) — both work. Cloudflare's
     preview will show `send.contact.data-polish.com` either way.
   - **Mail server** (MX only) — the value Resend shows (`feedback-smtp.<region>.amazonses.com`).
   - **Priority** (MX only) — `10`.
   - **Content** (TXT only) — paste the value verbatim, no surrounding
     quotes.
   - **Proxy status**: **DNS only** (the grey cloud, not orange).
     Cloudflare's proxy doesn't apply to mail records and turning it on
     will silently break verification.
   - **TTL**: Auto.

### Common slip

If you accidentally end up with `send.contact.data-polish.com.data-polish.com`
in the resulting record, you double-suffixed it — strip the trailing
`.data-polish.com` from the Name you typed in.

### TXT value quoting

Some DNS UIs auto-wrap TXT values in quotes. Cloudflare handles this
correctly, but if the resulting record preview shows literal `"..."`
inside the value, strip them — the quotes are syntax, not content.

---

## Step 3 — Wait, then verify

DNS propagation is usually <5 min but can be 24h depending on TTL.

Back on Resend's domain page, click **Verify**. Each record turns green
when Resend successfully reads it from public DNS. You need **all four
green** (MX, SPF, DKIM, DMARC) before sending unlocks for real
recipients.

If a record stays red after 30 minutes:

```sh
# Sanity-check DNS yourself
dig +short TXT  send.contact.data-polish.com
dig +short MX   send.contact.data-polish.com
dig +short TXT  resend._domainkey.contact.data-polish.com
dig +short TXT  _dmarc.contact.data-polish.com
```

Each should print the value Resend asked for. Empty output = the record
didn't land where it needs to. Re-check the Name field at the DNS
provider — that's the usual culprit.

---

## Step 4 — Update the API config

Once Resend shows the domain as **Verified**, switch the sender address.

### Locally (`.env`)

```
RESEND_FROM_EMAIL=noreply@contact.data-polish.com
DEV_MODE=
```

(Leave `DEV_MODE` blank or remove the line entirely — it must NOT
evaluate truthy in production, or codes will keep logging instead of
emailing.)

### On Render

Service → Environment → edit the two variables above to the same
values. Render redeploys automatically when you save.

Optional but recommended: also set up a **Reply-To** display name later
by sending from `"Data Polish <noreply@contact.data-polish.com>"`. We can
do that with a small change to `api/magic_link.py` if you ever want
replies to land somewhere readable.

---

## Step 5 — Smoke test

```sh
curl -X POST https://data-polish-api.onrender.com/auth/magic/request \
     -H 'Content-Type: application/json' \
     -d '{"email":"[email]@gmail.com"}'
```

Then tail the API logs. You should see:

```
INFO api.magic_link: Resend accepted magic-link email for nirav.neel@gmail.com (id=..., from=noreply@contact.data-polish.com)
```

and the email should land in your inbox within a few seconds. **No
`DEV_MODE magic-link code for ...` line** — if you see one, `DEV_MODE`
is still set somewhere.

---

## Common gotchas

- **Code arrives in spam.** Brand new sending domain has zero reputation
  with Gmail/Outlook. Send a few yourself, mark "Not spam" once, give
  it a week. Resend's own
  [deliverability docs](https://resend.com/docs/dashboard/domains/cluster)
  walk through warmup.
- **"From address must match verified domain."** Your
  `RESEND_FROM_EMAIL` doesn't end in `@contact.data-polish.com`.
- **DNS provider only lets you set TTL ≥ 1h.** Fine — initial setup is
  one-time. Higher TTL just means slower future rotations.
- **You verified `data-polish.com` instead of `contact.data-polish.com`.**
  Different domains as far as Resend is concerned. Add the subdomain.
