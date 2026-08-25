# Module Contest & Cheating Pipeline — GitHub Actions Cron

Runs the Module Contest / Mid Module Clearance / Cheating detection pipeline
on a schedule, writing 6 tabs into one Google Sheet. Ported from the Colab
notebook (`Module_contest_fixed.ipynb`) into a standalone script for
unattended execution.

## Files

```
.
├── .github/workflows/module-contest-cron.yml   # the scheduled workflow
├── pipeline.py                                  # the pipeline itself
├── requirements.txt                             # Python dependencies
└── README.md
```

## One-time setup

### 1. Create a Google service account

1. In [Google Cloud Console](https://console.cloud.google.com/), create (or
   reuse) a project, then go to **IAM & Admin → Service Accounts → Create
   Service Account**.
2. Enable the **Google Sheets API** and **Google Drive API** for that
   project (APIs & Services → Library → search each → Enable).
3. On the service account, go to **Keys → Add Key → Create new key → JSON**.
   This downloads a `.json` file — you'll paste its *entire contents* into a
   GitHub secret in step 3 below.
4. Open the target Google Sheet (`14asHS-hP-dS5-gOggfnFTlbRi9OE23s_-dmcs3dFLRA`)
   and **share it** with the service account's email address (found in the
   JSON file as `client_email`, looks like
   `something@your-project.iam.gserviceaccount.com`) — grant **Editor**
   access.

### 2. Get a Metabase API key

In Metabase: profile icon → **Settings → Admin settings → API Keys → Create
API Key**. Copy the key value — you won't be able to see it again after
leaving the page.

### 3. Add GitHub repo secrets

In your repo: **Settings → Secrets and variables → Actions → New repository
secret**. Add two:

| Secret name            | Value                                                        |
|-------------------------|--------------------------------------------------------------|
| `METABASE_API_KEY`      | The API key from step 2                                     |
| `SERVICE_ACCOUNT_JSON`  | The **entire contents** of the JSON file from step 1, pasted as-is |

### 4. Push these files to the repo

Commit `pipeline.py`, `requirements.txt`, and `.github/workflows/module-contest-cron.yml`
to your repo's default branch. GitHub only picks up workflows that live at
`.github/workflows/*.yml` on a branch that's actually been pushed.

## Running it

- **On schedule**: the workflow runs daily at 03:00 UTC by default. Edit the
  `cron:` line in `module-contest-cron.yml` to change this — cron syntax is
  `minute hour day-of-month month day-of-week`, always in UTC regardless of
  your local timezone.
- **Manually**: go to the **Actions** tab → **Module Contest & Cheating
  Pipeline** → **Run workflow**.

## Why this can take a while

Several of the underlying Metabase questions (cards 6396, 9717 in
particular) are slow — 3-5 minutes each is normal, not a bug. `pipeline.py`
accounts for this:

- Each card fetch gets up to **8 minutes** before being considered timed
  out, with growing backoff (30s → 60s → 120s → 240s) if it hits a hard
  connection reset instead of a clean response.
- Every card that's used by more than one section (`card 6289` is used by
  all four sections, for example) is **fetched once and cached** for the
  rest of that run — this is the biggest lever on total runtime, since it
  turns ~20 total calls across the 4 sections into 11 real fetches.
- The three-source lecture-data fetch (cards 6396 + 9717 + 9656) **degrades
  gracefully**: if one of the three keeps failing even after retries, the
  pipeline logs a warning and continues with the other two rather than
  aborting the whole run.

Realistic total runtime: roughly 15-40 minutes depending on how slow the
Metabase cards are being that day. The workflow's `timeout-minutes: 90`
gives real headroom above that without letting a genuinely stuck run go
forever.

## If a run fails

Check the **Actions** tab → the failed run → **Run pipeline** step. Error
messages are written to be actionable — e.g. a missing-share error will name
the exact service-account email to add as an Editor; a Metabase failure will
name the specific card, its HTTP status, and a snippet of the response body.

If a specific card is *consistently* slow or failing (not just once), that's
usually fixable on the Metabase side — check the question directly in the
Metabase UI for its own error/runtime, and look for `-- INDEX
RECOMMENDATIONS` comments at the top of its SQL (missing indexes were the
root cause the last time this happened, on card 6396).
