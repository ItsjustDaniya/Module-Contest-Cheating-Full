#!/usr/bin/env python3
"""
Module Contest / Mid Module Clearance / Cheating Pipeline
Unattended GitHub Actions cron version.

Ported from Module_contest_fixed.ipynb (Colab notebook) into a standalone
script for scheduled execution. Writes 6 tabs into one Google Sheet:
  - Module Clearance          (Module Contest, scored + gem/non-gem split)
  - MC_Raw_2                  (Module Contest, raw per-user scores)
  - Mid Module Clearance       (Mid-Module Contest, scored + gem/non-gem split)
  - Mid_MC_Raw                 (Mid-Module Contest, raw per-user scores)
  - Cheating                   (Module Contest cheating/red-flag detection)
  - Cheating-Mid-Module        (Mid-Module cheating/red-flag detection)

Key differences from the Colab notebook:
  - Auth: METABASE_API_KEY + GOOGLE_SERVICE_ACCOUNT_JSON come from GitHub
    Actions secrets (env vars) instead of `google.colab.userdata` /
    `google.colab.auth.authenticate_user()`.
  - Every Metabase card fetch goes through `metabase_request()`, which:
      * Prints progress + elapsed time for every call (nothing runs silently).
      * Waits up to 8 minutes per attempt — several of these cards
        (6396, 9717 in particular) routinely take 3-5 minutes to run.
      * Retries hard connection resets (seen on card 9656) with growing
        backoff (30s -> 60s -> 120s -> 240s -> 240s, ~8 min total), separate
        from the transport-level retries the SESSION already does for
        429/5xx.
      * CACHES each card's result for the lifetime of one run. Several cards
        (6289 especially) are fetched up to 4x across the 4 sections below —
        caching turns that into 1 real fetch + 3 instant reuses, which is
        the single biggest lever on total runtime given how slow some of
        these queries are.
  - The Mid-Module lecture-data fetch (cards 6396 + 9717 + 9656 combined)
    degrades gracefully: if one of the three fails even after retries, the
    pipeline continues with the other two instead of aborting the whole run.
  - Fixed a stray syntax bug from the notebook (`worksheet.clear() 1`).
  - Any uncaught exception exits non-zero so the GitHub Actions run goes red.
"""

import os
import sys
import re
import json
import time
import traceback

import requests
import pandas as pd
import numpy as np
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

start_time = time.time()

# ═══════════════════════════════════════════════════════════════════════════
# ENV & AUTH
# ═══════════════════════════════════════════════════════════════════════════
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

missing = [n for n, v in [
    ("METABASE_API_KEY", METABASE_API_KEY),
    ("SERVICE_ACCOUNT_JSON", service_account_json),
] if not v]
if missing:
    raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")

service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

METABASE_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"
SHEET_KEY = "14asHS-hP-dS5-gOggfnFTlbRi9OE23s_-dmcs3dFLRA"

# ═══════════════════════════════════════════════════════════════════════════
# RETRY-HARDENED SESSION (transport-level: 429/5xx/connection resets)
# ═══════════════════════════════════════════════════════════════════════════
SESSION = requests.Session()
_adapter = HTTPAdapter(
    max_retries=Retry(
        total=4,
        connect=4,
        read=2,
        backoff_factor=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["POST", "GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    ),
    pool_connections=10,
    pool_maxsize=10,
)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)
requests.post = SESSION.post

METABASE_HEADERS = {
    "Content-Type": "application/json",
    "X-Api-Key": METABASE_API_KEY,
}


def safe_open_sheet(title):
    try:
        return gc.open(title)
    except gspread.exceptions.SpreadsheetNotFound:
        raise RuntimeError(
            f"❌ Could not open Google Sheet '{title}'. Either the title "
            f"doesn't match exactly, or it hasn't been shared with this "
            f"service account: {service_info.get('client_email')}. "
            "Share it as Editor, then re-run."
        )


def safe_open_by_key(key):
    try:
        return gc.open_by_key(key)
    except gspread.exceptions.SpreadsheetNotFound:
        raise RuntimeError(
            f"❌ Could not open Google Sheet with key '{key}'. Share it with "
            f"this service account as Editor: {service_info.get('client_email')}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# METABASE FETCH HELPERS
# ═══════════════════════════════════════════════════════════════════════════
_card_cache = {}  # card_id -> parsed JSON, cached for the lifetime of this run


def metabase_request(card_id, label=None, timeout=480, max_conn_retries=5,
                      conn_backoff=30, max_conn_backoff=240):
    """POST to a Metabase card's query/json endpoint with clear diagnostics,
    visible progress logging, generous per-attempt timeout, and growing
    backoff on hard connection resets.

    Default timeout is 8 minutes: cards like 6396 routinely take 3-5 minutes
    to run legitimately, so 8 min gives real headroom for a slow-but-healthy
    query to finish, without letting a genuinely broken/hanging card block
    the cron job indefinitely.

    Transient 429/5xx and connection resets are already retried at the
    transport level by SESSION's Retry adapter above — but a hard connection
    reset ("Remote end closed connection without response", seen on card
    9656 after heavy back-to-back queries) can exhaust those transport
    retries too, especially under backend load. This adds an extra
    application-level retry for that failure mode specifically, with
    30s -> 60s -> 120s -> 240s -> 240s backoff (~7.5-8 min total across 5
    attempts), plus a short pause before every call so heavy queries aren't
    fired back-to-back.
    """
    label = label or f"card {card_id}"
    url = f'{METABASE_BASE}/api/card/{card_id}/query/json'
    backoff = conn_backoff

    for conn_attempt in range(1, max_conn_retries + 1):
        time.sleep(3)  # brief courtesy pause between calls
        suffix = f" (connection retry {conn_attempt}/{max_conn_retries})" if conn_attempt > 1 else ""
        print(f"→ Fetching {label}{suffix}...")
        start = time.time()
        try:
            res = requests.post(url, headers=METABASE_HEADERS, timeout=timeout)
        except requests.exceptions.Timeout:
            raise RuntimeError(
                f"⏱️ Timed out fetching {label} after {timeout}s (URL: {url}). "
                "The underlying Metabase question is likely too slow/heavy — "
                "consider adding a result cache, narrowing its date range, or "
                "optimizing the query on the Metabase side."
            )
        except requests.exceptions.ConnectionError as e:
            elapsed = time.time() - start
            if conn_attempt == max_conn_retries:
                raise RuntimeError(
                    f"🔌 Connection error fetching {label} after {max_conn_retries} attempts "
                    f"(URL: {url}): {e}\n"
                    "This is a hard connection reset, not a slow-query timeout — the "
                    "Metabase backend is likely crashing/OOMing on this query, possibly "
                    "under load from prior heavy queries in the same run. Check this "
                    "card directly in the Metabase UI, and consider the same fix as "
                    "card 6396 (missing indexes / needs a result cache)."
                )
            print(f"🔌 Connection error fetching {label} after {elapsed:.1f}s: {e} "
                  f"— retrying in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_conn_backoff)
            continue
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"❌ Request failed fetching {label} (URL: {url}): {e}")

        elapsed = time.time() - start
        if res.status_code != 200:
            raise RuntimeError(
                f"❌ {label} returned HTTP {res.status_code} after {elapsed:.1f}s (URL: {url}).\n"
                f"Response body (first 500 chars):\n{res.text[:500]}"
            )
        print(f"✓ {label} done in {elapsed:.1f}s — status 200, body length {len(res.text)}")
        return res


def metabase_json(res, label=None):
    """Parse a Metabase response's JSON body, with clear diagnostics instead
    of a bare JSONDecodeError when the body is empty/HTML/not valid JSON."""
    label = label or "Metabase response"
    try:
        return res.json()
    except (json.JSONDecodeError, requests.exceptions.JSONDecodeError):
        raise RuntimeError(
            f"❌ {label} did not return valid JSON (status {res.status_code}, "
            f"body length {len(res.text)}).\n"
            f"Response body (first 500 chars):\n{res.text[:500]}"
        )


def fetch_card(card_id, label=None, optional=False, **kwargs):
    """Fetch a card's data as parsed JSON, cached for this run. Repeated
    calls for the same card_id (several cards are used by 2-4 sections
    below) reuse the first successful fetch instead of hitting Metabase
    again — the single biggest lever on total runtime.

    If optional=True, a failure (after all retries) is logged as a warning
    and None is returned instead of raising, so one chronically-flaky card
    doesn't take down sections that don't strictly need it.
    """
    label = label or f"card {card_id}"
    if card_id in _card_cache:
        print(f"↺ Reusing cached {label} (already fetched this run)")
        return _card_cache[card_id]

    try:
        res = metabase_request(card_id, label, **kwargs)
        data = metabase_json(res, label)
    except RuntimeError as e:
        if optional:
            print(f"⚠️  {label} failed and is being SKIPPED: {e}")
            return None
        raise

    _card_cache[card_id] = data
    return data


# ═══════════════════════════════════════════════════════════════════════════
# SHARED: month-name normalization (unified across all 4 sections — the
# notebook had 3 slightly-drifted copies of this dict; this is the merged,
# canonical version)
# ═══════════════════════════════════════════════════════════════════════════
MONTH_REPLACEMENTS = {
    'Data Science Certification - December 2022': '2022 12 Dec',
    'Data Science Certification - January 2023': '2023 01 Jan',
    'Data Science Certification  - February 2023': '2023 02 Feb',
    'Data Science Certification  - March 2023': '2023 03 Mar',
    'Professional Certificate Course In Data Science - April 2023': '2023 04 April',
    'Professional Certificate Course In Data Science - September 2023': '2023 09 Sept',
    'Professional Certificate Course In Data Science - June 2023': '2023 06 June',
    'Professional Certificate Course In Data Science - July 2023': '2023 07 July',
    'Professional Certificate Course In Data Science - August 2023': '2023 08 Aug',
    'Professional Certificate Course In Data Science - May 2023': '2023 05 May',
    'Professional Certificate Course In Data Science - October 2023': '2023 10 Oct',
    'Professional Certificate Course In Data Science - November 2023': '2023 11 Nov',
    'Professional Certificate Course In Data Science - December 2023': '2023 12 Dec',
    'Professional Certificate Course In Data Science - January 2024': '2024 13 Jan',
    'Professional Certificate Course In Data Science - February 2024': '2024 14 Feb',
    'Professional Certificate Course In Data Science - March 2024': '2024 15 March',
    'Professional Certificate Course In Data Science - April 2024': '2024 16 April',
    'Professional Certificate Course In Data Science - May 2024': '2024 17 May',
    'Professional Certificate Course In Data Science - June 2024': '2024 18 June',
    'Professional Certificate Course In Data Science - July 2024': '2024 19 July',
    'Professional Certificate Course In Data Science - August 2024': '2024 20 Aug',
    'Professional Certificate Course In Data Science - September 2024': '2024 21 Sept',
    'Professional Certificate Course In Data Science - October 2024': '2024 22 Oct',
    'Professional Certificate Course In Data Science - November 2024': '2024 23 Nov',
    'Professional Certificate Course In Data Science - December 2024': '2024 24 Dec',
    'Professional Certificate Course In Data Science - January 2025': '2025 25 Jan',
    'Professional Certificate Course In Data Science - February 2025': '2025 26 Feb',
    'Professional Certificate Course In Data Science - March 2025': '2025 27 March',
    'Professional Certificate Course In Data Science - April 2025': '2025 28 April',
    'Professional Certificate Course In Data Science - May 2025': '2025 29 May',
    'Professional Certificate Course In Data Science & AI - June 2025': '2025 30 June',
    'Professional Certificate Course In Data Science - July 2025': '2025 31 July',
    'Professional Certificate Course In Data Science August 2025': '2025 32 Aug',
    'Professional Certificate Course In Data Science September 2025': '2025 33 Sept',
    'Professional Certificate Course In Data Science October 2025': '2025 34 October',
    'Professional Certificate Course In Data Science November 2025': '2025 35 November',
    'Professional Certificate Course In Data Science December 2025': '2025 36 December',
    'Professional Certificate Course In Data Science January 2026': '2026 37 January',
    'Professional Certificate Course In Data Science February 2026': '2026 38 Febraury',
    'Professional Certificate Course In Data Science March 2026': '2026 39 March',
    'Professional Certificate Course In Data Science April 2026': '2026 40 April',
    'Professional Certificate Course In Data Science May 2026': '2026 41 May',
    'Professional Certificate Course In Data Science June 2026': '2026 42 June',
    'Professional Certificate Course In Data Science July 2026': '2026 43 July',
    'Professional Certificate Course In Data Science August 2026': '2026 44 August',
    'Newton Advantage - Data Analytics 2025': 'Advantage Aug 2025',
}
_MONTH_PATTERN = '|'.join(re.escape(k) for k in MONTH_REPLACEMENTS.keys())


def normalize_batch_names(series):
    def replace_month(match):
        return MONTH_REPLACEMENTS.get(match.group(0), match.group(0))
    return series.str.replace(_MONTH_PATTERN, replace_month, regex=True)


def calculate_total_score(row, has_09):
    """Score weighting: coding-heavy modules are 40% MCQ / 60% coding;
    Power BI and EDA 2 are MCQ-only. `has_09` toggles whether 'DS 09 ML 2'
    is included in the coding-heavy bucket (present in the Module Contest
    section, absent in the Cheating sections — preserved from the notebook
    rather than unified, since the module lineup differs slightly)."""
    mcq_score = pd.to_numeric(row['MCQ_score'], errors='coerce')
    coding_score = pd.to_numeric(row['Coding_score'], errors='coerce')
    mcq_score = mcq_score if pd.notna(mcq_score) else 0
    coding_score = coding_score if pd.notna(coding_score) else 0
    coding_heavy = ['DS 02 Spreadsheets', 'DS 04 SQL', 'DS 05 Python', 'DS 06 EDA 1', 'DS 08 ML 1']
    if has_09:
        coding_heavy = coding_heavy + ['DS 09 ML 2']
    if row['module_name'] in coding_heavy:
        return mcq_score * 0.4 + coding_score * 0.6
    elif row['module_name'] in ['DS 03 Power BI', 'DS 07 EDA 2']:
        return mcq_score
    else:
        return 0


print("🔎 ENV CHECK")
print(f"   Metabase API key   : {'[SET]' if METABASE_API_KEY else '[MISSING]'}")
print(f"   SA client_email    : {service_info.get('client_email')}")

# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE BODY
# ═══════════════════════════════════════════════════════════════════════════
try:
    sheet = safe_open_by_key(SHEET_KEY)

    # ── MODULE CONTEST ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("MODULE CONTEST")
    print("=" * 60)

    df1_2 = pd.DataFrame(fetch_card(8057, 'card 8057'))
    df1 = df1_2

    df2 = pd.DataFrame(fetch_card(6391, 'card 6391'))
    df2 = df2.rename(columns={'contest_title': 'contest_name'})

    au_raw = fetch_card(6289, 'card 6289')  # cached & reused by every section below

    df5 = pd.merge(df2, df1, on=['user_id', 'student_name', 'admin_unit_name',
                                  'contest_date', 'module_name'], how='outer')
    df5 = df5[['user_id', 'student_name', 'admin_unit_name', 'contest_date', 'module_name',
               'contest_name_x', 'contest_name_y', 'module_wise_score', 'per_module_marks']]
    df3 = df5.rename(columns={'module_wise_score': 'MCQ_score', 'per_module_marks': 'Coding_score'})
    df3['MCQ_score'] = pd.to_numeric(df3['MCQ_score'], errors='coerce').fillna(0)
    df3['Coding_score'] = pd.to_numeric(df3['Coding_score'], errors='coerce').fillna(0)
    df3['Total Score'] = df3.apply(lambda r: calculate_total_score(r, has_09=True), axis=1)
    df3 = df3.sort_values(by='Total Score', ascending=False)

    df4 = df3.groupby(['user_id', 'student_name', 'admin_unit_name', 'module_name']).agg({
        'MCQ_score': 'first', 'Coding_score': 'first', 'Total Score': 'max'
    }).reset_index()

    df_mc = df4.rename(columns={'Total Score': 'net_module_marks', 'admin_unit_name': 'au_batch_name'})
    df_mc = df_mc[['user_id', 'student_name', 'au_batch_name', 'module_name',
                    'net_module_marks', 'MCQ_score', 'Coding_score']]
    df_mc['net_module_marks'] = np.where(df_mc['net_module_marks'] == 0, np.nan, df_mc['net_module_marks'])

    df_au = pd.DataFrame(au_raw)
    df_au = df_au[['user_id', 'label', 'au_batch_name', 'gem_label']]
    df_au = df_au[df_au['label'].isin(['Enrolled', 'DS Advantage', 'Advantage +'])]

    screened_df = pd.merge(df_au, df_mc, on=['user_id', 'au_batch_name'], how='left')
    df_au = df_au.rename(columns={'au_batch_name': 'admin_unit_name'})
    screened_df = screened_df.rename(columns={'au_batch_name': 'admin_unit_name'})
    df_au = df_au.groupby(['admin_unit_name', 'label', 'gem_label'], dropna=False)['user_id'] \
        .nunique().reset_index().rename(columns={'user_id': 'Batch_strength'})

    merge_cols = ['admin_unit_name', 'label', 'gem_label']
    df_au[merge_cols] = df_au[merge_cols].fillna('NA')
    screened_df[merge_cols] = screened_df[merge_cols].fillna('NA')
    module_clearance = pd.merge(df_au, screened_df, on=merge_cols, how='inner')
    module_clearance['admin_unit_name'] = normalize_batch_names(module_clearance['admin_unit_name'])

    worksheet = sheet.worksheet("Module Clearance")
    worksheet.clear()
    set_with_dataframe(worksheet, module_clearance, include_index=False, include_column_header=True)
    print(f"✅ Wrote Module Clearance: {len(module_clearance)} rows")

    worksheet = sheet.worksheet("MC_Raw_2")
    worksheet.clear()
    set_with_dataframe(worksheet, df3, include_index=False, include_column_header=True)
    print(f"✅ Wrote MC_Raw_2: {len(df3)} rows")

    # ── MID MODULE CLEARANCE ────────────────────────────────────────
    print("\n" + "=" * 60)
    print("MID MODULE CLEARANCE")
    print("=" * 60)

    # Three lecture-data sources combined. Degrade gracefully if one is
    # chronically flaky (e.g. card 9656's hard connection resets) rather
    # than losing the whole section.
    lecture_sources = [(6396, 'card 6396'), (9717, 'card 9717'), (9656, 'card 9656')]
    frames = []
    failed_labels = []
    for cid, lbl in lecture_sources:
        data = fetch_card(cid, lbl, optional=True)
        if data is not None:
            frames.append(pd.DataFrame(data))
        else:
            failed_labels.append(lbl)
    if not frames:
        raise RuntimeError(
            "❌ All three Mid-MC lecture-data sources (6396, 9717, 9656) failed — "
            "cannot continue Mid Module Clearance without at least one source."
        )
    if failed_labels:
        print(f"⚠️  Proceeding with {len(frames)}/3 lecture-data sources — "
              f"missing: {', '.join(failed_labels)}.")
    df1 = pd.concat(frames, ignore_index=True).drop_duplicates()

    df2 = pd.DataFrame(fetch_card(6397, 'card 6397'))
    df2 = df2.rename(columns={'contest_title': 'contest_name'})

    df5 = pd.merge(df2, df1, on=['user_id', 'student_name', 'admin_unit_name',
                                  'contest_date', 'module_name'], how='outer')
    df5 = df5[['user_id', 'student_name', 'admin_unit_name', 'contest_date', 'module_name',
               'contest_name_x', 'contest_name_y', 'module_wise_score', 'per_module_marks']]
    df3 = df5.rename(columns={'module_wise_score': 'MCQ_score', 'per_module_marks': 'Coding_score'})
    df3['MCQ_score'] = pd.to_numeric(df3['MCQ_score'], errors='coerce').fillna(0)
    df3['Coding_score'] = pd.to_numeric(df3['Coding_score'], errors='coerce').fillna(0)
    df3['Total Score'] = df3.apply(lambda r: calculate_total_score(r, has_09=True), axis=1)
    df3 = df3.sort_values(by='Total Score', ascending=False)

    df4 = df3.groupby(['user_id', 'student_name', 'admin_unit_name', 'module_name']).agg({
        'MCQ_score': 'first', 'Coding_score': 'first', 'Total Score': 'max'
    }).reset_index()

    df_mid = df4.rename(columns={'Total Score': 'net_module_marks', 'admin_unit_name': 'au_batch_name'})
    df_mid = df_mid[['user_id', 'student_name', 'au_batch_name', 'module_name',
                      'net_module_marks', 'MCQ_score', 'Coding_score']]
    df_mid['net_module_marks'] = np.where(df_mid['net_module_marks'] == 0, np.nan, df_mid['net_module_marks'])

    df_au = pd.DataFrame(au_raw)  # reused from cache — no re-fetch
    df_au = df_au[['user_id', 'label', 'au_batch_name', 'gem_label']]
    df_au = df_au[df_au['label'].isin(['Enrolled', 'DS Advantage', 'Advantage +'])]

    screened_df = pd.merge(df_au, df_mid, on=['user_id', 'au_batch_name'], how='left')
    df_au = df_au.rename(columns={'au_batch_name': 'admin_unit_name'})
    screened_df = screened_df.rename(columns={'au_batch_name': 'admin_unit_name'})
    df_au = df_au.groupby(['admin_unit_name', 'label', 'gem_label'], dropna=False)['user_id'] \
        .nunique().reset_index().rename(columns={'user_id': 'Batch_strength'})

    merge_cols = ['admin_unit_name', 'label', 'gem_label']
    df_au[merge_cols] = df_au[merge_cols].fillna('NA')
    screened_df[merge_cols] = screened_df[merge_cols].fillna('NA')
    mid_module_clearance = pd.merge(df_au, screened_df, on=merge_cols, how='inner')
    mid_module_clearance['admin_unit_name'] = normalize_batch_names(mid_module_clearance['admin_unit_name'])

    worksheet = sheet.worksheet("Mid Module Clearance")
    worksheet.clear()
    set_with_dataframe(worksheet, mid_module_clearance, include_index=False, include_column_header=True)
    print(f"✅ Wrote Mid Module Clearance: {len(mid_module_clearance)} rows")

    worksheet = sheet.worksheet("Mid_MC_Raw")
    worksheet.clear()
    set_with_dataframe(worksheet, df3, include_index=False, include_column_header=True)
    print(f"✅ Wrote Mid_MC_Raw: {len(df3)} rows")

    # ── CHEATING MODULE CONTEST ─────────────────────────────────────
    print("\n" + "=" * 60)
    print("CHEATING MODULE CONTEST")
    print("=" * 60)

    df1_2 = pd.DataFrame(fetch_card(8057, 'card 8057'))  # cached — no re-fetch
    df_41 = df1_2

    df2 = pd.DataFrame(fetch_card(6391, 'card 6391'))  # cached — no re-fetch
    df2 = df2.rename(columns={'contest_title': 'contest_name'})

    df5 = pd.merge(df2, df_41, on=['user_id', 'student_name', 'admin_unit_name',
                                    'contest_date', 'module_name'], how='outer')
    df5 = df5[['user_id', 'student_name', 'admin_unit_name', 'contest_date', 'module_name',
               'contest_name_x', 'contest_name_y', 'module_wise_score', 'per_module_marks',
               'contest_id_x', 'contest_id_y']]
    df3 = df5.rename(columns={'module_wise_score': 'MCQ_score', 'per_module_marks': 'Coding_score'})
    df3['Total Score'] = df3.apply(lambda r: calculate_total_score(r, has_09=False), axis=1)

    df_3 = pd.DataFrame(fetch_card(2537, 'card 2537'))
    df_3 = df_3[['Total Red Flags', 'Assignment ID', 'Full screen exit count',
                 'Unique Times Assignments Opened', 'id', 'Marked Cheating Product',
                 'Assignment', 'Batch', 'Suspected Cheater Check', 'Tab Switch count', 'Link']]
    df_3 = df_3.rename(columns={'id': 'user_id', 'Assignment ID': 'contest_id_x',
                                 'Total Red Flags': 'Red_Flags_coding', 'Link': 'Link_coding',
                                 'Marked Cheating Product': 'Marked_Cheating_Coding'})

    df_4 = pd.DataFrame(fetch_card(6364, 'card 6364'))
    df_4 = df_4[['Total Red Flags', 'user_id', 'Link', 'Full screen exit count',
                 'Unique Times Assignments Opened', 'Assessment',
                 'Marked Cheating Product', 'Batch', 'Assessment ID', 'Name',
                 'Suspected Cheater Check', 'Tab Switch count']]
    df_4 = df_4.rename(columns={'Assessment ID': 'contest_id_y', 'Total Red Flags': 'Red_Flags_mcq',
                                 'Link': 'Link_MCQ', 'Marked Cheating Product': 'Marked_Cheating_MCQ'})

    df_1 = pd.merge(df3, df_4, on=['user_id', 'contest_id_y'], how='left')
    df_2 = pd.merge(df_1, df_3, on=['user_id', 'contest_id_x'], how='left')
    df_2['Red_Flags_mcq'] = df_2['Red_Flags_mcq'].fillna(value=0)
    df_2['Red_Flags_coding'] = df_2['Red_Flags_coding'].fillna(value=0)
    df_2['Red_flags'] = df_2['Red_Flags_coding'].astype("int64") + df_2['Red_Flags_mcq'].astype("int64")
    df_2 = df_2.sort_values(by='Total Score', ascending=False)

    df = df_2.rename(columns={'Total Score': 'net_module_marks', 'admin_unit_name': 'au_batch_name'})
    df = df[['user_id', 'student_name', 'au_batch_name', 'module_name', 'net_module_marks', 'Red_flags',
             'Link_MCQ', 'Link_coding', 'MCQ_score', 'Coding_score', 'contest_date',
             'contest_name_x', 'contest_name_y', 'Marked_Cheating_Coding', 'Marked_Cheating_MCQ']]
    df['net_module_marks'] = np.where(df['net_module_marks'] == 0, np.nan, df['net_module_marks'])

    df_au = pd.DataFrame(au_raw)  # cached — no re-fetch
    df_au = df_au[['user_id', 'label', 'au_batch_name', 'gem_label']]
    df_au = df_au[df_au['label'].isin(['Enrolled', 'DS Advantage', 'Advantage +'])]

    df = pd.merge(df_au, df, on=['user_id', 'au_batch_name'], how='left')
    batch_strength = df_au.groupby(['au_batch_name', 'label', 'gem_label'], dropna=False)['user_id'] \
        .nunique().reset_index().rename(columns={'user_id': 'Batch_strength'})
    merge_cols = ['au_batch_name', 'label', 'gem_label']
    batch_strength[merge_cols] = batch_strength[merge_cols].fillna('NA')
    df[merge_cols] = df[merge_cols].fillna('NA')
    df = pd.merge(batch_strength, df, on=merge_cols, how='inner')

    df5 = pd.DataFrame(fetch_card(6439, 'card 6439'))
    df5 = df5.rename(columns={'admin_unit_name': 'au_batch_name'})
    df5['Assignment_Completion'] = df5['q_attempted'].astype('float') / df5['total_questions'].astype('float')
    df1 = pd.merge(df, df5, on=['user_id', 'student_name', 'au_batch_name', 'module_name'], how='left')

    df6 = pd.DataFrame(fetch_card(6330, 'card 6330'))
    df6 = df6.rename(columns={'admin_unit_name': 'au_batch_name'})
    df6['Assessment_Completion'] = df6['total_mcqs_correct'].astype('float') / df6['total_mcqs_released'].astype('float')
    df6 = df6[df6['user_id'].isin(df5['user_id'])]

    cheating_mc = pd.merge(df1, df6, on=['user_id', 'student_name', 'au_batch_name', 'module_name'], how='left')
    cheating_mc['au_batch_name'] = normalize_batch_names(cheating_mc['au_batch_name'])

    worksheet = sheet.worksheet("Cheating")
    worksheet.clear()
    set_with_dataframe(worksheet, cheating_mc, include_index=False, include_column_header=True)
    print(f"✅ Wrote Cheating: {len(cheating_mc)} rows")

    # ── CHEATING MID-MODULE CONTEST ─────────────────────────────────
    print("\n" + "=" * 60)
    print("CHEATING MID-MODULE CONTEST")
    print("=" * 60)

    frames = []
    failed_labels = []
    for cid, lbl in lecture_sources:  # same three cards as Mid Module Clearance — all cached
        data = fetch_card(cid, lbl, optional=True)
        if data is not None:
            frames.append(pd.DataFrame(data))
        else:
            failed_labels.append(lbl)
    if not frames:
        raise RuntimeError(
            "❌ All three Mid-MC lecture-data sources (6396, 9717, 9656) failed — "
            "cannot continue Cheating Mid-Module without at least one source."
        )
    if failed_labels:
        print(f"⚠️  Proceeding with {len(frames)}/3 lecture-data sources — "
              f"missing: {', '.join(failed_labels)}.")
    df1 = pd.concat(frames, ignore_index=True).drop_duplicates()

    df2 = pd.DataFrame(fetch_card(6397, 'card 6397'))  # cached — no re-fetch
    df2 = df2.rename(columns={'contest_title': 'contest_name'})

    df5 = pd.merge(df2, df1, on=['user_id', 'student_name', 'admin_unit_name',
                                  'contest_date', 'module_name'], how='outer')
    df5 = df5[['user_id', 'student_name', 'admin_unit_name', 'contest_date', 'module_name',
               'contest_name_x', 'contest_name_y', 'module_wise_score', 'per_module_marks',
               'contest_id_x', 'contest_id_y']]
    df3 = df5.rename(columns={'module_wise_score': 'MCQ_score', 'per_module_marks': 'Coding_score'})
    df3['Total Score'] = df3.apply(lambda r: calculate_total_score(r, has_09=False), axis=1)

    df_3 = pd.DataFrame(fetch_card(2537, 'card 2537'))  # cached — no re-fetch
    df_3 = df_3[['Total Red Flags', 'Assignment ID', 'Full screen exit count',
                 'Unique Times Assignments Opened', 'id', 'Marked Cheating Product',
                 'Assignment', 'Batch', 'Suspected Cheater Check', 'Tab Switch count', 'Link']]
    df_3 = df_3.rename(columns={'id': 'user_id', 'Assignment ID': 'contest_id_x',
                                 'Total Red Flags': 'Red_Flags_coding', 'Link': 'Link_coding',
                                 'Marked Cheating Product': 'Marked_Cheating_Coding'})

    df_4 = pd.DataFrame(fetch_card(6364, 'card 6364'))  # cached — no re-fetch
    df_4 = df_4[['Total Red Flags', 'user_id', 'Link', 'Full screen exit count',
                 'Unique Times Assignments Opened', 'Assessment',
                 'Marked Cheating Product', 'Batch', 'Assessment ID', 'Name',
                 'Suspected Cheater Check', 'Tab Switch count']]
    df_4 = df_4.rename(columns={'Assessment ID': 'contest_id_y', 'Total Red Flags': 'Red_Flags_mcq',
                                 'Link': 'Link_MCQ', 'Marked Cheating Product': 'Marked_Cheating_MCQ'})

    df_1 = pd.merge(df3, df_4, on=['user_id', 'contest_id_y'], how='left')
    df_2 = pd.merge(df_1, df_3, on=['user_id', 'contest_id_x'], how='left')
    df_2['Red_Flags_mcq'] = df_2['Red_Flags_mcq'].fillna(value=0)
    df_2['Red_Flags_coding'] = df_2['Red_Flags_coding'].fillna(value=0)
    df_2['Red_flags'] = df_2['Red_Flags_coding'].astype("int64") + df_2['Red_Flags_mcq'].astype("int64")
    df_2 = df_2.sort_values(by='Total Score', ascending=False)

    df = df_2.rename(columns={'Total Score': 'net_module_marks', 'admin_unit_name': 'au_batch_name'})
    df = df[['user_id', 'student_name', 'au_batch_name', 'module_name', 'net_module_marks', 'Red_flags',
             'Link_MCQ', 'Link_coding', 'MCQ_score', 'Coding_score', 'contest_date',
             'contest_name_x', 'contest_name_y', 'Marked_Cheating_Coding', 'Marked_Cheating_MCQ']]
    df['net_module_marks'] = np.where(df['net_module_marks'] == 0, np.nan, df['net_module_marks'])

    df_au = pd.DataFrame(au_raw)  # cached — no re-fetch
    df_au = df_au[['user_id', 'label', 'au_batch_name', 'gem_label']]
    df_au = df_au[df_au['label'].isin(['Enrolled', 'DS Advantage', 'Advantage +'])]

    df = pd.merge(df_au, df, on=['user_id', 'au_batch_name'], how='left')
    batch_strength = df_au.groupby(['au_batch_name', 'label', 'gem_label'], dropna=False)['user_id'] \
        .nunique().reset_index().rename(columns={'user_id': 'Batch_strength'})
    merge_cols = ['au_batch_name', 'label', 'gem_label']
    batch_strength[merge_cols] = batch_strength[merge_cols].fillna('NA')
    df[merge_cols] = df[merge_cols].fillna('NA')
    df = pd.merge(batch_strength, df, on=merge_cols, how='inner')

    df5 = pd.DataFrame(fetch_card(6439, 'card 6439'))  # cached — no re-fetch
    df5 = df5.rename(columns={'admin_unit_name': 'au_batch_name'})
    df5['Assignment_Completion'] = df5['q_attempted'].astype('float') / df5['total_questions'].astype('float')
    df1 = pd.merge(df, df5, on=['user_id', 'student_name', 'au_batch_name', 'module_name'], how='left')

    df6 = pd.DataFrame(fetch_card(6330, 'card 6330'))  # cached — no re-fetch
    df6 = df6.rename(columns={'admin_unit_name': 'au_batch_name'})
    df6['Assessment_Completion'] = df6['total_mcqs_correct'].astype('float') / df6['total_mcqs_released'].astype('float')
    df6 = df6[df6['user_id'].isin(df5['user_id'])]

    cheating_mid_mc = pd.merge(df1, df6, on=['user_id', 'student_name', 'au_batch_name', 'module_name'], how='left')
    cheating_mid_mc['au_batch_name'] = normalize_batch_names(cheating_mid_mc['au_batch_name'])

    worksheet = sheet.worksheet("Cheating-Mid-Module")
    worksheet.clear()
    set_with_dataframe(worksheet, cheating_mid_mc, include_index=False, include_column_header=True)
    print(f"✅ Wrote Cheating-Mid-Module: {len(cheating_mid_mc)} rows")

except Exception as e:
    print(f"❌ Pipeline failed: {e}")
    traceback.print_exc()
    sys.exit(1)

mins, secs = divmod(time.time() - start_time, 60)
print(f"\n🎯 Pipeline completed successfully in {int(mins)}m {int(secs)}s")
print(f"   Unique Metabase cards fetched: {len(_card_cache)} "
      f"(reused via cache for repeat calls within this run)")
sys.exit(0)
