FROM WORKLESS TO WORKLESSES

Work Find Agent — README

A minimal, modular pipeline (S1–S4) for job discovery.
Today it works with JustJoin; tomorrow—adapters for major job boards & APIs.

1) Project overview (as it was) + short description

Goal. Automate the loop “search → collect links → scrape details → enrich/export” with small scripts you can schedule or run ad-hoc.

Series S1–S4.

S1 — Prepare / Seed (READY): bootstrap folders & state, verify base URL, warm Playwright storage.

S2 — Collect Links (READY): open JustJoin search for each JOB_NAME × LOCATION, human-like no-click scrolling, collect unique job URLs into data/links.jsonl.

S3 — Scrape Details (WIP): visit each URL, extract fields (title, company, salary, stack, etc.) to data/jobs.jsonl.

S4 — Enrich & Export (WIP): clean/dedupe, infer tags, export to JSONL/CSV/MD for analysis or outreach.

Design. Source-specific logic sits in S2/S3 adapters. JustJoin is first; next up: LinkedIn, NoFluff, Pracuj, Indeed, HH, etc.

S2 scrolling model (current).

Wait for content → hover first card (no click).

Scroll via small steps and ArrowDown in batches of 10, with 1–2s settles.

Collect after each batch (so new items are saved continuously).

Overlay mitigation: detect dialogs/fixed overlays, send Escape, restore overflow, refocus scroll target.

Optional: accept cookies if allowed by config.

URL-level dedupe (run-local & file-level when writing).

2) How to run S1 and S2 (and what they do)
Prereqs
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
playwright install --with-deps

S1 — Prepare / Seed (READY)

What it does

Creates folders: data/, errors/, screens/.

Initializes state.json (e.g., base_url) and storage_state.json (Playwright auth/cookies).

Verifies that base URL is reachable.

Run

python -m src.s1_prepare
# or
python src/s1_prepare.py

S2 — Collect Links (READY)

What it does

For each JOB_NAME × LOCATION, opens:

https://justjoin.it/job-offers/<location>?keyword=<job>


Human-like scroll without navigation:

Hover first card to “wake” lazy lists.

Press ArrowDown in batches of 10, short pauses between keys, 1–2s settle, then collect anchors every batch.

Detect/soft-close overlays (dialogs/fixed banners), optionally accept cookies if enabled.

Appends only new URLs to data/links.jsonl (URL-based dedupe).

Run

python -m src.s2_collect_links
# sample output:
# [INFO] Open search: https://justjoin.it/job-offers/remote?keyword=QA+Automation  (job='QA Automation', location='remote')
# [OK] Added 17 NEW hrefs to data/links.jsonl. Total known hrefs: 42


Tip. While tuning selectors/behavior, set "HEADFUL": true and "ALLOW_COOKIE_CLICK": true in config.

3) Configuration

Lookup order

Environment: CONFIG=/path/to/config.json

<repo_root>/config/config.json

src/s2_collect_links.py sibling config.json

Fallback to in-code DEFAULT_CONFIG

Example (with defaults)

{
  "JOB_NAMES": ["QA Automation"],
  "LOCATIONS": ["poland-remote", "remote"],
  "HEADFUL": true,
  "TARGET_INDEXES": 1000,
  "FAIL_FAST": true,

  "ALLOW_COOKIE_CLICK": false,
  "ALLOW_LOAD_MORE_CLICK": false
}


Fields

JOB_NAMES, LOCATIONS — Cartesian matrix of searches.

HEADFUL — show browser window (debug-friendly).

TARGET_INDEXES — early stop when a specific data-index appears (optional).

FAIL_FAST — stop on first error (good for CI) vs. best-effort.

ALLOW_COOKIE_CLICK — allow clicking “Accept cookies” (often required to enable scrolling).

ALLOW_LOAD_MORE_CLICK — keep false for strict no-click; enable only if a site truly requires the button.

Runtime state

state.json (may include "base_url": "https://justjoin.it/").

storage_state.json (Playwright auth/cookies persisted between runs).

Other info
Data formats (JSONL)

data/links.jsonl (S2 output — new URLs only)

{"id":"jj-12","data_index":"12","job_name":"QA Automation","location":"remote","url":"https://justjoin.it/job-offer/...","new_href":true}
{"id":"jj-13","data_index":"13","job_name":"QA Automation","location":"poland-remote","url":"https://justjoin.it/job-offer/...","new_href":true}


data/jobs.jsonl (S3 output — structured details)

{
  "id":"job-5a1f",
  "source":"justjoin",
  "title":"QA Automation Engineer",
  "company":"Acme",
  "location":"Remote, PL",
  "salary":{"min":14000,"max":20000,"currency":"PLN","period":"gross/month"},
  "seniority":"mid",
  "tech":["Python","Pytest","Playwright","CI/CD"],
  "posted_at":"2025-08-25",
  "source_url":"https://justjoin.it/job-offer/...",
  "scraped_at":"2025-08-27T19:55:21Z"
}


data/export.jsonl / data/export.csv (S4 output — cleaned & shareable)

{
  "title":"QA Automation Engineer",
  "company":"Acme",
  "location":"Remote, PL",
  "salary_range":"14k–20k PLN",
  "seniority":"mid",
  "stack":"Python, Pytest, Playwright",
  "url":"https://justjoin.it/job-offer/..."
}

Directory layout
.
├─ config/
│  └─ config.json
├─ data/
│  ├─ links.jsonl          # S2 output
│  ├─ jobs.jsonl           # S3 output
│  └─ export.(jsonl|csv|md)# S4 output
├─ errors/                 # time-stamped .txt diagnostics
├─ screens/                # time-stamped screenshots
├─ src/
│  ├─ s1_prepare.py
│  ├─ s2_collect_links.py
│  ├─ s3_scrape_details.py
│  ├─ s4_enrich_export.py
│  └─ common.py
└─ requirements.txt

Status

S1 — Ready

S2 — Ready

S3 — WIP (updated daily)

S4 — WIP (updated daily)

Troubleshooting

No scrolling / nothing collected

Set "ALLOW_COOKIE_CLICK": true (many sites block scroll until consent).

Keep "HEADFUL": true to observe UI; watch console for overlay mitigation logs.

Few/no results

Validate JOB_NAMES/LOCATIONS produce results on the site manually.

Duplicates

S2 dedupes by URL only. S4 can add content-hash dedupe if needed.

Timeouts / flaky SPA

Increase settle times (1.2–2.2s), keep ArrowDown batches, and consider proxy/user-agent rotation later.

Roadmap

Multi-source adapters (LinkedIn, NoFluff, Pracuj, Indeed, HH, …).

Anti-bot hygiene (proxies, UA rotation, timing envelopes).

S4 enrichment (skill normalization, seniority/contract inference, scoring & alerts).

CI/scheduling (Makefile targets, GitHub Actions/Cron, container images).

Connectors to Airtable/Notion/Sheets, webhooks for outreach automations.