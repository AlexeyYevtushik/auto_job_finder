# FROM WORKLESS TO WORKLESS

## Work Find Agent

A minimal, modular pipeline (S1–S4) for job discovery.
Today it works with JustJoin with your Google Account (for JustJoin account is created).

## S1
Prepare / Seed (READY): bootstrap folders & state, verify base URL, warm Playwright storage.

## S2
Collect Links (READY): open JustJoin search for each JOB_NAME × LOCATION, human-like no-click scrolling, collect unique job URLs into data/links.jsonl.

## How to run S1 and S2 

### Prereqs:

python -m venv .venv

#### Windows: 

.venv\Scripts\activate


#### macOS/Linux:

source .venv/bin/activate

#### then...

pip install -r requirements.txt

playwright install --with-deps


## S1:
Initializes state.json (e.g., base_url) and storage_state.json (Playwright auth/cookies) for other scripts run. Need to run only once or if you need to relogin. (READY)

### Run:

python -m src.s1_prepare

## S2:
Collect Links (READY). Appends only new URLs to data/links.jsonl (URL-based dedupe).

### Run:

python -m src.s2_collect_links

sample output:
[INFO] Open search: https://justjoin.it/job-offers/remote?keyword=QA+Automation  (job='QA Automation', location='remote')
[OK] Added 17 NEW hrefs to data/links.jsonl. Total known hrefs: 42


Tip. While tuning selectors/behavior, set "HEADFUL": true and "ALLOW_COOKIE_CLICK": true in config.

3) Configuration
Environment: CONFIG=/path/to/config.json
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



Data formats (JSONL)

data/links.jsonl (S2 output — new URLs only)

{"id":"jj-12","data_index":"12","job_name":"QA Automation","location":"remote","url":"https://justjoin.it/job-offer/...","new_href":true}
{"id":"jj-13","data_index":"13","job_name":"QA Automation","location":"poland-remote","url":"https://justjoin.it/job-offer/...","new_href":true}


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

Status:

S1 — Ready
S2 — Ready
S3 — WIP (updated daily)
S4 — WIP (updated daily)
