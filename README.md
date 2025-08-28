# FROM WORKLESS TO WORKLESS

## Work Find Agent

Minimal agent that can work with 1000th of vacansies, finds appropriate vacations for you, Applying by you for Easy Apply, Works with browser_use with others vacansies

### S1
Prepare / Seed (READY): Gives you to log in to JustJoin

### S2
Collect Links (READY): Collects links to vacansies on JustJoin

### S3
Filter Links (READY): If vacancy contains one of keywords, Pressing Apply (saves new URL for real vacancy, or sets easy_apply = true)

### S4
Applying to easy_apply

### S5
Apply on different sites using browser_use

## How to run
### Prereqs:

```markdown
python -m venv .venv
```

#### Windows: 
```markdown
.venv\Scripts\activate
```

#### macOS/Linux:
```markdown
source .venv/bin/activate
```
#### then...

```markdown
pip install -r requirements.txt
playwright install --with-deps
```

## S1:
### Run:
```markdown
python -m src.s1_prepare
```
## S2:
### Run:
```markdown
python -m src.s2_collect_links
```
Tip. While tuning selectors/behavior, set "HEADFUL": true and "ALLOW_COOKIE_CLICK": true in config.

## S3:
### Run:
```markdown
python -m s3_filter_descriptions.py
```

## S4:
### Run:
```markdown
python -m src.s2_collect_links
```

## Configuration
   
Environment: CONFIG=/path/to/config.json
Example (with defaults)

```markdown
{
  "JOB_NAMES": ["QA Automation"],
  "LOCATIONS": ["remote", "poland-remote"],
  "HEADFUL": true,
  "TARGET_INDEXES": 1000,
  "FAIL_FAST": false,
  "LIMIT": 10,
  "ALLOW_COOKIE_CLICK": true,
  "KEYWORDS": ["Playwright", "Python", "JavaScript/TypeScript"],
  "REQUIRE_CONFIRMATION": true,
  "INTRODUCE_YOURSELF": "Github: https://github.com/AlexeyYevtushik\nLinkedIn: https://www.linkedin.com/in/alexey-yevtushik/"
}
```


## Data formats (JSONL)
```markdown
{"id":"jj-12","data_index":"12","job_name":"QA Automation","location":"remote","url":"https://justjoin.it/job-offer/...","new_href":true}
{"id":"jj-13","data_index":"13","job_name":"QA Automation","location":"poland-remote","url":"https://justjoin.it/job-offer/...","new_href":true}
```

## Directory layout
```markdown
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
```
## Status:

S1 — Ready

S2 — Ready

S3 — Ready

S4 — Ready

S5 - Next Plan
