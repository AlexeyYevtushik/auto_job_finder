# FROM WORKLESS TO WORKLESS

## Work Find Agent

Auto Job Finder is a minimal agent that can handle thousands of job postings on justjoin.it. It filters vacancies by keywords, automatically applies to relevant ones via Easy Apply, and prepares the rest for manual submission. (Plan) The project also integrates with browser_use to support external application forms.

##How to prepare to run
1. Download Python   
   - Download Python [Windows/macOS/Linux] (https://www.python.org/downloads/)   
   - Install Python on Windows, check **"Add Python to PATH"** during installation.

2. Install and update PIP:
```markdown
python -m pip install --upgrade pip
```
3. Install requirements and browsers:
```markdown
pip install -r requirements.txt
playwright install --with-deps
```
4. Update config/config.json, set your parameters:
```markdown
  "JOB_NAMES": ["QA Automation"],
  "KEYWORDS": ["Playwright", "Python", "JavaScript/TypeScript"],
  "INTRODUCE_YOURSELF": "Github: _your_link_to_github_\nLinkedIn: _your_link_to_linkedin_",
```
##How to run
```markdown
python -m src.run_pipeline
```
First run will take a lot of time (1 - 3 hours), after finish it will close: 
1) Run
2) Login to justjoin.it in opened Chromium browser (you have 5 minutes to login)
3) Wait (sometimes Chromium browsers will appear - finds link to vacansies, checks vacansy, easy apply vacancy scripts). 
4) In data/manual_work.jsonl you will have final vacansies urls to work manually (Temporary, planning browser_use)   

## Status:
S5 - Create possibility to apply by browser_use
