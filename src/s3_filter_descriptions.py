# src/s3_filter_descriptions.py
# Async Playwright worker:
# - Reads first <LIMIT> rows from data/links.jsonl (only where new_href==true)
# - Opens each URL sequentially
# - Extracts full Job Description (JD)
# - Runs keyword check
# - Clicks "Apply" if keywords matched
# - Detects if "easy apply" form is present
# - Writes results into data/filtered_links.jsonl
# - Marks a link as consumed (new_href=false) ONLY IF processing succeeded

import asyncio
import json
import os
import re
import traceback
from contextlib import suppress
from urllib.parse import urljoin, urlparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Union

from playwright.async_api import (
    async_playwright, Page, Browser, BrowserContext
)

from .common import (
    DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR,
    LINKS_JSONL, FILTERED_JSONL, STORAGE_STATE_JSON,
    read_jsonl, append_jsonl,
    now_iso, human_sleep
)

# ---------------------------- Utilities ----------------------------

DEFAULT_KEYWORDS = ["python", "playwright", "javascript", "typescript"]

def _normalize_keyword_token(tok: str) -> List[str]:
    parts = re.split(r"[,\s/]+", tok)
    return [p.strip().lower() for p in parts if p.strip()]

def normalize_keywords(src: Union[str, List[str], None]) -> List[str]:
    toks: List[str] = []
    if isinstance(src, list):
        for t in src:
            toks.extend(_normalize_keyword_token(str(t)))
    elif isinstance(src, str):
        toks.extend(_normalize_keyword_token(src))
    else:
        toks.extend(DEFAULT_KEYWORDS)
    seen, out = set(), []
    for t in toks:
        if t and t not in seen:
            seen.add(t); out.append(t)
    return out or DEFAULT_KEYWORDS[:]

async def slow_scroll_page_to_bottom(page: Page, step_px: int = 400, max_steps: int = 120, pause_s: float = 3.6):
    for _ in range(max_steps):
        try:
            done = await page.evaluate(
                """(step) => {
                    const el = document.scrollingElement || document.documentElement;
                    el.scrollBy(0, step || 400);
                    return Math.ceil(el.scrollTop + window.innerHeight) >= el.scrollHeight - 2;
                }""",
                step_px
            )
        except Exception:
            done = True
        if done:
            break
        await asyncio.sleep(pause_s)

async def get_job_description_text(page: Page) -> str:
    try:
        blocks = page.locator("xpath=//h2/../../")
        cnt = await blocks.count()
        if cnt > 0:
            texts = []
            for i in range(min(cnt, 8)):
                blk = blocks.nth(i)
                with suppress(Exception):
                    await blk.scroll_into_view_if_needed()
                for _ in range(40):
                    try:
                        handle = await blk.element_handle()
                        at_bottom = await page.evaluate(
                            """(el) => {
                                const r = el.getBoundingClientRect();
                                const vpH = window.innerHeight || document.documentElement.clientHeight;
                                return (r.bottom <= vpH + 2);
                            }""",
                            handle
                        )
                    except Exception:
                        at_bottom = True
                    if at_bottom:
                        break
                    with suppress(Exception):
                        await page.mouse.wheel(0, 320)
                    await asyncio.sleep(3.6)
                await asyncio.sleep(7.5)
                with suppress(Exception):
                    t = (await blk.inner_text(timeout=2000)).strip()
                    if len(t) > 50:
                        texts.append(t)
            if texts:
                texts.sort(key=len, reverse=True)
                return texts[0]
    except Exception:
        pass

    candidates = [
        '[data-testid="job-description"]',
        '[data-test="job-description"]',
        '[data-testid="offer-description"]',
        '[data-testid="sections"]',
        'section:has(h2:has-text("Job Description"))',
        'section:has(h2:has-text("Opis"))',
        'section:has(h2:has-text("Description"))',
        "article",
        "main",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            n = await loc.count()
            if n > 0:
                texts = []
                for i in range(min(n, 6)):
                    with suppress(Exception):
                        t = await loc.nth(i).inner_text(timeout=2000)
                        if t and len(t.strip()) > 50:
                            texts.append(t.strip())
                if texts:
                    texts.sort(key=len, reverse=True)
                    return texts[0]
        except Exception:
            continue

    for sel in ["div[role='main']", "#__next main", "body"]:
        with suppress(Exception):
            t = await page.locator(sel).inner_text(timeout=2000)
            if t and t.strip():
                return t.strip()
    return ""

async def detect_application_form(page: Page) -> bool:
    hints = [
        "application form", "apply for this job", "apply now",
        "wyślij aplikację", "formularz aplikacyjny", "aplikuj",
        "upload cv", "resume", "cover letter"
    ]
    try:
        body_text = (await page.locator("body").inner_text(timeout=2000)).lower()
        if any(h in body_text for h in hints):
            return True
    except Exception:
        pass

    selectors = [
        "form:has(input), form:has(textarea), form:has(button)",
        "form[action*='apply'], form[action*='application']",
        "[data-testid*='apply']",
        "button:has-text('Apply')",
        "button:has-text('Aplikuj')",
        "button:has-text('Submit')",
        "input[type='file']",
        "input[name*='resume'], input[name*='cv']",
        "textarea[name*='cover'], textarea[name*='motivation']",
    ]
    for sel in selectors:
        try:
            if await page.locator(sel).count() > 0:
                return True
        except Exception:
            continue
    return False

def find_keywords(text: str, keywords: List[str]) -> Tuple[bool, List[str]]:
    text_l = text.lower()
    matched = [kw for kw in keywords if kw in text_l]
    return (len(matched) > 0, matched)

def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)

# ----------------------------- links.jsonl helpers -----------------------------

def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def take_new_links(limit: int) -> List[Dict[str, Any]]:
    """
    Returns first <limit> rows from links.jsonl where new_href==true.
    DOES NOT modify links.jsonl.
    """
    all_rows = list(read_jsonl(LINKS_JSONL))
    new_rows = [r for r in all_rows if r.get("new_href") is True]
    if limit and limit > 0:
        return new_rows[:limit]
    return new_rows

def mark_link_consumed(row: Dict[str, Any]) -> None:
    """
    Sets new_href=false for the specific row (by url or id) and rewrites links.jsonl.
    Use only after successful processing of that row.
    """
    key = row.get("url") or row.get("id")
    if not key:
        return
    all_rows = list(read_jsonl(LINKS_JSONL))
    changed = False
    for r in all_rows:
        k = r.get("url") or r.get("id")
        if k == key:
            if r.get("new_href") is not False:
                r["new_href"] = False
                changed = True
            break
    if changed:
        _write_jsonl(Path(LINKS_JSONL), all_rows)

# ------------------------------- Per-row processor -------------------------------

async def process_one(
    ctx: BrowserContext,
    row: Dict[str, Any],
    keywords: List[str],
    headful: bool,
    fail_fast: bool
) -> bool:
    """
    Process one link: open page, extract JD, check keywords,
    try to click Apply, detect easy apply, write result.
    Returns True on success (even if keywords not matched), False on failure.
    """
    page: Page | None = None
    url = row.get("url")
    if not url:
        return False

    try:
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        with suppress(Exception):
            await page.wait_for_load_state("networkidle", timeout=8000)

        await slow_scroll_page_to_bottom(page)
        desc_full = await get_job_description_text(page)

        keyword_exists, matched = find_keywords(desc_full, keywords)

        result = {
            "id": row.get("id"),
            "data_index": row.get("data_index"),
            "url": url,
            "final_url": url,
            "keyword_exists": keyword_exists,
            "matched_keywords": matched,
            "easy_apply": False,
            # full JD saved here
            "description_sample": desc_full,
            "processed_at": now_iso(),
        }

        if not keyword_exists:
            append_jsonl(FILTERED_JSONL, result)
            with suppress(Exception): await page.close()
            return True

        # Try Apply flow
        easy_apply, final_url = await click_apply_and_detect(ctx, page)
        result["easy_apply"] = bool(easy_apply)
        result["final_url"]  = final_url

        append_jsonl(FILTERED_JSONL, result)

        # Close extra tabs
        for p in list(ctx.pages):
            if p is page:
                continue
            with suppress(Exception): await p.close()
        with suppress(Exception): await page.close()
        return True

    except Exception:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENSHOTS_DIR / f"s3_{safe_filename(row.get('id') or 'item')}_{ts}.png"
        txt = ERRORS_DIR / f"s3_{safe_filename(row.get('id') or 'item')}_{ts}.txt"
        with suppress(Exception):
            if page: await page.screenshot(path=str(png), full_page=True)
        with txt.open("w", encoding="utf-8") as f:
            f.write(f"TIME: {now_iso()}\nURL: {url}\n\nTRACEBACK:\n{traceback.format_exc()}\n")
        print(f"[ERROR] s3_filter: saved {png.name} and {txt.name}")
        if fail_fast:
            raise
        return False
    finally:
        if page and not page.is_closed():
            with suppress(Exception): await page.close()

# ---------------------- CLICK APPLY + DESTINATION DETECTION ----------------------
# (unchanged from your version; omitted here for brevity if you already have it)
# Paste your existing click_apply_and_detect(...) here
async def click_apply_and_detect(ctx: BrowserContext, page: Page) -> tuple[bool, str]:
    import re
    candidates = [
        re.compile(r"\bapply now?\b", re.I),
        re.compile(r"\bapply\b", re.I),
        re.compile(r"\baplikuj\b", re.I),
        re.compile(r"wyślij", re.I),
    ]

    apply = None
    for rx in candidates:
        loc = page.get_by_role("button", name=rx)
        if await loc.count() > 0:
            apply = loc.first; break
        loc = page.get_by_role("link", name=rx)
        if await loc.count() > 0:
            apply = loc.first; break
    if not apply:
        for txt in ["Apply now", "Apply", "Aplikuj", "Wyślij"]:
            loc = page.locator(f"a:has-text('{txt}')")
            if await loc.count() > 0:
                apply = loc.first; break
    if not apply:
        easy_now = await detect_application_form(page)
        return (easy_now, page.url)

    candidate_href = None
    with suppress(Exception):
        raw_hint = await apply.evaluate(
            """el => {
                const root = el.closest('a') || (el.tagName.toLowerCase()==='a' ? el : null) || el;
                const keys = ['href','data-href','data-url','data-redirect','data-link','data-external-url','data-apply-url'];
                for (const k of keys) {
                  const v = root.getAttribute(k);
                  if (v && v.trim()) return v.trim();
                }
                const oc = root.getAttribute('onclick') || el.getAttribute('onclick');
                if (oc) {
                  const m = oc.match(/https?:\/\/[^\s'"]+/);
                  if (m) return m[0];
                }
                const form = root.closest && root.closest('form');
                if (form && form.action) return form.action;
                return null;
            }"""
        )
        if raw_hint:
            candidate_href = urljoin(page.url, raw_hint)

    popup_ctx_task  = asyncio.create_task(ctx.wait_for_event("page",   timeout=15000))
    popup_page_task = asyncio.create_task(page.wait_for_event("popup", timeout=15000))
    nav_task        = asyncio.create_task(page.wait_for_event("framenavigated", timeout=15000))

    with suppress(Exception):
        await apply.scroll_into_view_if_needed()
        await apply.hover()

    did_click = False
    with suppress(Exception):
        box = await apply.bounding_box()
        if box:
            x = box["x"] + min(box["width"]  / 2 + 10, max(box["width"]  - 1, 1))
            y = box["y"] + min(box["height"] / 2 + 10, max(box["height"] - 1, 1))
            await page.mouse.move(x, y); await page.mouse.down(); await page.mouse.up()
            did_click = True
    if not did_click:
        with suppress(Exception):
            await apply.click(timeout=8000); did_click = True
    if not did_click:
        with suppress(Exception):
            await apply.evaluate("el => el.click()"); did_click = True

    form_on_origin = False
    for _ in range(12):
        if await detect_application_form(page):
            form_on_origin = True
            break
        await asyncio.sleep(0.5)

    await asyncio.sleep(4)

    target_page = None
    for t in (popup_ctx_task, popup_page_task):
        if t.done():
            with suppress(Exception):
                target_page = t.result()
                if target_page: break
    if not target_page:
        with suppress(Exception):
            target_page = await popup_ctx_task
    if not target_page:
        with suppress(Exception):
            target_page = await popup_page_task
    if not target_page:
        with suppress(Exception):
            await nav_task
        target_page = page

    with suppress(Exception):
        await target_page.wait_for_load_state("domcontentloaded", timeout=10000)
    with suppress(Exception):
        await target_page.wait_for_load_state("networkidle",      timeout=8000)

    final_url = (target_page.url or "").strip()

    def is_external(u: str) -> bool:
        try:
            return "justjoin.it" not in (urlparse(u).netloc or "").lower()
        except Exception:
            return False

    if (target_page is page) and candidate_href and (not final_url or final_url == page.url) and is_external(candidate_href):
        try:
            manual = await ctx.new_page()
            await manual.goto(candidate_href, wait_until="domcontentloaded", timeout=20000)
            with suppress(Exception):
                await manual.wait_for_load_state("networkidle", timeout=8000)
            target_page = manual
            final_url   = manual.url or candidate_href
        except Exception:
            final_url   = candidate_href

    if (target_page is page) and (not final_url or final_url == page.url):
        with suppress(Exception):
            hrefs = await page.eval_on_selector_all(
                "a[href]", "els => els.slice(0,80).map(a => a.href || a.getAttribute('href'))"
            )
            for h in hrefs or []:
                if not h: continue
                abs_h = urljoin(page.url, h)
                if is_external(abs_h):
                    try:
                        manual = await ctx.new_page()
                        await manual.goto(abs_h, wait_until="domcontentloaded", timeout=20000)
                        with suppress(Exception):
                            await manual.wait_for_load_state("networkidle", timeout=8000)
                        target_page = manual
                        final_url   = manual.url or abs_h
                        break
                    except Exception:
                        final_url   = abs_h
                        break

    if not final_url:
        final_url = page.url

    form_on_target = await detect_application_form(target_page)
    easy_apply = bool(form_on_origin or form_on_target)
    return (easy_apply, final_url)

# ------------------------------- Main async runner -------------------------------

def _load_config() -> Dict[str, Any]:
    cfg_path = os.environ.get("CONFIG", "config/config.json")
    p = Path(cfg_path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON in {cfg_path} at line {e.lineno}, column {e.colno}: {e.msg}"
        ) from e

async def run_with_config():
    cfg = _load_config()

    headful   = bool(cfg.get("HEADFUL", True))
    fail_fast = bool(cfg.get("FAIL_FAST", False))
    limit     = int(cfg.get("LIMIT", 0))
    keywords  = normalize_keywords(cfg.get("KEYWORDS"))

    # Read candidates WITHOUT consuming them
    rows = take_new_links(limit)
    if not rows:
        print("[INFO] No new links with new_href=true found.")
        return

    storage_state = str(STORAGE_STATE_JSON) if Path(STORAGE_STATE_JSON).exists() else None

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=not headful,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx_kwargs = {}
        if storage_state:
            ctx_kwargs["storage_state"] = storage_state
        ctx: BrowserContext = await browser.new_context(**ctx_kwargs)
        ctx.set_default_timeout(15000)

        for idx, row in enumerate(rows, start=1):
            ok = False
            try:
                ok = await process_one(ctx, row, keywords, headful, fail_fast)
            except Exception:
                # Already logged in process_one; honor FAIL_FAST by re-raising there
                ok = False
            if ok:
                # Consume only successful rows
                mark_link_consumed(row)
            # human-like delay regardless of outcome
            human_sleep(120, 260)

        await ctx.close()
        await browser.close()

def main():
    asyncio.run(run_with_config())

if __name__ == "__main__":
    main()
