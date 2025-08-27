# src/s3_filter_descriptions.py
# Async Playwright: read data/links.jsonl, open each URL (sequentially),
# extract Job Description, keyword-check, click Apply when matched,
# write augmented rows to data/filtered_links.jsonl.

import asyncio
import argparse
import json
import re
import traceback
from contextlib import suppress
from urllib.parse import urljoin, urlparse  # <- add urlparse
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Tuple

from playwright.async_api import (
    async_playwright, Page, Browser, BrowserContext,
    TimeoutError as PWTimeout
)

from .common import (
    DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR,
    LINKS_JSONL, FILTERED_JSONL, STATE_JSON, STORAGE_STATE_JSON,
    read_jsonl, append_jsonl, load_json, dump_json,
    now_iso, human_sleep
)

# ---------------------------- Utilities / helpers ----------------------------

DEFAULT_KEYWORDS = ["python", "playwright", "javascript", "typescript"]

def normalize_keywords(s: str | None) -> List[str]:
    if not s:
        return DEFAULT_KEYWORDS[:]
    parts = re.split(r"[,\s/]+", s)
    toks = [p.strip().lower() for p in parts if p.strip()]
    seen, out = set(), []
    for t in toks:
        if t not in seen:
            seen.add(t); out.append(t)
    return out or DEFAULT_KEYWORDS[:]

async def slow_scroll_page_to_bottom(page: Page, step_px: int = 400, max_steps: int = 120, pause_s: float = 3.6):
    """
    Slowly scroll the PAGE to the bottom (human-ish, ~10x slower than before).
    Helps trigger lazy content (incl. JD fragments).
    """
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
    """
    Prefer //h2/../../ (grandparent of any H2). Slowly scroll the PAGE until this
    container is fully revealed, then return its full inner_text. Fallback to robust CSS.
    """
    try:
        blocks = page.locator("xpath=//h2/../../")
        cnt = await blocks.count()
        if cnt > 0:
            texts = []
            for i in range(min(cnt, 8)):
                blk = blocks.nth(i)
                with suppress(Exception):
                    await blk.scroll_into_view_if_needed()

                # Scroll PAGE until bottom of this block is visible (very slow)
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
                    await asyncio.sleep(3.6)   # 10x slower

                await asyncio.sleep(7.5)       # 10x slower settle

                with suppress(Exception):
                    t = (await blk.inner_text(timeout=2000)).strip()
                    if len(t) > 50:
                        texts.append(t)

            if texts:
                texts.sort(key=len, reverse=True)
                return texts[0]
    except Exception:
        pass

    # Fallback heuristics
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
            if await loc.count() > 0:
                n = min(await loc.count(), 6)
                texts = []
                for i in range(n):
                    with suppress(Exception):
                        t = await loc.nth(i).inner_text(timeout=2000)
                        if t:
                            t = t.strip()
                            if len(t) > 50:
                                texts.append(t)
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
    """
    Heuristics to tell if an application form is visible on the page.
    Works for JustJoin modal/forms and common ATS (Typeform, Lever, Greenhouse, etc.).
    """
    # Fast textual hints
    text_hints = [
        "application form", "apply for this job", "apply now",
        "wyślij aplikację", "formularz aplikacyjny", "aplikuj",
        "upload cv", "resume", "cover letter"
    ]
    try:
        body_text = (await page.locator("body").inner_text(timeout=2000)).lower()
        if any(h in body_text for h in text_hints):
            return True
    except Exception:
        pass

    # Structural hints: inputs/buttons typical for apply flows
    structural_candidates = [
        "form:has(input), form:has(textarea), form:has(button)",
        "form[action*='apply'], form[action*='application']",
        "[data-testid*='apply']",
        "button:has-text('Apply')",
        "button:has-text('Aplikuj')",
        "button:has-text('Submit')",
        "input[type='file']",                     # resume upload
        "input[name*='resume'], input[name*='cv']",
        "textarea[name*='cover'], textarea[name*='motivation']",
    ]
    for sel in structural_candidates:
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

def truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n].rstrip() + "…"

def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)

def build_search_url(base_url: str, job: str) -> str:
    # kept for parity with other scripts (not used here)
    return base_url

# ---------------------- CLICK APPLY + DESTINATION DETECTION ----------------------


async def click_apply_and_detect(ctx: BrowserContext, page: Page) -> tuple[bool, str]:
    """
    Click Apply with a trusted user gesture, wait for popup/same-tab nav,
    and mark easy_apply True if an application form is visible EITHER
    on the origin page (modal) OR on the destination page.
    Returns: (easy_apply, final_url)
    """
    import re
    from contextlib import suppress
    from urllib.parse import urljoin, urlparse

    candidates = [
        re.compile(r"\bapply now?\b", re.I),
        re.compile(r"\bapply\b", re.I),
        re.compile(r"\baplikuj\b", re.I),
        re.compile(r"wyślij", re.I),
    ]

    # ---- find Apply control
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
        # No control; still check for form on current page
        easy_now = await detect_application_form(page)
        return (easy_now, page.url)

    # ---- best-guess external href (fallbacks later)
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
    candidate_href = urljoin(page.url, raw_hint) if 'raw_hint' in locals() and raw_hint else None

    # ---- listeners BEFORE click
    popup_ctx_task  = asyncio.create_task(ctx.wait_for_event("page",   timeout=15000))
    popup_page_task = asyncio.create_task(page.wait_for_event("popup", timeout=15000))
    nav_task        = asyncio.create_task(page.wait_for_event("framenavigated", timeout=15000))

    # ---- real user-like click (hover + coord click; then fallbacks)
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

    # ---- POLL the ORIGIN page for a modal form (captures JustJoin overlays)
    form_on_origin = False
    for _ in range(12):  # ~6s total (12 * 0.5s), runs while we also wait for popups/nav
        if await detect_application_form(page):
            form_on_origin = True
            break
        await asyncio.sleep(0.5)

    # ---- allow late popups/nav
    await asyncio.sleep(4)  # total ~10s together with the poll above

    # ---- collect popup if any
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

    # or same-tab nav
    if not target_page:
        with suppress(Exception):
            await nav_task
        target_page = page

    with suppress(Exception):
        await target_page.wait_for_load_state("domcontentloaded", timeout=10000)
    with suppress(Exception):
        await target_page.wait_for_load_state("networkidle",      timeout=8000)

    final_url = (target_page.url or "").strip()

    # ---- fallbacks (manually open hinted or first external anchor)
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

    # ---- ALSO check for a form on the DESTINATION page
    form_on_target = await detect_application_form(target_page)
    easy_apply = bool(form_on_origin or form_on_target)

    return (easy_apply, final_url)


# ------------------------------- Per-row processor -------------------------------

async def process_one(
    ctx: BrowserContext,
    row: Dict[str, Any],
    keywords: List[str],
    headful: bool,
    fail_fast: bool
) -> None:
    page: Page | None = None
    url = row.get("url")
    if not url:
        return

    try:
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        with suppress(Exception):
            await page.wait_for_load_state("networkidle", timeout=8000)

        # Visible slow scroll (helps reveal lazy sections + you see it working)
        await slow_scroll_page_to_bottom(page)

        # JD (slow-scroll inside the JD helper)
        desc = await get_job_description_text(page)
        desc_sample = truncate(desc, 800)

        # Keyword check (use the normalized default list here or pass in keywords)
        keyword_exists, matched = find_keywords(desc, keywords)

        result = {
            "id": row.get("id"),
            "data_index": row.get("data_index"),
            "url": url,
            "final_url": url,
            "keyword_exists": keyword_exists,
            "matched_keywords": matched,
            "easy_apply": False,
            "description_sample": desc_sample,
            "processed_at": now_iso(),
        }

        if not keyword_exists:
            append_jsonl(FILTERED_JSONL, result)
            await page.close()
            return

        # Apply & detect destination (≤2 tabs)
        easy_apply, final_url = await click_apply_and_detect(ctx, page)
        result["easy_apply"] = bool(easy_apply)
        result["final_url"]  = final_url


        append_jsonl(FILTERED_JSONL, result)


        # Close extra pages so we never exceed 2 open tabs
        for p in list(ctx.pages):
            if p is page:
                continue
            with suppress(Exception):
                await p.close()

        with suppress(Exception):
            await page.close()

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
    finally:
        if page and not page.is_closed():
            with suppress(Exception):
                await page.close()


# ------------------------------- Main async runner -------------------------------

async def run(
    headful: bool,
    keywords_csv: str,
    limit: int,
    fail_fast: bool
):
    Path(FILTERED_JSONL).parent.mkdir(parents=True, exist_ok=True)
    Path(FILTERED_JSONL).touch(exist_ok=True)

    keywords = normalize_keywords(keywords_csv)

    rows = list(read_jsonl(LINKS_JSONL))
    if limit > 0:
        rows = rows[:limit]

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

        for row in rows:
            await process_one(ctx, row, keywords, headful, fail_fast)
            human_sleep(120, 260)

        await ctx.close()
        await browser.close()

def main():
    parser = argparse.ArgumentParser(
        description="Open job links, extract Job Description, keyword-filter, and click Apply when matched."
    )
    parser.add_argument("--headful", type=str, default="true", help="true/false")
    parser.add_argument("--keywords", type=str, default="Python,Playwright,JavaScript,TypeScript",
                        help="Comma/slash/space separated keywords (case-insensitive).")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N links (0 = all).")
    parser.add_argument("--fail-fast", action="store_true", default=False,
                        help="Stop on first error (otherwise continue).")
    args = parser.parse_args()

    headful = str(args.headful).lower() == "true"

    asyncio.run(run(
        headful=headful,
        keywords_csv=args.keywords,
        limit=args.limit,
        fail_fast=args.fail_fast
    ))

if __name__ == "__main__":
    main()
