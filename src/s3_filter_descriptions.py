# ---------------------------------------------------------------------------
# Purpose: Filter job descriptions and detect easy apply (S3 async worker).
# Steps:
# 1) Load config & keywords; read new_href=true links. 2) Open URL, extract JD, match keywords.
# 3) Click Apply: if JJ/in-page modal → easy_apply=True; if popup/nav → final_url to destination, easy_apply=False; if no Apply → outdated.
# 4) Clean description_sample to visible rows (from "All offers" up to before "Apply") and pretty-write each JSON record (multi-line) to filtered_links.jsonl.
# 5) Mark link consumed, throttle between items/batches.
# ---------------------------------------------------------------------------

import asyncio
import json
import os
import re
import random
import traceback
from contextlib import suppress
from urllib.parse import urlparse, urljoin
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Union, Optional
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from .common import (
    DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR,
    LINKS_JSONL, FILTERED_JSONL, STORAGE_STATE_JSON,
    read_jsonl, append_jsonl,  # keep import; we override with our pretty writer below
    now_iso, human_sleep
)

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

def _log(msg: str) -> None:
    print(f"[S3] {msg}", flush=True)

def append_pretty_jsonl(path: Union[str, Path], obj: Dict[str, Any]) -> None:
    """Pretty-print each JSON object (multi-line) to .jsonl."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(obj, ensure_ascii=False, indent=1) + "\n")

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

def _is_justjoin(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
        return host.endswith("justjoin.it")
    except Exception:
        return False

async def detect_jj_easy_apply_modal(page: Page) -> bool:
    if not _is_justjoin(page.url or ""):
        return False
    try:
        dlg = page.locator("[role='dialog']")
        if await dlg.count() == 0:
            dlg = page.locator("div[aria-modal='true']")
        if await dlg.count() == 0:
            return False
        has_heading = await dlg.filter(has_text=re.compile(r"Application form|Formularz aplikacyjny", re.I)).count() > 0
        has_form  = await dlg.locator("form").count() > 0
        has_apply = await dlg.locator("button:has-text('Apply'), button:has-text('Aplikuj')").count() > 0
        hint_intro = await dlg.locator("textarea[placeholder*='Introduce yourself' i]").count() > 0
        hint_name  = await dlg.filter(has_text=re.compile(r"First and last name|Imię i nazwisko", re.I)).count() > 0
        hint_mail  = await dlg.filter(has_text=re.compile(r"Email", re.I)).count() > 0
        score = sum([has_heading, has_form, has_apply, hint_intro, (hint_name or hint_mail)])
        return has_form and has_apply and score >= 3
    except Exception:
        return False

async def detect_easy_apply_modal(page: Page) -> bool:
    try:
        dlg = page.locator("[role='dialog'], div[aria-modal='true'], .modal, .Dialog, .dialog, [class*='modal']")
        if await dlg.count() == 0:
            return False
        dlg = dlg.first
        has_form   = await dlg.locator("form").count() > 0
        has_inputs = (await dlg.locator("input[type='email'], input[type='text'], input[type='file'], textarea").count()) > 0
        has_cv     = (await dlg.locator("input[type='file'], [data-testid*='cv' i], [data-test*='cv' i]").count()) > 0
        text_hits = await dlg.filter(
            has_text=re.compile(
                r"(apply|send application|submit application|application form|confirm application|"
                r"aplikuj|wyślij|formularz aplikacyjny|złóż aplikację)", re.I)
        ).count() > 0
        has_submit_btn = (
            await dlg.locator(
                "button:has-text('Apply'), button:has-text('Submit'), button:has-text('Send'), "
                "button:has-text('Aplikuj'), button:has-text('Wyślij')"
            ).count()
        ) > 0
        score = sum([has_form, has_inputs, text_hits, has_submit_btn]) + (1 if has_cv else 0)
        return score >= 3
    except Exception:
        return False

def find_keywords(text: str, keywords: List[str]) -> Tuple[bool, List[str]]:
    text_l = text.lower()
    matched = [kw for kw in keywords if kw in text_l]
    return (len(matched) > 0, matched)

def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)

def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def take_new_links(limit: int) -> List[Dict[str, Any]]:
    all_rows = list(read_jsonl(LINKS_JSONL))
    new_rows = [r for r in all_rows if r.get("new_href") is True]
    if limit and limit > 0:
        return new_rows[:limit]
    return new_rows

def mark_link_consumed(row: Dict[str, Any]) -> None:
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

async def _extract_probable_href(page: Page, loc) -> Optional[str]:
    with suppress(Exception):
        href = await loc.get_attribute("href")
        if href:
            return urljoin(page.url, href)
    with suppress(Exception):
        href = await loc.get_attribute("data-href")
        if href:
            return urljoin(page.url, href)
    with suppress(Exception):
        href = await loc.get_attribute("data-url")
        if href:
            return urljoin(page.url, href)
    with suppress(Exception):
        handle = await loc.element_handle()
        if handle:
            href = await page.evaluate(
                """(el) => {
                    const a = el.closest && el.closest('a');
                    return a ? a.href : null;
                }""",
                handle
            )
            if href:
                return href
    return None

# ---- description_sample cleaning helpers (for pretty, visible rows) ----

import re as _re

def _strip_invisibles(text: str) -> str:
    if not text:
        return ""
    return _re.sub(r"[\u200B-\u200D\uFEFF]", "", text)

def _slice_between_markers(lines: List[str]) -> List[str]:
    """
    Keep from the FIRST 'All offers' (inclusive) up to BEFORE the NEXT 'Apply' (exclusive).
    Case-insensitive. If 'All offers' missing -> return original. If 'Apply' missing -> keep to end.
    """
    norm = [ln.strip() for ln in lines]
    try:
        start = next(i for i, ln in enumerate(norm) if ln.lower() == "all offers")
    except StopIteration:
        return lines
    end = None
    for j in range(start + 1, len(norm)):
        if norm[j].lower() == "apply":
            end = j
            break
    return lines[start:end] if end is not None else lines[start:]

def to_visible_rows(text: str) -> List[str]:
    if not text:
        return []
    t = _strip_invisibles(text).replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.rstrip() for ln in t.split("\n")]
    lines = _slice_between_markers(lines)
    lines = [ln.strip() for ln in lines if ln.strip()]
    return lines

# ------------------------------------------------------------------------

async def process_one(ctx: BrowserContext, row: Dict[str, Any], keywords: List[str], headful: bool, fail_fast: bool) -> bool:
    page: Optional[Page] = None
    url = row.get("url")
    if not url:
        return False
    try:
        _log(f'New link is processing: "{url}"')
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        with suppress(Exception):
            await page.wait_for_load_state("networkidle", timeout=8000)
        await slow_scroll_page_to_bottom(page)
        desc_full = await get_job_description_text(page)

        # CLEAN description into visible rows:
        desc_rows = to_visible_rows(desc_full)

        keyword_exists, matched = find_keywords(desc_full, keywords)

        result = {
            "id": row.get("id"),
            "data_index": row.get("data_index"),
            "url": url,
            "final_url": url,
            "keyword_exists": keyword_exists,
            "matched_keywords": matched,
            "easy_apply": False,
            "description_sample": desc_rows,  # store as list of visible rows
            "processed_at": now_iso(),
            "processed": False,
        }

        if not keyword_exists:
            result["processed"] = True
            append_pretty_jsonl(FILTERED_JSONL, result)
            _log("Processed")
            with suppress(Exception): await page.close()
            return True

        info = await click_apply_and_detect(ctx, page)
        result["easy_apply"] = bool(info["easy_apply"])
        result["final_url"]  = info["final_url"]

        if not info["apply_found"]:
            result["outdated"]  = True
            result["processed"] = True
            _log("Processed")
            append_pretty_jsonl(FILTERED_JSONL, result)
            with suppress(Exception): await page.close()
            return True

        append_pretty_jsonl(FILTERED_JSONL, result)
        _log("Processed")

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

async def find_apply_button(page: Page):
    regexes = [re.compile(pat, re.I) for pat in [
        r"\bapply now?\b", r"\bapply\b", r"\bsubmit application\b", r"\bsend application\b",
        r"\baplikuj\b", r"\bwyślij\b"
    ]]
    for rx in regexes:
        for by_role in ("button", "link"):
            loc = page.get_by_role(by_role, name=rx)
            if await loc.count() > 0:
                return loc.first
    candidates = [
        "[data-testid*='apply' i]", "[data-test*='apply' i]",
        "button[type='submit']", "button[name*='apply' i]", "[aria-label*='apply' i]",
        "a[href*='apply' i]", "button:has-text('Apply')", "a:has-text('Apply')",
        "button:has-text('Aplikuj')", "a:has-text('Aplikuj')",
        "button:has-text('Submit')", "button:has-text('Send')",
        "a:has-text('Submit')", "a:has-text('Send')",
    ]
    for sel in candidates:
        loc = page.locator(sel)
        if await loc.count() > 0:
            return loc.first
    return None

async def _cancel_pending(tasks: List[asyncio.Task]) -> None:
    if not tasks:
        return
    for t in tasks:
        if not t.done():
            t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

async def click_apply_and_detect(ctx: BrowserContext, page: Page) -> Dict[str, Any]:
    apply = await find_apply_button(page)
    if not apply:
        _log(f"[{page.url}] Apply button NOT found")
        return {"apply_found": False, "clicked": False, "easy_apply": False, "final_url": page.url or "", "mode": "none"}

    _log("Pressing Apply")

    pages_before = list(ctx.pages)
    orig_url = page.url
    pre_href = await _extract_probable_href(page, apply)

    with suppress(Exception):
        await apply.scroll_into_view_if_needed()
        await apply.hover()

    clicked = False
    with suppress(Exception):
        await apply.click(no_wait_after=True); clicked = True
    if not clicked:
        with suppress(Exception):
            await apply.evaluate("el => el.click()"); clicked = True

    try:
        for _ in range(60):  # ~12s
            try:
                if await detect_easy_apply_modal(page) or await detect_jj_easy_apply_modal(page):
                    final_url = page.url or ""
                    easy = _is_justjoin(final_url)
                    return {"apply_found": True, "clicked": clicked, "easy_apply": easy, "final_url": final_url, "mode": "modal"}
            except Exception:
                pass

            new_pages = [p for p in ctx.pages if p not in pages_before]
            if new_pages:
                new_page = new_pages[0]
                with suppress(Exception):
                    await new_page.wait_for_load_state("domcontentloaded", timeout=15000)
                with suppress(Exception):
                    await new_page.wait_for_load_state("networkidle", timeout=8000)
                final_url = new_page.url or (pre_href or "")
                if not final_url:
                    final_url = page.url or ""
                # popup/new tab → easy_apply = False
                return {"apply_found": True, "clicked": clicked, "easy_apply": False, "final_url": final_url, "mode": "popup"}

            if (page.url or "") != (orig_url or ""):
                with suppress(Exception):
                    await page.wait_for_load_state("domcontentloaded", timeout=8000)
                with suppress(Exception):
                    await page.wait_for_load_state("networkidle", timeout=6000)
                final_url = page.url or ""
                # same-tab navigation → easy_apply = False
                return {"apply_found": True, "clicked": clicked, "easy_apply": False, "final_url": final_url, "mode": "nav"}

            await asyncio.sleep(0.2)

        # timeout
        final_url = pre_href or (page.url or "")
        return {"apply_found": True, "clicked": clicked, "easy_apply": False, "final_url": final_url, "mode": "timeout"}
    except Exception:
        final_url = pre_href or (page.url or "")
        return {"apply_found": True, "clicked": clicked, "easy_apply": False, "final_url": final_url, "mode": "error"}
    finally:
        extras = [p for p in ctx.pages if p not in pages_before and p is not page]
        for p in extras:
            with suppress(Exception):
                await p.close()

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
    storage_state = str(STORAGE_STATE_JSON) if Path(STORAGE_STATE_JSON).exists() else None

    short_min = int(cfg.get("SHORT_TIMEOUT_MIN", 60))
    short_max = int(cfg.get("SHORT_TIMEOUT_MAX", 180))
    long_min  = int(cfg.get("LONG_TIMEOUT_MIN", 300))
    long_max  = int(cfg.get("LONG_TIMEOUT_MAX", 660))

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=not headful,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-popup-blocking"
            ]
        )
        ctx_kwargs = {}
        if storage_state:
            ctx_kwargs["storage_state"] = storage_state
        ctx: BrowserContext = await browser.new_context(**ctx_kwargs)
        ctx.set_default_timeout(15000)

        batch_num = 0
        while True:
            rows = take_new_links(limit)
            if not rows:
                if batch_num == 0:
                    print("[INFO] No new links with new_href=true found.")
                break

            batch_num += 1
            print(f"[S3] === BATCH #{batch_num}: processing {len(rows)} item(s) ===")

            for idx, row in enumerate(rows, start=1):
                ok = False
                try:
                    ok = await process_one(ctx, row, keywords, headful, fail_fast)
                except Exception:
                    ok = False
                if ok:
                    mark_link_consumed(row)
                await asyncio.sleep(random.uniform(short_min, short_max))

            has_more = bool(take_new_links(1))
            if has_more:
                print(f"[S3] Batch done. Waiting {int(long_min//60)}–{int(long_max//60)} minutes before next batch...")
                has_more = bool(take_new_links(1))

            if has_more:
                wait_s = random.uniform(long_min, long_max)
                print(
                    f"[S3] Batch done. Waiting ~{int(wait_s//60)} minutes "
                    f"({int(wait_s)} s) before next batch..."
                )
                await asyncio.sleep(wait_s)
            else:
                print("[S3] All new_href:true links are processed.")

        await ctx.close()
        await browser.close()

def main():
    asyncio.run(run_with_config())

if __name__ == "__main__":
    main()
