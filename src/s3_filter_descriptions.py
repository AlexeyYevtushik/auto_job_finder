# -----------------------------------------------------------------------------
# Purpose: Filter job descriptions and detect easy apply (S3 async worker).
# Behavior:
# - Reads new_href=true links, visits pages, extracts/cleans description,
#   finds keywords, detects Apply flow with the following priority:
#     1) Look for "1-click Apply": if present -> click, wait for "Application Completed".
#        On success => easy_apply=true, processed=true.
#     2) Otherwise click normal "Apply" and WAIT ONLY for a NEW TAB (popup).
#        If new tab appears => final_url=<new tab url>, easy_apply=false, processed=false.
#        If no new tab => easy_apply=false, processed=false (mode='no_new_tab').
# - If no Apply button at all => processed=true, outdated=true.
#
# - Writes results into filtered_links.jsonl using **one line per JSON object**.
# - IMPORTANT: Upsert semantics (shallow merge) with forced key order on write.
# - File I/O details:
#     * JSONL is strictly one object per line; no pretty/multi-line support.
#     * Updates are done via a temp file + atomic replace for safety.
# - Field order in every written/updated line:
#     * final_url, (other keys), url, description_sample
# -----------------------------------------------------------------------------

import asyncio
import json
import os
import re
import random
import traceback
import tempfile
import shutil
from contextlib import suppress
from urllib.parse import urlparse, urljoin
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Union, Optional

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

from .common import (
    DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR,
    LINKS_JSONL, FILTERED_JSONL, STORAGE_STATE_JSON,
    read_jsonl, append_jsonl,  # we keep these imports; only single-line JSONL is used
    now_iso, human_sleep
)

DEFAULT_KEYWORDS = ["python", "playwright", "javascript", "typescript"]


# ------------------------------- Logging -------------------------------------

def _log(msg: str) -> None:
    print(f"[S3] {msg}", flush=True)

def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)


# ------------------------- One-line JSONL + UPSERT ---------------------------

_SPECIAL_FIRST = "final_url"
_SPECIAL_MID_LAST = ("url", "description_sample")

def _ordered_for_dump(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a new dict ordered as:
      - final_url (if present)
      - all other keys in their current relative order (excluding special)
      - url (if present)
      - description_sample (if present)
    """
    out: Dict[str, Any] = {}
    if _SPECIAL_FIRST in d:
        out[_SPECIAL_FIRST] = d[_SPECIAL_FIRST]
    for k, v in d.items():
        if k == _SPECIAL_FIRST or k in _SPECIAL_MID_LAST:
            continue
        out[k] = v
    for k in _SPECIAL_MID_LAST:
        if k in d:
            out[k] = d[k]
    return out

def _dump_one_line(obj: Dict[str, Any]) -> str:
    """Dump object as a single-line JSON string with enforced key order."""
    return json.dumps(_ordered_for_dump(obj), ensure_ascii=False)

def _iter_jsonl_one_line(p: Path):
    """
    Yield dicts from a JSONL file where each line is exactly one JSON object.
    Lines that fail to parse are skipped.
    """
    if not p.exists():
        return
    with p.open("r", encoding="utf-8", newline="\n") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                # Skip malformed lines rather than fail the whole run
                continue

def _upsert_filtered_record_oneline(record: Dict[str, Any], match_by_final_url: bool = False) -> None:
    """
    Upsert into FILTERED_JSONL (one-line objects):
      - If existing line with same 'id' (and optionally same 'final_url') is found:
          shallow-merge 'record' into it, then replace that line with ordered single-line JSON.
      - Else: append a new single-line JSON object at the end.
    Atomic replace is used on update for safety.
    """
    path = Path(FILTERED_JSONL)
    rec_id = str(record.get("id") or "")
    rec_final_url = str(record.get("final_url") or "")

    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", encoding="utf-8", newline="\n") as fh:
            fh.write(_dump_one_line(record) + "\n")
        return

    found = False
    with path.open("r", encoding="utf-8", newline="\n") as src, \
         tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        for line in src:
            raw = line.rstrip("\n")
            s = raw.strip()
            if not s:
                # preserve blank lines if any
                tmp.write(line)
                continue
            try:
                obj = json.loads(s)
            except Exception:
                # keep malformed lines untouched
                tmp.write(line)
                continue

            if isinstance(obj, dict) and not found:
                oid = str(obj.get("id") or "")
                o_final = str(obj.get("final_url") or "")
                is_match = (oid == rec_id) and (o_final == rec_final_url if match_by_final_url else True)
                if is_match:
                    obj.update(record)  # shallow merge
                    tmp.write(_dump_one_line(obj) + "\n")
                    found = True
                    continue

            # non-matching line: copy as-is
            tmp.write(line)

        if not found:
            tmp.write(_dump_one_line(record) + "\n")

    shutil.move(str(tmp_path), str(path))


# ----------------------------- Keyword helpers -------------------------------

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
            seen.add(t)
            out.append(t)
    return out or DEFAULT_KEYWORDS[:]

def find_keywords(text: str, keywords: List[str]) -> Tuple[bool, List[str]]:
    text_l = text.lower()
    matched = [kw for kw in keywords if kw in text_l]
    return (len(matched) > 0, matched)


# ------------------------------- Page helpers --------------------------------

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
                # nudge rendering
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

    # Fallback candidates
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


# ---------------------- One-click Apply + completion helpers ------------------

_ONECLICK_RX = re.compile(
    r"\b(?:1[\-\u2011\u2013\u2014]?\s*click|one\s*click)\s*apply\b",
    re.I
)

# Completion signals. Keep liberal to handle toasts and dialogs in EN/PL.
_APP_DONE_RX = re.compile(
    r"(application (?:completed|complete|sent|submitted)|"
    r"(?:thanks|thank you).{0,40}(?:applic)|"
    r"(?:dziękujemy|wysłano|złożono).{0,40}(?:aplikacj))",
    re.I
)

async def find_one_click_apply(page: Page):
    """Find a '1-click Apply' CTA by role-first + fallback text filters."""
    # Role-first (most robust)
    for role in ("button", "link"):
        loc = page.get_by_role(role, name=_ONECLICK_RX)
        if await loc.count() > 0:
            return loc.first

    # Generic fallbacks
    candidates = ["button", "a", "[data-testid]", "[data-test]", "[aria-label]"]
    for sel in candidates:
        loc = page.locator(sel).filter(has_text=_ONECLICK_RX)
        if await loc.count() > 0:
            return loc.first
    return None

async def wait_application_completed(page: Page, timeout_ms: int = 20000) -> bool:
    """
    Wait for a visible signal that application has been completed/submitted.
    We search the entire document for matching text (toast/dialog/inline).
    """
    try:
        await page.wait_for_function(
            """(rx) => {
                const re = new RegExp(rx, 'i');
                const scan = (node) => {
                  if (!node) return false;
                  const txt = (node.innerText || node.textContent || '').trim();
                  if (txt && re.test(txt)) return true;
                  for (const el of (node.querySelectorAll?.('*') || [])) {
                    const t = (el.innerText || el.textContent || '').trim();
                    if (t && re.test(t)) return true;
                  }
                  return false;
                };
                return scan(document.body);
            }""",
            arg=_APP_DONE_RX.pattern,
            timeout=timeout_ms
        )
        return True
    except Exception:
        # As a fallback, quickly check common containers
        try:
            dlg = page.locator(
                "[role='dialog'], [aria-modal='true'], .modal, .dialog, "
                "[class*='toast' i], [class*='notification' i]"
            )
            if await dlg.count() > 0:
                match = await dlg.filter(has_text=_APP_DONE_RX).count()
                return match > 0
        except Exception:
            pass
        return False


# --------------------------- Apply detection flow ----------------------------

async def find_apply_button(page: Page):
    """
    Generic Apply button (non 1-click). We keep it simple and broad.
    """
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

async def click_apply_and_detect(ctx: BrowserContext, page: Page) -> Dict[str, Any]:
    """
    Detection policy required by the user:
      1) Try "1-click Apply".
         - If found, click and wait for "Application Completed".
           -> easy_apply=true; if completed => processed=true.
      2) Otherwise click normal "Apply" and WAIT ONLY for a NEW TAB.
         - If a new tab appears, final_url = new tab URL; easy_apply=false; processed=false.
         - If no new tab appears in time, return mode='no_new_tab' (no modal/nav handling).
      3) If no Apply at all, return apply_found=False.
    """
    # 1) PRIORITY: 1-click Apply
    one_click = await find_one_click_apply(page)
    if one_click:
        _log("Found 1-click Apply -> clicking")
        with suppress(Exception):
            await one_click.scroll_into_view_if_needed()
            await one_click.hover()
        clicked = False
        with suppress(Exception):
            await one_click.click(no_wait_after=True); clicked = True
        if not clicked:
            with suppress(Exception):
                await one_click.evaluate("el => el.click()"); clicked = True

        app_done = await wait_application_completed(page, timeout_ms=20000)
        return {
            "apply_found": True,
            "one_click": True,
            "app_completed": bool(app_done),
            "clicked": clicked,
            "easy_apply": True,            # 1-click = easy_apply
            "final_url": page.url or "",
            "mode": "oneclick_success" if app_done else "oneclick_timeout"
        }

    # 2) FALLBACK: normal Apply -> WAIT ONLY for a NEW TAB
    apply = await find_apply_button(page)
    if not apply:
        _log(f"[{page.url}] Apply button NOT found")
        return {
            "apply_found": False,
            "one_click": False,
            "app_completed": False,
            "clicked": False,
            "easy_apply": False,
            "final_url": page.url or "",
            "mode": "none"
        }

    _log("Pressing Apply (expecting a new tab only)")
    pages_before = list(ctx.pages)
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

    # Wait only for a new tab/popup, not for modal or same-tab navigation
    try:
        for _ in range(60):  # ~12s total (60 * 200ms)
            new_pages = [p for p in ctx.pages if p not in pages_before]
            if new_pages:
                new_page = new_pages[0]
                with suppress(Exception):
                    await new_page.wait_for_load_state("domcontentloaded", timeout=15000)
                with suppress(Exception):
                    await new_page.wait_for_load_state("networkidle", timeout=8000)
                final_url = new_page.url or (pre_href or "") or (page.url or "")
                return {
                    "apply_found": True,
                    "one_click": False,
                    "app_completed": False,
                    "clicked": clicked,
                    "easy_apply": False,
                    "final_url": final_url,
                    "mode": "popup"
                }
            await asyncio.sleep(0.2)

        # No popup/new tab in time -> "no_new_tab"
        final_url = pre_href or (page.url or "")
        return {
            "apply_found": True,
            "one_click": False,
            "app_completed": False,
            "clicked": clicked,
            "easy_apply": False,
            "final_url": final_url,
            "mode": "no_new_tab"
        }
    except Exception:
        final_url = pre_href or (page.url or "")
        return {
            "apply_found": True,
            "one_click": False,
            "app_completed": False,
            "clicked": clicked,
            "easy_apply": False,
            "final_url": final_url,
            "mode": "error"
        }
    finally:
        # Close unexpected extras (if any)
        extras = [p for p in ctx.pages if p not in pages_before and p is not page]
        for p in extras:
            with suppress(Exception):
                await p.close()


# ------------------ Description cleaning to visible rows ---------------------

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


# ----------------------------- Link management -------------------------------

def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def take_new_links(limit: int) -> List[Dict[str, Any]]:
    all_rows = list(read_jsonl(LINKS_JSONL))  # assumed one-line JSONL reader
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


# ------------------------------ Config loading -------------------------------

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


# --------------------------------- S3 core -----------------------------------

async def process_one(ctx: BrowserContext, row: Dict[str, Any], keywords: List[str], headful: bool, fail_fast: bool) -> bool:
    page: Optional[Page] = None
    url = row.get("url")
    if not url:
        return False
    try:
        _log(f'Processing new link: "{url}"')
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        with suppress(Exception):
            await page.wait_for_load_state("networkidle", timeout=8000)

        # Scroll (as requested) before trying to detect the apply flow
        await slow_scroll_page_to_bottom(page)

        # Extract description + keywords
        desc_full = await get_job_description_text(page)
        desc_rows = to_visible_rows(desc_full)
        keyword_exists, matched = find_keywords(desc_full, keywords)

        # Prepare result (note: final_url initially equals url)
        result = {
            "id": row.get("id"),
            "data_index": row.get("data_index"),
            "final_url": url,  # order enforced on write
            "keyword_exists": keyword_exists,
            "matched_keywords": matched,
            "easy_apply": False,
            "processed_at": now_iso(),
            "processed": False,
            "url": url,  # placed later in ordered dump
            "description_sample": desc_rows,  # placed last in ordered dump
        }

        # If no keywords -> mark processed=true directly
        if not keyword_exists:
            result["processed"] = True
            _upsert_filtered_record_oneline(result, match_by_final_url=True)
            _log("Processed (no keywords matched)")
            with suppress(Exception):
                await page.close()
            return True

        # Detect apply path
        info = await click_apply_and_detect(ctx, page)
        result["easy_apply"] = bool(info["easy_apply"])
        result["final_url"] = info["final_url"]

        # No Apply at all -> mark as outdated + processed
        if not info["apply_found"]:
            result["outdated"] = True
            result["processed"] = True
            _upsert_filtered_record_oneline(result, match_by_final_url=False)
            _log("Processed (apply not found) -> outdated=true")
            with suppress(Exception):
                await page.close()
            return True

        # Successful 1-click completion
        if info.get("one_click") and info.get("app_completed"):
            result["processed"] = True
            _upsert_filtered_record_oneline(result, match_by_final_url=False)
            _log("Processed (1-click completed)")
            with suppress(Exception):
                await page.close()
            return True

        # Normal Apply branch (WAIT ONLY for a new tab)
        # - popup/new tab -> final_url already set by detector
        # - no new tab -> keep processed=False (to be handled in later stages if needed)
        _upsert_filtered_record_oneline(result, match_by_final_url=False)
        _log(f"Processed (mode={info.get('mode')})")

        # Cleanup extra pages
        for p in list(ctx.pages):
            if p is page:
                continue
            with suppress(Exception):
                await p.close()
        with suppress(Exception):
            await page.close()
        return True

    except Exception:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENSHOTS_DIR / f"s3_{safe_filename(row.get('id') or 'item')}_{ts}.png"
        txt = ERRORS_DIR / f"s3_{safe_filename(row.get('id') or 'item')}_{ts}.txt"
        with suppress(Exception):
            if page:
                await page.screenshot(path=str(png), full_page=True)
        with txt.open("w", encoding="utf-8") as f:
            f.write(f"TIME: {now_iso()}\nURL: {url}\n\nTRACEBACK:\n{traceback.format_exc()}\n")
        print(f"[ERROR] s3_filter: saved {png.name} and {txt.name}")
        if fail_fast:
            raise
        return False
    finally:
        if page and not page.is_closed():
            with suppress(Exception):
                await page.close()


async def run_with_config():
    cfg = _load_config()
    headful = bool(cfg.get("HEADFUL", True))
    fail_fast = bool(cfg.get("FAIL_FAST", False))
    limit = int(cfg.get("LIMIT", 0))
    keywords = normalize_keywords(cfg.get("KEYWORDS"))
    storage_state = str(STORAGE_STATE_JSON) if Path(STORAGE_STATE_JSON).exists() else None

    short_min = int(cfg.get("SHORT_TIMEOUT_MIN", 60))
    short_max = int(cfg.get("SHORT_TIMEOUT_MAX", 180))
    long_min = int(cfg.get("LONG_TIMEOUT_MIN", 300))
    long_max = int(cfg.get("LONG_TIMEOUT_MAX", 660))

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=not headful,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-popup-blocking",
            ],
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
