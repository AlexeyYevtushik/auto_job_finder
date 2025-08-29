# src/s2_collect_links.py
# -------------------------------------------------------------------------------
# CONFIG-DRIVEN LINK COLLECTOR (S2) — ASYNC VERSION
# - Config: config/config.json (or env CONFIG=<path>)
# - For each JOB_NAME × LOCATION:
#     * Opens https://justjoin.it/job-offers/<location>?keyword=<job>
#     * Finds FIRST //ul/li[@data-index]//a[@href], hovers it (NO CLICKS)
#     * LOOP until page bottom:
#         - collect all visible anchors (URL-dedupe within run)
#         - try scroll by 100px; if no movement -> press ArrowDown 10 times
#     * Appends NEW rows to data/links.jsonl (global URL dedupe)
#     * Row includes: id, data_index, job_name, location, url, new_href:true
# - TARGET_INDEXES early-stop: stop when encountering that data-index value.
# - STRICT NO-CLICK mode (cookie click toggleable).
# - If no FIRST element by //ul/li[@data-index]//a[@href] -> print "No vacancies" and skip.
# -------------------------------------------------------------------------------

import os, json, urllib.parse, random, asyncio, contextlib, traceback
from typing import List, Tuple, Set
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

from .common import (
    read_jsonl, append_jsonl, load_json,
    LINKS_JSONL, STATE_JSON, now_iso,
    ERRORS_DIR, SCREENSHOTS_DIR, STORAGE_STATE_JSON
)

# ----------------------------- Config handling -----------------------------

DEFAULT_CONFIG = {
    "JOB_NAMES": ["QA Automation"],
    "LOCATIONS": ["poland-remote", "remote"],
    "HEADFUL": True,
    "TARGET_INDEXES": 1000,
    "FAIL_FAST": True,

    # strict no-click toggles
    "ALLOW_COOKIE_CLICK": False,
    "ALLOW_LOAD_MORE_CLICK": False,  # reserved; not used (we don't click "Show more")
}

def _guess_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]

def load_config() -> dict:
    """
    Load JSON config from:
      1) env CONFIG (path to JSON),
      2) <repo_root>/config/config.json,
      3) <this_dir>/config.json,
      else DEFAULT_CONFIG.
    """
    # 1) CONFIG env var
    env_path = os.environ.get("CONFIG")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass  # fall through

    # 2) repo_root/config/config.json
    repo_guess = _guess_repo_root() / "config" / "config.json"
    if repo_guess.is_file():
        try:
            return json.loads(repo_guess.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 3) src/s2_collect_links.py sibling config.json
    sibling = Path(__file__).with_name("config.json")
    if sibling.is_file():
        try:
            return json.loads(sibling.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 4) fallback
    return DEFAULT_CONFIG.copy()

# ----------------------------- Helpers -----------------------------

def build_search_url(base_url: str, job: str, location: str) -> str:
    """
    https://justjoin.it/job-offers/<location>?keyword=<job>
    """
    base = base_url.rstrip("/")
    path = f"/job-offers/{location.strip('/')}"
    q = urllib.parse.urlencode({"keyword": job})
    return f"{base}{path}?{q}"

def preload_seen_urls() -> Set[str]:
    """
    Collect ALL previously-saved URLs from data/links.jsonl for URL-based dedupe.
    """
    seen: Set[str] = set()
    for row in read_jsonl(LINKS_JSONL):
        u = row.get("url")
        if isinstance(u, str) and u.strip():
            seen.add(u.strip())
    return seen

async def async_handle_error(page: Page | None, prefix: str, step_info: str, fail_fast: bool):
    """Screenshot + trace writer for async flow (best effort)."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    png = SCREENSHOTS_DIR / f"{prefix}_{ts}.png"
    txt = ERRORS_DIR / f"{prefix}_{ts}.txt"

    try:
        if page:
            await page.screenshot(path=str(png), full_page=True)
    except Exception:
        pass

    tb = traceback.format_exc()
    txt.write_text(
        f"TIME: {now_iso()}\nSTEP: {step_info}\n\nTRACEBACK:\n{tb}\n",
        encoding="utf-8"
    )
    print(f"[ERROR] {prefix}: saved {png.name} and {txt.name}")
    if fail_fast:
        raise

async def safe_close(context: BrowserContext | None, browser: Browser | None) -> None:
    with contextlib.suppress(Exception):
        if context:
            await context.close()
    with contextlib.suppress(Exception):
        if browser:
            await browser.close()

TARGET_STOP_VALUE: str | None = None  # (keep this global)

# ------------------------------ Playwright helpers ------------------------------

async def _accept_cookies_if_any(page: Page, allow_click: bool) -> None:
    """
    Best-effort cookie handling. If clicking is not allowed, do nothing.
    """
    if not allow_click:
        return
    try:
        for sel in [
            "button:has-text('Accept')",
            "button:has-text('I agree')",
            "button:has-text('Akceptuj')",
            "button:has-text('Zgadzam')",
        ]:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=1000)
                break
    except Exception:
        pass

# Find scroll container selector (if any), resilient to SPA re-renders (selector only)
async def _locate_scroll_container_selector(page: Page) -> str | None:
    token = "".join(random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(8))
    try:
        sel = await page.evaluate("""
        (tok) => {
          const first =
            document.querySelector("ul li[data-index] a[href*='/job-offer/']") ||
            document.querySelector("[data-index] a[href*='/job-offer/']") ||
            document.querySelector("a[href*='/job-offer/']");
          const findScrollable = (start) => {
            let el = start;
            while (el) {
              const cs = getComputedStyle(el);
              const ovy = cs.overflowY;
              if ((ovy === 'auto' || ovy === 'scroll') && el.scrollHeight > el.clientHeight + 8) {
                return el;
              }
              el = el.parentElement;
            }
            return null;
          };
          const sc = findScrollable(first || document.body);
          if (sc && sc !== document.body && sc !== document.documentElement) {
            sc.setAttribute("data-jjf-scroll", tok);
            return `[data-jjf-scroll="${tok}"]`;
          }
          return null;
        }
        """, token)
        return sel
    except Exception:
        return None

async def _focus_scroll_target(page: Page, container_sel: str | None) -> None:
    try:
        await page.evaluate(
            """
            (sel) => {
              const el = sel ? document.querySelector(sel)
                             : (document.scrollingElement || document.documentElement);
              if (!el) return false;
              if (el !== document.body && el !== document.documentElement && !el.hasAttribute('tabindex')) {
                el.setAttribute('tabindex','-1');
              }
              try { el.focus({ preventScroll: true }); } catch(e) { el.focus(); }
              return true;
            }
            """,
            container_sel
        )
    except Exception:
        pass

async def _is_at_bottom(page: Page, container_sel: str | None) -> bool:
    try:
        at_doc_bottom = await page.evaluate(
            "() => { const el = document.scrollingElement || document.documentElement; "
            "return Math.ceil(el.scrollTop + window.innerHeight) >= el.scrollHeight - 2; }"
        )
    except Exception:
        at_doc_bottom = False

    at_cont_bottom = False
    if container_sel:
        with contextlib.suppress(Exception):
            at_cont_bottom = await page.evaluate(
                "(sel) => { const el = document.querySelector(sel); if (!el) return false; "
                "return Math.ceil(el.scrollTop + el.clientHeight) >= el.scrollHeight - 2; }",
                container_sel
            )
    return bool(at_doc_bottom or at_cont_bottom)

async def _scroll_step_100(page: Page, container_sel: str | None) -> bool:
    """
    Try to scroll by exactly 100px. Return True if any scrollTop changed.
    """
    try:
        return await page.evaluate(
            """
            (sel) => {
              const tryEl = (el, dy=100) => {
                if (!el) return false;
                const before = el.scrollTop || 0;
                if (typeof el.scrollBy === 'function') el.scrollBy(0, dy);
                else el.scrollTop = before + dy;
                return (el.scrollTop || 0) !== before;
              };

              // 1) dedicated container
              if (sel) {
                const el = document.querySelector(sel);
                if (tryEl(el)) return true;
              }

              // 2) window/document
              const de = document.scrollingElement || document.documentElement;
              const beforeWin = (window.scrollY || de.scrollTop || 0);
              window.scrollBy(0, 100);
              const afterWin = (window.scrollY || de.scrollTop || 0);
              if (afterWin !== beforeWin) return true;

              // 3) nearest scrollable ancestor of first card (safety)
              const first =
                document.querySelector("ul li[data-index] a[href*='/job-offer/']") ||
                document.querySelector("[data-index] a[href*='/job-offer/']") ||
                document.querySelector("a[href*='/job-offer/']");
              let cur = first ? first.parentElement : null;
              let hops = 0;
              while (cur && hops < 8) {
                const cs = getComputedStyle(cur);
                if ((cs.overflowY === 'auto' || cs.overflowY === 'scroll') && cur.scrollHeight > cur.clientHeight + 8) {
                  const b = cur.scrollTop || 0;
                  cur.scrollTop = b + 100;
                  if ((cur.scrollTop || 0) !== b) return true;
                }
                cur = cur.parentElement;
                hops++;
              }

              return false;
            }
            """,
            container_sel
        )
    except Exception:
        return False

async def _press_arrow_down_10(page: Page) -> None:
    with contextlib.suppress(Exception):
        await page.keyboard.press("Escape")  # dismiss focus traps
    for _ in range(10):
        with contextlib.suppress(Exception):
            await page.keyboard.press("ArrowDown")
        await asyncio.sleep(random.uniform(0.06, 0.12))

async def _detect_and_close_overlays(page: Page) -> None:
    """
    Soft overlay mitigation: ESC a couple times + restore overflow on html/body.
    """
    for _ in range(2):
        with contextlib.suppress(Exception):
            await page.keyboard.press("Escape")
        await asyncio.sleep(0.12)

    with contextlib.suppress(Exception):
        await page.evaluate("""
        () => {
          const html = document.documentElement;
          const body = document.body;
          const htmlHidden = getComputedStyle(html).overflowY === 'hidden' || getComputedStyle(html).overflow === 'hidden';
          const bodyHidden = getComputedStyle(body).overflowY === 'hidden' || getComputedStyle(body).overflow === 'hidden';
          if (htmlHidden) { html.style.overflowY = 'auto'; html.style.overflow = 'auto'; }
          if (bodyHidden) { body.style.overflowY = 'auto'; body.style.overflow = 'auto'; }
        }
        """)

# ------------------------------ Core: hover + incremental collect + scroll ------------------------------

XPATH_STRICT_FIRST = "xpath=//ul/li[@data-index]//a[@href]"
XPATH_ANCHORS_UNION = (
    "xpath=("
    "//ul/li[@data-index]//a[@href] | "
    "//*[(self::li or self::div) and @data-index]//a[@href] | "
    "//a[contains(@href, '/job-offer/')]"
    ")"
)
XPATH_IDX_PARENT = "xpath=ancestor::*[(self::li or self::div) and @data-index][1]"

async def _hover_first_required(page: Page) -> bool:
    """
    Find FIRST //ul/li[@data-index]//a[@href], hover it, NO CLICK.
    """
    loc = page.locator(XPATH_STRICT_FIRST)
    try:
        await loc.first.wait_for(timeout=4000)
    except Exception:
        return False
    with contextlib.suppress(Exception):
        await loc.first.hover(timeout=1500)
    return True

async def _collect_visible_anchors(page: Page, seen_local: Set[str], results: List[Tuple[str, str]]) -> Tuple[bool, int]:
    """
    Sweep current DOM and append NEW (data_index, abs_url) into results, local dedupe by URL.
    Returns (hit_target_stop, added_count)
    """
    added = 0
    anchors = page.locator(XPATH_ANCHORS_UNION)
    try:
        count = await anchors.count()
    except Exception:
        count = 0

    for i in range(count):
        try:
            a = anchors.nth(i)

            idx_parent = a.locator(XPATH_IDX_PARENT).first
            di = None
            try:
                if await idx_parent.count() > 0:
                    raw = await idx_parent.get_attribute("data-index")
                    di = raw.strip() if raw else None
            except Exception:
                di = None

            href = (await a.get_attribute("href")) or ""
            if not href:
                continue
            abs_url = urllib.parse.urljoin(page.url, href)

            if abs_url in seen_local:
                continue

            di_eff = di or f"u{len(results)+1}"
            results.append((di_eff, abs_url))
            seen_local.add(abs_url)
            added += 1

            if TARGET_STOP_VALUE and di and di == str(TARGET_STOP_VALUE):
                return True, added
        except Exception:
            continue
    return False, added

async def collect_incremental(page: Page) -> list[tuple[str, str]]:
    """
    Incremental collector:
      - ensure first required anchor exists + hover
      - find/focus scroll container
      - loop:
          * collect visible anchors
          * if bottom -> break
          * try scroll by 100px; if no movement -> ArrowDown x10
          * overlay mitigation occasionally
      - return all collected pairs
    """
    # Wait initial load
    with contextlib.suppress(Exception):
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
    with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=15000)

    # FIRST anchor + hover
    if not await _hover_first_required(page):
        print("[INFO] No vacancies detected on this page (no FIRST //ul/li[@data-index]//a[@href]).")
        return []

    # Locate scroll target and focus
    container_sel = await _locate_scroll_container_selector(page)
    await _focus_scroll_target(page, container_sel)

    results: List[Tuple[str, str]] = []
    seen_local: Set[str] = set()
    stalls = 0
    sweeps_without_add = 0

    while True:
        # 1) collect visible anchors
        hit_stop, added = await _collect_visible_anchors(page, seen_local, results)
        if hit_stop:
            break

        # bottom?
        if await _is_at_bottom(page, container_sel):
            break

        if added == 0:
            sweeps_without_add += 1
        else:
            sweeps_without_add = 0

        # 2) try scroll by 100px
        moved = await _scroll_step_100(page, container_sel)
        if not moved:
            stalls += 1
            await _press_arrow_down_10(page)
        else:
            stalls = 0

        # mitigate & refocus occasionally
        if stalls in (2, 4) or sweeps_without_add in (2, 4):
            await _detect_and_close_overlays(page)
            await _focus_scroll_target(page, container_sel)

        # small human-like pause
        await asyncio.sleep(random.uniform(0.06, 0.16))

        # loop continues until bottom reached

    # final sweep (grab anything newly visible at the very bottom)
    await _collect_visible_anchors(page, seen_local, results)
    return results

# ------------------------------ Saver / Dedupe -------------------------------

def save_if_new_href(data_index: str, url: str, seen_urls: Set[str],
                     job_name: str, location: str) -> bool:
    if not url:
        return False
    u = url.strip()
    if u in seen_urls:
        return False

    append_jsonl(
        LINKS_JSONL,
        {
            "id": f"jj-{data_index}",
            "data_index": str(data_index),
            "job_name": job_name,
            "location": location,
            "url": u,
            "new_href": True,
        },
    )
    seen_urls.add(u)
    return True

# -------------------------------------- Runner --------------------------------------

async def collect_for(job_name: str, location: str, headful: bool, fail_fast: bool,
                      allow_cookie_click: bool, allow_load_more_click: bool) -> None:
    page: Page | None = None
    browser: Browser | None = None
    context: BrowserContext | None = None
    try:
        state = load_json(STATE_JSON, {})
        base_url = state.get("base_url") or "https://justjoin.it/"

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=not headful)
            ctx_kwargs = {}
            if Path(STORAGE_STATE_JSON).exists():
                ctx_kwargs["storage_state"] = str(STORAGE_STATE_JSON)
            context = await browser.new_context(**ctx_kwargs)
            page = await context.new_page()

            search_url = build_search_url(base_url, job_name, location)
            print(f"[INFO] Open search: {search_url}  (job='{job_name}', location='{location}')")

            await page.goto(search_url, wait_until="domcontentloaded")
            with contextlib.suppress(Exception):
                await page.wait_for_load_state("networkidle", timeout=6000)

            # cookies (click only if allowed)
            await _accept_cookies_if_any(page, allow_cookie_click)

            # incremental collect (hover -> collect/scroll loop)
            pairs = await collect_incremental(page)

            # save with global URL-dedupe
            seen_urls = preload_seen_urls()
            if not pairs:
                print(f"[INFO] No collectable anchors for job='{job_name}', location='{location}'.")
            else:
                added = 0
                for data_index, url in pairs:
                    if save_if_new_href(data_index, url, seen_urls, job_name, location):
                        added += 1
                print(f"[OK] Added {added} NEW hrefs to {LINKS_JSONL}. Total known hrefs: {len(seen_urls)}")

            with contextlib.suppress(Exception):
                await context.storage_state(path=str(STORAGE_STATE_JSON))

    except Exception:
        await async_handle_error(page, "s2_collect", f"collect_for[{job_name}|{location}]", fail_fast)
    finally:
        await safe_close(context, browser)

# -------------------------------------- Main ----------------------------------------

async def main_async():
    cfg = load_config()
    job_names = list(cfg.get("JOB_NAMES", DEFAULT_CONFIG["JOB_NAMES"]))
    locations = list(cfg.get("LOCATIONS", DEFAULT_CONFIG["LOCATIONS"]))
    headful = bool(cfg.get("HEADFUL", True))
    target_idx = cfg.get("TARGET_INDEXES", None)
    fail_fast = bool(cfg.get("FAIL_FAST", False))
    allow_cookie_click = bool(cfg.get("ALLOW_COOKIE_CLICK", False))
    allow_load_more_click = bool(cfg.get("ALLOW_LOAD_MORE_CLICK", False))

    global TARGET_STOP_VALUE
    TARGET_STOP_VALUE = str(target_idx) if target_idx is not None else None

    Path(LINKS_JSONL).parent.mkdir(parents=True, exist_ok=True)
    Path(LINKS_JSONL).touch(exist_ok=True)

    run_no = 0
    for li, location in enumerate(locations):
        for ji, job_name in enumerate(job_names):
            run_no += 1
            print(f"\n=== RUN #{run_no}: job='{job_name}' | location='{location}' ===")
            await collect_for(
                job_name,
                location,
                headful=headful,
                fail_fast=fail_fast,
                allow_cookie_click=allow_cookie_click,
                allow_load_more_click=allow_load_more_click
            )

            is_last = (li == len(locations)-1) and (ji == len(job_names)-1)
            if not is_last:
                delay = random.randint(60, 240)  # 1–4 minutes
                print(f"[PAUSE] Waiting {delay} seconds before next run…")
                await asyncio.sleep(delay)

if __name__ == "__main__":
    asyncio.run(main_async())
