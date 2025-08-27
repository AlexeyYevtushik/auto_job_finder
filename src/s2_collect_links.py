# src/s2_collect_links.py
# -------------------------------------------------------------------------------
# CONFIG-DRIVEN LINK COLLECTOR (S2) — ASYNC VERSION
# - Config: config/config.json (or env CONFIG=<path>)
# - For each JOB_NAME × LOCATION:
#     * Opens https://justjoin.it/job-offers/<location>?keyword=<job>
#     * Walks li/div[@data-index] job anchors + fallback anchors containing '/job-offer/'
#     * Human-like scroll: 2–5 short scrolls, then 1–2s pause; repeat until end
#     * Appends NEW rows to data/links.jsonl (DEDUPE **BY URL** only)
#     * Row includes: id, data_index, job_name, location, url, new_href:true
# - Waits 1–4 minutes between runs.
# - TARGET_INDEXES early-stop: stop when encountering that data-index value.
# - Robust to SPA re-renders (no element handles kept; scroll container tracked by selector).
# - STRICT NO-CLICK mode by default (configurable).
# -------------------------------------------------------------------------------

import os, json, urllib.parse, random, asyncio, time, contextlib
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

    # NEW: strict no-click mode (both False by default)
    "ALLOW_COOKIE_CLICK": False,
    "ALLOW_LOAD_MORE_CLICK": False,
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
    env_path = os.environ.get("CONFIG")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))

    repo_guess = _guess_repo_root() / "config" / "config.json"
    if repo_guess.is_file():
        return json.loads(repo_guess.read_text(encoding="utf-8"))

    sibling = Path(__file__).with_name("config.json")
    if sibling.is_file():
        return json.loads(sibling.read_text(encoding="utf-8"))

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

def to_abs(current_url: str, href: str) -> str:
    return urllib.parse.urljoin(current_url, href)

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

def normalize_index_value(val: str | None) -> str | None:
    if not val:
        return None
    s = str(val).strip()
    return s if s else None

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

    txt.write_text(
        f"TIME: {now_iso()}\nSTEP: {step_info}\n\nTRACEBACK:\n",
        encoding="utf-8"
    )
    print(f"[ERROR] {prefix}: saved {png.name} and {txt.name}")
    if fail_fast:
        raise

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
                # one click only if explicitly allowed
                await loc.first.click(timeout=1000)
                break
    except Exception:
        pass


TARGET_STOP_VALUE: str | None = None  # (keep this global)

# ---------- Scroll container discovery that survives SPA re-renders (selector only)

async def _locate_scroll_container_selector(page: Page) -> str | None:
    """
    Marks the scrollable container that holds job cards with a unique data attribute
    and returns a CSS selector string for it. If nothing special is found, returns None
    to signal 'use document.scrollingElement'.
    """
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
    """
    Focuses the scroll container (or the document scroller) WITHOUT clicking.
    Makes it keyboard-focusable by adding tabindex=-1 if needed.
    """
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

async def _scroll_by(page: Page, container_sel: str | None, step: int):
    """Scroll either the tagged container (by selector) or the document."""
    try:
        await page.evaluate(
            """
            (sel, dy) => {
              const el = sel ? document.querySelector(sel)
                             : (document.scrollingElement || document.documentElement);
              if (!el) return;
              if (typeof el.scrollBy === 'function') el.scrollBy(0, dy);
              else el.scrollTop = (el.scrollTop || 0) + dy;
            }
            """,
            container_sel, step
        )
    except Exception:
        with contextlib.suppress(Exception):
            await page.mouse.wheel(0, step)

async def _maybe_click_load_more(page: Page, allow_click: bool) -> bool:
    """
    Optionally click 'Load more'/'Show more'/PL variants if present.
    Returns True if clicked. With allow_click=False, never clicks.
    """
    if not allow_click:
        return False
    labels = ["Load more", "Show more", "Pokaż więcej", "Zobacz więcej"]
    for lab in labels:
        try:
            loc = page.locator(f"button:has-text('{lab}')")
            if await loc.count() > 0 and await loc.first.is_enabled():
                await loc.first.click(timeout=1500)
                await asyncio.sleep(0.5)
                return True
        except Exception:
            continue
    return False

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

async def _arrow_down_batch(page: Page, presses: int = 10) -> None:
    # keep target focused
    with contextlib.suppress(Exception):
        await page.keyboard.press("Escape")  # dismiss stray focus traps softly
    for _ in range(presses):
        with contextlib.suppress(Exception):
            await page.keyboard.press("ArrowDown")
        await asyncio.sleep(random.uniform(0.08, 0.18))
    # settle a bit so the SPA can render newly revealed items
    await asyncio.sleep(random.uniform(1.0, 2.0))


async def _detect_and_close_overlays(page: Page) -> dict:
    """
    Detect common scroll-blocking overlays and try to dismiss them softly:
      - [role=dialog]/[role=alertdialog]
      - position:fixed; top:0; big height (>= 40% viewport)
      - body/html overflow hidden
    Actions: send ESC a few times; restore overflow on html/body.
    Returns a dict with diagnostics.
    """
    diag = {"found": False, "restored_overflow": False, "esc_sent": 0}

    # Send a few ESC presses (soft close)
    for _ in range(3):
        with contextlib.suppress(Exception):
            await page.keyboard.press("Escape")
            diag["esc_sent"] += 1
        await asyncio.sleep(0.15)

    try:
        res = await page.evaluate("""
        () => {
          const html = document.documentElement;
          const body = document.body;

          const getVis = el => {
            const cs = getComputedStyle(el);
            return !(cs.display === 'none' || cs.visibility === 'hidden' || parseFloat(cs.opacity || '1') === 0);
          };

          const dialogs = Array.from(document.querySelectorAll('[role="dialog"], [role="alertdialog"]')).filter(getVis);

          const fixedTop = Array.from(document.querySelectorAll('body *')).filter(el => {
            const cs = getComputedStyle(el);
            if (!getVis(el)) return false;
            if (cs.position !== 'fixed') return false;
            const top = parseInt(cs.top || '0', 10);
            const h = el.getBoundingClientRect().height;
            return (top === 0) && (h >= window.innerHeight * 0.4);
          });

          const htmlHidden = getComputedStyle(html).overflowY === 'hidden' || getComputedStyle(html).overflow === 'hidden';
          const bodyHidden = getComputedStyle(body).overflowY === 'hidden' || getComputedStyle(body).overflow === 'hidden';

          // Try to restore overflow if hidden
          let restored = false;
          if (htmlHidden) { html.style.overflowY = 'auto'; html.style.overflow = 'auto'; restored = true; }
          if (bodyHidden) { body.style.overflowY = 'auto'; body.style.overflow = 'auto'; restored = true; }

          return {
            dialogs: dialogs.length,
            fixedTop: fixedTop.length,
            restored
          };
        }
        """)
        diag["found"] = (res.get("dialogs", 0) > 0) or (res.get("fixedTop", 0) > 0)
        diag["restored_overflow"] = bool(res.get("restored"))
    except Exception:
        pass

    # A couple more ESC presses after style restore
    if diag["found"]:
        for _ in range(2):
            with contextlib.suppress(Exception):
                await page.keyboard.press("Escape")
                diag["esc_sent"] += 1
            await asyncio.sleep(0.15)

    return diag


async def _arrow_down_to_bottom(page: Page, container_sel: str | None, max_presses: int = 1000) -> bool:
    """
    Fallback: press ArrowDown repeatedly (with 1–2s pauses) until bottom or cap.
    Occasionally send End to nudge. Returns True if bottom reached.
    """
    # make sure the scroll target has focus
    await _focus_scroll_target(page, container_sel)

    for i in range(max_presses):
        with contextlib.suppress(Exception):
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.7, 1.0))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(random.uniform(0.1, 0.4))
        await asyncio.sleep(random.uniform(2.0, 3.0))

        if await _is_at_bottom(page, container_sel):
            return True
    return await _is_at_bottom(page, container_sel)

async def _scroll_step(page: Page, container_sel: str | None, dy: int = 100) -> bool:
    try:
        moved = await page.evaluate(
            """
            (sel, dy) => {
              const getDocEl = () => document.scrollingElement || document.documentElement;
              const tryEl = (el) => {
                if (!el) return false;
                const before = el.scrollTop || 0;
                if (typeof el.scrollBy === 'function') el.scrollBy(0, dy);
                else el.scrollTop = before + dy;
                return (el.scrollTop || 0) !== before;
              };

              // container
              if (sel) {
                const el = document.querySelector(sel);
                if (tryEl(el)) return true;
              }

              // window/document
              const docEl = getDocEl();
              const beforeWin = (window.scrollY || docEl.scrollTop || 0);
              window.scrollBy(0, dy);
              const afterWin = (window.scrollY || docEl.scrollTop || 0);
              if (afterWin !== beforeWin) return true;

              // ancestors of first card
              const first = document.querySelector("ul li[data-index] a[href*='/job-offer/'], [data-index] a[href*='/job-offer/'], a[href*='/job-offer/']");
              let cur = first ? first.parentElement : null;
              let hops = 0;
              while (cur && hops < 8) {
                const cs = getComputedStyle(cur);
                if ((cs.overflowY === 'auto' || cs.overflowY === 'scroll') && cur.scrollHeight > cur.clientHeight + 8) {
                  const b = cur.scrollTop || 0;
                  cur.scrollTop = b + dy;
                  if ((cur.scrollTop || 0) !== b) return true;
                }
                cur = cur.parentElement;
                hops++;
              }

              // tallest scrollable element
              let best = null, maxH = 0;
              for (const el of Array.from(document.querySelectorAll('body *'))) {
                const cs = getComputedStyle(el);
                if ((cs.overflowY === 'auto' || cs.overflowY === 'scroll') && el.scrollHeight > el.clientHeight + 8) {
                  if (el.scrollHeight > maxH) { maxH = el.scrollHeight; best = el; }
                }
              }
              if (best) {
                const b = best.scrollTop || 0;
                best.scrollTop = b + dy;
                if ((best.scrollTop || 0) !== b) return true;
              }

              return false;
            }
            """,
            container_sel, dy
        )
        if moved:
            return True

        with contextlib.suppress(Exception):
            await page.keyboard.press("PageDown")
            return True

        return False
    except Exception:
        return False

# ------------------------------ Core collector --------------------------------

async def human_simple_scroll(page: Page, container_sel: str | None) -> None:
    """
    Step 100px -> wait 1–2s -> until bottom.
    If scroll doesn't move 5x in a row: try to detect/close overlays (ESC + restore overflow).
    If scroll doesn't move 10x in a row: fallback to ArrowDown until bottom.
    """
    no_move_streak = 0
    while True:
        moved = await _scroll_step(page, container_sel, dy=100)
        if not moved:
            no_move_streak += 1
        else:
            no_move_streak = 0

        await asyncio.sleep(random.uniform(1.0, 2.0))

        # bottom?
        if await _is_at_bottom(page, container_sel):
            break

        # 5 attempts w/o movement => try overlay mitigation
        if no_move_streak == 5:
            diag = await _detect_and_close_overlays(page)
            print(f"[INFO] Overlay mitigation: found={diag['found']} restored_overflow={diag['restored_overflow']} esc_sent={diag['esc_sent']}")
            # refocus scroll target after overlays
            await _focus_scroll_target(page, container_sel)

        # 10 attempts w/o movement => switch to ArrowDown fallback
        if no_move_streak >= 10:
            print("[WARN] Scroll didn't move for 10 attempts; switching to ArrowDown fallback...")
            reached = await _arrow_down_to_bottom(page, container_sel, max_presses=400)
            if reached:
                break
            else:
                # even fallback failed — stop to avoid infinite loop
                print("[ERROR] ArrowDown fallback did not reach bottom; stopping.")
                break


async def get_urls_and_indexes(
    page: Page,
    allow_click_load_more: bool  # kept for signature compatibility, unused
) -> list[tuple[str, str]]:  # (data_index, absolute_url)
    XPATH_ANCHORS = (
        "xpath=("
        "//ul/li[@data-index]//a[@href] | "
        "//*[(self::li or self::div) and @data-index]//a[@href] | "
        "//a[contains(@href, '/job-offer/')]"
        ")"
    )
    XPATH_IDX_PARENT = "xpath=ancestor::*[(self::li or self::div) and @data-index][1]"

    results: List[Tuple[str, str]] = []
    seen_local_urls: Set[str] = set()

    # 1) Wait content
    with contextlib.suppress(Exception):
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
    with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=15000)
    await page.wait_for_selector(XPATH_ANCHORS, timeout=20000)

    # 2) Find/focus scroller (NO CLICK)
    container_sel = await _locate_scroll_container_selector(page)
    await _focus_scroll_target(page, container_sel)

    # 3) Hover first card (NO CLICK)
    anchors = page.locator(XPATH_ANCHORS)
    if await anchors.count() > 0:
        with contextlib.suppress(Exception):
            await anchors.first.hover(timeout=1500)

    # helper: sweep current DOM and append NEW urls (local dedupe)
    async def sweep_and_collect() -> tuple[bool, int]:
        added = 0
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

                if abs_url in seen_local_urls:
                    continue

                di_eff = di or f"u{len(results)+1}"
                results.append((di_eff, abs_url))
                seen_local_urls.add(abs_url)
                added += 1

                if TARGET_STOP_VALUE and di and di == str(TARGET_STOP_VALUE):
                    return True, added
            except Exception:
                continue
        return False, added

    # initial sweep (collect what’s visible at the top)
    early, _ = await sweep_and_collect()
    if early:
        return results

    # 4) MAIN LOOP: 10 ArrowDown presses -> collect -> repeat
    no_progress = 0
    prev_seen_total = await anchors.count()
    while True:
        # ensure focus on the target (SPA may swap containers)
        await _focus_scroll_target(page, container_sel)

        # batch: 10 ArrowDown presses with small delays
        await _arrow_down_batch(page, presses=10)

        # collect newly revealed items
        early, added = await sweep_and_collect()
        if early:
            return results

        # stop if bottom reached
        if await _is_at_bottom(page, container_sel):
            break

        # progress detection
        with contextlib.suppress(Exception):
            cur_total = await anchors.count()
        progressed = (cur_total > prev_seen_total) or (added > 0)
        prev_seen_total = cur_total

        if progressed:
            no_progress = 0
        else:
            no_progress += 1
            # overlay mitigation after two "no progress" batches
            if no_progress == 2:
                diag = await _detect_and_close_overlays(page)
                print(f"[INFO] Overlay mitigation: found={diag['found']} restored_overflow={diag['restored_overflow']} esc_sent={diag['esc_sent']}")
                await _focus_scroll_target(page, container_sel)
            # gentle nudge with End every 3rd stall
            if no_progress % 3 == 0:
                with contextlib.suppress(Exception):
                    await page.keyboard.press("End")
                await asyncio.sleep(0.4)
            # safety: break after too many stalls to avoid infinite loops
            if no_progress >= 8:
                print("[WARN] No scroll progress for 8 batches — stopping to avoid hang.")
                break

    # final sweep
    await sweep_and_collect()
    return results

# ------------------------------ Saver / Dedupe -------------------------------

def save_if_new_href(data_index: str, url: str, seen_urls: Set[str],
                     job_name: str, location: str) -> bool:
    """
    Append to data/links.jsonl IFF URL is NEW (URL-based dedupe).
    Row shape:
      {"id":"jj-<data-index>", "data_index":"<data-index>", "job_name":..., "location":..., "url":"<url>", "new_href": true}
    """
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
    page = None
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

            seen_urls = preload_seen_urls()
            pairs = await get_urls_and_indexes(page, allow_click_load_more=allow_load_more_click)

            added = 0
            for data_index, url in pairs:
                if save_if_new_href(data_index, url, seen_urls, job_name, location):
                    added += 1

            with contextlib.suppress(Exception):
                await context.storage_state(path=str(STORAGE_STATE_JSON))

            print(f"[OK] Added {added} NEW hrefs to {LINKS_JSONL}. Total known hrefs: {len(seen_urls)}")

            await context.close()
            await browser.close()

    except Exception:
        await async_handle_error(page, "s2_collect", f"collect_for[{job_name}|{location}]", fail_fast)
        try:
            if context:
                await context.close()
        finally:
            if browser:
                await browser.close()

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
