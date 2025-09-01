# -----------------------------------------------------------------------------
# s2_collect_links.py — Async JustJoin.it link collector
#
# - Loads config (default + config/config.json).
# - Builds search URL for each JOB_NAME × LOCATION.
# - Opens search page with Playwright Chromium (headful/headless).
# - Hovers first job card anchor to activate list.
# - Scrolls down 100px steps; if stuck, presses ArrowDown.
# - After each scroll, scans anchors //ul/li[@data-index]//a[@href].
# - Deduplicates globally (data/links.jsonl) and locally in run.
# - Saves new rows: {id, data_index, job_name, location, url, new_href:true}.
# - Stops when LIMIT reached (strictly > LIMIT to mark limit_hit), bottom, or timeout.
# - After each run waits: SHORT or LONG timeout depending on limit_hit.
# - Saves screenshots + error trace if exceptions (errors/, screenshots/).
# - Updates storage_state.json for session persistence.
# - Safe close of context/browser always.
# - Idempotent: does not duplicate existing URLs.
# - Run via: python -m src.s2_collect_links
# -----------------------------------------------------------------------------

import os, json, urllib.parse, random, asyncio, contextlib, traceback, time
from typing import List, Tuple, Set, Optional
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from .common import (
    read_jsonl, append_jsonl, load_json,
    LINKS_JSONL, STATE_JSON, now_iso,
    ERRORS_DIR, SCREENSHOTS_DIR, STORAGE_STATE_JSON
)

DEFAULT_CONFIG = {
    "JOB_NAMES": ["QA Automation"],
    "LOCATIONS": ["poland-remote", "remote"],
    "HEADFUL": True,
    "TARGET_INDEXES": 1000,
    "FAIL_FAST": True,
    "ALLOW_COOKIE_CLICK": False,
    "MAX_LOOP_SECONDS": 320,
    "SLEEP_MIN": 0.06,
    "SLEEP_MAX": 0.16,
    # NOTE: LIMIT intentionally NOT in defaults — must come from config/config.json
    "SHORT_TIMEOUT_MIN": 60,
    "SHORT_TIMEOUT_MAX": 180,
    "LONG_TIMEOUT_MIN": 300,
    "LONG_TIMEOUT_MAX": 660,
}

def log(msg: str, **kv):
    ts = datetime.now().strftime("%H:%M:%S")
    extras = " ".join(f"{k}={v}" for k,v in kv.items())
    print(f"[INFO] {ts} {msg} {extras}".strip())

def _guess_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]

def load_config() -> dict:
    env_path = os.environ.get("CONFIG")
    if env_path and Path(env_path).is_file():
        return {**DEFAULT_CONFIG, **json.loads(Path(env_path).read_text("utf-8"))}
    repo_guess = _guess_repo_root() / "config" / "config.json"
    if repo_guess.is_file():
        return {**DEFAULT_CONFIG, **json.loads(repo_guess.read_text("utf-8"))}
    return DEFAULT_CONFIG.copy()

def build_search_url(base_url: str, job: str, location: str) -> str:
    base = base_url.rstrip("/")
    path = f"/job-offers/{location.strip('/')}"
    q = urllib.parse.urlencode({"keyword": job})
    return f"{base}{path}?{q}"

def preload_seen_urls() -> Set[str]:
    seen: Set[str] = set()
    for row in read_jsonl(LINKS_JSONL):
        # tolerate dirty lines
        if isinstance(row, dict):
            u = row.get("url")
            if isinstance(u, str) and u.strip():
                seen.add(u.strip())
    return seen

async def async_handle_error(page: Optional[Page], prefix: str, step_info: str, fail_fast: bool):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    png = SCREENSHOTS_DIR / f"{prefix}_{ts}.png"
    txt = ERRORS_DIR / f"{prefix}_{ts}.txt"
    try:
        if page: await page.screenshot(path=str(png), full_page=True)
    except: pass
    tb = traceback.format_exc()
    txt.write_text(f"{now_iso()}\nSTEP:{step_info}\n{tb}\n","utf-8")
    print(f"[ERROR] {prefix} saved {png.name} {txt.name}")
    if fail_fast: raise

async def safe_close(context: Optional[BrowserContext], browser: Optional[Browser]):
    with contextlib.suppress(Exception):
        if context: await context.close()
    with contextlib.suppress(Exception):
        if browser: await browser.close()

TARGET_STOP_VALUE: Optional[str] = None
XPATH_STRICT = "xpath=//ul/li[@data-index]//a[@href]"

async def _hover_first(page: Page) -> bool:
    try:
        loc = page.locator(XPATH_STRICT)
        await loc.first.wait_for(timeout=4000)
        await loc.first.hover(timeout=1500)
        return True
    except:
        return False

async def _is_at_bottom(page: Page) -> bool:
    js = "() => {const el=document.scrollingElement||document.documentElement;return Math.ceil(el.scrollTop+el.clientHeight)>=el.scrollHeight-2;}"
    try:
        return await page.evaluate(js)
    except:
        return False

async def _scroll_step(page: Page) -> bool:
    js = "() => {const el=document.scrollingElement||document.documentElement;let b=el.scrollTop;window.scrollBy(0,100);return (el.scrollTop-b)>0;}"
    try:
        return await page.evaluate(js)
    except:
        return False

async def _press_down(page: Page):
    with contextlib.suppress(Exception): await page.keyboard.press("Escape")
    for _ in range(10):
        with contextlib.suppress(Exception): await page.keyboard.press("ArrowDown")
        await asyncio.sleep(0.05)

def _save_new_if_needed(di: str, url: str, seen_global: Set[str], job: str, loc: str) -> bool:
    if not url or url in seen_global:
        return False
    append_jsonl(LINKS_JSONL, {
        "id": f"jj-{di}",
        "data_index": str(di),
        "job_name": job,
        "location": loc,
        "url": url,
        "new_href": True
    })
    seen_global.add(url)
    log("Collected new", url=url)
    return True

async def _scan_and_save(page: Page, seen_global: Set[str], job: str, loc: str, results_in_run: List[Tuple[str,str]]) -> int:
    added_count = 0
    try:
        handles = await page.locator(XPATH_STRICT).element_handles()
    except:
        return 0
    for h in handles:
        try:
            href = await h.get_attribute("href")
            if not href:
                continue
            abs_url = urllib.parse.urljoin(page.url, href)
            if any(abs_url == u for _, u in results_in_run):
                continue
            di = "u" + str(len(results_in_run) + 1)
            if _save_new_if_needed(di, abs_url, seen_global, job, loc):
                results_in_run.append((di, abs_url))
                added_count += 1
                if TARGET_STOP_VALUE and di == str(TARGET_STOP_VALUE):
                    break
        except:
            continue
    return added_count

async def collect_incremental(page: Page, cfg: dict, job: str, loc: str, seen_global: Set[str]) -> Tuple[int, bool]:
    # LIMIT comes strictly from user config; if missing or 0 => unlimited
    limit = int(cfg.get("LIMIT", 0) or 0)

    try:
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
    except:
        pass

    if not await _hover_first(page):
        log("Process started, but no anchors found")
        return 0, False

    results_in_run: List[Tuple[str,str]] = []
    start = time.monotonic()
    log("Process started to find jobs...", job=job, location=loc, limit=limit)

    total_new = await _scan_and_save(page, seen_global, job, loc, results_in_run)

    # limit_hit is only True when strictly more than LIMIT were collected
    limit_hit = (limit > 0 and total_new > limit)
    if limit_hit:
        log("Finished (limit_hit)", total=total_new)
        return total_new, True

    bottom_reached = False
    while True:
        if time.monotonic() - start > cfg["MAX_LOOP_SECONDS"]:
            print("[WARN] Loop timeout reached")
            break
        if await _is_at_bottom(page):
            bottom_reached = True
            break
        moved = await _scroll_step(page)
        if not moved:
            await _press_down(page)
        await asyncio.sleep(random.uniform(cfg["SLEEP_MIN"], cfg["SLEEP_MAX"]))
        total_new += await _scan_and_save(page, seen_global, job, loc, results_in_run)

        # again, strictly > LIMIT to count as "limit_hit"
        if limit > 0 and total_new > limit:
            limit_hit = True
            break

        if TARGET_STOP_VALUE and any(di == str(TARGET_STOP_VALUE) for di, _ in results_in_run):
            break

    if bottom_reached:
        log("Scrolled down")

    log("Finished", total=total_new)
    return total_new, limit_hit

async def collect_for(job: str, loc: str, cfg: dict) -> bool:
    page=None; browser=None; ctx=None
    try:
        state = load_json(STATE_JSON, {})
        base = state.get("base_url") or "https://justjoin.it/"
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=not cfg["HEADFUL"])
            ctx = await browser.new_context(storage_state=str(STORAGE_STATE_JSON) if Path(STORAGE_STATE_JSON).exists() else None)
            page = await ctx.new_page()
            url = build_search_url(base, job, loc)
            log("Opening search", url=url)
            await page.goto(url, wait_until="domcontentloaded")

            seen_global = preload_seen_urls()
            added, limit_hit = await collect_incremental(page, cfg, job, loc, seen_global)
            log("Done", added=added, total=len(seen_global), limit_hit=limit_hit)

            with contextlib.suppress(Exception):
                await ctx.storage_state(path=str(STORAGE_STATE_JSON))
            return limit_hit
    except Exception:
        await async_handle_error(page, "s2_collect", f"{job}|{loc}", cfg["FAIL_FAST"])
        return False
    finally:
        await safe_close(ctx, browser)

async def main_async():
    cfg = load_config()
    global TARGET_STOP_VALUE
    if cfg.get("TARGET_INDEXES"):
        TARGET_STOP_VALUE = str(cfg["TARGET_INDEXES"])

    # timeouts (seconds)
    short_min = int(cfg.get("SHORT_TIMEOUT_MIN", 60))
    short_max = int(cfg.get("SHORT_TIMEOUT_MAX", 180))
    long_min  = int(cfg.get("LONG_TIMEOUT_MIN", 300))
    long_max  = int(cfg.get("LONG_TIMEOUT_MAX", 660))

    # just to make it explicit in logs which LIMIT is active
    active_limit = int(cfg.get("LIMIT", 0) or 0)
    log("Config loaded", limit=active_limit, short=f"{short_min}-{short_max}s", long=f"{long_min}-{long_max}s")

    Path(LINKS_JSONL).parent.mkdir(parents=True, exist_ok=True)
    Path(LINKS_JSONL).touch(exist_ok=True)

    for loc in cfg["LOCATIONS"]:
        for job in cfg["JOB_NAMES"]:
            limit_hit = await collect_for(job, loc, cfg)
            if limit_hit:
                wait = random.randint(long_min, long_max)
            else:
                wait = random.randint(short_min, short_max)
            log("Waiting for next run", minutes=round(wait / 60, 1), limit_hit=limit_hit)
            await asyncio.sleep(wait)

if __name__ == "__main__":
    asyncio.run(main_async())
