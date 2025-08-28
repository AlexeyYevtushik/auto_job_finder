# src/s4_easy_apply.py
# S4: Auto-complete "easy apply" forms on justjoin.it.
# - Input: data/filtered_links.jsonl
# - Takes ONLY rows where easy_apply=true AND processed=false
# - Visits row["url"], opens Application form (same page or popup), fills "Introduce yourself", ticks consents if present, submits.
# - Success criteria: processed=true ONLY when Application Confirmation is visible.
#
# Run:
#   python -m src.s4_easy_apply
#
# Config: config/config.json
#   HEADFUL (bool), LIMIT (int), FAIL_FAST (bool), INTRODUCE_YOURSELF (str),
#   ALLOW_COOKIE_CLICK (bool)

import asyncio
import json
import re
import traceback
from contextlib import suppress
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from playwright.async_api import (
    async_playwright, Page, Browser, BrowserContext
)

from .common import (
    DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR, STORAGE_STATE_JSON,
    read_jsonl, now_iso, human_sleep
)

def _log(msg: str) -> None:
    print(f"[S3] {msg}", flush=True)

# ---------- constants/paths ----------
INPUT_JSONL = DATA_DIR / "filtered_links.jsonl"
CONFIG_PATH = Path("config/config.json")

# ---------- small utils ----------
def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s or "item")

def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {
            "HEADFUL": True,
            "LIMIT": 0,
            "FAIL_FAST": False,
            "ALLOW_COOKIE_CLICK": True,
            "INTRODUCE_YOURSELF": "Github: https://github.com/AlexeyYevtushik\nLinkedIn: https://www.linkedin.com/in/alexey-yevtushik/",
        }
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def match_row(r: Dict[str, Any], key_id, val_id, key_url, val_url) -> bool:
    """Prefer strict match by both id & url when both present; else fallback."""
    has_id = key_id in r and val_id is not None
    has_url = key_url in r and val_url is not None
    if has_id and has_url:
        return r.get(key_id) == val_id and r.get(key_url) == val_url
    if has_id:
        return r.get(key_id) == val_id
    if has_url:
        return r.get(key_url) == val_url
    return False

def update_row_inplace(match_id_key: str, match_id_val, match_url_key: str, match_url_val, patch: Dict[str, Any]) -> bool:
    """Patch matched row and rewrite JSONL. Returns True if updated."""
    rows = list(read_jsonl(INPUT_JSONL))
    updated = False
    for r in rows:
        if match_row(r, match_id_key, match_id_val, match_url_key, match_url_val):
            r.update(patch)
            updated = True
            break
    if updated:
        write_jsonl(INPUT_JSONL, rows)
    return updated

def _print(msg: str):
    print(f"[S4] {msg}", flush=True)

# ---------- cookie wall ----------
async def maybe_accept_cookies(page: Page, total_wait_ms: int = 12000) -> bool:
    selectors = [
        "#onetrust-accept-btn-handler",
        "button#onetrust-accept-btn-handler",
        "#CybotCookiebotDialogBodyButtonAccept",
        ".truste-button2",
        "button[aria-label*='Accept']",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "button:has-text('Akceptuj')",
        "button:has-text('Zgadzam')",
        "button:has-text('OK')",
        "button:has-text('Got it')",
        "button:has-text('Rozumiem')",
        "[data-testid*='cookie'][data-testid*='accept']",
        "[class*='cookie'] button:has-text('OK')",
    ]
    texts = ["Accept all", "Accept", "Akceptuj", "Zgadzam", "OK", "Got it", "Rozumiem"]

    step = 300
    waited = 0
    while waited <= total_wait_ms:
        for sel in selectors:
            with suppress(Exception):
                loc = page.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click()
                    _print(f"Cookie banner clicked via selector: {sel}")
                    return True
        for t in texts:
            with suppress(Exception):
                loc = page.get_by_role("button", name=re.compile(rf"^{re.escape(t)}$", re.I))
                if await loc.count() > 0:
                    await loc.first.click()
                    _print(f"Cookie banner clicked via role text: {t}")
                    return True
        await asyncio.sleep(step / 1000)
        waited += step
    return False

# ---------- find/fill helpers ----------
_APPLY_TEXTS = [
    "Apply now", "Apply", "Send application", "Submit application",
    "Aplikuj", "Wyślij", "Wyślij aplikację", "Zgłoś kandydaturę",
    "I'm interested", "I’m interested",
]

async def find_apply_button_candidates(page: Page):
    for t in _APPLY_TEXTS:
        locs = [
            page.get_by_role("button", name=re.compile(rf"\b{re.escape(t)}\b", re.I)),
            page.get_by_role("link",   name=re.compile(rf"\b{re.escape(t)}\b", re.I)),
            page.locator(f"button:has-text('{t}')"),
            page.locator(f"a:has-text('{t}')"),
        ]
        for loc in locs:
            with suppress(Exception):
                if await loc.count() > 0 and await loc.first.is_visible():
                    return loc.first
    with suppress(Exception):
        loc = page.locator("a[href*='#apply'], button[id*='apply'], a[id*='apply']")
        if await loc.count() > 0 and await loc.first.is_visible():
            return loc.first
    return None

async def wait_for_application_form(page: Page) -> bool:
    selectors = [
        "[role='dialog'] form",
        "form:has(button:has-text('Apply'))",
        "form:has(button:has-text('Aplikuj'))",
        "form:has(button:has-text('Wyślij'))",
        "form",
    ]
    text_markers = [
        re.compile(r"application form", re.I),
        re.compile(r"apply", re.I),
        re.compile(r"aplikuj", re.I),
        re.compile(r"wyślij", re.I),
    ]
    for sel in selectors:
        with suppress(Exception):
            if await page.locator(sel).first.is_visible(timeout=1500):
                return True
    for rx in text_markers:
        with suppress(Exception):
            if await page.get_by_text(rx).first.is_visible(timeout=800):
                return True
    with suppress(Exception):
        if await page.locator("form").first.is_visible(timeout=800):
            return True
    return False

async def open_application_form(page: Page, allow_cookie_click: bool) -> Tuple[bool, Page]:
    """
    Open the Application form:
    - Handles cookie wall.
    - Clicks Apply (same page) or captures popup and switches to it.
    - If popup opens, tries cookies again on popup.
    Returns (form_opened, active_page).
    """
    active = page

    if allow_cookie_click:
        with suppress(Exception):
            await maybe_accept_cookies(active)

    # up to 6 attempts: search → click → wait → scroll
    for attempt in range(6):
        apply = await find_apply_button_candidates(active)
        if apply:
            popup: Optional[Page] = None
            with suppress(Exception):
                async with active.context.expect_page(timeout=3000) as popup_info:
                    with suppress(Exception):
                        await apply.scroll_into_view_if_needed()
                        await apply.hover()
                    await apply.click(timeout=5000)
                popup = await popup_info.value
            if popup:
                active = popup
                _print("Apply opened a popup; switching to new page")
                with suppress(Exception):
                    await active.wait_for_load_state("domcontentloaded", timeout=8000)
                with suppress(Exception):
                    await active.wait_for_load_state("networkidle", timeout=8000)
                if allow_cookie_click:
                    with suppress(Exception):
                        await maybe_accept_cookies(active)

            # wait for form (on active page)
            for _ in range(10):
                if await wait_for_application_form(active):
                    return True, active
                await asyncio.sleep(0.5)

        # scroll cycle to discover sticky bars / lazy sections
        with suppress(Exception):
            await active.mouse.wheel(0, 900)
        await asyncio.sleep(0.5)
        with suppress(Exception):
            await active.mouse.wheel(0, -700)
        await asyncio.sleep(0.3)

    # last-ditch: JS click
    apply = await find_apply_button_candidates(active)
    if apply:
        with suppress(Exception):
            await apply.evaluate("el => el.click()")
        for _ in range(12):
            if await wait_for_application_form(active):
                return True, active
            await asyncio.sleep(0.4)

    return False, active

async def fill_introduce_yourself(page: Page, intro_text: str) -> bool:
    label_rx = [
        re.compile(r"introduce yourself", re.I),
        re.compile(r"message", re.I),
        re.compile(r"cover letter", re.I),
        re.compile(r"tell.*yourself", re.I),
        re.compile(r"notes?", re.I),
        re.compile(r"wiadomość|list motywacyjny", re.I),
    ]
    for rx in label_rx:
        loc = page.get_by_role("textbox", name=rx)
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.fill(intro_text, timeout=4000)
                return True

    ph_rx = ["Introduce yourself", "Message", "Cover letter", "Tell us", "Notes", "Wiadomość", "List motywacyjny"]
    for ph in ph_rx:
        loc = page.locator(f"textarea[placeholder*='{ph}'], input[placeholder*='{ph}']")
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.fill(intro_text, timeout=4000)
                return True

    for scope in ["[role='dialog']", "form", "body"]:
        loc = page.locator(f"{scope} textarea")
        if await loc.count() > 0:
            with suppress(Exception):
                if await loc.first.is_visible():
                    await loc.first.fill(intro_text, timeout=4000)
                    return True
    return False

async def tick_consents_if_present(page: Page, max_to_tick: int = 3) -> int:
    ticked = 0
    label_hints = [re.compile(r"consent|agree|accept|privacy|terms|rodo|gdpr", re.I)]

    boxes = page.get_by_role("checkbox")
    try:
        count = await boxes.count()
    except Exception:
        count = 0

    for i in range(count):
        if ticked >= max_to_tick:
            break
        cb = boxes.nth(i)
        try:
            if not await cb.is_visible():
                continue
            if await cb.is_checked():
                continue
            near_ok = True
            with suppress(Exception):
                lbl = await cb.evaluate("""el => {
                    const id = el.getAttribute('id');
                    const label = id ? document.querySelector(`label[for="${id}"]`) : el.closest('label');
                    return (label && label.innerText) ? label.innerText.trim() : '';
                }""")
                if lbl:
                    near_ok = any(r.search(lbl) for r in label_hints)
            if near_ok:
                await cb.check()
                ticked += 1
        except Exception:
            continue

    if ticked < max_to_tick:
        raw_boxes = page.locator("input[type='checkbox']")
        try:
            raw_count = await raw_boxes.count()
        except Exception:
            raw_count = 0
        for i in range(raw_count):
            if ticked >= max_to_tick:
                break
            cb = raw_boxes.nth(i)
            try:
                if not await cb.is_visible():
                    continue
                if await cb.is_checked() or await cb.is_disabled():
                    continue
                near_ok = True
                with suppress(Exception):
                    lbl = await cb.evaluate("""el => {
                        const id = el.getAttribute('id');
                        const label = id ? document.querySelector(`label[for="${id}"]`) : el.closest('label');
                        return (label && label.innerText) ? label.innerText.trim() : '';
                    }""")
                    if lbl:
                        near_ok = any(r.search(lbl) for r in label_hints)
                if near_ok:
                    await cb.check()
                    ticked += 1
            except Exception:
                continue
    return ticked

async def click_final_apply_in_form(page: Page) -> bool:
    labels = [
        re.compile(r"\bapply\b", re.I),
        re.compile(r"\bsend\b", re.I),
        re.compile(r"\bsubmit\b", re.I),
        re.compile(r"aplikuj", re.I),
        re.compile(r"wyślij", re.I),
    ]
    for rx in labels:
        loc = page.get_by_role("button", name=rx)
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.click()
                return True
    for txt in ["Apply", "Aplikuj", "Wyślij", "Send", "Submit"]:
        loc = page.locator(f"[role='dialog'] button:has-text('{txt}'), form button:has-text('{txt}')")
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.click()
                return True
    return False

async def wait_for_confirmation(page: Page) -> bool:
    markers = [
        re.compile(r"application (sent|submitted|completed)", re.I),
        re.compile(r"thank you( for (your )?application)?", re.I),
        re.compile(r"dziękujemy|aplikacja wysłana", re.I),
        re.compile(r"\bapplied\b", re.I),
        re.compile(r"\bconfirmation\b", re.I),
    ]
    for _ in range(30):
        for rx in markers:
            with suppress(Exception):
                if await page.get_by_text(rx).first.is_visible(timeout=500):
                    return True
        await asyncio.sleep(0.5)
    return False

# ---------- per row ----------
async def process_one(
    ctx: BrowserContext,
    row: Dict[str, Any],
    intro_text: str,
    fail_fast: bool,
    allow_cookie_click: bool,
) -> None:
    if not (row.get("easy_apply") is True and row.get("processed") is False):
        return
    url = row.get("url")
    if not url:
        return

    base_page: Optional[Page] = None
    active_page: Optional[Page] = None
    log = {
        "last_attempt_at": now_iso(),
        "s4_form_found": False,
        "s4_introduce_filled": False,
        "s4_consents_ticked": 0,
        "s4_submit_clicked": False,
        "s4_confirmation": False,
        "s4_error": None,
    }

    id_key, url_key = "id", "url"
    id_val, url_val = row.get("id"), row.get("url")

    try:
        base_page = await ctx.new_page()
        await base_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        with suppress(Exception):
            await base_page.wait_for_load_state("networkidle", timeout=8000)

        # 1) Открыть форму (cookie wall + Apply; возможно в попапе)
        form_open, active_page = await open_application_form(base_page, allow_cookie_click=allow_cookie_click)
        log["s4_form_found"] = form_open
        if not form_open or not active_page:
            raise RuntimeError("Application form did not appear (Apply trigger not clickable or cookie wall unresolved)")

        # 2) Заполнить поле "Introduce yourself"
        log["s4_introduce_filled"] = await fill_introduce_yourself(active_page, intro_text)

        # 3) Поставить галочки (если есть)
        log["s4_consents_ticked"] = await tick_consents_if_present(active_page)

        # 4) Нажать финальную кнопку в форме
        log["s4_submit_clicked"] = await click_final_apply_in_form(active_page)

        # 5) Подтверждение
        log["s4_confirmation"] = await wait_for_confirmation(active_page)

        # ---- console log on success ----
        if log["s4_confirmation"]:
            _print(f"Application Completed: {row.get('id') or row.get('url')}")

        # --- решаем processed: ТОЛЬКО при confirmation=True ---
        patch = {**log}
        if log["s4_confirmation"]:
            patch["processed"] = True

        updated = update_row_inplace(id_key, id_val, url_key, url_val, patch)
        if not updated:
            _print("WARN: row patch did not match any record (check id/url).")

        with suppress(Exception):
            if active_page and not active_page.is_closed():
                await active_page.close()
        with suppress(Exception):
            if base_page and not base_page.is_closed():
                await base_page.close()

    except Exception as e:
        log["s4_error"] = f"{type(e).__name__}: {e}"
        update_row_inplace(id_key, id_val, url_key, url_val, {**log})

        # diagnostics
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENSHOTS_DIR / f"s4_{safe_filename(row.get('id') or row.get('url'))}_{ts}.png"
        txt = ERRORS_DIR / f"s4_{safe_filename(row.get('id') or row.get('url'))}_{ts}.txt"
        with suppress(Exception):
            if active_page and not active_page.is_closed():
                await active_page.screenshot(path=str(png), full_page=True)
            elif base_page and not base_page.is_closed():
                await base_page.screenshot(path=str(png), full_page=True)
        with txt.open("w", encoding="utf-8") as f:
            f.write(f"TIME: {now_iso()}\nURL: {url}\n\nTRACEBACK:\n{traceback.format_exc()}\n")

        if fail_fast:
            raise
    finally:
        with suppress(Exception):
            if active_page and not active_page.is_closed():
                await active_page.close()
        with suppress(Exception):
            if base_page and not base_page.is_closed():
                await base_page.close()

# ---------- runner ----------
async def run():
    cfg = load_config()
    headful = bool(cfg.get("HEADFUL", True))
    limit = int(cfg.get("LIMIT", 0))  # 0 = all
    fail_fast = bool(cfg.get("FAIL_FAST", False))
    intro_text = str(cfg.get("INTRODUCE_YOURSELF", "")).strip()
    allow_cookie_click = bool(cfg.get("ALLOW_COOKIE_CLICK", True))

    INPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    INPUT_JSONL.touch(exist_ok=True)

    rows_all = list(read_jsonl(INPUT_JSONL))
    rows = [r for r in rows_all if r.get("easy_apply") is True and r.get("processed") is False]
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
            await process_one(
                ctx,
                row,
                intro_text=intro_text,
                fail_fast=fail_fast,
                allow_cookie_click=allow_cookie_click,
            )
            human_sleep(160, 320)

        await ctx.close()
        await browser.close()

def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
