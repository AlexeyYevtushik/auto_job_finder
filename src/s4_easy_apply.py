# src/s4_easy_apply.py
# S4: Auto-complete "easy apply" forms on justjoin.it.
# - Input: data/filtered_links.jsonl (rows with easy_apply=true)
# - Visits row["url"], clicks Apply, fills "Introduce yourself", consents, submits.
# - Output log: data/easy_apply_log.jsonl (one JSON per line).
#
# Run:
#   python -m src.s4_easy_apply --headful true --limit 0 \
#       --intro-text "https://github.com/AlexeyYevtushik https://www.linkedin.com/in/alexey-yevtushik/" \
#       --fail-fast

import asyncio
import argparse
import json
import re
import traceback
from contextlib import suppress
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from playwright.async_api import (
    async_playwright, Page, Browser, BrowserContext,
    TimeoutError as PWTimeout
)

from .common import (
    DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR,
    FILTERED_JSONL, STORAGE_STATE_JSON,
    read_jsonl, append_jsonl, now_iso, human_sleep
)

EASY_APPLY_LOG = DATA_DIR / "easy_apply_log.jsonl"

# -------------- helpers --------------

def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s or "item")

async def find_apply_button(page: Page):
    """
    Try multiple finders to get the Apply button/link.
    Return a Locator or None.
    """
    labels_rx = [
        re.compile(r"\bapply now?\b", re.I),
        re.compile(r"\bapply\b", re.I),
        re.compile(r"\baplikuj\b", re.I),
        re.compile(r"wyślij", re.I),
        re.compile(r"send application", re.I),
        re.compile(r"submit application", re.I),
    ]
    for rx in labels_rx:
        loc = page.get_by_role("button", name=rx)
        if await loc.count() > 0:
            return loc.first
        loc = page.get_by_role("link", name=rx)
        if await loc.count() > 0:
            return loc.first

    # Fallback: <a> text contains
    for txt in ["Apply now", "Apply", "Aplikuj", "Wyślij", "Send application", "Submit application"]:
        loc = page.locator(f"a:has-text('{txt}')")
        if await loc.count() > 0:
            return loc.first

    # Last resort: any button containing "Apply" or Polish equivalents
    loc = page.locator("button:has-text('Apply'), button:has-text('Aplikuj'), button:has-text('Wyślij')")
    if await loc.count() > 0:
        return loc.first

    return None

async def wait_for_application_form(page: Page) -> bool:
    """
    Wait for the 'Application form' to appear on the SAME PAGE (modal or inline).
    We look for typical containers/markers.
    """
    selectors = [
        "[role='dialog'] form",
        "form:has(button:has-text('Apply'))",
        "form:has(button:has-text('Aplikuj'))",
        "form:has(button:has-text('Wyślij'))",
        "form",
    ]
    # quick text markers often present:
    text_markers = [
        re.compile(r"application form", re.I),
        re.compile(r"apply", re.I),
        re.compile(r"aplikuj", re.I),
        re.compile(r"wyślij", re.I),
    ]

    # First try: a dialog + form
    for sel in selectors:
        with suppress(Exception):
            if await page.locator(sel).first.is_visible(timeout=3000):
                return True

    # Try text markers
    for rx in text_markers:
        with suppress(Exception):
            if await page.get_by_text(rx).first.is_visible(timeout=2000):
                return True

    # Last chance: just wait a moment then check again for a visible form
    await asyncio.sleep(2)
    with suppress(Exception):
        if await page.locator("form").first.is_visible(timeout=2000):
            return True

    return False

async def fill_introduce_yourself(page: Page, intro_text: str) -> bool:
    """
    Locate a free-text field for 'Introduce yourself' and fill it.
    We try common placeholders/labels and, failing that, the first textarea in the form/dialog.
    """
    # Try role=textbox with label/placeholder-like names:
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
            try:
                await loc.first.fill(intro_text, timeout=4000)
                return True
            except Exception:
                pass

    # Placeholders on input/textarea
    ph_rx = ["Introduce yourself", "Message", "Cover letter", "Tell us", "Notes", "Wiadomość", "List motywacyjny"]
    for ph in ph_rx:
        loc = page.locator(f"textarea[placeholder*='{ph}'], input[placeholder*='{ph}']")
        if await loc.count() > 0:
            try:
                await loc.first.fill(intro_text, timeout=4000)
                return True
            except Exception:
                pass

    # Fallback: first visible textarea inside a dialog/form
    for scope in ["[role='dialog']", "form", "body"]:
        loc = page.locator(f"{scope} textarea")
        if await loc.count() > 0:
            with suppress(Exception):
                if await loc.first.is_visible():
                    await loc.first.fill(intro_text, timeout=4000)
                    return True

    return False

async def tick_consents_if_present(page: Page, max_to_tick: int = 3) -> int:
    """
    Tick up to N visible, enabled, unchecked checkboxes that look like consents (GDPR, terms).
    """
    ticked = 0
    # Prefer checkboxes near consent-like labels
    label_hints = [
        re.compile(r"consent|agree|accept|privacy|terms|rodo|gdpr", re.I),
    ]

    # Easy path: role=checkbox (respects visibility)
    boxes = page.get_by_role("checkbox")
    count = 0
    try:
        count = await boxes.count()
    except Exception:
        count = 0

    for i in range(count):
        if ticked >= max_to_tick:
            break
        cb = boxes.nth(i)
        try:
            # Is it visible and not checked?
            if not await cb.is_visible():
                continue
            state = await cb.is_checked()
            if state:
                continue
            # Check nearby label text if possible
            near_ok = True
            with suppress(Exception):
                lbl = await cb.evaluate("""el => {
                    const id = el.getAttribute('id');
                    const label = id ? document.querySelector(`label[for="${id}"]`) : el.closest('label');
                    return (label && label.innerText) ? label.innerText.trim() : '';
                }""")
                if lbl:
                    near_ok = any(r.search(lbl) for r in label_hints)
            # If we couldn't resolve label text, still try (some sites block programmatic reading)
            if near_ok:
                await cb.check()
                ticked += 1
        except Exception:
            continue

    # Fallback: raw inputs if roles missing
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
                checked = await cb.is_checked()
                disabled = await cb.is_disabled()
                if checked or disabled:
                    continue
                # Similar label heuristic:
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

async def click_submit(page: Page) -> bool:
    """
    Click final Apply/Send/Submit inside the form/dialog.
    """
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
    # Fallback textual
    for txt in ["Apply", "Aplikuj", "Wyślij", "Send", "Submit"]:
        loc = page.locator(f"button:has-text('{txt}')")
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.click()
                return True
    return False

async def wait_for_confirmation(page: Page) -> bool:
    """
    Wait for confirmation text/marker that indicates application was sent.
    """
    markers = [
        re.compile(r"application sent|application submitted|thank you|dziękujemy|aplikacja wysłana", re.I),
        re.compile(r"applied", re.I),
        re.compile(r"confirmation", re.I),
        re.compile(r"thank you for (your )?application", re.I),
    ]
    # Give the UI up to ~10s total
    for _ in range(20):
        for rx in markers:
            with suppress(Exception):
                if await page.get_by_text(rx).first.is_visible(timeout=500):
                    return True
        await asyncio.sleep(0.5)
    return False

# -------------- per row --------------

async def process_one(
    ctx: BrowserContext,
    row: Dict[str, Any],
    intro_text: str,
    fail_fast: bool
) -> None:
    """
    Only handles rows with easy_apply=true. Opens row['url'], clicks Apply,
    waits form, fills intro, ticks consents, submits, waits confirmation, logs result.
    """
    if not row.get("easy_apply", False):
        return
    url = row.get("url")
    if not url:
        return

    page: Optional[Page] = None
    log = {
        "id": row.get("id"),
        "data_index": row.get("data_index"),
        "url": url,
        "attempted_at": now_iso(),
        "form_found": False,
        "introduce_filled": False,
        "consents_ticked": 0,
        "submit_clicked": False,
        "confirmation": False,
        "error": None,
    }

    try:
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        with suppress(Exception):
            await page.wait_for_load_state("networkidle", timeout=8000)

        # Find & click Apply
        apply = await find_apply_button(page)
        if not apply:
            raise RuntimeError("Apply button not found")

        with suppress(Exception):
            await apply.scroll_into_view_if_needed()
            await apply.hover()
        # trusted click first
        did_click = False
        with suppress(Exception):
            await apply.click(timeout=8000)
            did_click = True
        if not did_click:
            with suppress(Exception):
                await apply.evaluate("el => el.click()")

        # Wait form
        log["form_found"] = await wait_for_application_form(page)
        if not log["form_found"]:
            raise RuntimeError("Application form did not appear")

        # Fill introduce yourself
        log["introduce_filled"] = await fill_introduce_yourself(page, intro_text)

        # Tick consents if present
        log["consents_ticked"] = await tick_consents_if_present(page)

        # Submit
        log["submit_clicked"] = await click_submit(page)

        # Confirmation
        log["confirmation"] = await wait_for_confirmation(page)

        log["state"] = "succesed" if log.get("confirmation") else "for work"

        append_jsonl(EASY_APPLY_LOG, log)

        with suppress(Exception):
            await page.close()

    except Exception as e:
        log["error"] = f"{type(e).__name__}: {e}"
        log["state"] = "for work"
        append_jsonl(EASY_APPLY_LOG, log)

        # diagnostics
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENSHOTS_DIR / f"s4_{safe_filename(row.get('id'))}_{ts}.png"
        txt = ERRORS_DIR / f"s4_{safe_filename(row.get('id'))}_{ts}.txt"
        with suppress(Exception):
            if page:
                await page.screenshot(path=str(png), full_page=True)
        with txt.open("w", encoding="utf-8") as f:
            f.write(f"TIME: {now_iso()}\nURL: {url}\n\nTRACEBACK:\n{traceback.format_exc()}\n")

        if fail_fast:
            raise
    finally:
        if page and not page.is_closed():
            with suppress(Exception):
                await page.close()

# -------------- runner --------------

async def run(headful: bool, limit: int, fail_fast: bool, intro_text: str):
    Path(EASY_APPLY_LOG).parent.mkdir(parents=True, exist_ok=True)
    Path(EASY_APPLY_LOG).touch(exist_ok=True)

    rows = [r for r in read_jsonl(FILTERED_JSONL) if r.get("easy_apply") is True]
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
            await process_one(ctx, row, intro_text=intro_text, fail_fast=fail_fast)
            # tiny human-like pause between applications
            human_sleep(160, 320)

        await ctx.close()
        await browser.close()

def main():
    parser = argparse.ArgumentParser(
        description="S4: Auto-complete easy-apply forms from filtered_links.jsonl (easy_apply=true)."
    )
    parser.add_argument("--headful", type=str, default="true", help="true/false (default true)")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N easy-apply links (0 = all).")
    parser.add_argument(
        "--intro-text",
        type=str,
        default="https://github.com/AlexeyYevtushik   https://www.linkedin.com/in/alexey-yevtushik/",
        help="Text to put into the 'Introduce yourself' field."
    )
    parser.add_argument("--fail-fast", action="store_true", default=False, help="Stop on first error.")
    args = parser.parse_args()

    headful = str(args.headful).lower() == "true"

    asyncio.run(run(
        headful=headful,
        limit=args.limit,
        fail_fast=args.fail_fast,
        intro_text=args.intro_text
    ))

if __name__ == "__main__":
    main()
