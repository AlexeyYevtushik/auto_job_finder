# -----------------------------------------------------------------------------
# s4_easy_apply.py
#
# Purpose:
# 1) Visit each row where easy_apply == true AND processed == false (from data/filtered_links.jsonl).
# 2) If a visible "Apply" opener is NOT found (and no form is already open), mark that row
#    as outdated=true and processed=true (and do NOT modify other fields).
# 3) Otherwise run the easy-apply flow (fill message, set selects/consents, submit, wait for confirmation).
# 4) Respects config/config.json for HEADFUL, LIMIT, FAIL_FAST, cookies, timeouts, etc.
#
# IMPORTANT FIXES:
# - Added robust JSONL reader (supports pretty-printed multi-line JSON objects).
# - get_pending_rows() and update_row_inplace() now use the robust reader.
# - Fixed outdated patch: {"outdated": True, "processed": True}.
# - Fixed minor JS typos (&& instead of 'and') in evaluate() snippets.
# -----------------------------------------------------------------------------

import asyncio
import json
import re
import random
import traceback
from contextlib import suppress
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Union, Iterable

from playwright.async_api import async_playwright, Page, Browser, BrowserContext, Frame

from .common import (
    DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR, STORAGE_STATE_JSON,
    now_iso, human_sleep  # keep imports stable with your repo
)

INPUT_JSONL = DATA_DIR / "filtered_links.jsonl"
CONFIG_PATH = Path("config/config.json")


# ------------------------------- Utils & IO ----------------------------------

def log(msg: str) -> None:
    print(f"[S4] {msg}", flush=True)

def step(tag: str, msg: str) -> None:
    log(f"[{tag}] {msg}")

def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s or "item")


def load_config() -> Dict[str, Any]:
    """Load config/config.json or fall back to defaults."""
    if not CONFIG_PATH.exists():
        return {
            "HEADFUL": True,
            "LIMIT": 0,
            "FAIL_FAST": False,
            "ALLOW_COOKIE_CLICK": True,
            "ALLOW_SAMEPAGE_OPENER": False,
            "INTRODUCE_YOURSELF": "Github: https://github.com/AlexeyYevtushik\nLinkedIn: https://www.linkedin.com/in/alexey-yevtushik/",
            "SHORT_TIMEOUT_MIN": 160,
            "SHORT_TIMEOUT_MAX": 320,
            "LONG_TIMEOUT_MIN": 600,
            "LONG_TIMEOUT_MAX": 1260,
        }
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    """Rewrite a JSONL file (one compact JSON per line)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            if isinstance(r, dict):
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _robust_iter_json_objects(p: Path) -> Iterable[Dict[str, Any]]:
    """
    Robustly stream JSON objects from a file where objects may be:
    - one per line (.jsonl), or
    - pretty-printed across multiple lines, back-to-back.

    This solves the "no pending rows" issue when filtered_links.jsonl is formatted with indentation.
    """
    if not p.exists():
        return
    dec = json.JSONDecoder()
    buf: List[str] = []
    depth = 0
    in_str = False
    esc = False

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            for ch in line:
                buf.append(ch)
                if in_str:
                    if esc:
                        esc = False
                    elif ch == "\\":
                        esc = True
                    elif ch == '"':
                        in_str = False
                else:
                    if ch == '"':
                        in_str = True
                    elif ch == "{":
                        depth += 1
                    elif ch == "}":
                        depth -= 1

                if depth == 0 and buf and any(c.strip() for c in buf):
                    s = "".join(buf).strip()
                    if not s:
                        buf = []
                        continue
                    try:
                        obj, idx = dec.raw_decode(s)
                    except Exception:
                        # keep accumulating if not decodable yet
                        continue
                    if isinstance(obj, dict):
                        yield obj
                    rest = s[idx:].lstrip()
                    buf = list(rest) if rest else []

        # trailing remainder
        if any(c.strip() for c in buf):
            s = "".join(buf).strip()
            try:
                obj, _ = dec.raw_decode(s)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                pass


def _coerce_row(obj: Any) -> Optional[Dict[str, Any]]:
    """Accept dict or JSON string; ignore anything else."""
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return None
        try:
            data = json.loads(s)
            return data if isinstance(data, dict) else None
        except Exception:
            return None
    return None


def match_row(r: Dict[str, Any], key_id, val_id, key_url, val_url) -> bool:
    """Match by id+url (if both provided); otherwise match by whichever is present."""
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
    """
    Read the entire filtered_links.jsonl robustly, update the first matching row with 'patch',
    and rewrite the file (compact JSONL). Do NOT touch other fields.
    """
    rows = [r for r in (_coerce_row(x) for x in _robust_iter_json_objects(INPUT_JSONL)) if r is not None]
    updated = False
    for r in rows:
        if match_row(r, match_id_key, match_id_val, match_url_key, match_url_val):
            r.update(patch)
            updated = True
            break
    if updated:
        write_jsonl(INPUT_JSONL, rows)
    return updated


def get_pending_rows(limit: int) -> List[Dict[str, Any]]:
    """
    Pending rows = easy_apply == true AND processed == false.
    Uses the robust JSON reader, so pretty-printed files are supported.
    """
    all_rows = [r for r in (_coerce_row(x) for x in _robust_iter_json_objects(INPUT_JSONL)) if r is not None]
    pending: List[Dict[str, Any]] = []
    for r in all_rows:
        easy = (r.get("easy_apply") is True)
        processed = (r.get("processed") is True)
        if easy and not processed:
            pending.append(r)
    if limit and limit > 0:
        return pending[:limit]
    return pending


# ----------------------------- Page helpers ----------------------------------

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
    step("COOKIES", "check: start")
    waited = 0
    while waited <= total_wait_ms:
        for sel in selectors:
            with suppress(Exception):
                loc = page.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click()
                    step("COOKIES", f"accepted via selector: {sel}")
                    return True
        for t in texts:
            with suppress(Exception):
                loc = page.get_by_role("button", name=re.compile(rf"^{re.escape(t)}$", re.I))
                if await loc.count() > 0:
                    await loc.first.click()
                    step("COOKIES", f"accepted via role text: {t}")
                    return True
        await asyncio.sleep(0.3)
        waited += 300
    step("COOKIES", "banner not found (ok)")
    return False


_APPLY_OPEN_TEXTS = [
    "Apply now", "Apply", "Send application", "Submit application",
    "Aplikuj", "Wyślij", "Wyślij aplikację", "Zgłoś kandydaturę",
    "I'm interested", "I’m interested",
]
_FORM_SUBMIT_TEXTS = ["Apply", "Aplikuj", "Wyślij", "Send", "Submit"]


async def _is_inside_form_or_dialog(loc) -> bool:
    try:
        return await loc.evaluate("""
            el => !!(
              el.closest('form') ||
              el.closest('[role="dialog"]') ||
              el.closest('[aria-modal="true"]') ||
              el.closest('.modal, .dialog, .popup, .MuiDialog-root, .MuiModal-root, .chakra-modal__content, .ant-modal')
            )
        """)
    except Exception:
        return False


async def _looks_like_submit(loc) -> bool:
    try:
        return await loc.evaluate("""
            el => {
              const type = (el.getAttribute('type') || '').toLowerCase();
              const tst  = (el.getAttribute('data-testid') || '').toLowerCase();
              const aria = (el.getAttribute('aria-label') || '').toLowerCase();
              if (type === 'submit') return true;
              if (tst.includes('submit')) return true;
              if (aria.includes('submit')) return true;
              if (el.closest('form')) return true;
              return false;
            }
        """)
    except Exception:
        return False


async def find_page_apply_opener(page: Page):
    """Find an 'Apply' opener on the page (not inside a form/dialog)."""
    for t in _APPLY_OPEN_TEXTS:
        locs = [
            page.get_by_role("button", name=re.compile(rf"\b{re.escape(t)}\b", re.I)),
            page.get_by_role("link",   name=re.compile(rf"\b{re.escape(t)}\b", re.I)),
            page.locator(f"button:has-text('{t}')"),
            page.locator(f"a:has-text('{t}')"),
        ]
        for loc in locs:
            try:
                if await loc.count() == 0:
                    continue
                cand = loc.first
                if not await cand.is_visible():
                    continue
                if await _is_inside_form_or_dialog(cand):
                    continue
                if await _looks_like_submit(cand):
                    continue
                return cand
            except Exception:
                continue
    # best-effort fallback
    try:
        loc = page.locator("a[href*='#apply'], button[id*='apply'], a[id*='apply']")
        if await loc.count() > 0:
            cand = loc.first
            if await cand.is_visible() and not await _is_inside_form_or_dialog(cand) and not await _looks_like_submit(cand):
                return cand
    except Exception:
        pass
    return None


async def _has_inputs_inside(scope: Union[Page, Frame], root_css: str) -> bool:
    probes = [
        f"{root_css} textarea",
        f"{root_css} input[type='text']",
        f"{root_css} input[type='email']",
        f"{root_css} input[type='file']",
        f"{root_css} input:not([type])",
        f"{root_css} select",
        f"{root_css} [role='combobox']",
        f"{root_css} [role='textbox']",
    ]
    for sel in probes:
        with suppress(Exception):
            loc = scope.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                return True
    return False


async def is_true_application_form_on(scope: Union[Page, Frame]) -> bool:
    """Heuristics to detect a 'true' application form in the given scope."""
    dialog_roots = ["[role='dialog']", ".modal", ".dialog", ".popup", ".MuiDialog-root", ".chakra-modal__content"]
    for dr in dialog_roots:
        for txt in _FORM_SUBMIT_TEXTS:
            btn = scope.locator(f"{dr} button:has-text('{txt}')")
            if await btn.count() > 0 and await btn.first.is_visible():
                if await _has_inputs_inside(scope, dr):
                    return True

    forms = scope.locator("form")
    try:
        n = await forms.count()
    except Exception:
        n = 0

    for i in range(n):
        f = forms.nth(i)
        try:
            if not await f.is_visible():
                continue
            ok_btn = False
            for txt in _FORM_SUBMIT_TEXTS:
                if await f.locator(f"button:has-text('{txt}')").count() > 0:
                    ok_btn = True
                    break
            if not ok_btn:
                continue
            if await _has_inputs_inside(f, ":scope"):
                return True
        except Exception:
            continue
    return False


async def find_form_scope(page: Page) -> Optional[Union[Page, Frame]]:
    if await is_true_application_form_on(page):
        return page
    for fr in page.frames:
        with suppress(Exception):
            if await is_true_application_form_on(fr):
                return fr
    return None


async def is_true_application_form_anywhere(page: Page) -> bool:
    if await is_true_application_form_on(page):
        return True
    for fr in page.frames:
        with suppress(Exception):
            if await is_true_application_form_on(fr):
                return True
    return False


# -------------------------- Confirmation detection ---------------------------

_CONFIRM_PATTERNS = [
    r"\bapplication confirmation\b",
    r"your application has been sent",
    r"\bhas been sent to\b",
    r"\bapplication saved!?\b",
    r"view application history",
    r"application (sent|submitted|completed|received)",
    r"thank you(\s+for (your )?application)?",
    r"we('|’)ve received your application",
    r"your application has been (sent|submitted|received)",
    r"we will contact you",
    r"\bsuccess(?!.*(payment|order))\b",
    r"\ball set\b",
    r"\bsubmitted\b",
    r"\bapplied\b",
    r"\bconfirmation\b",
    r"dziękujemy( za (twoją )?aplikacj[ęe])?",
    r"\baplikacj[ae] wysłan[aeao]\b",
    r"twoja aplikacja (została )?wysłan[aeao]",
    r"zgłoszenie (zostało )?wysłan[aeao]",
    r"przyjęliśmy twoją aplikacj[ęe]",
]
_CONFIRM_URL_HINTS = ["applied", "application-sent", "application_submitted", "submitted", "thanks", "thank-you", "confirmation"]
_COMPILED_CONFIRM = [re.compile(p, re.I) for p in _CONFIRM_PATTERNS]


async def _matches_any_text(scope: Union[Page, Frame], regexes: List[re.Pattern], scope_css: Optional[str] = None) -> bool:
    target = scope.locator(scope_css) if scope_css else scope
    try:
        for rx in regexes:
            loc = target.get_by_text(rx, exact=False)
            with suppress(Exception):
                if await loc.count() > 0 and await loc.first.is_visible(timeout=400):
                    return True
    except Exception:
        pass
    return False


async def _dialog_title_is_confirmation(scope: Union[Page, Frame]) -> bool:
    js = r"""
    () => {
      const dlg = document.querySelector('[role="dialog"]');
      if (!dlg) return false;
      const lab = dlg.getAttribute('aria-labelledby');
      if (lab) {
        const el = document.getElementById(lab);
        if (el && /application confirmation/i.test(el.innerText || '')) return true;
      }
      const t = dlg.querySelector('h1, h2, h3');
      if (t && /application confirmation/i.test((t.innerText || ''))) return true;
      return false;
    }
    """
    try:
        return await scope.evaluate(js)
    except Exception:
        return False


async def _any_success_dialog(scope: Union[Page, Frame]) -> bool:
    if await _dialog_title_is_confirmation(scope):
        return True
    dialog_roots = ["[role='dialog']", ".modal", ".dialog", ".popup", ".MuiDialog-root", ".chakra-modal__content"]
    for dr in dialog_roots:
        if await _matches_any_text(scope, _COMPILED_CONFIRM, dr):
            return True
        dlg = scope.locator(dr)
        try:
            if await dlg.count() == 0 or not await dlg.first.is_visible():
                continue
        except Exception:
            continue
        icon_like = dlg.locator("svg[aria-label*='success' i], [class*='success' i] svg, .icon-success, [data-status='success']")
        try:
            if await icon_like.count() > 0 and not await _has_inputs_inside(scope, dr):
                return True
        except Exception:
            pass
    return False


async def _any_success_toast(scope: Union[Page, Frame]) -> bool:
    toasts = [".toast", ".Toastify__toast", ".chakra-toast", "[class*='toast']",
              ".MuiSnackbar-root", ".ant-message", ".ant-notification"]
    for t in toasts:
        container = scope.locator(t)
        try:
            if await container.count() == 0 or not await container.first.is_visible():
                continue
        except Exception:
            continue
        if await _matches_any_text(scope, _COMPILED_CONFIRM, t):
            return True
    try:
        toast = scope.get_by_text(re.compile(r"Application saved!?"))
        if await toast.count() > 0 and await toast.first.is_visible():
            return True
    except Exception:
        pass
    return False


async def _url_looks_confirmed(scope: Union[Page, Frame]) -> bool:
    with suppress(Exception):
        url = scope.url.lower()
        for hint in _CONFIRM_URL_HINTS:
            if hint in url:
                return True
    return False


async def _body_inner_text_has_confirmation(scope: Union[Page, Frame]) -> bool:
    try:
        txt = await scope.evaluate("() => (document.body && document.body.innerText) ? document.body.innerText : ''")
        low = txt.lower()
        for rx in _COMPILED_CONFIRM:
            if rx.search(low):
                return True
    except Exception:
        pass
    return False


async def detect_confirmation_on(scope: Union[Page, Frame]) -> bool:
    if await _any_success_dialog(scope):
        return True
    if await _any_success_toast(scope):
        return True
    if await _url_looks_confirmed(scope):
        return True
    if await _body_inner_text_has_confirmation(scope):
        return True
    return False


async def detect_confirmation_anywhere(root: Union[Page, Frame]) -> bool:
    page = root if isinstance(root, Page) else root.page
    if await detect_confirmation_on(page):
        return True
    for fr in page.frames:
        with suppress(Exception):
            if await detect_confirmation_on(fr):
                return True
    return False


async def wait_for_confirmation(root: Union[Page, Frame], checks: int = 180, delay_sec: float = 0.5) -> bool:
    for i in range(1, checks + 1):
        if await detect_confirmation_anywhere(root):
            step("CONFIRM", f"detected at check {i}/{checks}")
            return True
        await asyncio.sleep(delay_sec)
    step("CONFIRM", "no confirmation after waiting")
    return False


# ------------------------------ Fill helpers ---------------------------------

_INTRO_LABELS = [
    re.compile(r"introduce yourself", re.I),
    re.compile(r"message", re.I),
    re.compile(r"cover letter", re.I),
    re.compile(r"tell.*yourself", re.I),
    re.compile(r"notes?", re.I),
    re.compile(r"wiadomość|list motywacyjny", re.I),
]
_INTRO_PLACEHOLDERS = ["Introduce yourself", "Message", "Cover letter", "Tell us", "Notes", "Wiadomość", "List motywacyjny"]


async def fill_introduce_yourself(scope: Union[Page, Frame], intro_text: str) -> bool:
    for rx in _INTRO_LABELS:
        loc = scope.get_by_role("textbox", name=rx)
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.fill(intro_text, timeout=4000)
                return True
    for ph in _INTRO_PLACEHOLDERS:
        loc = scope.locator(f"textarea[placeholder*='{ph}'], input[placeholder*='{ph}']")
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.fill(intro_text, timeout=4000)
                return True
    for r in ["[role='dialog']", "form"]:
        loc = scope.locator(f"{r} textarea")
        if await loc.count() > 0:
            with suppress(Exception):
                if await loc.first.is_visible():
                    await loc.first.fill(intro_text, timeout=4000)
                    return True
    return False


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


async def confirm_intro_value(scope: Union[Page, Frame], intro_text: str) -> bool:
    target = _norm(intro_text)
    for rx in _INTRO_LABELS:
        loc = scope.get_by_role("textbox", name=rx)
        if await loc.count() > 0:
            with suppress(Exception):
                val = await loc.first.input_value()
                if _norm(val).find(target) >= 0:
                    return True
    for ph in _INTRO_PLACEHOLDERS:
        loc = scope.locator(f"textarea[placeholder*='{ph}'], input[placeholder*='{ph}']")
        if await loc.count() > 0:
            with suppress(Exception):
                val = await loc.first.input_value()
                if _norm(val).find(target) >= 0:
                    return True
    for r in ["[role='dialog']", "form"]:
        loc = scope.locator(f"{r} textarea")
        if await loc.count() > 0:
            with suppress(Exception):
                val = await loc.first.input_value()
                if _norm(val).find(target) >= 0:
                    return True
    return False


async def wait_intro_confirmed(scope: Union[Page, Frame], intro_text: str, checks: int = 8, delay_sec: float = 0.3) -> bool:
    for _ in range(checks):
        if await confirm_intro_value(scope, intro_text):
            return True
        await asyncio.sleep(delay_sec)
    return False


async def set_positive_selects_if_present(scope: Union[Page, Frame], max_to_set: int = 5) -> int:
    set_count = 0
    aff_rx = re.compile(r"^(yes|tak|agree|zgadzam|accept|consent|true)$", re.I)

    # <select>
    for r in ["[role='dialog']", "form"]:
        selects = scope.locator(f"{r} select")
        try:
            n = await selects.count()
        except Exception:
            n = 0
        for i in range(n):
            if set_count >= max_to_set:
                return set_count
            sel = selects.nth(i)
            try:
                if not await sel.is_visible() or await sel.is_disabled():
                    continue
                with suppress(Exception):
                    val = await sel.input_value()
                    if val and aff_rx.search(val):
                        continue
                options = await sel.locator("option").all_text_contents()
                choice = None
                for opt in options:
                    if aff_rx.search((opt or "").strip()):
                        choice = (opt or "").strip()
                        break
                if choice:
                    await sel.select_option(label=choice)
                    set_count += 1
            except Exception:
                continue

    # [role=combobox]
    for r in ["[role='dialog']", "form"]:
        combos = scope.locator(f"{r} [role='combobox']")
        try:
            n = await combos.count()
        except Exception:
            n = 0
        for i in range(n):
            if set_count >= max_to_set:
                return set_count
            cb = combos.nth(i)
            try:
                if not await cb.is_visible():
                    continue
                with suppress(Exception):
                    await cb.click()
                items = scope.locator("[role='option'], li[role='option'], div[role='option']")
                cnt = await items.count()
                picked = False
                for j in range(cnt):
                    it = items.nth(j)
                    try:
                        txt = (await it.text_content() or "").strip()
                        if aff_rx.search(txt):
                            await it.click()
                            set_count += 1
                            picked = True
                            break
                    except Exception:
                        continue
                if not picked:
                    pass
            except Exception:
                continue

    return set_count


async def tick_consents_if_present(scope: Union[Page, Frame], max_to_tick: int = 3) -> int:
    ticked = 0
    label_hints = [re.compile(r"consent|agree|accept|privacy|terms|rodo|gdpr", re.I)]

    # ARIA checkboxes
    boxes = scope.get_by_role("checkbox")
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

    # Raw checkboxes
    if ticked < max_to_tick:
        raw_boxes = scope.locator("input[type='checkbox']")
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


# ------------------------------- Submit path ---------------------------------

async def get_active_form_scope(scope: Union[Page, Frame]) -> Optional[str]:
    scopes = ["[role='dialog'] form", "[role='dialog']", "form"]
    for r in scopes:
        try:
            root = scope.locator(r)
            if await root.count() == 0 or not await root.first.is_visible():
                continue
            has_submit = False
            for txt in _FORM_SUBMIT_TEXTS:
                if await scope.locator(f"{r} button:has-text('{txt}')").count() > 0:
                    has_submit = True
                    break
            if has_submit:
                return r
        except Exception:
            continue
    return None


async def audit_form_completeness(scope: Union[Page, Frame]) -> Dict[str, Any]:
    active_scope = await get_active_form_scope(scope)
    if not active_scope:
        return {"ok": False, "required_total": 0, "missing": [{"type": "scope", "name": "", "reason": "scope_not_found"}], "scope": None}

    report = await scope.evaluate(r"""
    (sel) => {
      const root = document.querySelector(sel);
      if (!root) return {ok:false, required_total:0, missing:[{type:'scope',name:'',reason:'root_not_found'}], scope: sel};
      const q = s => Array.from(root.querySelectorAll(s));
      const isReq = (el) => {
        if (el.disabled) return false;
        const attr = el.required || el.getAttribute('aria-required') === 'true' || el.dataset.required === 'true';
        let labelText = '';
        const id = el.id;
        if (id) {
          const byFor = root.querySelector(`label[for="${id}"]`);
          if (byFor) labelText = byFor.textContent || '';
        }
        if (!labelText) {
          const wrap = el.closest('label');
          if (wrap) labelText = wrap.textContent || '';
        }
        const likely = /(\*|required|wymagane|obowiązkowe)/i.test(labelText || '');
        return !!(attr || likely);
      };
      const result = {ok: true, required_total: 0, missing: [], scope: sel, _radioGroups: {}};
      const fields = q('input, textarea, select');
      for (const el of fields) {
        const tag = (el.tagName || '').toLowerCase();
        const type = (el.getAttribute('type') || '').toLowerCase();
        const name = el.getAttribute('name') || '';
        if (!isReq(el)) continue;
        result.required_total++;
        if (type === 'hidden') continue;
        if (tag === 'select') {
          const val = (el.value || '').trim();
          const text = el.options && el.selectedIndex >= 0 ? (el.options[el.selectedIndex].text || '') : '';
          const badText = /^(select|choose|wybierz|--|)$/i.test((text || '').trim());
          if (!val || badText) result.missing.push({type:'select', name, reason:'empty'});
          continue;
        }
        if (type === 'checkbox') {
          if (!el.checked) result.missing.push({type:'checkbox', name, reason:'unchecked'});
          continue;
        }
        if (type === 'radio') {
          if (!name) { result.missing.push({type:'radio', name:'', reason:'no-name'}); continue; }
          (result._radioGroups[name] ||= []).push(el.checked);
          continue;
        }
        if (type === 'file') {
          if (!el.files || el.files.length === 0) result.missing.push({type:'file', name, reason:'no-file'});
          continue;
        }
        const val = (el.value || '').trim();
        if (!val) result.missing.push({type: type || tag, name, reason:'empty'});
      }
      for (const [g, checks] of Object.entries(result._radioGroups)) {
        if (!checks.some(Boolean)) result.missing.push({type:'radio', name:g, reason:'none-checked'});
      }
      delete result._radioGroups;
      result.ok = result.missing.length === 0;
      return result;
    }
    """, active_scope)

    return {
        "ok": bool(report.get("ok")),
        "required_total": int(report.get("required_total", 0)),
        "missing": list(report.get("missing", [])),
        "scope": report.get("scope"),
    }


async def is_form_ready_to_submit(scope: Union[Page, Frame]) -> Tuple[bool, Dict[str, Any]]:
    report = await audit_form_completeness(scope)
    return report["ok"], report


async def click_final_apply_in_form(scope: Union[Page, Frame]) -> bool:
    active_scope = await get_active_form_scope(scope)
    if not active_scope:
        return False

    candidates: List[str] = []
    for txt in _FORM_SUBMIT_TEXTS:
        candidates.append(f"{active_scope} button:has-text('{txt}')")
        candidates.append(f"{active_scope} [role='button']:has-text('{txt}')")
    candidates += [
        f"{active_scope} button[type='submit']",
        f"{active_scope} [data-testid*='submit']",
        f"{active_scope} button[class*='MuiButton-root']",
    ]

    matches = None
    for sel in candidates:
        loc = scope.locator(sel)
        try:
            if await loc.count() > 0:
                matches = loc
                break
        except Exception:
            continue
    if not matches:
        return False

    count = await matches.count()
    for idx in reversed(range(count)):
        btn = matches.nth(idx)
        try:
            if not await btn.is_visible():
                continue
            with suppress(Exception):
                await btn.scroll_into_view_if_needed()
            is_disabled = False
            with suppress(Exception):
                is_disabled = await btn.is_disabled()
            if is_disabled:
                continue
            with suppress(Exception):
                await btn.hover()
            with suppress(Exception):
                await btn.click(timeout=5000)
                return True
            with suppress(Exception):
                await btn.evaluate("el => el.click()")
                return True
        except Exception:
            continue
    return False


async def click_final_apply_if_ready(scope: Union[Page, Frame]) -> Tuple[bool, Dict[str, Any]]:
    ready, report = await is_form_ready_to_submit(scope)
    if not ready:
        return False, report
    clicked = await click_final_apply_in_form(scope)
    return clicked, report


async def open_application_form(page: Page, allow_cookie_click: bool, allow_samepage_opener: bool = False) -> Tuple[bool, Union[Page, Frame]]:
    active: Union[Page, Frame] = page

    if allow_cookie_click:
        with suppress(Exception):
            await maybe_accept_cookies(page)

    # If form already present
    pre = await find_form_scope(page)
    if pre:
        step("FORM", "true form already visible -> OK")
        return True, pre

    # Try to find an opener on the job page
    opener = await find_page_apply_opener(page)
    if not opener:
        step("FORM", "opener not found -> cannot open form")
        # Allow a short wait in case form appears dynamically without clicking an opener
        for i in range(1, 11):
            pre2 = await find_form_scope(page)
            if pre2:
                step("FORM", f"form appeared without opener (check {i}/10) -> OK")
                return True, pre2
            await asyncio.sleep(0.5)
        return False, page

    # Try popup first (common on some providers)
    step("FORM", "opener found -> try popup click")
    popup: Optional[Page] = None
    try:
        async with page.context.expect_page(timeout=2500) as popup_info:
            with suppress(Exception):
                await opener.scroll_into_view_if_needed()
                await opener.hover()
            await opener.click(timeout=5000)
        popup = await popup_info.value
    except Exception:
        popup = None

    if popup:
        active = popup
        step("FORM", "popup detected -> switched")
        with suppress(Exception):
            await popup.wait_for_load_state("domcontentloaded", timeout=8000)
        with suppress(Exception):
            await popup.wait_for_load_state("networkidle", timeout=8000)
        if allow_cookie_click:
            with suppress(Exception):
                await maybe_accept_cookies(popup)
        for i in range(1, 11):
            sc = await find_form_scope(popup)
            if sc:
                step("FORM", f"true form visible in popup (check {i}/10) -> OK")
                return True, sc
            await asyncio.sleep(0.5)
        step("FORM", "true application form did not appear in popup")
        return False, popup

    # Same-page opener
    if not allow_samepage_opener:
        step("FORM", "no popup; same-page opener disallowed -> waiting for form without clicking")
        for i in range(1, 11):
            sc = await find_form_scope(page)
            if sc:
                step("FORM", f"true form visible (check {i}/10) -> OK")
                return True, sc
            await asyncio.sleep(0.5)
        step("FORM", "true application form not visible without same-page click")
        return False, page

    step("FORM", "no popup; same-page opener allowed -> clicking")
    with suppress(Exception):
        await opener.scroll_into_view_if_needed()
        await opener.hover()
    with suppress(Exception):
        await opener.click(timeout=3000)
    with suppress(Exception):
        await opener.evaluate("el => el.click()")

    for i in range(1, 11):
        sc = await find_form_scope(page)
        if sc:
            step("FORM", f"true form visible (check {i}/10) -> OK")
            return True, sc
        await asyncio.sleep(0.5)

    step("FORM", "true application form did not appear after same-page click")
    return False, page


# ------------------------------- Main worker ---------------------------------

async def process_one(
    ctx: BrowserContext,
    row: Dict[str, Any],
    intro_text: str,
    fail_fast: bool,
    allow_cookie_click: bool,
    allow_samepage_opener: bool,
) -> None:
    # Guard: only easy_apply==true AND processed==false
    if not (row.get("easy_apply") is True and row.get("processed") is False):
        return

    # Navigate to the job URL (for in-page JustJoin forms, 'url' is correct)
    url = row.get("url")
    if not url:
        return

    rid = row.get("id") or url
    base_page: Optional[Page] = None
    active_scope: Optional[Union[Page, Frame]] = None

    log_row = {
        "last_attempt_at": now_iso(),
        "s4_form_found": False,
        "s4_introduce_filled": False,
        "s4_intro_verified": False,
        "s4_selects_set": 0,
        "s4_consents_ticked": 0,
        "s4_form_ready": False,
        "s4_form_ready_missing": [],
        "s4_form_required_total": 0,
        "s4_submit_clicked": False,
        "s4_confirmation": False,
        "s4_error": None,
    }

    id_key, url_key = "id", "url"
    id_val, url_val = row.get("id"), row.get("url")

    step("ROW", f"start: {rid}")
    try:
        step("NAV", f"goto: {url}")
        base_page = await ctx.new_page()
        await base_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        with suppress(Exception):
            await base_page.wait_for_load_state("networkidle", timeout=8000)

        if allow_cookie_click:
            with suppress(Exception):
                await maybe_accept_cookies(base_page)

        # If neither opener nor pre-visible form is present, mark as outdated
        pre_form = await find_form_scope(base_page)
        if not pre_form:
            opener_probe = await find_page_apply_opener(base_page)
            if opener_probe is None:
                step("APPLY", "no opener -> mark outdated & processed")
                # IMPORTANT: processed=True (previous code mistakenly used 'false')
                update_row_inplace(id_key, id_val, url_key, url_val, {"outdated": True, "processed": True})
                return

        # Try to open the form
        step("FORM", "open: start")
        form_open, active_scope = await open_application_form(
            base_page,
            allow_cookie_click=allow_cookie_click,
            allow_samepage_opener=allow_samepage_opener
        )
        log_row["s4_form_found"] = form_open
        if not form_open or not active_scope:
            raise RuntimeError("Could not open a TRUE application form (opener not clicked or form not shown)")

        # Fill "Introduce yourself" (best-effort)
        step("FILL", "introduce-yourself: start")
        log_row["s4_introduce_filled"] = await fill_introduce_yourself(active_scope, intro_text)
        step("FILL", f"introduce-yourself: {'OK' if log_row['s4_introduce_filled'] else 'not found'}")

        # Verify it actually contains our text
        step("FILL", "introduce-yourself: verify")
        log_row["s4_intro_verified"] = await wait_intro_confirmed(active_scope, intro_text)
        if not log_row["s4_intro_verified"] and log_row["s4_introduce_filled"]:
            step("FILL", "introduce-yourself: re-fill & verify again")
            with suppress(Exception):
                await fill_introduce_yourself(active_scope, intro_text)
            log_row["s4_intro_verified"] = await wait_intro_confirmed(active_scope, intro_text)
        step("FILL", f"introduce-yourself: {'VERIFIED' if log_row['s4_intro_verified'] else 'NOT VERIFIED'}")

        # Best-effort: set positive selects & tick consents
        step("FILL", "positive-selects: start")
        log_row["s4_selects_set"] = await set_positive_selects_if_present(active_scope)
        step("FILL", f"positive-selects: set={log_row['s4_selects_set']}")

        step("FILL", "consents: start")
        log_row["s4_consents_ticked"] = await tick_consents_if_present(active_scope)
        step("FILL", f"consents: ticked={log_row['s4_consents_ticked']}")

        # Quick path: if intro verified, try submit immediately
        if log_row["s4_intro_verified"]:
            step("SUBMIT", "try submit immediately")
            quick_clicked = await click_final_apply_in_form(active_scope)
            log_row["s4_submit_clicked"] = quick_clicked
            if quick_clicked:
                step("SUBMIT", "click final submit: OK")
                step("CONFIRM", "wait: start")
                log_row["s4_confirmation"] = await wait_for_confirmation(active_scope)
                step("CONFIRM", f"result: {'OK' if log_row['s4_confirmation'] else 'NO CONFIRMATION'}")

        # If still not confirmed, check readiness, then try again
        if not log_row["s4_submit_clicked"] and not log_row["s4_confirmation"]:
            step("SUBMIT", "readiness check: start")
            ready, ready_report = await is_form_ready_to_submit(active_scope)
            log_row["s4_form_ready"] = ready
            log_row["s4_form_ready_missing"] = ready_report.get("missing", [])
            log_row["s4_form_required_total"] = ready_report.get("required_total", 0)

            if not ready and any(m.get("reason") == "scope_not_found" for m in log_row["s4_form_ready_missing"]):
                step("SUBMIT", "form scope not found -> check confirmation immediately")
                if await detect_confirmation_anywhere(active_scope):
                    log_row["s4_confirmation"] = True
                else:
                    log_row["s4_confirmation"] = await wait_for_confirmation(active_scope)

            step("SUBMIT", f"readiness: {'READY' if ready else 'NOT READY'}; required_total={log_row['s4_form_required_total']}; missing={log_row['s4_form_ready_missing']}")

            if not log_row["s4_confirmation"]:
                if ready and log_row["s4_intro_verified"]:
                    step("SUBMIT", "click final submit: start")
                    clicked, _ = await click_final_apply_if_ready(active_scope)
                    log_row["s4_submit_clicked"] = clicked
                    step("SUBMIT", f"click final submit: {'OK' if log_row['s4_submit_clicked'] else 'NOT CLICKED'}")
                else:
                    if not log_row["s4_intro_verified"]:
                        step("SUBMIT", "blocked: intro not verified -> skip submit")
                    elif not ready:
                        step("SUBMIT", "blocked: form not ready -> skip submit")

                if not log_row["s4_confirmation"]:
                    step("CONFIRM", "wait: start")
                    log_row["s4_confirmation"] = await wait_for_confirmation(active_scope)
                    step("CONFIRM", f"result: {'OK' if log_row["s4_confirmation"] else 'NO CONFIRMATION'}")

        if log_row["s4_confirmation"]:
            step("ROW", f"Application Completed -> {rid}")

        # Patch row with latest telemetry; mark processed only on confirmation
        patch = {**log_row}
        if log_row["s4_confirmation"]:
            patch["processed"] = True

        updated = update_row_inplace(id_key, id_val, url_key, url_val, patch)
        if not updated:
            step("WARN", "row patch did not match any record (check id/url).")

    except Exception as e:
        log_row["s4_error"] = f"{type(e).__name__}: {e}"
        step("ERROR", log_row["s4_error"])
        # Save error telemetry
        update_row_inplace(id_key, id_val, url_key, url_val, {**log_row})

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENSHOTS_DIR / f"s4_{safe_filename(rid)}_{ts}.png"
        txt = ERRORS_DIR / f"s4_{safe_filename(rid)}_{ts}.txt"

        with suppress(Exception):
            if isinstance(active_scope, Page) and not active_scope.is_closed():
                await active_scope.screenshot(path=str(png), full_page=True)
            elif base_page and not base_page.is_closed():
                await base_page.screenshot(path=str(png), full_page=True)
        with txt.open("w", encoding="utf-8") as f:
            f.write(f"TIME: {now_iso()}\nURL: {url}\n\nTRACEBACK:\n{traceback.format_exc()}\n")

        if fail_fast:
            raise
    finally:
        with suppress(Exception):
            if isinstance(active_scope, Page) and not active_scope.is_closed():
                await active_scope.close()
        with suppress(Exception):
            if base_page and not base_page.is_closed():
                await base_page.close()
        step("ROW", f"end: {rid}")


# --------------------------------- Runner ------------------------------------

async def run():
    cfg = load_config()
    headful = bool(cfg.get("HEADFUL", True))
    limit = int(cfg.get("LIMIT", 0))
    fail_fast = bool(cfg.get("FAIL_FAST", False))
    intro_text = str(cfg.get("INTRODUCE_YOURSELF", "")).strip()
    allow_cookie_click = bool(cfg.get("ALLOW_COOKIE_CLICK", True))
    allow_samepage_opener = bool(cfg.get("ALLOW_SAMEPAGE_OPENER", False))

    global SHORT_TIMEOUT_MIN, SHORT_TIMEOUT_MAX, LONG_TIMEOUT_MIN, LONG_TIMEOUT_MAX
    SHORT_TIMEOUT_MIN = int(cfg.get("SHORT_TIMEOUT_MIN", 160))
    SHORT_TIMEOUT_MAX = int(cfg.get("SHORT_TIMEOUT_MAX", 320))
    LONG_TIMEOUT_MIN  = int(cfg.get("LONG_TIMEOUT_MIN", 600))
    LONG_TIMEOUT_MAX  = int(cfg.get("LONG_TIMEOUT_MAX", 1260))

    INPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    INPUT_JSONL.touch(exist_ok=True)

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

        batch_no = 0
        while True:
            pending = get_pending_rows(limit)
            total_left = len(get_pending_rows(0))
            if not pending:
                step("RUN", "no pending rows (easy_apply=true & processed=false) -> done")
                break

            batch_no += 1
            step("RUN", f"batch #{batch_no}: to process now = {len(pending)} (total pending = {total_left})")

            for idx, row in enumerate(pending, 1):
                step("RUN", f"row {idx}/{len(pending)} in batch #{batch_no}")
                await process_one(
                    ctx,
                    row,
                    intro_text=intro_text,
                    fail_fast=fail_fast,
                    allow_cookie_click=allow_cookie_click,
                    allow_samepage_opener=allow_samepage_opener,
                )
                await asyncio.sleep(random.uniform(SHORT_TIMEOUT_MIN, SHORT_TIMEOUT_MAX))

            if limit <= 0:
                step("RUN", "LIMIT<=0 -> processed all once, exiting")
                break

            step("RUN", f"batch pause: ~{int(LONG_TIMEOUT_MIN//60)}–{int(LONG_TIMEOUT_MAX//60)} minutes (await asyncio.sleep)")
            await asyncio.sleep(random.uniform(LONG_TIMEOUT_MIN, LONG_TIMEOUT_MAX))

        await ctx.close()
        await browser.close()

    step("RUN", "done")


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
