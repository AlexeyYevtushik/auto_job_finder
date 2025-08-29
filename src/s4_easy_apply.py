# src/s4_easy_apply.py
# S4: Auto-complete "easy apply" forms on justjoin.it.
# - Reads data/filtered_links.jsonl, processes rows where easy_apply=true AND processed=false
# - Opens listing, clicks page-level Apply (opener) ONCE, waits for true application form (or popup),
#   fills "Introduce yourself", verifies it was set, sets positive selects/combos, ticks consents,
#   submits ONLY after verification + readiness, then waits for Application Confirmation.
# - Sets processed=true ONLY when a confirmation is detected.

import asyncio
import json
import re
import traceback
from contextlib import suppress
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

from .common import (
    DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR, STORAGE_STATE_JSON,
    read_jsonl, now_iso, human_sleep
)

# ---------- constants/paths ----------
INPUT_JSONL = DATA_DIR / "filtered_links.jsonl"
CONFIG_PATH = Path("config/config.json")

# ---------- logging ----------
def log(msg: str) -> None:
    print(f"[S4] {msg}", flush=True)

def step(tag: str, msg: str) -> None:
    log(f"[{tag}] {msg}")

# ---------- helpers ----------
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

# ---------- cookies ----------
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

# ---------- form detection ----------
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
    """Exclude elements that are submit-like (defensive)."""
    try:
        return await loc.evaluate("""
            el => {
              const type = (el.getAttribute('type') || '').toLowerCase();
              const tst  = (el.getAttribute('data-testid') || '').toLowerCase();
              const aria = (el.getAttribute('aria-label') || '').toLowerCase();
              if (type === 'submit') return true;
              if (tst.includes('submit')) return true;
              if (aria.includes('submit')) return true;
              // if it's inside a FORM, it's not a page opener
              if (el.closest('form')) return true;
              return false;
            }
        """)
    except Exception:
        return False

async def find_page_apply_opener(page: Page):
    """Find ONLY the button/link that OPENS the form (exclude elements inside a form/dialog or submit-like)."""
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
    # fallback by id/href hints; still exclude inside a form/dialog
    try:
        loc = page.locator("a[href*='#apply'], button[id*='apply'], a[id*='apply']")
        if await loc.count() > 0:
            cand = loc.first
            if await cand.is_visible() and not await _is_inside_form_or_dialog(cand) and not await _looks_like_submit(cand):
                return cand
    except Exception:
        pass
    return None

async def _has_inputs_inside(scope: Page, root_css: str) -> bool:
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

async def is_true_application_form(page: Page) -> bool:
    """
    TRUE app form = dialog or form having BOTH:
    (a) a submit button with expected text AND (b) at least one input/textarea/select/combobox.
    """
    dialog_roots = ["[role='dialog']", ".modal", ".dialog", ".popup", ".MuiDialog-root", ".chakra-modal__content"]
    for dr in dialog_roots:
        for txt in _FORM_SUBMIT_TEXTS:
            btn = page.locator(f"{dr} button:has-text('{txt}')")
            if await btn.count() > 0 and await btn.first.is_visible():
                if await _has_inputs_inside(page, dr):
                    return True

    forms = page.locator("form")
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

# ---------- confirmation detection ----------
_CONFIRM_PATTERNS = [
    r"\bapplication confirmation\b",
    r"your application has been sent",
    r"\bhas been sent to\b",
    r"\bapplication saved!?",
    r"view application history",
    r"application (sent|submitted|completed|received)",
    r"thank you(\s+for (your )?application)?",
    r"we('|’)ve received your application",
    r"your application has been (sent|submitted|received)",
    r"we will contact you",
    r"success(?!.*(payment|order))",
    r"\ball set\b",
    r"\bsubmitted\b",
    r"\bapplied\b",
    r"\bconfirmation\b",
    # PL
    r"dziękujemy( za (twoją )?aplikacj[ęe])?",
    r"aplikacj[ae] wysłan[aeao]",
    r"twoja aplikacja (została )?wysłan[aeao]",
    r"zgłoszenie (zostało )?wysłan[aeao]",
    r"przyjęliśmy twoją aplikacj[ęe]",
]
_CONFIRM_URL_HINTS = ["applied", "application-sent", "application_submitted", "submitted", "thanks", "thank-you", "confirmation"]
def _compile_rx_list(patterns: List[str]) -> List[re.Pattern]:
    return [re.compile(p, re.I) for p in patterns]
_COMPILED_CONFIRM = _compile_rx_list(_CONFIRM_PATTERNS)

async def _matches_any_text(page: Page, regexes: List[re.Pattern], scope_css: Optional[str] = None) -> bool:
    scope = page.locator(scope_css) if scope_css else page
    try:
        for rx in regexes:
            loc = scope.get_by_text(rx, exact=False)
            with suppress(Exception):
                if await loc.count() > 0 and await loc.first.is_visible(timeout=400):
                    return True
    except Exception:
        pass
    return False

async def _dialog_title_is_confirmation(page: Page) -> bool:
    js = """
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
        return await page.evaluate(js)
    except Exception:
        return False

async def _any_success_dialog(page: Page) -> bool:
    if await _dialog_title_is_confirmation(page):
        return True
    dialog_roots = ["[role='dialog']", ".modal", ".dialog", ".popup", ".MuiDialog-root", ".chakra-modal__content"]
    for dr in dialog_roots:
        if await _matches_any_text(page, _COMPILED_CONFIRM, dr):
            return True
        dlg = page.locator(dr)
        try:
            if await dlg.count() == 0 or not await dlg.first.is_visible():
                continue
        except Exception:
            continue
        icon_like = dlg.locator("svg[aria-label*='success' i], [class*='success' i] svg, .icon-success, [data-status='success']")
        try:
            if await icon_like.count() > 0 and not await _has_inputs_inside(page, dr):
                return True
        except Exception:
            pass
    return False

async def _any_success_toast(page: Page) -> bool:
    toasts = [".toast", ".Toastify__toast", ".chakra-toast", "[class*='toast']",
              ".MuiSnackbar-root", ".ant-message", ".ant-notification"]
    for t in toasts:
        container = page.locator(t)
        try:
            if await container.count() == 0 or not await container.first.is_visible():
                continue
        except Exception:
            continue
        if await _matches_any_text(page, _COMPILED_CONFIRM, t):
            return True
    try:
        toast = page.get_by_text(re.compile(r"Application saved!?"))
        if await toast.count() > 0 and await toast.first.is_visible():
            return True
    except Exception:
        pass
    return False

async def _url_looks_confirmed(page: Page) -> bool:
    with suppress(Exception):
        url = page.url.lower()
        for hint in _CONFIRM_URL_HINTS:
            if hint in url:
                return True
    return False

async def _body_inner_text_has_confirmation(page: Page) -> bool:
    try:
        txt = await page.evaluate("() => (document.body && document.body.innerText) ? document.body.innerText : ''")
        low = txt.lower()
        for rx in _COMPILED_CONFIRM:
            if rx.search(low):
                return True
    except Exception:
        pass
    return False

async def detect_confirmation(page: Page) -> bool:
    if await _any_success_dialog(page):
        step("CONFIRM", "dialog matched -> OK")
        return True
    if await _any_success_toast(page):
        step("CONFIRM", "toast matched -> OK")
        return True
    if await _url_looks_confirmed(page):
        step("CONFIRM", "URL hint matched -> OK")
        return True
    if await _body_inner_text_has_confirmation(page):
        step("CONFIRM", "body.innerText matched -> OK")
        return True
    return False

async def wait_for_confirmation(page: Page, checks: int = 40, delay_sec: float = 0.5) -> bool:
    for i in range(1, checks + 1):
        if await detect_confirmation(page):
            step("CONFIRM", f"detected at check {i}/{checks}")
            return True
        await asyncio.sleep(delay_sec)
    step("CONFIRM", "no confirmation after waiting")
    return False

# ---------- fill helpers ----------
_INTRO_LABELS = [
    re.compile(r"introduce yourself", re.I),
    re.compile(r"message", re.I),
    re.compile(r"cover letter", re.I),
    re.compile(r"tell.*yourself", re.I),
    re.compile(r"notes?", re.I),
    re.compile(r"wiadomość|list motywacyjny", re.I),
]
_INTRO_PLACEHOLDERS = ["Introduce yourself", "Message", "Cover letter", "Tell us", "Notes", "Wiadomość", "List motywacyjny"]

async def fill_introduce_yourself(page: Page, intro_text: str) -> bool:
    # 1) by accessible name
    for rx in _INTRO_LABELS:
        loc = page.get_by_role("textbox", name=rx)
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.fill(intro_text, timeout=4000)
                return True
    # 2) by placeholder
    for ph in _INTRO_PLACEHOLDERS:
        loc = page.locator(f"textarea[placeholder*='{ph}'], input[placeholder*='{ph}']")
        if await loc.count() > 0:
            with suppress(Exception):
                await loc.first.fill(intro_text, timeout=4000)
                return True
    # 3) first visible textarea in dialog/form
    for scope in ["[role='dialog']", "form"]:
        loc = page.locator(f"{scope} textarea")
        if await loc.count() > 0:
            with suppress(Exception):
                if await loc.first.is_visible():
                    await loc.first.fill(intro_text, timeout=4000)
                    return True
    return False

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

async def confirm_intro_value(page: Page, intro_text: str) -> bool:
    """Check that any candidate intro field now contains the intro_text."""
    target = _norm(intro_text)
    # role=textbox fields with matching labels
    for rx in _INTRO_LABELS:
        loc = page.get_by_role("textbox", name=rx)
        if await loc.count() > 0:
            with suppress(Exception):
                val = await loc.first.input_value()
                if _norm(val).find(target) >= 0:
                    return True
    # placeholders
    for ph in _INTRO_PLACEHOLDERS:
        loc = page.locator(f"textarea[placeholder*='{ph}'], input[placeholder*='{ph}']")
        if await loc.count() > 0:
            with suppress(Exception):
                val = await loc.first.input_value()
                if _norm(val).find(target) >= 0:
                    return True
    # any textarea in dialog/form
    for scope in ["[role='dialog']", "form"]:
        loc = page.locator(f"{scope} textarea")
        if await loc.count() > 0:
            with suppress(Exception):
                val = await loc.first.input_value()
                if _norm(val).find(target) >= 0:
                    return True
    return False

async def wait_intro_confirmed(page: Page, intro_text: str, checks: int = 8, delay_sec: float = 0.3) -> bool:
    for _ in range(checks):
        if await confirm_intro_value(page, intro_text):
            return True
        await asyncio.sleep(delay_sec)
    return False

async def set_positive_selects_if_present(page: Page, max_to_set: int = 5) -> int:
    set_count = 0
    aff_rx = re.compile(r"^(yes|tak|agree|zgadzam|accept|consent|true)$", re.I)
    scopes = ["[role='dialog']", "form"]

    for scope in scopes:
        selects = page.locator(f"{scope} select")
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

    for scope in scopes:
        combos = page.locator(f"{scope} [role='combobox']")
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
                items = page.locator("[role='option'], li[role='option'], div[role='option']")
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
                    with suppress(Exception):
                        await page.keyboard.press("Escape")
            except Exception:
                continue
    return set_count

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

# ---------- form readiness / completeness ----------
async def get_active_form_scope(page: Page) -> Optional[str]:
    scopes = ["[role='dialog'] form", "[role='dialog']", "form"]
    for scope in scopes:
        try:
            root = page.locator(scope)
            if await root.count() == 0 or not await root.first.is_visible():
                continue
            has_submit = False
            for txt in _FORM_SUBMIT_TEXTS:
                if await page.locator(f"{scope} button:has-text('{txt}')").count() > 0:
                    has_submit = True
                    break
            if has_submit:
                return scope
        except Exception:
            continue
    return None

async def audit_form_completeness(page: Page) -> Dict[str, Any]:
    scope = await get_active_form_scope(page)
    if not scope:
        return {"ok": False, "required_total": 0, "missing": [{"type": "scope", "name": "", "reason": "scope_not_found"}], "scope": None}

    report = await page.evaluate("""
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
    """, scope)

    return {
        "ok": bool(report.get("ok")),
        "required_total": int(report.get("required_total", 0)),
        "missing": list(report.get("missing", [])),
        "scope": report.get("scope"),
    }

async def is_form_ready_to_submit(page: Page) -> Tuple[bool, Dict[str, Any]]:
    report = await audit_form_completeness(page)
    return report["ok"], report

async def click_final_apply_in_form(page: Page) -> bool:
    """
    Robustly click the in-form Apply:
    - Prefer last visible, enabled button with expected text within the active form/dialog.
    - Scroll into view; try normal click, then JS click fallback.
    """
    scope = await get_active_form_scope(page)
    if not scope:
        return False

    candidates: List[str] = []
    for txt in _FORM_SUBMIT_TEXTS:
        candidates.append(f"{scope} button:has-text('{txt}')")
        candidates.append(f"{scope} [role='button']:has-text('{txt}')")
    candidates += [
        f"{scope} button[type='submit']",
        f"{scope} [data-testid*='submit']",
        f"{scope} button[class*='MuiButton-root']",
    ]

    matches = None
    for sel in candidates:
        loc = page.locator(sel)
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

async def click_final_apply_if_ready(page: Page) -> Tuple[bool, Dict[str, Any]]:
    ready, report = await is_form_ready_to_submit(page)
    if not ready:
        return False, report
    clicked = await click_final_apply_in_form(page)
    return clicked, report

# ---------- core: open form (ONE CLICK ONLY) ----------
async def open_application_form(page: Page, allow_cookie_click: bool) -> Tuple[bool, Page]:
    """
    Click the PAGE-LEVEL Apply opener **ONCE** unless a TRUE form is already visible.
    Handles popup or same-page form. Never clicks the final submit.
    Returns (form_opened, active_page).
    """
    active = page

    if allow_cookie_click:
        with suppress(Exception):
            await maybe_accept_cookies(active)

    # If already visible, no opener click
    if await is_true_application_form(active):
        step("FORM", "true form already visible -> OK")
        return True, active

    # Find opener ONCE
    opener = await find_page_apply_opener(active)
    if not opener:
        step("FORM", "opener not found -> cannot open form")
        return False, active

    # Click opener ONCE
    step("FORM", "opener found -> single click")
    before_url = ""
    with suppress(Exception):
        before_url = active.url

    popup: Optional[Page] = None
    try:
        async with active.context.expect_page(timeout=2500) as popup_info:
            with suppress(Exception):
                await opener.scroll_into_view_if_needed()
                await opener.hover()
            await opener.click(timeout=5000)
        popup = await popup_info.value
    except Exception:
        with suppress(Exception):
            await opener.click(timeout=3000)
        with suppress(Exception):
            await opener.evaluate("el => el.click()")

    if popup:
        active = popup
        step("FORM", "popup detected -> switched")
        with suppress(Exception):
            await active.wait_for_load_state("domcontentloaded", timeout=8000)
        with suppress(Exception):
            await active.wait_for_load_state("networkidle", timeout=8000)
        if allow_cookie_click:
            with suppress(Exception):
                await maybe_accept_cookies(active)
    else:
        with suppress(Exception):
            after_url = active.url
            if after_url != before_url:
                step("FORM", f"same-page opener clicked, URL changed: {before_url} -> {after_url}")
            else:
                step("FORM", "same-page opener clicked, URL unchanged (in-page dialog likely)")

    # Wait up to ~5s for the true form to appear
    for i in range(1, 11):
        if await is_true_application_form(active):
            step("FORM", f"true form visible (check {i}/10) -> OK")
            return True, active
        await asyncio.sleep(0.5)

    step("FORM", "true application form did not appear after single opener click")
    return False, active

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

    rid = row.get("id") or url
    base_page: Optional[Page] = None
    active_page: Optional[Page] = None
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

        # 1) OPEN FORM (single opener click; never submit)
        step("FORM", "open: start (single opener)")
        form_open, active_page = await open_application_form(base_page, allow_cookie_click=allow_cookie_click)
        log_row["s4_form_found"] = form_open
        if not form_open or not active_page:
            raise RuntimeError("Could not open a TRUE application form (opener not clicked or form not shown)")

        # 2) FILL INTRO
        step("FILL", "introduce-yourself: start")
        log_row["s4_introduce_filled"] = await fill_introduce_yourself(active_page, intro_text)
        step("FILL", f"introduce-yourself: {'OK' if log_row['s4_introduce_filled'] else 'not found'}")

        # 2.1) VERIFY INTRO (retry once if needed)
        step("FILL", "introduce-yourself: verify")
        log_row["s4_intro_verified"] = await wait_intro_confirmed(active_page, intro_text)
        if not log_row["s4_intro_verified"] and log_row["s4_introduce_filled"]:
            step("FILL", "introduce-yourself: re-fill & verify again")
            with suppress(Exception):
                await fill_introduce_yourself(active_page, intro_text)
            log_row["s4_intro_verified"] = await wait_intro_confirmed(active_page, intro_text)

        step("FILL", f"introduce-yourself: {'VERIFIED' if log_row['s4_intro_verified'] else 'NOT VERIFIED'}")

        # 3) OTHER FIELDS
        step("FILL", "positive-selects: start")
        log_row["s4_selects_set"] = await set_positive_selects_if_present(active_page)
        step("FILL", f"positive-selects: set={log_row['s4_selects_set']}")

        step("FILL", "consents: start")
        log_row["s4_consents_ticked"] = await tick_consents_if_present(active_page)
        step("FILL", f"consents: ticked={log_row['s4_consents_ticked']}")

        # 4) READINESS + ONLY THEN SUBMIT
        step("SUBMIT", "readiness check: start")
        ready, ready_report = await is_form_ready_to_submit(active_page)
        log_row["s4_form_ready"] = ready
        log_row["s4_form_ready_missing"] = ready_report.get("missing", [])
        log_row["s4_form_required_total"] = ready_report.get("required_total", 0)
        step("SUBMIT", f"readiness: {'READY' if ready else 'NOT READY'}; "
                       f"required_total={log_row['s4_form_required_total']}; "
                       f"missing={log_row['s4_form_ready_missing']}")

        if ready and log_row["s4_intro_verified"]:
            step("SUBMIT", "click final submit: start")
            log_row["s4_submit_clicked"], _ = await click_final_apply_if_ready(active_page)
            step("SUBMIT", f"click final submit: {'OK' if log_row['s4_submit_clicked'] else 'NOT CLICKED'}")
        else:
            if not log_row["s4_intro_verified"]:
                step("SUBMIT", "blocked: intro not verified -> skip submit")
            else:
                step("SUBMIT", "blocked: form not ready -> skip submit")

        # 5) CONFIRMATION (modal/toast/URL/body)
        step("CONFIRM", "wait: start")
        log_row["s4_confirmation"] = await wait_for_confirmation(active_page)
        step("CONFIRM", f"result: {'OK' if log_row['s4_confirmation'] else 'NO CONFIRMATION'}")

        if log_row["s4_confirmation"]:
            step("ROW", f"Application Completed -> {rid}")

        patch = {**log_row}
        if log_row["s4_confirmation"]:
            patch["processed"] = True

        updated = update_row_inplace(id_key, id_val, url_key, url_val, patch)
        if not updated:
            step("WARN", "row patch did not match any record (check id/url).")

    except Exception as e:
        log_row["s4_error"] = f"{type(e).__name__}: {e}"
        step("ERROR", log_row["s4_error"])
        update_row_inplace(id_key, id_val, url_key, url_val, {**log_row})

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENSHOTS_DIR / f"s4_{safe_filename(rid)}_{ts}.png"
        txt = ERRORS_DIR / f"s4_{safe_filename(rid)}_{ts}.txt"
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
        step("ROW", f"end: {rid}")

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

    step("RUN", f"rows to process: {len(rows)}")

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

        for idx, row in enumerate(rows, 1):
            step("RUN", f"row {idx}/{len(rows)}")
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
    step("RUN", "done")

def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
