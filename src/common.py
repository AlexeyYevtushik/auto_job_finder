import json, os, time, traceback, random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
import os, shutil
from typing import Iterator  # add this at the top imports

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

DATA_DIR = Path("data")
ERRORS_DIR = DATA_DIR / "errors"
SCREENSHOTS_DIR = DATA_DIR / "screenshots"
STATE_JSON = DATA_DIR / "state.json"
STORAGE_STATE_JSON = DATA_DIR / "storage_state.json"
LINKS_JSONL = DATA_DIR / "links.jsonl"
FILTERED_JSONL = DATA_DIR / "filtered_links.jsonl"

for d in (DATA_DIR, ERRORS_DIR, SCREENSHOTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------- JSON helpers ----------
def load_json(path: Path, default: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Надёжное чтение JSON.
    - Если файла нет — вернём default/{}.
    - Если файл пустой/битый — сохраним бэкап и вернём default/{}.
    - Если внутри не dict — тоже вернём default/{}.
    """
    if not path.exists():
        return default or {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else (default or {})
    except json.JSONDecodeError:
        # Бэкапим битый файл в errors, чтобы можно было посмотреть, что внутри
        try:
            backup = ERRORS_DIR / f"{path.name}.corrupt.{ts()}"
            shutil.copyfile(path, backup)
            print(f"[WARN] Corrupt JSON at {path}. Backed up to {backup}")
        except Exception:
            pass
        return default or {}


def dump_json(path: Path, data: Dict[str, Any]) -> None:
    """
    Атомарная запись JSON: через временный файл и os.replace.
    Это защищает от обнуления файла при внезапном завершении процесса.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)  # атомарно подменяет

def append_jsonl(path: Path, item: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

# ---------- JSON helpers ----------
def read_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    if not path.exists():
        # generator early-exit: no value!
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # skip broken lines instead of crashing
                continue

# ---------- time / ids ----------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# ---------- Playwright ----------
def launch_browser(headful: bool = True) -> tuple[Browser, BrowserContext]:
    pw = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=not headful,
        args=["--disable-blink-features=AutomationControlled"]
    )

    # load storage_state as dict if file exists
    if STORAGE_STATE_JSON.exists():
        with STORAGE_STATE_JSON.open("r", encoding="utf-8") as f:
            storage_state = json.load(f)
        context = browser.new_context(storage_state=storage_state)
    else:
        context = browser.new_context()

    context.set_default_timeout(15000)
    return browser, context

def save_storage_state(context: BrowserContext) -> None:
    context.storage_state(path=str(STORAGE_STATE_JSON))

def human_sleep(min_ms=120, max_ms=320):
    time.sleep(random.uniform(min_ms/1000, max_ms/1000))

# ---------- error handling ----------
def handle_error(page: Page | None, script_name: str, fail_fast: bool, step_info: str = ""):
    """Сохраняет скрин + traceback; при fail_fast=True — выбрасывает исключение, иначе лишь печатает и продолжает."""
    stamp = ts()
    png = ERRORS_DIR / f"{script_name}_{stamp}.png"
    txt = ERRORS_DIR / f"{script_name}_{stamp}.txt"
    try:
        if page:
            page.screenshot(path=str(png), full_page=True)
    except Exception:
        pass
    tb = traceback.format_exc()
    with txt.open("w", encoding="utf-8") as f:
        f.write(f"TIME: {now_iso()}\nSCRIPT: {script_name}\nSTEP: {step_info}\n\nTRACEBACK:\n{tb}\n")
    print(f"[ERROR] {script_name}: saved {png.name} and {txt.name}")
    if fail_fast:
        raise

# ---------- selectors ----------
def is_logged_in(page: Page) -> bool:
    """
    Эвристика: ищем признаки авторизации (иконка профиля/меню). Селекторы могут меняться, поэтому 2-3 варианта.
    """
    candidates = [
        '[data-testid="user-menu"]',
        'button:has-text("Log out")',
        '*[aria-label="My profile"]'
    ]
    for sel in candidates:
        try:
            if page.locator(sel).first.is_visible():
                return True
        except Exception:
            continue
    return False
