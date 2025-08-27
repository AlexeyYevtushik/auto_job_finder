import argparse
from pathlib import Path
from .common import (
    launch_browser, save_storage_state, load_json, dump_json,
    STATE_JSON, now_iso, is_logged_in, human_sleep, handle_error
)

def main():
    parser = argparse.ArgumentParser(description="Manual login to justjoin.it and save state/base_url")
    parser.add_argument("--base-url", default="https://justjoin.it/",
                        help="Base URL to open first")
    parser.add_argument("--wait-seconds", type=int, default=180,
                        help="How long to wait for you to finish login (seconds)")
    parser.add_argument("--headful", type=str, default="true",
                        help="true/false")
    parser.add_argument("--fail-fast", action="store_true", default=False)
    args = parser.parse_args()

    headful = str(args.headful).lower() == "true"
    browser, context = launch_browser(headful=headful)
    page = context.new_page()

    try:
        page.goto(args.base_url, wait_until="domcontentloaded")
        # Если уже логин, сохраняем сразу
        if not is_logged_in(page):
            print(f"[INFO] Please log in manually. Waiting up to {args.wait_seconds}s...")
            total = args.wait_seconds
            step = 3
            while total > 0:
                if is_logged_in(page):
                    break
                human_sleep(300, 600)
                total -= step
            if not is_logged_in(page):
                raise RuntimeError("Login not completed in time.")

        save_storage_state(context)
        state = load_json(STATE_JSON, {})
        state.update({
            "base_url": args.base_url,
            "storage_state_path": str(Path("data/storage_state.json").as_posix()),
            "last_login_at": now_iso(),
            "login": {"method": "manual"}
        })
        dump_json(STATE_JSON, state)
        print("[OK] Login state saved to data/storage_state.json and data/state.json")
    except Exception:
        handle_error(page, "s1_login", args.fail_fast, step_info="login_and_save")
    finally:
        context.close()
        browser.close()

if __name__ == "__main__":
    main()
