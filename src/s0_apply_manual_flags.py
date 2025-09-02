# src/s0_apply_manual_flags.py
# Read data/manual_work.jsonl and for each record with processed==true, find a matching record
# in data/filtered_links.jsonl (match by id AND final_url) and set its processed field to true.

from pathlib import Path
import json
import sys
from typing import Dict, Any, Iterable, List, Tuple

FILTERED = Path("data/filtered_links.jsonl")
MANUAL   = Path("data/manual_work.jsonl")

def iter_json_objects(p: Path) -> Iterable[Dict[str, Any]]:
    dec, buf, depth, in_str, esc = json.JSONDecoder(), [], 0, False, False
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
                    if s:
                        obj, idx = dec.raw_decode(s)
                        yield obj
                        rest = s[idx:].lstrip()
                        buf = [*rest] if rest else []
        if any(c.strip() for c in buf):
            yield dec.raw_decode("".join(buf).strip())[0]

def load_jsonl(p: Path) -> List[Dict[str, Any]]:
    return list(iter_json_objects(p)) if p.exists() else []

def write_jsonl_pretty(p: Path, rows: List[Dict[str, Any]]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, indent=1) + "\n")
    tmp.replace(p)

def main():
    if not MANUAL.exists():
        # Finish normally without error if manual_work.jsonl is missing
        print(f"[S0] {MANUAL} not found. Skipping manual flag application.")
        return

    if not FILTERED.exists():
        print(f"[S0] Missing {FILTERED}", file=sys.stderr)
        sys.exit(1)

    manual_rows = load_jsonl(MANUAL)
    filtered_rows = load_jsonl(FILTERED)

    # Build set of (id, final_url) pairs from manual where processed==True
    targets: set[Tuple[str, str]] = set()
    for m in manual_rows:
        if m.get("processed") is True:
            mid = (m.get("id") or "").strip()
            mfinal = (m.get("final_url") or "").strip()
            if mid and mfinal:
                targets.add((mid, mfinal))

    if not targets:
        print("[S0] No manual records with processed=true. Nothing to update.")
        return

    updated = 0
    for r in filtered_rows:
        rid = (r.get("id") or "").strip()
        rfinal = (r.get("final_url") or "").strip()
        if (rid, rfinal) in targets and r.get("processed") is not True:
            r["processed"] = True
            updated += 1

    if updated > 0:
        write_jsonl_pretty(FILTERED, filtered_rows)
        print(f"[S0] Updated processed=true for {updated} record(s) in {FILTERED}")
    else:
        print("[S0] No matching records needed updates.")

if __name__ == "__main__":
    main()
