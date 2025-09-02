# src/s0_apply_manual_overlays.py
# Накладывает поля из data/manual_work.jsonl на соответствующие объекты
# в data/filtered_links.jsonl по (id, final_url).
# - Обновляет существующие значения.
# - Добавляет новые поля (каждое на новой строке из-за indent=1).
# - Порядок объектов и их набор сохраняются (только значения/новые ключи).

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
            # indent=1 -> каждый ключ/новый параметр на новой строке
            f.write(json.dumps(r, ensure_ascii=False, indent=1) + "\n")
    tmp.replace(p)

def main():
    if not MANUAL.exists():
        print(f"[S0] {MANUAL} not found. Skipping overlays.")
        return
    if not FILTERED.exists():
        print(f"[S0] Missing {FILTERED}", file=sys.stderr)
        sys.exit(1)

    manual_rows = load_jsonl(MANUAL)
    filtered_rows = load_jsonl(FILTERED)

    # Подготовим “оверлеи” по (id, final_url)
    overlays: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for m in manual_rows:
        mid = (m.get("id") or "").strip()
        mfinal = (m.get("final_url") or "").strip()
        if not mid or not mfinal:
            continue
        # Берём все поля, кроме ключей для матчинга
        ov = {k: v for k, v in m.items() if k not in ("id", "final_url")}
        if not ov:
            continue
        # Последняя запись в manual по паре перекрывает предыдущие
        overlays[(mid, mfinal)] = ov

    if not overlays:
        print("[S0] No overlays to apply. Nothing to update.")
        return

    updated_count = 0
    changed_rows = 0
    for r in filtered_rows:
        key = ((r.get("id") or "").strip(), (r.get("final_url") or "").strip())
        ov = overlays.get(key)
        if not ov:
            continue
        before = json.dumps(r, ensure_ascii=False, sort_keys=False)
        # Накладываем только значения (обновляем/добавляем ключи)
        for k, v in ov.items():
            r[k] = v
        after = json.dumps(r, ensure_ascii=False, sort_keys=False)
        if before != after:
            changed_rows += 1
            updated_count += len(ov)

    if changed_rows > 0:
        write_jsonl_pretty(FILTERED, filtered_rows)
        print(f"[S0] Applied overlays for {changed_rows} object(s); updated/added {updated_count} field(s).")
    else:
        print("[S0] No matching records needed updates.")

if __name__ == "__main__":
    main()
