# src/s7_collect_manual_work.py
# Build data/manual_work.jsonl from filtered_links.jsonl:
# take processed==false and keep only id, final_url, processed, description_sample
# where description_sample is cleaned to:
# - keep content starting from the first "All offers" line (remove everything before it)
# - stop BEFORE the next "Apply" line (remove that "Apply" line and everything after it)
# - drop empty rows and invisible chars

from pathlib import Path
import json, sys, re

IN = Path("data/filtered_links.jsonl")
OUT = Path("data/manual_work.jsonl")

def iter_json_objects(p: Path):
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

def _strip_invisibles(text: str) -> str:
    if not text:
        return ""
    # remove zero-width/invisible characters
    return re.sub(r"[\u200B-\u200D\uFEFF]", "", text)

def _slice_between_markers(lines):
    """
    Keep from the FIRST 'All offers' (inclusive) up to BEFORE the NEXT 'Apply' (exclusive).
    If 'All offers' missing -> return original lines.
    If 'Apply' after it missing -> keep until end.
    Comparisons are case-insensitive and trimmed.
    """
    norm = [ln.strip() for ln in lines]
    # find first "All offers"
    try:
        start = next(i for i, ln in enumerate(norm) if ln.lower() == "all offers")
    except StopIteration:
        return lines  # no slice if marker not found

    # find next "Apply" after start
    end = None
    for j in range(start + 1, len(norm)):
        if norm[j].lower() == "apply":
            end = j
            break

    if end is None:
        sliced = lines[start:]
    else:
        sliced = lines[start:end]  # exclude the 'Apply' row itself

    return sliced

def to_visible_rows(text: str):
    if not text:
        return []
    t = _strip_invisibles(text)
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.rstrip() for ln in t.split("\n")]

    # slice between 'All offers' … 'Apply'
    lines = _slice_between_markers(lines)

    # drop empty rows
    lines = [ln.strip() for ln in lines if ln.strip()]

    return lines

def main():
    if not IN.exists():
        print(f"[S7] Missing {IN}", file=sys.stderr)
        sys.exit(1)

    OUT.parent.mkdir(parents=True, exist_ok=True)

    cnt = 0
    with OUT.open("w", encoding="utf-8", newline="\n") as out:
        for obj in iter_json_objects(IN):
            if obj.get("processed") is False:
                rec = {
                    "id": obj.get("id"),
                    "final_url": obj.get("final_url") or obj.get("url"),
                    "processed": False,
                }
                # new line before new parameter; description_sample cleaned and sliced
                rec["description_sample"] = to_visible_rows(obj.get("description_sample"))

                # Pretty JSON so each field starts on a new line
                out.write(json.dumps(rec, ensure_ascii=False, indent=1) + "\n")
                cnt += 1

    print(f"[S7] Wrote {cnt} records → {OUT}")

if __name__ == "__main__":
    main()
