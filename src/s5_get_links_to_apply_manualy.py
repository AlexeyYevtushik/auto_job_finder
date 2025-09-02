# src/s5_get_links_to_apply_manualy.py
# Build data/manual_work.jsonl from data/filtered_links.jsonl
# - take records with processed == false
# - keep only: id, final_url (fallback: url), processed:false, description_sample (cleaned)
# Cleaning rules for description_sample:
#   * accepts str | list[str] | None
#   * removes zero-width/invisible chars and empty lines
#   * normalizes newlines
#   * optionally slices from the first 'All offers'/'Wszystkie oferty' (inclusive)
#     up to BEFORE the next 'Apply'/'Aplikuj' (exclusive)
#
# Run:
#   python -m src.s5_get_links_to_apply_manualy

from pathlib import Path
import json, sys, re
from typing import Dict, Any, Iterable, List, Optional

IN = Path("data/filtered_links.jsonl")
OUT = Path("data/manual_work.jsonl")


def iter_json_objects(p: Path) -> Iterable[Dict[str, Any]]:
    """
    Robustly stream JSON objects from a file where objects may be:
    - one per line (classic .jsonl), or
    - pretty-printed across multiple lines, back-to-back.
    """
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
                    yield obj
                    rest = s[idx:].lstrip()
                    buf = list(rest) if rest else []

        # trailing remainder
        if any(c.strip() for c in buf):
            s = "".join(buf).strip()
            obj, _ = dec.raw_decode(s)
            yield obj


def _strip_invisibles(text: str) -> str:
    if not text:
        return ""
    # remove zero-width/invisible characters
    return re.sub(r"[\u200B-\u200D\uFEFF]", "", text)


def _slice_between_markers(lines: List[str]) -> List[str]:
    """
    Keep from the FIRST 'All offers'/'Wszystkie oferty' (inclusive)
    to BEFORE the FIRST 'Apply'/'Aplikuj' after it (exclusive).
    If start marker missing -> return the original lines.
    If end marker missing -> keep until the end.
    """
    start_markers = {"all offers", "wszystkie oferty"}
    end_markers = {"apply", "aplikuj"}

    norm = [ln.strip().lower() for ln in lines]
    start: Optional[int] = next((i for i, ln in enumerate(norm) if ln in start_markers), None)
    if start is None:
        return lines

    end: Optional[int] = next(
        (j for j, ln in enumerate(norm[start + 1 :], start + 1) if ln in end_markers),
        None,
    )
    return lines[start:] if end is None else lines[start:end]


def to_visible_rows(text_or_lines) -> List[str]:
    """
    Accepts str | list[str] | None.
    Returns a cleaned list[str].
    """
    if not text_or_lines:
        return []

    lines: List[str] = []

    if isinstance(text_or_lines, list):
        for item in text_or_lines:
            if item is None:
                continue
            s = _strip_invisibles(str(item))
            s = s.replace("\r\n", "\n").replace("\r", "\n")
            lines.extend([ln.rstrip() for ln in s.split("\n")])
    else:
        s = _strip_invisibles(str(text_or_lines))
        s = s.replace("\r\n", "\n").replace("\r", "\n")
        lines = [ln.rstrip() for ln in s.split("\n")]

    # optional slice between markers
    lines = _slice_between_markers(lines)

    # drop empty lines
    return [ln.strip() for ln in lines if ln.strip()]


def main():
    if not IN.exists():
        print(f"[S5] Missing {IN}", file=sys.stderr)
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
                # normalize description_sample (str | list[str] -> list[str])
                rec["description_sample"] = to_visible_rows(obj.get("description_sample"))
                out.write(json.dumps(rec, ensure_ascii=False, indent=1) + "\n")
                cnt += 1

    print(f"[S5] Wrote {cnt} records → {OUT}")


if __name__ == "__main__":
    main()
