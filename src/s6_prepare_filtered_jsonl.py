# src/s6_prepare_filtered_jsonl.py
# S6: Prepare data/filtered_links.jsonl in-place (no CLI args).
# - Moves url, final_url, description_sample to the end of each object
# - Pretty-print multi-line JSON per record
# - Safely rewrites the original file

from pathlib import Path
import json
import sys
import tempfile
import os

INPUT_PATH = Path("data/filtered_links.jsonl")
POSTFIX_KEYS = ("url", "final_url", "description_sample")

def reorder_keys(obj: dict) -> dict:
    new_obj = {}
    for k in obj.keys():
        if k not in POSTFIX_KEYS:
            new_obj[k] = obj[k]
    for k in POSTFIX_KEYS:
        if k in obj:
            new_obj[k] = obj[k]
    return new_obj

def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {lineno}: {e}") from e

def write_pretty_jsonl(records, out_fp):
    for rec in records:
        ordered = reorder_keys(rec)
        pretty = json.dumps(ordered, ensure_ascii=False, indent=2)
        out_fp.write(pretty)
        out_fp.write("\n\n")  # blank line between objects

def main():
    if not INPUT_PATH.exists():
        print(f"[S6] Input not found: {INPUT_PATH}", file=sys.stderr)
        sys.exit(1)

    records = list(iter_jsonl(INPUT_PATH))

    # write to temp file in same dir, then replace atomically
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", delete=False, dir=str(INPUT_PATH.parent)) as tmp:
        tmp_name = tmp.name
        write_pretty_jsonl(records, tmp)

    os.replace(tmp_name, INPUT_PATH)
    print(f"[S6] Done. Rewrote {len(records)} objects → {INPUT_PATH}")

if __name__ == "__main__":
    main()
