# src/s6_prepare_filtered_jsonl.py
# S6: Reformat data/filtered_links.jsonl in-place: reorder keys, pretty-print JSON, atomic replace. Robustly parses JSONL with either one-line objects or multi-line objects (with/without blank lines) by tracking brace depth and strings.

from pathlib import Path
import json, sys, tempfile, os

INPUT_PATH = Path("data/filtered_links.jsonl")
POSTFIX_KEYS = ("url", "final_url", "description_sample")

def reorder_keys(o: dict) -> dict:
    return {**{k:v for k,v in o.items() if k not in POSTFIX_KEYS},
            **{k:v for k,v in o.items() if k in POSTFIX_KEYS}}

def iter_json_objects(path: Path):
    dec, buf, depth, in_str, esc = json.JSONDecoder(), [], 0, False, False
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            for ch in line:
                buf.append(ch)
                if in_str:
                    if esc: esc=False
                    elif ch=="\\": esc=True
                    elif ch=='"': in_str=False
                else:
                    if ch=='"': in_str=True
                    elif ch=='{': depth+=1
                    elif ch=='}': depth-=1
                if depth==0 and buf and any(c.strip() for c in buf):
                    s="".join(buf).strip()
                    obj, idx = dec.raw_decode(s)
                    yield obj
                    rest=s[idx:].lstrip()
                    buf=[*rest] if rest else []
        if any(c.strip() for c in buf):
            yield dec.raw_decode("".join(buf).strip())[0]

def write_pretty_jsonl(records, out_fp):
    for rec in records:
        out_fp.write(json.dumps(reorder_keys(rec), ensure_ascii=False, indent=2))
        out_fp.write("\n\n")

def main():
    if not INPUT_PATH.exists():
        print(f"[S6] Input not found: {INPUT_PATH}", file=sys.stderr); sys.exit(1)
    records=list(iter_json_objects(INPUT_PATH))
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", delete=False, dir=str(INPUT_PATH.parent)) as tmp:
        write_pretty_jsonl(records, tmp); tmp_name=tmp.name
    os.replace(tmp_name, INPUT_PATH)
    print(f"[S6] Done. Rewrote {len(records)} objects → {INPUT_PATH}")

if __name__=="__main__": main()
