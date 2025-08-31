# src/s7_collect_manual_work.py
# Build data/manual_work.jsonl from filtered_links.jsonl: take processed==false and keep only id, final_url, processed.

from pathlib import Path
import json, sys

IN = Path("data/filtered_links.jsonl")
OUT = Path("data/manual_work.jsonl")

def iter_json_objects(p: Path):
    dec, buf, depth, in_str, esc = json.JSONDecoder(), [], 0, False, False
    with p.open("r", encoding="utf-8") as f:
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
                    if s:
                        obj, idx = dec.raw_decode(s)
                        yield obj
                        rest=s[idx:].lstrip()
                        buf=[*rest] if rest else []
        if any(c.strip() for c in buf):
            yield dec.raw_decode("".join(buf).strip())[0]

def main():
    if not IN.exists(): print(f"[S7] Missing {IN}", file=sys.stderr); sys.exit(1)
    cnt=0
    with OUT.open("w", encoding="utf-8", newline="\n") as out:
        for obj in iter_json_objects(IN):
            if obj.get("processed") is False:
                rec={"id": obj.get("id"),
                     "final_url": obj.get("final_url") or obj.get("url"),
                     "processed": False}
                out.write(json.dumps(rec, ensure_ascii=False)+"\n")
                cnt+=1
    print(f"[S7] Wrote {cnt} records → {OUT}")

if __name__=="__main__": main()
