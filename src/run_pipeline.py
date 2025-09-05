# src/run_pipeline.py
# Orchestrate S-steps using values from config/config.json (PIPELINE.SEQ, .SLEEP_SECONDS, .KEEP_GOING, .FORCE_S1). Runs with no CLI args.

from pathlib import Path
import json, subprocess, sys, time

ROOT  = Path(__file__).resolve().parents[1]
SRC   = ROOT / "src"
STATE = ROOT / "data" / "storage_state.json"
CFG   = ROOT / "config" / "config.json"

CANDIDATES = {
  "s0": ["s0_apply_manual_flags"],
  "s1": ["s1_prepare","s1_manual_login","s1_login"],
  "s2": ["s2_collect_links"],
  "s3": ["s3_filter_descriptions","s3_scrape_details"],
  "s5": ["s5_get_links_to_apply_manualy"],
}

def resolve(step: str) -> str:
    for name in CANDIDATES.get(step, []):
        if "*" in name:
            for f in SRC.glob(f"{name}.py"): return f"src.{f.stem}"
        if (SRC/f"{name}.py").exists(): return f"src.{name}"
    hits = sorted(SRC.glob(f"{step}_*.py"))
    if hits: return f"src.{hits[0].stem}"
    raise SystemExit(f"[runner] No module for {step} in {SRC}")

def parse_seq(seq) -> list[str]:
    tokens = seq if isinstance(seq, list) else [t.strip() for t in str(seq).split(",") if t.strip()]
    out=[]
    for tok in tokens:
        if "x" in tok: s, n = tok.split("x",1); out += [s]*int(n)
        elif "*" in tok: s, n = tok.split("*",1); out += [s]*int(n)
        else: out.append(tok)
    return out

def run(mod: str) -> int:
    print(f"[runner] → python -m {mod}")
    return subprocess.call([sys.executable,"-m",mod], cwd=str(ROOT))

def load_opts():
    opts={"SEQ":"s0,s1,s2,s3,s5","SLEEP_SECONDS":0.0,"KEEP_GOING":False,"FORCE_S1":False}
    if CFG.exists():
        cfg=json.loads(CFG.read_text(encoding="utf-8"))
        p=cfg.get("PIPELINE") or {}
        opts["SEQ"]=p.get("SEQ", cfg.get("RUN_SEQ", opts["SEQ"]))
        opts["SLEEP_SECONDS"]=p.get("SLEEP_SECONDS", cfg.get("RUN_SLEEP_SECONDS", opts["SLEEP_SECONDS"]))
        opts["KEEP_GOING"]=p.get("KEEP_GOING", cfg.get("RUN_KEEP_GOING", opts["KEEP_GOING"]))
        opts["FORCE_S1"]=p.get("FORCE_S1", cfg.get("RUN_FORCE_S1", opts["FORCE_S1"]))
    return opts

def main():
    o=load_opts(); steps=parse_seq(o["SEQ"])
    # Always ensure s0 runs first (deduplicate if present later)
    if not steps or steps[0] != "s0":
        if "s0" in steps:
            steps = ["s0"] + [s for s in steps if s != "s0"]
        else:
            print("[runner] prepending s0 to sequence")
            steps = ["s0"] + steps

    for step in steps:
        if step=="s1" and STATE.exists() and not o["FORCE_S1"]:
            print("[runner] skip s1 (storage_state.json exists)"); rc=0
        else:
            rc=run(resolve(step))
        if rc!=0 and not o["KEEP_GOING"]: raise SystemExit(rc)
        if o["SLEEP_SECONDS"]: time.sleep(float(o["SLEEP_SECONDS"]))
    print("[runner] done")

if __name__=="__main__": main()
