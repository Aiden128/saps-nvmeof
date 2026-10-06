"""Validate RocksDB runs: usage validate_rocksdb.py STOCK_FULL_DIR SAPS_FULL_DIR."""
import glob, json, re, os, csv, sys

def io(f):
    try:
        b = json.load(open(f))["bdevs"][0]
        return b["num_read_ops"], b["read_latency_ticks"]
    except Exception:
        return None

STOCK, SAPS = (sys.argv[1:3] if len(sys.argv) >= 3 else ("stock_run/full", "saps_run/full"))
for base, mode in ((STOCK, "stock"), (SAPS, "saps")):
    for d in sorted(glob.glob(f"{base}/{mode}_*/rep_*")):
        fault = d.split("/")[-2].split("_", 1)[1]
        tl = open(d + "/spdk_tgt.log", errors="replace").read()
        paths = sorted(set(re.findall(r"path_id=(\d)", tl)))
        prof = bool(re.search(r"SAPSQ_M enabled tid=0 num_t=1 num_p=3 .*bypass_d=0 bypass_fsm=0 "
                              r"health_coupling_mode=0 health_source=0", tl))
        hmin = ""
        if os.path.exists(d + "/sampler.csv"):
            vals = [float(r["hf1"]) for r in csv.DictReader(open(d + "/sampler.csv")) if r.get("hf1")]
            hmin = f"{min(vals):.4f}" if vals else "none"
        a = io(d + "/target_pathB_iostat_pre.json")
        b = io(d + "/target_pathB_iostat_post.json")
        bstat = (f"B_reads={b[0]-a[0]} B_mean_us={(b[1]-a[1])/max(b[0]-a[0],1)/1000:.0f}"
                 if a and b else "no-iostat")
        m = json.load(open(d + "/manifest.json"))
        print(f"{mode:5s} {fault:7s} {d[-5:]} rc={m['db_bench_rc']} man={m['fault_manifested']} "
              f"ops={m['ops_per_sec']} p99={m['p99_us']/1e3:.2f} paths={paths} prof={prof} "
              f"hf1min={hmin} {bstat}")
