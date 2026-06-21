#!/bin/bash
# sapsq_e1_static_fairness.sh — E1 driver
#
# Run 4 modes × 5 reps × 60s of saturated 4-tenant workload, weights=[3,1,1,1]。
# Aggregate per-rep + final pass/fail per mode。
#
# 對應 specs/dm-research-redesign-20260522.md §5 E1 (called "Static Scheduler
# Sanity" in spec)。
#
# Pre-conditions:
#   - node1 已透過 setup_arm1_tenants.sh 設好 4-NQN topology (或自訂 3-path
#     版本 via SETUP_ARM1_SCRIPT env)。本 driver 預設 single-path 4-NQN — E1
#     不依賴 multipath。
#   - bdevperf 已 build,DPA plugin 已編入。
#   - sapsq_dump 已編譯(若沒 → counter 部分 skip,IOPS/latency 仍會跑)。
#
# Outputs:
#   experiments/sapsq/e1_static_fairness/<ts>/<mode>/rep_<n>/...
#   experiments/sapsq/e1_static_fairness/<ts>/<mode>/aggregate.json (per mode)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e1_static_fairness/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
DEMAND=${DEMAND:-"200000,200000,200000,200000"}   # saturate
PATH_BASE=${PATH_BASE:-"200000,200000,200000,200000"}
PROBE=${PROBE:-"100,100,100,100"}
N_PATHS=${N_PATHS:-3}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

MODES=(stock m4_static spdk_bdev_qos sapsq)

echo "[E1] out_base=$OUT_BASE reps=$REPS duration=${DURATION}s weights=$WEIGHTS"

for mode in "${MODES[@]}"; do
    echo "[E1] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E1] mode=$mode rep=$r → $OUT"
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e1
    done
    # collect mode-level summary by averaging rep aggregates (simple)
    python3 - <<PY
import json, glob, statistics
from pathlib import Path
mode_dir = Path("$OUT_BASE/$mode")
aggs = sorted(mode_dir.glob("rep_*/aggregate.json"))
if not aggs:
    print(f"[E1] $mode: no rep aggregates found")
    raise SystemExit(0)
data = [json.loads(p.read_text()) for p in aggs]
jains = [d["metrics"]["weighted_jain"] for d in data
         if d["metrics"]["weighted_jain"] is not None]
devs = [d["metrics"]["max_share_deviation"] for d in data
        if d["metrics"]["max_share_deviation"] is not None]
passes = [d["verdict"]["pass"] for d in data]
summary = {
    "mode": "$mode",
    "reps": len(data),
    "weighted_jain_mean": (statistics.mean(jains) if jains else None),
    "weighted_jain_stdev": (statistics.stdev(jains) if len(jains) > 1 else 0),
    "max_dev_mean": (statistics.mean(devs) if devs else None),
    "pass_count": sum(1 for v in passes if v),
    "pass_rate": sum(1 for v in passes if v) / len(passes) if passes else 0,
}
(mode_dir / "aggregate.json").write_text(json.dumps(summary, indent=2))
print(f"[E1] $mode: pass {summary['pass_count']}/{summary['reps']} "
      f"jain_mean={summary['weighted_jain_mean']}")
PY
done

echo "[E1] done. results in $OUT_BASE"
