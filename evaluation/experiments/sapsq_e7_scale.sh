#!/bin/bash
# sapsq_e7_scale.sh — E7 driver (SAPS-Q scale to N=8 tenants × 3 paths)
#
# Reviewer attack addressed: "does SAPS-Q scale beyond the 4-tenant sweet spot?"
# Run 4 modes × 5 reps × 60s of saturated 8-tenant workload,
# weights=[3,2,1,1,1,1,1,1] (sum=11, non-trivial proportions)。
# Aggregate per-rep + final pass/fail per mode (E7 acceptance criteria)。
#
# Acceptance (per mode, post-aggregate):
#   (a) weighted Jain >= 0.99
#   (b) max per-tenant share deviation <= 8%
#   (c) total throughput >= 90% of (N=4 single-tenant baseline × 2)
#   (d) DPA epoch rate ≈ N=4 baseline (within 25%)
#
# Pre-conditions:
#   - node1: setup_arm1_sapsq_8t3p.sh on path (8 NQN × 3 listener each)
#   - bdevperf built, DPA plugin compiled, sapsq_dump optional
#   - For (c)/(d) scale-efficiency check, provide N=4 reference run via
#     E7_BASELINE_N4_DIR=<path/to/sapsq_e1_static_fairness/<ts>/sapsq/rep_0>
#
# Outputs:
#   experiments/sapsq/e7_scale/<ts>/<mode>/rep_<n>/...
#   experiments/sapsq/e7_scale/<ts>/<mode>/aggregate.json (per mode)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e7_scale/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
TENANTS=${TENANTS:-8}
# weights=[3,2,1,1,1,1,1,1] sums to 11; gives a non-trivial proportional split
WEIGHTS=${WEIGHTS:-"3,2,1,1,1,1,1,1"}
# all 8 tenants saturated
DEMAND=${DEMAND:-"200000,200000,200000,200000,200000,200000,200000,200000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000"}
PROBE=${PROBE:-"100,100,100"}
N_PATHS=${N_PATHS:-3}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_8t3p.sh"}

# Optional: N=4 baseline run dir for scale-efficiency verdict
E7_BASELINE_N4_DIR=${E7_BASELINE_N4_DIR:-""}

MODES=(stock m4_static spdk_bdev_qos sapsq)

echo "[E7] out_base=$OUT_BASE reps=$REPS duration=${DURATION}s tenants=$TENANTS weights=$WEIGHTS"
if [ -n "$E7_BASELINE_N4_DIR" ]; then
    echo "[E7] N=4 baseline: $E7_BASELINE_N4_DIR"
else
    echo "[E7] no N=4 baseline (scale-efficiency check will be informational)"
fi

for mode in "${MODES[@]}"; do
    echo "[E7] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E7] mode=$mode rep=$r → $OUT"
        python3 "$RUN_PY" \
            --tenants "$TENANTS" --weights "$WEIGHTS" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT"
        if [ -n "$E7_BASELINE_N4_DIR" ]; then
            python3 "$AGG_PY" --out "$OUT" --experiment e7 \
                --e7-baseline-n4-dir "$E7_BASELINE_N4_DIR" \
                --e7-tenants "$TENANTS"
        else
            python3 "$AGG_PY" --out "$OUT" --experiment e7 \
                --e7-tenants "$TENANTS"
        fi
    done
    # collect mode-level summary by averaging rep aggregates
    python3 - <<PY
import json, statistics
from pathlib import Path
mode_dir = Path("$OUT_BASE/$mode")
aggs = sorted(mode_dir.glob("rep_*/aggregate.json"))
if not aggs:
    print(f"[E7] $mode: no rep aggregates found")
    raise SystemExit(0)
data = [json.loads(p.read_text()) for p in aggs]
jains = [d["verdict"].get("weighted_jain") for d in data
         if d["verdict"].get("weighted_jain") is not None]
devs = [d["verdict"].get("max_share_deviation") for d in data
        if d["verdict"].get("max_share_deviation") is not None]
totals = [d["verdict"].get("total_iops") for d in data
         if d["verdict"].get("total_iops") is not None]
scale_effs = [d["verdict"].get("scale_efficiency_vs_n4x2") for d in data
              if d["verdict"].get("scale_efficiency_vs_n4x2") is not None]
passes = [d["verdict"]["pass"] for d in data]
summary = {
    "mode": "$mode",
    "reps": len(data),
    "tenants": $TENANTS,
    "weighted_jain_mean": (statistics.mean(jains) if jains else None),
    "weighted_jain_stdev": (statistics.stdev(jains) if len(jains) > 1 else 0),
    "max_dev_mean": (statistics.mean(devs) if devs else None),
    "total_iops_mean": (statistics.mean(totals) if totals else None),
    "scale_efficiency_mean":
        (statistics.mean(scale_effs) if scale_effs else None),
    "pass_count": sum(1 for v in passes if v),
    "pass_rate":
        sum(1 for v in passes if v) / len(passes) if passes else 0,
}
(mode_dir / "aggregate.json").write_text(json.dumps(summary, indent=2))
print(f"[E7] $mode: pass {summary['pass_count']}/{summary['reps']} "
      f"jain_mean={summary['weighted_jain_mean']} "
      f"scale_eff_mean={summary['scale_efficiency_mean']}")
PY
done

echo "[E7] done. results in $OUT_BASE"
