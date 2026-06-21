#!/bin/bash
# sapsq_e11_stress_n16.sh — E11 driver (SAPS-Q stress at schema max N=16 × 3 paths)
#
# Reviewer attack addressed: "does SAPS-Q hold up at SAPSQ_TENANT_MAX=16?"
# Extends E7 (N=8) to the DPA schema upper bound. Same 4 modes × 5 reps × 60s;
# weights=[3,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1] (sum=22, non-trivial proportions)。
# Aggregate per-rep + final pass/fail per mode (E11 acceptance criteria)。
#
# Acceptance (per mode, post-aggregate):
#   (a) weighted Jain >= 0.99
#   (b) max per-tenant share deviation <= 10% (E7 8%; loosened because N=16
#       has more discretization noise per tenant)
#   (c) total throughput >= 90% of (N=8 baseline × 2)
#   (d) DPA epoch rate within 25% of N=8 baseline (T×P + T log T scheduler
#       cost stays bounded — at T=16, P=3 → ~64 ops/epoch, sub-µs)
#
# Pre-conditions:
#   - node1: setup_arm1_sapsq_16t3p.sh (16 NQN × 3 listener each;
#     stride-16 port layout 4430-4445/4446-4461/4462-4477)
#   - bdevperf built, DPA plugin compiled, sapsq_dump optional
#   - For (c)/(d) scale-efficiency check, provide N=8 reference run via
#     E11_BASELINE_N8_DIR=<path/to/sapsq_e7_scale/<ts>/sapsq/rep_0>
#
# Outputs:
#   experiments/sapsq/e11_stress_n16/<ts>/<mode>/rep_<n>/...
#   experiments/sapsq/e11_stress_n16/<ts>/<mode>/aggregate.json (per mode)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e11_stress_n16/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
TENANTS=${TENANTS:-16}
# weights=[3,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1] sums to 20 (3+2+2 + 13×1);
# non-trivial split — tenant 0 gets 3/20=15%, tenants 1-2 get 2/20=10%
# each, remaining 13 tenants get 1/20=5% each.
WEIGHTS=${WEIGHTS:-"3,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1"}
# all 16 tenants saturated
DEMAND=${DEMAND:-"200000,200000,200000,200000,200000,200000,200000,200000,200000,200000,200000,200000,200000,200000,200000,200000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000"}
PROBE=${PROBE:-"100,100,100"}
N_PATHS=${N_PATHS:-3}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_16t3p.sh"}

# Optional: N=8 baseline run dir for scale-efficiency verdict
E11_BASELINE_N8_DIR=${E11_BASELINE_N8_DIR:-""}

MODES=(stock m4_static spdk_bdev_qos sapsq)

echo "[E11] out_base=$OUT_BASE reps=$REPS duration=${DURATION}s tenants=$TENANTS weights=$WEIGHTS"
if [ -n "$E11_BASELINE_N8_DIR" ]; then
    echo "[E11] N=8 baseline: $E11_BASELINE_N8_DIR"
else
    echo "[E11] no N=8 baseline (scale-efficiency check will be informational)"
fi

for mode in "${MODES[@]}"; do
    echo "[E11] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E11] mode=$mode rep=$r → $OUT"
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
        if [ -n "$E11_BASELINE_N8_DIR" ]; then
            python3 "$AGG_PY" --out "$OUT" --experiment e11 \
                --e11-baseline-n8-dir "$E11_BASELINE_N8_DIR" \
                --e11-tenants "$TENANTS"
        else
            python3 "$AGG_PY" --out "$OUT" --experiment e11 \
                --e11-tenants "$TENANTS"
        fi
    done
    # collect mode-level summary by averaging rep aggregates
    python3 - <<PY
import json, statistics
from pathlib import Path
mode_dir = Path("$OUT_BASE/$mode")
aggs = sorted(mode_dir.glob("rep_*/aggregate.json"))
if not aggs:
    print(f"[E11] $mode: no rep aggregates found")
    raise SystemExit(0)
data = [json.loads(p.read_text()) for p in aggs]
jains = [d["verdict"].get("weighted_jain") for d in data
         if d["verdict"].get("weighted_jain") is not None]
devs = [d["verdict"].get("max_share_deviation") for d in data
        if d["verdict"].get("max_share_deviation") is not None]
totals = [d["verdict"].get("total_iops") for d in data
         if d["verdict"].get("total_iops") is not None]
scale_effs = [d["verdict"].get("scale_efficiency_vs_n8x2") for d in data
              if d["verdict"].get("scale_efficiency_vs_n8x2") is not None]
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
print(f"[E11] $mode: pass {summary['pass_count']}/{summary['reps']} "
      f"jain_mean={summary['weighted_jain_mean']} "
      f"scale_eff_mean={summary['scale_efficiency_mean']}")
PY
done

echo "[E11] done. results in $OUT_BASE"
