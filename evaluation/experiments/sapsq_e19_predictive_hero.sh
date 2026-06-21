#!/bin/bash
# sapsq_e19_predictive_hero.sh — Predictive forecaster hero scenario (STUB)
#
# Pre-condition: M5 predictive Holt-Winters forecaster must be implemented
# (see specs/m-predictive-design.md). This is a stub that will fail until M5 lands.
#
# Adversarial workload pattern (built into run_sapsq.py once M5 lands):
#   Phase 1 (t=0-30s): baseline 100k IOPS each, all paths healthy
#   Phase 2 (t=30s):   t0 demand bursts to 400k IOPS (step change)
#   Phase 2 cont:      path B latency slowly drifts 27us → 50us → 200us over 30s
#
# Compared:
#   sapsq         — current reactive (baseline)
#   sapsq+predict — M5 predictive forecaster active
#
# Hero metrics:
#   t0 transient p99 in window [30s, 32s] (burst onset)
#     Reactive expects: high (waits for NEWMA to trigger)
#     Predictive expects: low (pre-allocated capacity from forecast)
#   Path B drift detection time
#     Reactive expects: ~30s (NEWMA threshold-based)
#     Predictive expects: ~15s (Holt-Winters trend slope-based)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

if ! python3 "$RUN_PY" --help 2>&1 | grep -q "enable-predictive\|forecast"; then
    echo "[E19] WARN: predictive forecaster flag not found in run_sapsq.py."
    echo "[E19] This driver is a STUB — M5 not yet implemented."
    echo "[E19] See specs/m-predictive-design.md for what needs to land first."
    exit 1
fi

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e19_predictive_hero/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-90}  # longer to capture pre/post-burst windows
SETUP="$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"
MODES=(sapsq sapsq_predict)

echo "[E19] out_base=$OUT_BASE"
for mode in "${MODES[@]}"; do
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E19] mode=$mode rep=$r"
        python3 "$RUN_PY" \
            --tenants 4 --weights "3,1,1,1" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "200000,200000,200000,200000" \
            --demand-iops-phase1 "100000,100000,100000,100000" \
            --demand-iops-phase2 "400000,100000,100000,100000" \
            --phase-transition-time 30 \
            --path-drift-target "27,200,27" \
            --path-drift-start 30 --path-drift-duration 30 \
            --probe-rate-iops "100,100,100,100" \
            --n-paths 3 --mode "$mode" \
            --setup-node1-script "$SETUP" \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e19
    done
done
echo "[E19] done. results in $OUT_BASE"
