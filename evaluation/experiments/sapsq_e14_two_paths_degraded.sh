#!/bin/bash
# sapsq_e14_two_paths_degraded.sh — E14 two-path degradation scenario
#
# Hypothesis: SAPS-Q must consolidate to a single healthy path when 2 out of 3
# paths simultaneously degrade. Test that t0 high-weight tenant maintains tail
# latency < 500us even when only path A remains healthy.
#
# Scenario:
#   t=0..30s: all 3 paths healthy, 4 tenants saturate
#   t=30s:    inject 5ms delay on BOTH path B AND path C (only A stays good)
#   t=30..60s: SAPS-Q must reroute all traffic to path A
#
# Modes:  stock / m4_static / spdk_bdev_qos / sapsq (all use round_robin baseline)
# Reps:   5 per mode
# Hero:   sapsq t0 p99 < 500us; baselines should pile on degraded paths

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e14_two_paths_degraded/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
SETUP="$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"
MODES=(stock m4_static spdk_bdev_qos sapsq)

# E14: inject on paths 1 (B) AND 2 (C)
INJECT_PATHS="1,2"

echo "[E14] out_base=$OUT_BASE"
for mode in "${MODES[@]}"; do
    echo "[E14] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E14] mode=$mode rep=$r"
        python3 "$RUN_PY" \
            --tenants 4 --weights "3,1,1,1" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "200000,200000,200000,200000" \
            --demand-iops "200000,200000,200000,200000" \
            --probe-rate-iops "100,100,100,100" \
            --n-paths 3 --mode "$mode" \
            --setup-node1-script "$SETUP" \
            --inject-path-degradation-list "$INJECT_PATHS" \
            --inject-time-s 30 \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e14
    done
done
echo "[E14] done. results in $OUT_BASE"
