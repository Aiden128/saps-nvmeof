#!/bin/bash
# sapsq_e15_asymmetric_capacity.sh — E15 asymmetric path capacity scenario
#
# Hypothesis: SAPS-Q should weight-distribute traffic proportional to path
# capacity. With path A=27us / B=50us / C=100us base delays, expected
# capacity ratio = 1 : 0.54 : 0.27 → traffic split ~55% / 30% / 15%.
#
# Stock baselines: round_robin selector wastes capacity on slow path C,
# pulling aggregate IOPS down to path-C-limited rate.
#
# No fault injection — this is a static heterogeneous-topology test.
#
# Uses setup_arm1_sapsq_4t3p_asymmetric.sh (per-path bdev_delay base differs).

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e15_asymmetric_capacity/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
SETUP="$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p_asymmetric.sh"
MODES=(stock m4_static spdk_bdev_qos sapsq)

echo "[E15] out_base=$OUT_BASE"
for mode in "${MODES[@]}"; do
    echo "[E15] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E15] mode=$mode rep=$r"
        python3 "$RUN_PY" \
            --tenants 4 --weights "3,1,1,1" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "200000,200000,200000,200000" \
            --demand-iops "200000,200000,200000,200000" \
            --probe-rate-iops "100,100,100,100" \
            --n-paths 3 --mode "$mode" \
            --setup-node1-script "$SETUP" \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e15
    done
done
echo "[E15] done. results in $OUT_BASE"
