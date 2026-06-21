#!/bin/bash
# sapsq_e16_path_flap.sh — E16 path-flap scenario
#
# Hypothesis: SAPS-Q hysteresis should prevent oscillation under rapid
# path-health state changes. Path B is toggled healthy↔degraded every 5s
# for the full 60s run, simulating an unstable network link.
#
# Expected: SAPS-Q's Slice 28 fault_type hysteresis + the recent fault-driven
# demotion fix (dpa_plugin_dev.c:4690) should:
#   1. Demote path B quickly when first degrade fires
#   2. Re-promote after warm-up probe period only after path stays healthy
#   3. Total reroute events bounded (no thrashing 5 Hz oscillation)
#
# Compared modes: stock / m4_static / spdk_bdev_qos / sapsq
# Reps: 5 per mode
# Hero metric: sapsq agg_state transitions per 60s < 10 (vs reactive thrashing > 50)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e16_path_flap/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
SETUP="$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"
MODES=(stock m4_static spdk_bdev_qos sapsq)

# Flap schedule: toggle path B every 5s for full 60s
# inject_flap_period_s=5 → 12 toggle events in 60s
FLAP_PERIOD_S=5

echo "[E16] out_base=$OUT_BASE flap_period=${FLAP_PERIOD_S}s"
for mode in "${MODES[@]}"; do
    echo "[E16] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E16] mode=$mode rep=$r"
        python3 "$RUN_PY" \
            --tenants 4 --weights "3,1,1,1" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "200000,200000,200000,200000" \
            --demand-iops "200000,200000,200000,200000" \
            --probe-rate-iops "100,100,100,100" \
            --n-paths 3 --mode "$mode" \
            --setup-node1-script "$SETUP" \
            --inject-path-flap 1 \
            --inject-flap-path 1 \
            --inject-flap-period-s "$FLAP_PERIOD_S" \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e16
    done
done
echo "[E16] done. results in $OUT_BASE"
