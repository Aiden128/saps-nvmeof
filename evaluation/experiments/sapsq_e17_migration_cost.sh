#!/bin/bash
# sapsq_e17_migration_cost.sh — E17 migration cost measurement (STUB)
#
# Hypothesis: SAPS-Q reroute incurs transient latency spike during path
# migration (existing in-flight IOs on bad path must drain or RDMA QP migrate).
# Compare TRANSIENT window (t=30s..35s, first 5s after inject) vs STEADY
# window (t=40s..60s, after migration settles) per-tenant p99.
#
# Pre-condition: requires aggregate_sapsq.py to compute per-window p99
# breakdowns. Currently aggregator only computes whole-run p99. This driver
# is a stub awaiting that aggregator extension.
#
# When implemented, the run is essentially E3 (single path B degraded at t=30s)
# but reports two separate p99 numbers per tenant:
#   t0_transient_p99_us: t=30s..35s (first 5s of post-inject)
#   t0_steady_p99_us:    t=40s..60s (last 20s, steady state)
#
# Hero claim: SAPS-Q transient p99 < 2× steady p99 (bounded migration cost),
# while baselines either never converge (m4_static stuck) or never migrate
# (stock/spdk_bdev_qos with active_passive masking).

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

# Check if aggregator supports per-window p99
if ! python3 "$AGG_PY" --help 2>&1 | grep -q "transient.*window\|migration"; then
    echo "[E17] STUB — aggregator does not yet support per-window p99 breakdown."
    echo "[E17] Needed: aggregate_sapsq.py --experiment e17 must split p99 into"
    echo "[E17]   transient (t=inject_time .. inject_time+5s) vs"
    echo "[E17]   steady (t=inject_time+10s .. end-of-run)"
    echo "[E17] Per-IO latency histogram already in bdevperf binary; aggregator"
    echo "[E17] just needs to filter samples by submit_tsc range."
    echo "[E17] Run E3 driver for now as a proxy (single-window p99 only)."
    exit 1
fi

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e17_migration_cost/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
SETUP="$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"
MODES=(stock m4_static spdk_bdev_qos sapsq)

echo "[E17] out_base=$OUT_BASE"
for mode in "${MODES[@]}"; do
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E17] mode=$mode rep=$r"
        python3 "$RUN_PY" \
            --tenants 4 --weights "3,1,1,1" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "200000,200000,200000,200000" \
            --demand-iops "200000,200000,200000,200000" \
            --probe-rate-iops "100,100,100,100" \
            --n-paths 3 --mode "$mode" \
            --setup-node1-script "$SETUP" \
            --inject-path-degradation 1 --inject-time-s 30 \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e17
    done
done
echo "[E17] done. results in $OUT_BASE"
