#!/bin/bash
# sapsq_e18_sla_hero.sh — SLA-aware scheduling hero scenario (STUB — depends on M-SLA build)
#
# Scenario: 4 tenants × 3 paths, SLA structure:
#   t0: hard SLA p99 < 500us
#   t1: soft SLA p99 < 1000us (best-effort fallback)
#   t2, t3: best-effort (no SLA)
#
# Three sub-scenarios:
#   A. Normal load: total demand within capacity, no degradation. All SLAs should be met.
#   B. Path B 5ms degradation at t=30s: SAPS-Q+SLA must keep t0 < 500us via reroute.
#   C. Saturated infeasible: demand > capacity. Hard SLA wins, best-effort starves gracefully.
#
# Modes compared:
#   stock                — no SLA, no QoS
#   m4_static            — static rate, no SLA awareness
#   spdk_bdev_qos        — SPDK QoS, no SLA awareness
#   sapsq                — current reactive max-min (no SLA reservation)
#   sapsq+sla            — NEW: SLA reservation + max-min (depends on M-SLA code)
#
# Pre-conditions:
#   - SLA fields must be plumbed through dpa_plugin_com.h schema
#   - sapsq_allocate must do 2-phase (SLA reservation, then max-min on remainder)
#   - run_sapsq.py must accept --tenant-sla-p99-us-list flag
#
# Outputs:
#   experiments/sapsq/e18_sla_hero/<ts>/<scenario>/<mode>/rep_<n>/...
#   Plus rollup aggregate.json per mode

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

# Detect whether SLA fields are available in run_sapsq.py
if ! python3 "$RUN_PY" --help 2>&1 | grep -q "tenant-sla-p99"; then
    echo "[E18] WARN: --tenant-sla-p99-us-list flag not found in run_sapsq.py."
    echo "[E18] This driver is a STUB — SLA-aware extension not yet implemented."
    echo "[E18] Run after M-SLA code lands. Aborting."
    exit 1
fi

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e18_sla_hero/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
SETUP="$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"
MODES=(stock m4_static spdk_bdev_qos sapsq sapsq_sla)

# Tenant SLAs:
#   t0 = 500us (hard)
#   t1 = 1000us (soft)
#   t2 = 0 (no SLA, best-effort)
#   t3 = 0 (no SLA, best-effort)
SLA_P99_LIST="500,1000,0,0"
SLA_PRIORITY_LIST="2,1,0,0"  # 2=hard, 1=soft, 0=best-effort

# Sub-scenario A: normal load, all paths healthy
run_scenario_A() {
    local mode=$1 rep=$2
    local OUT="$OUT_BASE/A_normal/$mode/rep_$rep"
    mkdir -p "$OUT"
    python3 "$RUN_PY" \
        --tenants 4 --weights "3,1,1,1" \
        --duration "$DURATION" --qd 32 \
        --path-base-iops "200000,200000,200000,200000" \
        --demand-iops "100000,100000,100000,100000" \
        --probe-rate-iops "100,100,100,100" \
        --tenant-sla-p99-us-list "$SLA_P99_LIST" \
        --tenant-sla-priority-list "$SLA_PRIORITY_LIST" \
        --n-paths 3 --mode "$mode" \
        --setup-node1-script "$SETUP" \
        --out-dir "$OUT"
    python3 "$AGG_PY" --out "$OUT" --experiment e18
}

# Sub-scenario B: path B degradation
run_scenario_B() {
    local mode=$1 rep=$2
    local OUT="$OUT_BASE/B_path_degrade/$mode/rep_$rep"
    mkdir -p "$OUT"
    python3 "$RUN_PY" \
        --tenants 4 --weights "3,1,1,1" \
        --duration "$DURATION" --qd 32 \
        --path-base-iops "200000,200000,200000,200000" \
        --demand-iops "100000,100000,100000,100000" \
        --probe-rate-iops "100,100,100,100" \
        --tenant-sla-p99-us-list "$SLA_P99_LIST" \
        --tenant-sla-priority-list "$SLA_PRIORITY_LIST" \
        --n-paths 3 --mode "$mode" \
        --setup-node1-script "$SETUP" \
        --inject-path-degradation 1 --inject-time-s 30 \
        --out-dir "$OUT"
    python3 "$AGG_PY" --out "$OUT" --experiment e18
}

# Sub-scenario C: saturated infeasible (demand 4x capacity)
run_scenario_C() {
    local mode=$1 rep=$2
    local OUT="$OUT_BASE/C_infeasible/$mode/rep_$rep"
    mkdir -p "$OUT"
    python3 "$RUN_PY" \
        --tenants 4 --weights "3,1,1,1" \
        --duration "$DURATION" --qd 32 \
        --path-base-iops "200000,200000,200000,200000" \
        --demand-iops "800000,800000,800000,800000" \
        --probe-rate-iops "100,100,100,100" \
        --tenant-sla-p99-us-list "$SLA_P99_LIST" \
        --tenant-sla-priority-list "$SLA_PRIORITY_LIST" \
        --n-paths 3 --mode "$mode" \
        --setup-node1-script "$SETUP" \
        --out-dir "$OUT"
    python3 "$AGG_PY" --out "$OUT" --experiment e18
}

echo "[E18] out_base=$OUT_BASE"
for sub in A B C; do
    for mode in "${MODES[@]}"; do
        for r in $(seq 0 $((REPS - 1))); do
            echo "[E18] scenario=$sub mode=$mode rep=$r"
            case "$sub" in
                A) run_scenario_A "$mode" "$r" ;;
                B) run_scenario_B "$mode" "$r" ;;
                C) run_scenario_C "$mode" "$r" ;;
            esac
        done
    done
done
echo "[E18] done. results in $OUT_BASE"
