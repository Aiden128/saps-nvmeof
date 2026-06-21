#!/bin/bash
# sapsq_e2_idle_tenant.sh — E2 driver (work-conserving with idle tenant)
#
# 4 tenants, weights=[3,1,1,1], tenant 1 idle during seconds 20-40 (middle).
# Compare m4_static vs sapsq:m4_static 應浪費 tenant 1 的 share,sapsq 應分回
# 給 active tenants。stock 沒 tenant 概念,跑來當 sanity (E2 不要求 stock pass)。
#
# 對應 specs/dm-research-redesign-20260522.md §5 E3 (called "Work-Conserving
# Idle Tenant" in spec)。
#
# Idle 機制(由 run_sapsq.py 實作):tenant 1 的 bdevperf process 在
# t=20s 收 SIGSTOP,t=40s 收 SIGCONT。其他 tenant 持續滿載。
#
# Pre-conditions:
#   同 E1。

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e2_idle_tenant/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
DEMAND=${DEMAND:-"200000,200000,200000,200000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000,200000"}
PROBE=${PROBE:-"100,100,100,100"}
N_PATHS=${N_PATHS:-3}
IDLE_SPEC=${IDLE_SPEC:-"1:20-40"}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

# stock has no tenant concept → drop. Only m4_static + sapsq make sense for E2。
MODES=(m4_static sapsq)

echo "[E2] out_base=$OUT_BASE reps=$REPS idle=$IDLE_SPEC"

for mode in "${MODES[@]}"; do
    echo "[E2] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E2] mode=$mode rep=$r → $OUT"
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --idle-tenant "$IDLE_SPEC" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e2
    done
done

echo "[E2] done. results in $OUT_BASE"
echo "[E2] NOTE: full work-conserving verdict requires per-second timeseries"
echo "[E2] of active tenants' aggregate IOPS during idle window — current"
echo "[E2] aggregator uses surrogate (weighted Jain on final IOPS)."
