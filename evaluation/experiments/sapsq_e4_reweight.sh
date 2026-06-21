#!/bin/bash
# sapsq_e4_reweight.sh — E4 driver (reweight convergence)
#
# 4 tenants, all saturated. Phase 1 (15s) weights=[1,1,1,1];
# phase 2 (45s) weights=[3,1,1,1]。量 reweight 後到 90% new target 的時間 +
# overshoot。
#
# 對應 specs/dm-research-redesign-20260522.md §5 E5 (called "Reweight And
# Recovery" in spec — labelled E4 in our naming for orchestration consistency)。
#
# Implementation note(實作策略 — relaunch 法):
#   DPA plugin 的 env var 只在 init 時讀一次,無 mid-run reconfig hook。所以
#   reweight 用 process relaunch:phase 1 跑 15s 後完全 kill,phase 2 用新
#   weights 立刻起來再跑 45s。phase1 / phase2 各自為一個完整 run_sapsq.py
#   invocation;aggregator 在 e4 mode 下會把兩 phase 串成 timeline。
#
# 不比 stock(stock 沒 tenant 概念);不比 spdk_bdev_qos(需要 mid-run 改 node1
# QoS,brittle 留 TODO)。只比 m4_static vs sapsq。
#
# Pre-conditions:
#   - 同 E1/E2 (node1 4-NQN setup)
#   - bdevperf + DPA plugin built;sapsq_dump 已編
#
# Outputs:
#   experiments/sapsq/e4_reweight/<ts>/<mode>/rep_<n>/phase1/
#   experiments/sapsq/e4_reweight/<ts>/<mode>/rep_<n>/phase2/
#   experiments/sapsq/e4_reweight/<ts>/<mode>/rep_<n>/aggregate.json (combined)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e4_reweight/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
PHASE1_DURATION=${PHASE1_DURATION:-15}
PHASE2_DURATION=${PHASE2_DURATION:-45}
WEIGHTS_P1=${WEIGHTS_P1:-"1,1,1,1"}
WEIGHTS_P2=${WEIGHTS_P2:-"3,1,1,1"}
DEMAND=${DEMAND:-"200000,200000,200000,200000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000,200000"}
PROBE=${PROBE:-"100,100,100,100"}
N_PATHS=${N_PATHS:-3}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

# stock:沒 tenant 概念 → skip
# spdk_bdev_qos:需要在 phase boundary 重新配置 node1 — brittle,留 TODO
# 只比 m4_static vs sapsq
MODES=(m4_static sapsq)

echo "[E4] out_base=$OUT_BASE reps=$REPS"
echo "[E4] phase1: ${PHASE1_DURATION}s weights=$WEIGHTS_P1"
echo "[E4] phase2: ${PHASE2_DURATION}s weights=$WEIGHTS_P2"

for mode in "${MODES[@]}"; do
    echo "[E4] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT/phase1" "$OUT/phase2"
        echo "[E4] mode=$mode rep=$r → $OUT"

        echo "[E4]   phase 1 (weights=$WEIGHTS_P1, ${PHASE1_DURATION}s)"
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS_P1" \
            --duration "$PHASE1_DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT/phase1"
        python3 "$AGG_PY" --out "$OUT/phase1" --experiment e1

        echo "[E4]   phase 2 (weights=$WEIGHTS_P2, ${PHASE2_DURATION}s, "
        echo "[E4]           skip node1 re-setup)"
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS_P2" \
            --duration "$PHASE2_DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --no-setup-node1 \
            --out-dir "$OUT/phase2"
        python3 "$AGG_PY" --out "$OUT/phase2" --experiment e1

        # combine phase1/phase2 into single E4 aggregate
        python3 "$AGG_PY" --out "$OUT" --experiment e4 \
            --e4-phase1-dir "$OUT/phase1" --e4-phase2-dir "$OUT/phase2" \
            --e4-weights-p1 "$WEIGHTS_P1" --e4-weights-p2 "$WEIGHTS_P2" \
            --e4-phase1-duration "$PHASE1_DURATION" \
            --e4-phase2-duration "$PHASE2_DURATION" \
            || echo "[E4] WARN: e4 aggregate failed for rep $r"
    done
done

echo "[E4] done. results in $OUT_BASE"
echo "[E4] NOTE: reweight time / overshoot is a proxy — process relaunch"
echo "[E4] takes a few seconds for socket+attach. Final-IOPS-only comparison"
echo "[E4] cannot resolve sub-second convergence;若要 sub-second 量測,需要"
echo "[E4] periodic dump (見 E6) + per-tenant rate timeline。"
