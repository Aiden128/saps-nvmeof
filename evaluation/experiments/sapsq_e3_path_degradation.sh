#!/bin/bash
# sapsq_e3_path_degradation.sh — E3 driver (path degradation with QoS)
#
# 4 tenants, weights=[3,1,1,1], all saturated; inject 5ms tc-netem on
# node1 path B 介面 at t=30s。Run all 4 modes (stock/m4_static/spdk_bdev_qos/sapsq)。
#
# 對應 specs/dm-research-redesign-20260522.md §5 E4 (called "Path Degradation
# With QoS" in spec — paper "M hero")。
#
# Pre-conditions:
#   - node1 須有 4-NQN × 3-path topology(每 tenant NQN listen 在 3 個 port)
#     ⚠️ 預設 setup_arm1_tenants.sh 只有 single-path;E3 需要 multipath 版本。
#     若該 setup 不存在,run_sapsq.py attach_cmds() 會嘗試 3 個 port 但前 2 個
#     會 fail → log warning;real run 前須先建好 multipath node1 setup。
#   - inject 預設用 path B (path index 1) 對應 node1 ENP iface
#     (run_sapsq.py 的 ARM1_PATH_B_IFACE 常數;預設 enp1s0f1np1 — 須依實機
#     檢查並覆寫)。

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e3_path_degradation/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
INJECT_AT=${INJECT_AT:-30}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
DEMAND=${DEMAND:-"200000,200000,200000,200000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000,200000"}
PROBE=${PROBE:-"100,100,100,100"}
N_PATHS=${N_PATHS:-3}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}
INJECT_PATH=${INJECT_PATH:-1}  # 1 = path B

MODES=(stock m4_static spdk_bdev_qos sapsq)

echo "[E3] out_base=$OUT_BASE reps=$REPS inject path=$INJECT_PATH at t=${INJECT_AT}s"

for mode in "${MODES[@]}"; do
    echo "[E3] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E3] mode=$mode rep=$r → $OUT"
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --inject-path-degradation "$INJECT_PATH" \
            --inject-time-s "$INJECT_AT" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e3
    done
done

echo "[E3] done. results in $OUT_BASE"
echo "[E3] cross-mode P99 comparison (paper hero number) must be computed"
echo "[E3] in a separate post-processing step — aggregate_sapsq.py only"
echo "[E3] judges a single run; PASS/FAIL across modes needs an explicit"
echo "[E3] compare-modes script."
