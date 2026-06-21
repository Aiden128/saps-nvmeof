#!/bin/bash
# sapsq_e9_priority_inversion.sh — E9 driver (priority inversion / work-conservation)
#
# 驗證 SAPS-Q 在「高權重租戶 (tenant 0) 實際需求遠低於其公平份額」場景下的
# 工作保守性 (work-conservation):
#   - tenant 0: weight=3, demand=20K IOPS (只用 ~⅓ 公平份額)
#   - tenant 1/2/3: weight=1, demand=200K (全力飽和)
#   - 3 paths × 200K IOPS cap = 600K total
#
# 驗收標準:
#   - tenant 0: 實際 IOPS ≈ 20K (需求綁定,不因高權重受罰)
#   - tenant 1/2/3: 平分剩餘容量 ≈ (600K - 20K) / 3 = 193K each
#   - active Jain (t1/t2/t3 only) ≥ 0.99
#   - 全系統利用率 ≥ 95% (SAPS-Q 把 t0 空出的份額分回去)
#   - SAPS-Q vs m4_static t1/t2/t3 吞吐提升 ≥ +50%
#     (m4_static token bucket 把 t1/t2/t3 限在靜態份額 1/6 × 600K = 100K each)
#
# 對應 specs/dm-research-redesign-20260522.md §5 E9 (Priority Inversion)。
#
# Pre-conditions:
#   - node1 已透過 setup_arm1_tenants.sh 設好 4-NQN 3-path topology。
#   - bdevperf 已 build,DPA plugin 已編入。
#   - sapsq_dump 已編譯 (optional;沒有 → counter 部分 skip)。
#
# Outputs:
#   experiments/sapsq/e9_priority_inversion/<ts>/<mode>/rep_<n>/...
#   experiments/sapsq/e9_priority_inversion/<ts>/aggregate.json (cross-mode verdict)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
EXP_DIR="$ROOT/experiments/sapsq/e9_priority_inversion/$TS"
mkdir -p "$EXP_DIR"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
# tenant 0 輕載 (20K),tenant 1/2/3 全力飽和 (200K)
DEMAND=${DEMAND:-"20000,200000,200000,200000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000"}
PROBE=${PROBE:-"100,100,100"}
N_PATHS=${N_PATHS:-3}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

MODES=(stock m4_static sapsq)

echo "[E9] out_base=$EXP_DIR reps=$REPS duration=${DURATION}s"
echo "[E9] weights=$WEIGHTS demand=$DEMAND"
echo "[E9] path_base=$PATH_BASE n_paths=$N_PATHS"

for mode in "${MODES[@]}"; do
    echo "[E9] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$EXP_DIR/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E9] mode=$mode rep=$r → $OUT"
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT" \
            2>&1 | tee "$OUT/run.log"
    done
done

# Cross-mode E9 verdict — passes all mode run dirs to aggregator
python3 "$AGG_PY" \
    --out "$EXP_DIR" \
    --experiment e9 \
    --e9-in-dir "$EXP_DIR" \
    --e9-demand-t0 20000 \
    --e9-path-base-iops 200000 \
    --e9-n-paths "$N_PATHS"

echo "[E9] done. Verdict in $EXP_DIR/aggregate.json"
