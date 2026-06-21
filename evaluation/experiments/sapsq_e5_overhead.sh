#!/bin/bash
# sapsq_e5_overhead.sh — E5 driver (overhead regression guard, healthy-only)
#
# E5 only tests overhead on healthy traffic; D-series hero regression-with-SAPS-Q
# is in E5b (scripts/sapsq_e5b_dseries_hero_with_sapsq.sh)。
#
# 兩個 scenario:
#   1. d0_off:SAPS-Q off (m4_static + M4 off 路徑) — 4-tenant healthy baseline
#   2. d0_on :SAPS-Q on  — 4-tenant healthy SAPS-Q
#
# Accept: D0 overhead = (d0_off - d0_on) / d0_off * 100 <= 5%
#         (preferably <= 2%)
#
# Audit follow-up (2026-05-22):
#   先前版本還包含 d1_on / d3_on,但這兩個 scenario call 的 setup script
#   (setup_arm1_D1.sh / setup_arm1_D3.sh) 是針對 D-series single-NQN topology
#   (/var/tmp/spdk_{a,b,c}.sock + nqn.2024-01.io.spdk:mptest);E5 setup 用的
#   setup_arm1_tenants.sh 已把 a/b/c sock 砍掉,改成 single nvmf_tgt + per-tenant
#   NQN。結果:D1/D3 fault injection 對 E5 topology silent fail → bdevperf attach
#   到不存在的 mptest → no data。故 E5 只保留 D0 overhead;D1/D3 hero
#   regression-with-SAPS-Q 改在 E5b 用正確的 single-NQN topology 跑。
#
# 對應 specs/dm-research-redesign-20260522.md §5 E6 (called "Overhead" in spec —
# labelled E5 here for orchestration consistency)。
#
# Outputs:
#   experiments/sapsq/e5_overhead/<ts>/d0_off/rep_<n>/...
#   experiments/sapsq/e5_overhead/<ts>/d0_on/rep_<n>/...

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e5_overhead/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
DEMAND=${DEMAND:-"200000,200000,200000,200000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000,200000"}
PROBE=${PROBE:-"100,100,100,100"}
N_PATHS=${N_PATHS:-3}

SETUP_HEALTHY=${SETUP_HEALTHY:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

run_scenario() {
    local label=$1
    local mode=$2
    local setup=$3
    local extra_args=$4
    echo "[E5] === scenario=$label mode=$mode setup=$setup ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$label/rep_$r"
        mkdir -p "$OUT"
        echo "[E5] $label rep=$r → $OUT"
        # shellcheck disable=SC2086
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --setup-node1-script "$setup" \
            --out-dir "$OUT" \
            $extra_args
        python3 "$AGG_PY" --out "$OUT" --experiment e1 || true
    done
}

# Scenario 1: D0 healthy SAPS-Q off (m4_static = M4 on, SAPS-Q off — baseline)
run_scenario "d0_off" "m4_static" "$SETUP_HEALTHY" ""
# Scenario 2: D0 healthy SAPS-Q on
run_scenario "d0_on" "sapsq" "$SETUP_HEALTHY" ""

# Combined verdict (overhead-only)
python3 "$AGG_PY" --out "$OUT_BASE" --experiment e5 \
    --e5-d0-off-dir "$OUT_BASE/d0_off" \
    --e5-d0-on-dir "$OUT_BASE/d0_on" \
    || echo "[E5] WARN: combined E5 aggregate failed"

echo "[E5] done. results in $OUT_BASE"
echo "[E5] NOTE: D1/D3 hero regression-with-SAPS-Q is verified separately by"
echo "[E5]       scripts/sapsq_e5b_dseries_hero_with_sapsq.sh under the correct"
echo "[E5]       single-NQN topology."
