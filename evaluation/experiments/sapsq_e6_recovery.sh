#!/bin/bash
# sapsq_e6_recovery.sh — E6 driver (recovery hysteresis)
#
# 4 tenants, weights=[3,1,1,1], all saturated。
# t=10s 注入 5ms tc-netem 到 path B,t=30s 移除。共 60s。
# 量 sapsq_path_health_q16[1] 的 timeseries — 100ms 取樣間隔。
#
# 不在 spec §5 顯式列出 — 驗證 DPA scheduler RECOVERING state 的 monotonic
# +8192 Q16.16/epoch ramp-up 行為(implementation choice from dpa_plugin.c)。
# 屬於 §4.2 path health to capacity 的 "RECOVERING: ramp up with hysteresis"
# 條目的 acceptance test。
#
# Acceptance:
#   (a) health drops within 100ms of inject (drop_time < 100ms)
#   (b) health recovers to >= 0.9 × HEALTHY within 5s of netem removal
#   (c) no oscillation (HEALTHY↔DEGRADED 跳變 ≤ 1 次)
#
# 只跑 sapsq mode(這是專門測 SAPS-Q recovery logic)。
#
# Pre-conditions:
#   - 同 E3 (4-NQN × 3-path multipath)
#   - sapsq_dump 必須編好(periodic dump 強依賴)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e6_recovery/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
INJECT_AT=${INJECT_AT:-10}
REMOVE_AT=${REMOVE_AT:-30}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
DEMAND=${DEMAND:-"200000,200000,200000,200000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000,200000"}
PROBE=${PROBE:-"100,100,100,100"}
N_PATHS=${N_PATHS:-3}
INJECT_PATH=${INJECT_PATH:-1}  # 1 = path B
DUMP_INTERVAL_MS=${DUMP_INTERVAL_MS:-100}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

if [ "$REMOVE_AT" -le "$INJECT_AT" ]; then
    echo "[E6] ERROR: REMOVE_AT ($REMOVE_AT) must be > INJECT_AT ($INJECT_AT)"
    exit 1
fi
if [ "$DURATION" -le "$REMOVE_AT" ]; then
    echo "[E6] ERROR: DURATION ($DURATION) must be > REMOVE_AT ($REMOVE_AT)"
    exit 1
fi

# 只有 sapsq mode — 這是 SAPS-Q 的 recovery logic 測試
MODES=(sapsq)

echo "[E6] out_base=$OUT_BASE reps=$REPS"
echo "[E6] inject path=$INJECT_PATH at t=${INJECT_AT}s, remove at t=${REMOVE_AT}s"
echo "[E6] periodic dump interval=${DUMP_INTERVAL_MS}ms (total run ${DURATION}s)"

for mode in "${MODES[@]}"; do
    echo "[E6] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E6] mode=$mode rep=$r → $OUT"
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
            --inject-remove-time-s "$REMOVE_AT" \
            --periodic-dump-interval-ms "$DUMP_INTERVAL_MS" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT"
        python3 "$AGG_PY" --out "$OUT" --experiment e6 \
            --e6-inject-at-s "$INJECT_AT" \
            --e6-remove-at-s "$REMOVE_AT" \
            --e6-target-path "$INJECT_PATH" \
            || echo "[E6] WARN: e6 aggregate failed for rep $r"
    done
done

echo "[E6] done. results in $OUT_BASE"
echo "[E6] timeseries snapshots at $OUT_BASE/$mode/rep_*/sapsq_periodic/"
