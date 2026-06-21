#!/bin/bash
# sapsq_e10_flap_stability.sh — E10 driver (flap-stability adversarial)
#
# 4 tenants, weights=[3,1,1,1], all saturated, randread 4K。Path B 在 60s 內
# 每 5s 切換一次 HEALTHY ↔ DEGRADED(6 個完整 toggle cycle):
#   t=5s/15s/25s/35s/45s/55s  → node1 tc-netem +5ms delay 加上去
#   t=10s/20s/30s/40s/50s/60s → 移除
#
# 目的:測試 classifier FLAP detection(SAPS_FAULT_FLAP=4)與 scheduler
# RECOVERING-state monotonic ramp 之間 hysteresis 的最壞 interaction。
# 只跑 SAPS-Q mode(對比 stock 對穩定性沒意義,stock 沒 classifier)。
#
# Acceptance(verdict_e10_flap;細節見 aggregate_sapsq.py):
#   (1) 預算震盪不過衝 (no budget oscillation overshoot)
#       per-second derivative of sapsq_tenant_path_rate_q32[t][1] 不得
#       每秒超過 ±50% mean 超過 2 次
#   (2) Per-tenant share stability:weighted Jain ≥ 0.95
#   (3) Health 合法值:sapsq_path_health_q16[1] 必須屬於
#       {0, 100, 19660, 26214, 32768, 40960, 49152, 57344, 65536} 集合
#       (QUARANTINE / PROBE / FLAP-0.3 / generic / ramp steps / HEALTHY)
#   (4) Recovery convergence(僅當 DURATION > 60s 才檢查;否則標
#       "needs extended-run trace")
#   (5) sapsq_total_epochs_consumed 跨 snapshot 必須 monotonic 遞增
#
# 不在 spec §5 顯式列出 — 屬於 §4.2 path health-to-capacity mapping 的
# "FLAP classification + RECOVERING ramp 不應互相打架" 的 stability test。
#
# Pre-conditions:
#   - 同 E3 / E6 (4-NQN × 3-path multipath)
#   - sapsq_dump 必須編好(periodic dump 強依賴)
#   - node1 ssh 可直接 sudo tc(NOPASSWD 或 sudo cached)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e10_flap_stability/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-3}
DURATION=${DURATION:-60}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
DEMAND=${DEMAND:-"100000,100000,100000,100000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000"}
PROBE=${PROBE:-"100,100,100"}
N_PATHS=${N_PATHS:-3}
DUMP_INTERVAL_MS=${DUMP_INTERVAL_MS:-250}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

# node1 path B iface — 必須對齊 run_sapsq.py 的 ARM1_PATH_B_IFACE
PATH_B_IFACE=${PATH_B_IFACE:-"enp1s0f1np1"}

# Toggle 排程(秒):INJECT 6 次、REMOVE 6 次,交錯 5s
INJECT_TIMES=(5 15 25 35 45 55)
REMOVE_TIMES=(10 20 30 40 50 60)

MODES=(sapsq)

echo "[E10] out_base=$OUT_BASE reps=$REPS"
echo "[E10] toggle pattern (path B iface=$PATH_B_IFACE):"
echo "[E10]   inject  at t=${INJECT_TIMES[*]} s"
echo "[E10]   remove  at t=${REMOVE_TIMES[*]} s"
echo "[E10] periodic dump interval=${DUMP_INTERVAL_MS}ms (total run ${DURATION}s)"

# 啟動 background flap toggle loop —— 用 wallclock-anchored sleep,避免漂移。
# 該 loop 進程獨立於 run_sapsq.py;run 結束後我們確保 final teardown 必跑。
start_toggle_loop() {
    local logfile="$1"
    local t0
    t0=$(date +%s)
    (
        for i in "${!INJECT_TIMES[@]}"; do
            local ti="${INJECT_TIMES[$i]}"
            local tr="${REMOVE_TIMES[$i]}"

            # — wait until t=ti, then inject —
            local now wait_inj
            now=$(date +%s)
            wait_inj=$((t0 + ti - now))
            [ "$wait_inj" -gt 0 ] && sleep "$wait_inj"
            echo "[E10-TOGGLE] t=${ti}s INJECT (cycle $((i+1))/${#INJECT_TIMES[@]})" \
                >> "$logfile"
            ssh node1 \
                "sudo tc qdisc add dev ${PATH_B_IFACE} root netem delay 5ms \
                 || sudo tc qdisc change dev ${PATH_B_IFACE} root netem delay 5ms" \
                >> "$logfile" 2>&1 || true

            # — wait until t=tr, then remove —
            now=$(date +%s)
            local wait_rem=$((t0 + tr - now))
            [ "$wait_rem" -gt 0 ] && sleep "$wait_rem"
            echo "[E10-TOGGLE] t=${tr}s REMOVE (cycle $((i+1))/${#INJECT_TIMES[@]})" \
                >> "$logfile"
            ssh node1 \
                "sudo tc qdisc del dev ${PATH_B_IFACE} root 2>/dev/null || true" \
                >> "$logfile" 2>&1 || true
        done
        echo "[E10-TOGGLE] done" >> "$logfile"
    ) &
    echo $!
}

# 最終保險:不論 toggle loop 跑到第幾 cycle,結束前一定要把 netem 清掉,避免
# 留下 stale qdisc 影響下一 rep。
final_teardown() {
    ssh node1 \
        "sudo tc qdisc del dev ${PATH_B_IFACE} root 2>/dev/null || true" \
        >/dev/null 2>&1 || true
}

for mode in "${MODES[@]}"; do
    echo "[E10] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        TOGGLE_LOG="$OUT/toggle.log"
        : > "$TOGGLE_LOG"
        echo "[E10] mode=$mode rep=$r → $OUT"

        # 先確保進入 rep 時 path B 是乾淨的
        final_teardown

        # 啟動 toggle loop(背景);run_sapsq.py 跑 ${DURATION}s
        TOGGLE_PID=$(start_toggle_loop "$TOGGLE_LOG")
        echo "[E10] toggle loop pid=$TOGGLE_PID log=$TOGGLE_LOG"

        # 不傳 --inject-path-degradation:run_sapsq.py 不要自己 schedule
        # inject;flap pattern 完全由本 driver 的 toggle loop 控制。
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS" \
            --duration "$DURATION" --qd 32 \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --periodic-dump-interval-ms "$DUMP_INTERVAL_MS" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT" || echo "[E10] WARN: run_sapsq rep=$r non-zero exit"

        # toggle loop 應該已自然結束(最後一個 remove 在 t=60s,與
        # DURATION 對齊);保險起見 kill + final teardown。
        wait "$TOGGLE_PID" 2>/dev/null || kill "$TOGGLE_PID" 2>/dev/null || true
        final_teardown

        python3 "$AGG_PY" --out "$OUT" --experiment e10 \
            --e10-target-path 1 \
            --e10-toggle-period-s 10 \
            --e10-n-cycles 6 \
            --e10-duration-s "$DURATION" \
            --e10-weights "$WEIGHTS" \
            || echo "[E10] WARN: e10 aggregate failed for rep $r"
    done
done

echo "[E10] done. results in $OUT_BASE"
echo "[E10] timeseries snapshots at $OUT_BASE/$mode/rep_*/sapsq_periodic/"
echo "[E10] toggle logs at $OUT_BASE/$mode/rep_*/toggle.log"
