#!/bin/bash
# sapsq_e12_multi_fault.sh — E12 driver (multi-simultaneous-fault adversarial)
#
# 設計目的 (scenario design):
#   SAPS-Q survivability test:4 tenants, weights=[3,1,1,1], all saturated,
#   randread 4K, 60s。
#   At t=10s, 同時注入 3 種 fault 到 3 條不同 path:
#     Path A: bimodal latency (bdev_delay 交替 100µs ↔ 5000µs, 週期 500ms)
#     Path B: 5ms fail-slow (bdev_delay 恆定 5000µs)
#     Path C: sct=3 sparse errors (bdev_error_inject_nvme_error 每 100ms 注入)
#   Hold all 3 faults for 40s (t=10 → t=50), remove all at t=50。
#   Run to t=60 觀察 partial recovery。
#   只跑 sapsq mode (survivability test)。
#
# 4t3p topology note:
#   inject_D1_wallclock.sh / inject_D3_sct3_sparse.sh 為 single-NQN topology
#   專用,bdev 名為 delay_B / EE_delay_B,不存在於 setup_arm1_sapsq_4t3p.sh。
#   4t3p topology 建立:
#     spdk_a.sock ← delay_t{0..3}_A / EE_delay_t{0..3}_A  (path A)
#     spdk_b.sock ← delay_t{0..3}_B / EE_delay_t{0..3}_B  (path B)
#     spdk_c.sock ← delay_t{0..3}_C / EE_delay_t{0..3}_C  (path C)
#   注意:user 原始 prompt 提到 "/var/tmp/spdk_tenants.sock" — 該 socket 在
#   4t3p topology 不存在;各 path 有獨立 socket。此處使用正確 per-path sockets。
#
# 使用方式:
#   REPS=3 bash scripts/sapsq_e12_multi_fault.sh
#
# 覆寫環境變數:
#   REPS               重複次數 (default 5)
#   DURATION           run 秒數 (default 60)
#   FAULT_START_S      fault 注入時間 (default 10)
#   FAULT_END_S        fault 移除時間 (default 50)
#   DUMP_INTERVAL_MS   periodic dump 間隔 ms (default 250)
#   SETUP_ARM1_SCRIPT  node1 setup script (default setup_arm1_sapsq_4t3p.sh)
#   NUM_TENANTS        4t3p 每 path 的 tenant 數 (default 4)
#   SLOW_LAT_US        Path A bimodal slow 延遲 µs (default 5000)
#   FAST_LAT_US        Path A bimodal fast 延遲 µs (default 100)
#   BIMODAL_PERIOD_MS  Path A bimodal 切換週期 ms (default 500)
#   SCT3_INTERVAL_MS   Path C sparse error 注入間隔 ms (default 100)
#   SCT3_BATCH_MAX     Path C 每次注入最大 batch size (default 15)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e12_multi_fault/$TS"
mkdir -p "$OUT_BASE"

# ── Tunables ──────────────────────────────────────────────────────────────────
REPS=${REPS:-5}
DURATION=${DURATION:-60}
FAULT_START_S=${FAULT_START_S:-10}
FAULT_END_S=${FAULT_END_S:-50}
DUMP_INTERVAL_MS=${DUMP_INTERVAL_MS:-250}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
DEMAND=${DEMAND:-"100000,100000,100000,100000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000"}
PROBE=${PROBE:-"100,100,100"}
NUM_TENANTS=${NUM_TENANTS:-4}
SLOW_LAT_US=${SLOW_LAT_US:-5000}
FAST_LAT_US=${FAST_LAT_US:-100}
BIMODAL_PERIOD_MS=${BIMODAL_PERIOD_MS:-500}
SCT3_INTERVAL_MS=${SCT3_INTERVAL_MS:-100}
SCT3_BATCH_MAX=${SCT3_BATCH_MAX:-15}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

# node1 SPDK paths
RPCPY=/home/user/spdk/scripts/rpc.py
SOCK_A=/var/tmp/spdk_a.sock
SOCK_B=/var/tmp/spdk_b.sock
SOCK_C=/var/tmp/spdk_c.sock

# ── Validation ────────────────────────────────────────────────────────────────
if [ "$FAULT_END_S" -le "$FAULT_START_S" ]; then
    echo "[E12] ERROR: FAULT_END_S ($FAULT_END_S) must be > FAULT_START_S ($FAULT_START_S)"
    exit 1
fi
if [ "$DURATION" -le "$FAULT_END_S" ]; then
    echo "[E12] ERROR: DURATION ($DURATION) must be > FAULT_END_S ($FAULT_END_S)"
    exit 1
fi

echo "[E12] out_base=$OUT_BASE reps=$REPS duration=${DURATION}s"
echo "[E12] fault window: t=${FAULT_START_S}s → t=${FAULT_END_S}s"
echo "[E12] path A: bimodal ${FAST_LAT_US}µs↔${SLOW_LAT_US}µs period=${BIMODAL_PERIOD_MS}ms"
echo "[E12] path B: fail-slow constant ${SLOW_LAT_US}µs"
echo "[E12] path C: sct=3 sparse errors every ${SCT3_INTERVAL_MS}ms batch ≤${SCT3_BATCH_MAX}"

# ── Path-A bimodal inject / stop ──────────────────────────────────────────────
# Alternates all delay_t{0..N-1}_A bdevs between FAST_LAT_US and SLOW_LAT_US
# with period BIMODAL_PERIOD_MS until the stop-signal file appears.

inject_path_a_bimodal_start() {
    local OUT_DIR="$1"
    local STOP_FILE="$OUT_DIR/.stop_path_a"
    local LOG="$OUT_DIR/inject_path_a.log"
    local HALF_MS=$(( BIMODAL_PERIOD_MS / 2 ))
    local HALF_S
    HALF_S=$(awk "BEGIN{printf \"%.3f\", ${HALF_MS}/1000}")

    log_inj() { echo "[$(date '+%H:%M:%S')] [E12-pathA] $*" | tee -a "$LOG"; }
    log_inj "bimodal start: fast=${FAST_LAT_US}µs slow=${SLOW_LAT_US}µs half=${HALF_MS}ms"

    (
        log_inj() { echo "[$(date '+%H:%M:%S')] [E12-pathA] $*" >> "$LOG"; }
        PHASE=fast
        while [ ! -f "$STOP_FILE" ]; do
            if [ "$PHASE" = "fast" ]; then
                LAT=$FAST_LAT_US
                NEXT=slow
            else
                LAT=$SLOW_LAT_US
                NEXT=fast
            fi
            for i in $(seq 0 $(( NUM_TENANTS - 1 ))); do
                BDEV="delay_t${i}_A"
                for METRIC in avg_read p99_read avg_write p99_write; do
                    ssh node1 "sudo ${RPCPY} -s ${SOCK_A} \
                        bdev_delay_update_latency ${BDEV} ${METRIC} ${LAT}" \
                        >>"$LOG" 2>&1 || log_inj "WARN: RPC ${BDEV} ${METRIC} failed"
                done
            done
            log_inj "phase=${PHASE} lat=${LAT}µs"
            PHASE=$NEXT
            sleep "$HALF_S"
        done
        # Cleanup: reset to FAST_LAT_US (baseline)
        log_inj "stop received — resetting path A to baseline ${FAST_LAT_US}µs"
        for i in $(seq 0 $(( NUM_TENANTS - 1 ))); do
            BDEV="delay_t${i}_A"
            for METRIC in avg_read p99_read avg_write p99_write; do
                ssh node1 "sudo ${RPCPY} -s ${SOCK_A} \
                    bdev_delay_update_latency ${BDEV} ${METRIC} ${FAST_LAT_US}" \
                    >>"$LOG" 2>&1 || true
            done
        done
        log_inj "path A reset done"
    ) &
    echo $! > "$OUT_DIR/.pid_path_a"
}

inject_path_a_bimodal_stop() {
    local OUT_DIR="$1"
    local STOP_FILE="$OUT_DIR/.stop_path_a"
    touch "$STOP_FILE"
    # Wait for background loop (stop file triggers cleanup + exit)
    local PID_FILE="$OUT_DIR/.pid_path_a"
    if [ -f "$PID_FILE" ]; then
        local PID
        PID=$(cat "$PID_FILE")
        wait "$PID" 2>/dev/null || true
    fi
}

# ── Path-B fail-slow inject / stop ───────────────────────────────────────────
# Sets all delay_t{0..N-1}_B to SLOW_LAT_US (5ms) immediately.

inject_path_b_failslow_start() {
    local OUT_DIR="$1"
    local LOG="$OUT_DIR/inject_path_b.log"
    log_inj() { echo "[$(date '+%H:%M:%S')] [E12-pathB] $*" | tee -a "$LOG"; }
    log_inj "fail-slow start: lat=${SLOW_LAT_US}µs"
    for i in $(seq 0 $(( NUM_TENANTS - 1 ))); do
        BDEV="delay_t${i}_B"
        for METRIC in avg_read p99_read avg_write p99_write; do
            ssh node1 "sudo ${RPCPY} -s ${SOCK_B} \
                bdev_delay_update_latency ${BDEV} ${METRIC} ${SLOW_LAT_US}" \
                >>"$LOG" 2>&1 \
                || log_inj "WARN: RPC ${BDEV} ${METRIC} failed"
        done
    done
    log_inj "all delay_t{0..$(( NUM_TENANTS-1 ))}_B set to ${SLOW_LAT_US}µs"
}

inject_path_b_failslow_stop() {
    local OUT_DIR="$1"
    local LOG="$OUT_DIR/inject_path_b.log"
    log_inj() { echo "[$(date '+%H:%M:%S')] [E12-pathB] $*" >> "$LOG"; }
    log_inj "fail-slow stop: resetting to baseline ${FAST_LAT_US}µs"
    for i in $(seq 0 $(( NUM_TENANTS - 1 ))); do
        BDEV="delay_t${i}_B"
        for METRIC in avg_read p99_read avg_write p99_write; do
            ssh node1 "sudo ${RPCPY} -s ${SOCK_B} \
                bdev_delay_update_latency ${BDEV} ${METRIC} ${FAST_LAT_US}" \
                >>"$LOG" 2>&1 || true
        done
    done
    log_inj "path B reset done"
}

# ── Path-C sct=3 sparse inject / stop ────────────────────────────────────────
# Runs a periodic loop injecting bdev_error_inject_nvme_error sct=3 on all
# EE_delay_t{0..N-1}_C bdevs until the stop-signal file appears.

inject_path_c_sct3_start() {
    local OUT_DIR="$1"
    local STOP_FILE="$OUT_DIR/.stop_path_c"
    local LOG="$OUT_DIR/inject_path_c.log"
    local SLEEP_S
    SLEEP_S=$(awk "BEGIN{printf \"%.3f\", ${SCT3_INTERVAL_MS}/1000}")

    log_inj() { echo "[$(date '+%H:%M:%S')] [E12-pathC] $*" | tee -a "$LOG"; }
    log_inj "sct=3 sparse start: interval=${SCT3_INTERVAL_MS}ms batch_max=${SCT3_BATCH_MAX}"

    # Pre-clear any stale injection
    for i in $(seq 0 $(( NUM_TENANTS - 1 ))); do
        ssh node1 "sudo ${RPCPY} -s ${SOCK_C} \
            bdev_error_inject_error EE_delay_t${i}_C clear failure" \
            >>"$LOG" 2>&1 || true
    done

    (
        log_inj() { echo "[$(date '+%H:%M:%S')] [E12-pathC] $*" >> "$LOG"; }
        ITER=0
        while [ ! -f "$STOP_FILE" ]; do
            ITER=$(( ITER + 1 ))
            BATCH=$(( (RANDOM % SCT3_BATCH_MAX) + 1 ))
            for i in $(seq 0 $(( NUM_TENANTS - 1 ))); do
                ssh node1 "sudo ${RPCPY} -s ${SOCK_C} \
                    bdev_error_inject_nvme_error --sct 3 --sc 0 \
                    EE_delay_t${i}_C read -n ${BATCH}" \
                    >>"$LOG" 2>&1 \
                    || log_inj "WARN: iter=${ITER} tenant=${i} RPC failed"
            done
            log_inj "iter=${ITER} batch=${BATCH}"
            sleep "$SLEEP_S"
        done
        # Cleanup: clear injection on all tenants
        log_inj "stop received — clearing sct=3 injection on path C"
        for i in $(seq 0 $(( NUM_TENANTS - 1 ))); do
            ssh node1 "sudo ${RPCPY} -s ${SOCK_C} \
                bdev_error_inject_error EE_delay_t${i}_C clear failure" \
                >>"$LOG" 2>&1 || true
        done
        log_inj "path C cleared"
    ) &
    echo $! > "$OUT_DIR/.pid_path_c"
}

inject_path_c_sct3_stop() {
    local OUT_DIR="$1"
    local STOP_FILE="$OUT_DIR/.stop_path_c"
    touch "$STOP_FILE"
    local PID_FILE="$OUT_DIR/.pid_path_c"
    if [ -f "$PID_FILE" ]; then
        local PID
        PID=$(cat "$PID_FILE")
        wait "$PID" 2>/dev/null || true
    fi
}

# ── Inject orchestration (runs in background subshell during bdevperf) ────────
# Fires all 3 _start at t=FAULT_START_S, all 3 _stop at t=FAULT_END_S.

run_inject_sequence() {
    local OUT_DIR="$1"
    local LOG="$OUT_DIR/inject_orch.log"
    echo "[$(date '+%H:%M:%S')] [E12-orch] sleeping ${FAULT_START_S}s before fault injection" >> "$LOG"
    sleep "$FAULT_START_S"
    echo "[$(date '+%H:%M:%S')] [E12-orch] t=${FAULT_START_S}s: firing all 3 faults" >> "$LOG"
    inject_path_a_bimodal_start "$OUT_DIR" &
    inject_path_b_failslow_start "$OUT_DIR" >> "$LOG" 2>&1 &
    inject_path_c_sct3_start "$OUT_DIR" &
    wait
    echo "[$(date '+%H:%M:%S')] [E12-orch] all 3 faults active; sleeping until t=${FAULT_END_S}s" >> "$LOG"
    sleep $(( FAULT_END_S - FAULT_START_S ))
    echo "[$(date '+%H:%M:%S')] [E12-orch] t=${FAULT_END_S}s: removing all 3 faults" >> "$LOG"
    inject_path_a_bimodal_stop "$OUT_DIR" &
    inject_path_b_failslow_stop "$OUT_DIR" >> "$LOG" 2>&1 &
    inject_path_c_sct3_stop "$OUT_DIR" &
    wait
    echo "[$(date '+%H:%M:%S')] [E12-orch] all faults removed; post-fault window until t=${DURATION}s" >> "$LOG"
}

# ── Main loop ─────────────────────────────────────────────────────────────────

for r in $(seq 0 $(( REPS - 1 ))); do
    OUT="$OUT_BASE/sapsq/rep_$r"
    mkdir -p "$OUT"
    echo "[E12] === rep=$r === out=$OUT"

    # Launch fault injection sequence in background
    run_inject_sequence "$OUT" &
    INJECT_PID=$!

    # Run sapsq mode
    python3 "$RUN_PY" \
        --tenants 4 --weights "$WEIGHTS" \
        --duration "$DURATION" --qd 32 \
        --path-base-iops "$PATH_BASE" \
        --demand-iops "$DEMAND" \
        --probe-rate-iops "$PROBE" \
        --n-paths 3 \
        --mode sapsq \
        --periodic-dump-interval-ms "$DUMP_INTERVAL_MS" \
        --setup-node1-script "$SETUP_ARM1_SCRIPT" \
        --out-dir "$OUT"

    # Ensure inject sequence has exited (cleanup paths)
    wait "$INJECT_PID" 2>/dev/null || true

    python3 "$AGG_PY" --out "$OUT" --experiment e12 \
        --e12-fault-start-s "$FAULT_START_S" \
        --e12-fault-end-s "$FAULT_END_S" \
        --e12-n-paths 3 \
        --e12-n-tenants 4 \
        || echo "[E12] WARN: e12 aggregate failed for rep $r"
done

echo "[E12] done. results in $OUT_BASE"
echo "[E12] periodic snapshots at $OUT_BASE/sapsq/rep_*/sapsq_periodic/"
