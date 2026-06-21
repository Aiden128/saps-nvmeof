#!/bin/bash
# sapsq_e5b_dseries_hero_with_sapsq.sh — E5b driver
#
# D-series hero regression-with-SAPS-Q: 確認 D1 / D3 在 SAPS-Q ON 下仍能 hold
# hero ratios within CI vs SAPS-Q OFF baseline。
#
# 為什麼是 E5b (不在 E5 內):
#   E5 (sapsq_e5_overhead.sh) 跑 4-tenant healthy topology
#   (setup_arm1_tenants.sh) — single nvmf_tgt + per-tenant NQN。D1 / D3 fault
#   injection script (setup_arm1_D1.sh / setup_arm1_D3.sh) 是針對 D-series
#   single-NQN topology (3 個 nvmf_tgt + nqn.2024-01.io.spdk:mptest)。兩者
#   topology 不相容,塞在同 driver 內會 silent fail (audit 2026-05-22 發現)。
#   故 E5b 用正確的 D-series topology 獨立跑。
#
# Topology (D-series):
#   - node1: 3 個 nvmf_tgt 跑 cores 0x3/0xc/0x30, 各自 listen on 4430/4431/4432
#   - 共用 NQN nqn.2024-01.io.spdk:mptest (multipath merge via shared UUID/NGUID)
#   - node2: single bdevperf,attach 3 controllers 到同一 bdev "mp",
#     讓 SPDK multipath 合併成單一 bdev "mpn1"
#
# Workload: single-tenant randread 4KB QD=32
#
# 4 個 scenario:
#   1. d1_sapsqoff: D1 fail-slow (setup_arm1_D1.sh), SAPS-Q OFF
#   2. d1_sapsqon : D1 fail-slow, SAPS-Q ON
#   3. d3_sapsqoff: D3 sct=3 sparse errors (setup_arm1_D3.sh), SAPS-Q OFF
#   4. d3_sapsqon : D3 sct=3 sparse, SAPS-Q ON
#
# Acceptance (見 aggregate_sapsq.py::verdict_e5b_dseries_hero):
#   - d1_sapsqoff IOPS >= 700K
#   - d1_sapsqon  IOPS >= 0.85 × d1_sapsqoff
#   - d3_sapsqoff max_tail_latency_ms < 100
#   - d3_sapsqon  max_tail_latency_ms < 4 × d3_sapsqoff
#
# 對應 specs/dm-research-redesign-20260522.md §5 E5 ("D1 hero should still
# hold within CI").
#
# Outputs:
#   experiments/sapsq/e5b_dseries_hero/<ts>/<scenario>/rep_<n>/...

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"
BDEVPERF="/home/user/spdk/build/examples/bdevperf"
RPCPY="/home/user/spdk/scripts/rpc.py"
BDEVPERF_PY="/home/user/spdk/examples/bdev/bdevperf/bdevperf.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e5b_dseries_hero/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
QD=${QD:-32}
IO_SIZE=${IO_SIZE:-4096}

# D-series topology constants
TARGET_IP="10.0.1.1"
NQN="nqn.2024-01.io.spdk:mptest"
PORTS=(4430 4431 4432)

# node1 setup scripts
SETUP_HEALTHY="$ROOT/experiments/3path_targets/setup_arm1.sh"
SETUP_D1="$ROOT/experiments/3path_targets/setup_arm1_D1.sh"
SETUP_D3="$ROOT/experiments/3path_targets/setup_arm1_D3.sh"

# node2 bdevperf socket
PERF_SOCK="/var/tmp/bdevperf_e5b.sock"

run_arm1() {
    local script=$1
    echo "[E5b] node1: bash $script"
    if ! ssh node1 "cd $ROOT && bash $script"; then
        echo "[E5b] FATAL: node1 script $script failed" >&2
        exit 1
    fi
}

run_one_rep() {
    local out=$1       # rep dir
    local sapsq=$2     # off | on

    mkdir -p "$out"
    local log="$out/bdevperf.log"
    local result_json="$out/result.json"
    local sapsq_summary="$out/sapsq_summary.json"

    # ── node2 cleanup ────────────────────────────────────────────────────────
    sudo rm -f "$PERF_SOCK"
    sudo pkill -9 -f "$BDEVPERF" 2>/dev/null || true
    sleep 1

    # ── Build env vars for SAPS-Q on/off ─────────────────────────────────────
    # D-series uses ports 4430/4431/4432 on the SAME NQN — DPA_PLUGIN_PATH_MAP_PORTS
    # maps each port to a distinct path_id so SAPS-Q per-path admit/probe works.
    local env_prefix=()
    if [ "$sapsq" = "on" ]; then
        env_prefix=(
            "DPA_PLUGIN_ROLE=standalone"
            "DPA_PLUGIN_DISABLE_INIT=0"
            "HOST_SAPS_ENABLED=0"
            "DPA_PLUGIN_SAMPLE_RATE=1"
            "SAPS_TRACE=0"
            "SAPS_Q_ENABLED=1"
            "SAPS_Q_MY_TENANT_ID=0"
            "SAPS_Q_WEIGHTS=65536"
            "SAPS_Q_DEMAND_Q32=429497"
            "SAPS_Q_PATH_BASE_IOPS_Q32=858993,858993,858993"
            "SAPS_Q_PROBE_RATE_Q32=429,429,429"
            "DPA_PLUGIN_PATH_MAP_PORTS=4430:0,4431:1,4432:2"
            "SAPS_M4_ENABLED=0"
            "SAPS_M5_DRR_ENABLED=0"
        )
    else
        env_prefix=(
            "DPA_PLUGIN_DISABLE_INIT=1"
            "SAPS_Q_ENABLED=0"
            "SAPS_M4_ENABLED=0"
            "SAPS_M5_DRR_ENABLED=0"
        )
    fi

    # ── Launch bdevperf with --wait-for-rpc ──────────────────────────────────
    echo "[E5b]   launching bdevperf (sapsq=$sapsq)"
    sudo numactl --cpunodebind=0 --membind=0 \
        env "${env_prefix[@]}" \
        "$BDEVPERF" \
            -m 0x3c \
            -r "$PERF_SOCK" \
            --wait-for-rpc \
            -g -s 384 \
            -q "$QD" -o "$IO_SIZE" -w randread \
            -t "$DURATION" \
            -z -l \
        > "$log" 2>&1 &
    local perf_pid=$!

    # ── Wait for RPC socket ──────────────────────────────────────────────────
    local i
    for i in $(seq 1 60); do
        [ -S "$PERF_SOCK" ] && break
        sleep 1
    done
    if [ ! -S "$PERF_SOCK" ]; then
        echo "[E5b]   ERROR: bdevperf RPC socket never appeared" >&2
        sudo kill -9 "$perf_pid" 2>/dev/null || true
        return 1
    fi

    # ── Configure bdev_nvme + attach 3 controllers + multipath policy ───────
    sudo "$RPCPY" -s "$PERF_SOCK" bdev_nvme_set_options --io-path-stat \
        > /dev/null 2>&1 || true
    sudo "$RPCPY" -s "$PERF_SOCK" framework_start_init > /dev/null
    sleep 1

    for port in "${PORTS[@]}"; do
        sudo "$RPCPY" -s "$PERF_SOCK" bdev_nvme_attach_controller \
            -b mp -t rdma -a "$TARGET_IP" -s "$port" \
            -f ipv4 -n "$NQN" --multipath multipath \
            >> "$out/attach.log" 2>&1 || true
    done
    sleep 2

    # Verify multipath bdev exists
    local bdev_name
    bdev_name=$(sudo "$RPCPY" -s "$PERF_SOCK" bdev_get_bdevs 2>/dev/null \
        | python3 -c "
import json, sys
try:
    bs = json.load(sys.stdin)
    for b in bs:
        if b.get('driver_specific', {}).get('nvme'):
            print(b['name'])
            break
except Exception:
    pass
") || true
    if [ -z "$bdev_name" ]; then
        echo "[E5b]   ERROR: no multipath nvme bdev attached" >&2
        sudo kill -9 "$perf_pid" 2>/dev/null || true
        return 1
    fi
    echo "[E5b]   multipath bdev=$bdev_name"

    # multipath policy = plugin when SAPS-Q on
    if [ "$sapsq" = "on" ]; then
        sudo "$RPCPY" -s "$PERF_SOCK" bdev_nvme_set_multipath_policy \
            -b "$bdev_name" -p plugin >> "$out/attach.log" 2>&1 || true
    fi

    # ── perform_tests ────────────────────────────────────────────────────────
    sudo timeout "$((DURATION + 30))" python3 "$BDEVPERF_PY" \
        -s "$PERF_SOCK" -t "$((DURATION + 15))" perform_tests \
        >> "$log" 2>&1 || true

    # ── teardown ─────────────────────────────────────────────────────────────
    wait "$perf_pid" 2>/dev/null || true
    sudo pkill -9 -f "$BDEVPERF" 2>/dev/null || true
    sudo rm -f "$PERF_SOCK"

    # ── Parse bdevperf log → sapsq_summary.json (per_tenant shape) ──────────
    python3 - "$log" "$result_json" "$sapsq_summary" "$sapsq" "$DURATION" <<'PY'
import json
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
result_json = Path(sys.argv[2])
summary_json = Path(sys.argv[3])
sapsq_mode = sys.argv[4]
duration_s = int(sys.argv[5])

content = ""
try:
    content = log_path.read_text(errors="replace")
except Exception:
    pass

iops = None
avg_lat_us = None
io_failed = None
# Find the JSON {"results": ...} block emitted by perform_tests
idx = 0
while True:
    i = content.find('"results"', idx)
    if i < 0:
        break
    start = content.rfind("{", 0, i)
    if start < 0:
        break
    try:
        j, _ = json.JSONDecoder().raw_decode(content, start)
        if isinstance(j, dict) and "results" in j:
            for res in j["results"]:
                v = res.get("iops", 0)
                if v and float(v) > 0:
                    iops = round(float(v), 0)
                    avg_lat_us = res.get("avg_latency_us")
                    io_failed = res.get("io_failed", 0)
                    break
            break
    except Exception:
        pass
    idx = i + 1

if not iops:
    periodic = [float(x) for x in re.findall(r"\s+([\d.]+)\s+IOPS,", content)]
    if len(periodic) >= 3:
        iops = round(sum(periodic) / len(periodic), 0)


def lat_pct(text, pct):
    m = re.search(rf"\s*{re.escape(pct)}%\s*:\s*([\d.]+)us", text)
    return round(float(m.group(1)), 3) if m else None


per_tenant_record = {
    "tenant_id": 0,
    "mode": ("sapsq" if sapsq_mode == "on" else "stock"),
    "nqn": "nqn.2024-01.io.spdk:mptest",
    "iops": iops,
    "io_failed": io_failed,
    "avg_lat_us": round(avg_lat_us, 3) if avg_lat_us else None,
    "lat_p50_us": lat_pct(content, "50.00000"),
    "lat_p99_us": lat_pct(content, "99.00000"),
    "lat_p999_us": lat_pct(content, "99.90000"),
    "lat_p9999_us": lat_pct(content, "99.99000"),
}

result_json.write_text(json.dumps(per_tenant_record, indent=2))

summary = {
    "mode": ("sapsq" if sapsq_mode == "on" else "stock"),
    "tenants": 1,
    "n_paths": 3,
    "duration_s": duration_s,
    "topology": "d_series_single_nqn",
    "per_tenant": [per_tenant_record],
}
summary_json.write_text(json.dumps(summary, indent=2))
print(f"[E5b]   parsed: iops={iops} p99={per_tenant_record['lat_p99_us']}")
PY
}

run_scenario() {
    local fault=$1     # d1 | d3
    local sapsq=$2     # off | on
    local setup_inject

    case "$fault" in
        d1) setup_inject="$SETUP_D1" ;;
        d3) setup_inject="$SETUP_D3" ;;
        *) echo "unknown fault $fault"; exit 1 ;;
    esac

    local label="${fault}_sapsq${sapsq}"
    echo "[E5b] === scenario=$label ==="

    for r in $(seq 0 $((REPS - 1))); do
        local out="$OUT_BASE/$label/rep_$r"
        echo "[E5b] $label rep=$r → $out"

        # 1) restore healthy state then inject fault
        run_arm1 "$SETUP_HEALTHY"
        run_arm1 "$setup_inject"

        # 2) run single-tenant bdevperf
        run_one_rep "$out" "$sapsq" || echo "[E5b] WARN: rep failed"
    done

    # leave node1 in healthy state at end of scenario
    run_arm1 "$SETUP_HEALTHY"
}

echo "[E5b] out_base=$OUT_BASE reps=$REPS duration=${DURATION}s"

# d1 first (fail-slow) then d3 (sct=3 sparse)
run_scenario d1 off
run_scenario d1 on
run_scenario d3 off
run_scenario d3 on

# Combined verdict
python3 "$AGG_PY" --out "$OUT_BASE" --experiment e5b \
    --e5b-d1-off-dir "$OUT_BASE/d1_sapsqoff" \
    --e5b-d1-on-dir  "$OUT_BASE/d1_sapsqon" \
    --e5b-d3-off-dir "$OUT_BASE/d3_sapsqoff" \
    --e5b-d3-on-dir  "$OUT_BASE/d3_sapsqon" \
    || echo "[E5b] WARN: combined E5b aggregate failed"

echo "[E5b] done. results in $OUT_BASE"
