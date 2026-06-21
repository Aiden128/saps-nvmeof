#!/bin/bash
# sapsq_e8_mixed_workload.sh — E8 driver (mixed IO-size fairness)
#
# 驗證 SAPS-Q admission cost 隨 nbytes 線性增長 (cost = max(1, nbytes/4096))
# 之後,大 IO 不會用 1 IO 的 token 偷跑整條 link 頻寬。
#
# Topology: 4 tenant × 3 path multipath。weights = [3,1,1,1] (latency-sensitive
# 4K reader 拿 50% 權重,其他 1/6 each)。
#   tenant 0: randread 4K   (latency-sensitive,  weight 3)
#   tenant 1: randwrite 64K (throughput-sensitive, weight 1) — 16× cost/IO
#   tenant 2: randread 4K   (background,         weight 1)
#   tenant 3: randread 4K   (background,         weight 1)
#
# 對應 specs/dm-research-redesign-20260522.md §4.4 host enforcement
# (cost = max(1, nbytes / 4096))。E8 是 nbytes plumbing 的 acceptance test:
# 沒 plumbing 時 tenant 1 用 1 IO 的 cost 推 16× bytes,fairness on bytes 全壞。
# 有 plumbing 時 tenant 1 應該被等比例 throttled。
#
# Pre-conditions:
#   - node1 已透過 setup_arm1_tenants.sh 設好 4-NQN 3-path topology。
#   - bdevperf build 含 dpa_plugin nbytes API 的最新版 (req->payload.size →
#     dpa_plugin_admission_check 第三參數)。
#   - sapsq_dump 已編譯 (optional;沒有 → counter 部分 skip)。
#
# Outputs:
#   experiments/sapsq/e8_mixed_workload/<ts>/<mode>/rep_<n>/...
#   experiments/sapsq/e8_mixed_workload/<ts>/<mode>/aggregate.json (per mode)

set -eo pipefail

ROOT="/home/user/DPA/nvme-of-controller"
RUN_PY="$ROOT/scripts/run_sapsq.py"
AGG_PY="$ROOT/scripts/aggregate_sapsq.py"

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="$ROOT/experiments/sapsq/e8_mixed_workload/$TS"
mkdir -p "$OUT_BASE"

REPS=${REPS:-5}
DURATION=${DURATION:-60}
WEIGHTS=${WEIGHTS:-"3,1,1,1"}
# 注意: tenant 1 是 64K writer,1 IOP 等於 16K read 的 16 倍 bytes。
# demand 用 IOPS 單位,設定時要考慮: tenant 1 demand IOPS 比較小是合理的
# (link IOPS 上限受 byte BW 限制)。
DEMAND=${DEMAND:-"100000,25000,100000,100000"}
PATH_BASE=${PATH_BASE:-"200000,200000,200000"}
PROBE=${PROBE:-"100,100,100"}
N_PATHS=${N_PATHS:-3}
# E8 per-tenant workload string — 嚴格對應 tenant index
PT_WORKLOAD=${PT_WORKLOAD:-"randread:4096,randwrite:65536,randread:4096,randread:4096"}
SETUP_ARM1_SCRIPT=${SETUP_ARM1_SCRIPT:-"$ROOT/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh"}

# E8 verdict 參數 (default: P99 < 1ms;單獨 baseline MB/s 沒給 → 跳過 proportional check)
E8_LAT_P99_MAX_US=${E8_LAT_P99_MAX_US:-1000}
E8_THROUGHPUT_SINGLE_MB=${E8_THROUGHPUT_SINGLE_MB:-0}

MODES=(stock m4_static sapsq)

echo "[E8] out_base=$OUT_BASE reps=$REPS duration=${DURATION}s weights=$WEIGHTS"
echo "[E8] per-tenant workload: $PT_WORKLOAD"

for mode in "${MODES[@]}"; do
    echo "[E8] === mode=$mode ==="
    for r in $(seq 0 $((REPS - 1))); do
        OUT="$OUT_BASE/$mode/rep_$r"
        mkdir -p "$OUT"
        echo "[E8] mode=$mode rep=$r → $OUT"
        python3 "$RUN_PY" \
            --tenants 4 --weights "$WEIGHTS" \
            --duration "$DURATION" --qd 32 \
            --per-tenant-workload "$PT_WORKLOAD" \
            --path-base-iops "$PATH_BASE" \
            --demand-iops "$DEMAND" \
            --probe-rate-iops "$PROBE" \
            --n-paths "$N_PATHS" \
            --mode "$mode" \
            --setup-node1-script "$SETUP_ARM1_SCRIPT" \
            --out-dir "$OUT"
    done
done

# Cross-mode E8 verdict — sapsq rep_0 is the canonical run; stock/m4_static
# rep_0 supply cross-mode comparison references
for r in $(seq 0 $((REPS - 1))); do
    SAPSQ_OUT="$OUT_BASE/sapsq/rep_$r"
    STOCK_OUT="$OUT_BASE/stock/rep_$r"
    M4_OUT="$OUT_BASE/m4_static/rep_$r"
    if [ -d "$SAPSQ_OUT" ]; then
        python3 "$AGG_PY" --out "$SAPSQ_OUT" --experiment e8 \
            --e8-latency-tenant 0 --e8-throughput-tenant 1 \
            --e8-latency-p99-us-max "$E8_LAT_P99_MAX_US" \
            --e8-throughput-single-baseline-mb "$E8_THROUGHPUT_SINGLE_MB" \
            --e8-stock-dir "$STOCK_OUT" \
            --e8-m4-static-dir "$M4_OUT" \
            || echo "[E8] WARN: rep $r aggregate failed"
    fi
done

# Mode-level summary (sapsq pass rate)
python3 - <<PY
import json, glob, statistics
from pathlib import Path
sapsq_dir = Path("$OUT_BASE/sapsq")
aggs = sorted(sapsq_dir.glob("rep_*/aggregate.json"))
if not aggs:
    print("[E8] sapsq: no rep aggregates found")
    raise SystemExit(0)
data = [json.loads(p.read_text()) for p in aggs]
passes = [d["verdict"]["pass"] for d in data]
lat_p99s = [d["verdict"].get("latency_p99_us") for d in data
            if d["verdict"].get("latency_p99_us") is not None]
thr_mbs = [d["verdict"].get("throughput_mb_per_s") for d in data
           if d["verdict"].get("throughput_mb_per_s") is not None]
summary = {
    "mode": "sapsq",
    "reps": len(data),
    "pass_count": sum(1 for v in passes if v),
    "pass_rate": sum(1 for v in passes if v) / len(passes) if passes else 0,
    "lat_p99_us_mean": statistics.mean(lat_p99s) if lat_p99s else None,
    "throughput_mb_mean": statistics.mean(thr_mbs) if thr_mbs else None,
}
(sapsq_dir / "aggregate.json").write_text(json.dumps(summary, indent=2))
print(f"[E8] sapsq: pass {summary['pass_count']}/{summary['reps']} "
      f"lat_p99_mean={summary['lat_p99_us_mean']}us "
      f"thr_mean={summary['throughput_mb_mean']}MB/s")
PY

echo "[E8] done. results in $OUT_BASE"
