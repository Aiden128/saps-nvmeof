#!/bin/bash
# drive_campaign.sh — RocksDB E2 rerun on arm-2 (2026-10-06).
#
# Target and initiator both run on arm-2: three nvmf_tgt processes (cores 40-45)
# listen on arm-2's own 10.0.0.2 through mlx5_0 RDMA, and the SAPS
# initiator (spdk_tgt, core 4, DPA plugin) attaches to them as three paths.
# Steps: provenance, hugepages, target setup, DB build + byte-identical preload,
# smoke gate, then stock and dpa_full x healthy/D1/D3 x 3 reps.
# On exit, the targets are stopped and nr_hugepages is restored.

set -uo pipefail

D=/mnt/nvme0n1p1/aiden/DPA/nvme-of-controller/experiments/realapp_nbd/e2_supplementary
REPO=/mnt/nvme0n1p1/aiden/DPA/nvme-of-controller
OUT="${OUT:-$D/run_$(date +%Y%m%d_%H%M%S)}"
HUGEPAGES="${HUGEPAGES:-24576}"     # 48 GiB of 2 MiB pages: 3 x 14 GiB targets + initiator
mkdir -p "$OUT"
exec > >(tee -a "$OUT/driver.log") 2>&1

log() { echo "[$(date +%H:%M:%S)] [drive] $*"; }

HP_BEFORE=$(cat /proc/sys/vm/nr_hugepages)
restore() {
    log "restore: stop targets, nr_hugepages -> $HP_BEFORE"
    bash "$D/setup_rocksdb_targets.sh" --cleanup
    echo "$HP_BEFORE" | sudo tee /proc/sys/vm/nr_hugepages >/dev/null
    grep -E "HugePages_(Total|Free)" /proc/meminfo
    log "DRIVER EXIT"
}
trap restore EXIT

log "OUT=$OUT"
{
    echo "date_utc=$(date -u +%FT%TZ)"
    echo "host=$(hostname)"
    echo "controller_head=$(git -C "$REPO" rev-parse HEAD)"
    sha256sum /home/aiden/spdk/build/bin/spdk_tgt /home/aiden/spdk/build/bin/nvmf_tgt /home/aiden/rocksdb/db_bench
    echo "topology=nvmf_tgt x3 on cores 40-45 at 10.0.0.2 (mlx5_0); initiator spdk_tgt core 4; db_bench cores 8-39"
    uname -a
} > "$OUT/provenance.txt"
cat "$OUT/provenance.txt"

log "hugepages: $HP_BEFORE -> $HUGEPAGES"
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
echo 1 | sudo tee /proc/sys/vm/compact_memory >/dev/null
echo "$HUGEPAGES" | sudo tee /proc/sys/vm/nr_hugepages >/dev/null
grep -E "HugePages_(Total|Free)" /proc/meminfo
[ "$(cat /proc/sys/vm/nr_hugepages)" -ge "$HUGEPAGES" ] || { log "ERROR: could not reserve $HUGEPAGES hugepages"; exit 1; }

log "=== target setup ==="
bash "$D/setup_rocksdb_targets.sh" || { log "ERROR: target setup failed"; exit 1; }

log "=== DB build + preload ==="
bash "$D/preload_rocksdb.sh" || { log "ERROR: preload failed"; exit 1; }

log "=== smoke gate ==="
SMOKE=1 SMOKE_MODE=stock OUTDIR="$OUT/smoke" bash "$D/run_rocksdb_fault.sh"
python3 - "$OUT/smoke/e2_results.csv" <<'PY' || { log "ERROR: smoke gate failed"; exit 1; }
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1])))
ok = rows and all(r["ops_per_sec"] not in ("", "None") and int(r["ops_per_sec"]) > 0
                  and r["db_bench_rc"] == "0" and r["fault_manifested"] == "1" for r in rows)
print("smoke rows:", rows)
sys.exit(0 if ok else 1)
PY
log "smoke gate PASS"

log "=== full run (stock) ==="
MODES=stock OUTDIR="$OUT/full" bash "$D/run_rocksdb_fault.sh"
log "ALL DONE results=$OUT/full/e2_results.csv"
