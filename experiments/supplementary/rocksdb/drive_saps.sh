#!/bin/bash
# drive_saps.sh — rerun only the evaluated SAPS profile (mode "saps") for the
# RocksDB test, after drive_stock.sh produced the stock rows.
# Smoke gate requires the M-series controller to initialize and to sample I/O.

set -uo pipefail

D=/mnt/nvme0n1p1/aiden/DPA/nvme-of-controller/experiments/realapp_nbd/e2_supplementary
OUT="${OUT:-$D/run_$(date +%Y%m%d_%H%M%S)_saps}"
export SPDK_TGT=/home/aiden/spdk/build/bin/spdk_tgt.saps   # built with -DDPA_PLUGIN_ENABLED so spdk_tgt calls dpa_plugin_init()
HUGEPAGES="${HUGEPAGES:-24576}"
mkdir -p "$OUT"
exec > >(tee -a "$OUT/driver.log") 2>&1
log() { echo "[$(date +%H:%M:%S)] [drive] $*"; }

HP_BEFORE=$(cat /proc/sys/vm/nr_hugepages)
restore() {
    log "restore: stop targets, nr_hugepages -> $HP_BEFORE"
    bash "$D/setup_rocksdb_targets.sh" --cleanup
    echo "$HP_BEFORE" | sudo tee /proc/sys/vm/nr_hugepages >/dev/null
    log "DRIVER EXIT"
}
trap restore EXIT

{ echo "date_utc=$(date -u +%FT%TZ)"; echo "controller_head=$(git -C /mnt/nvme0n1p1/aiden/DPA/nvme-of-controller rev-parse HEAD)"
  sha256sum "$SPDK_TGT" /home/aiden/spdk/build/bin/nvmf_tgt /home/aiden/rocksdb/db_bench; } > "$OUT/provenance.txt"

sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
echo 1 | sudo tee /proc/sys/vm/compact_memory >/dev/null
echo "$HUGEPAGES" | sudo tee /proc/sys/vm/nr_hugepages >/dev/null
[ "$(cat /proc/sys/vm/nr_hugepages)" -ge "$HUGEPAGES" ] || { log "ERROR: hugepages"; exit 1; }

bash "$D/setup_rocksdb_targets.sh" || { log "ERROR: target setup failed"; exit 1; }
bash "$D/preload_rocksdb.sh" || { log "ERROR: preload failed"; exit 1; }

log "=== smoke gate (saps) ==="
SMOKE=1 SMOKE_MODE=saps OUTDIR="$OUT/smoke" bash "$D/run_rocksdb_fault.sh"
G=$(find "$OUT/smoke" -type d -name g1 | head -1)
if ! grep -q "SAPSQ_M enabled" "$G/spdk_tgt.log"; then log "ERROR: SAPSQ_M not enabled"; grep -m5 -i -E "dpa_plugin|sapsq" "$G/spdk_tgt.log"; exit 1; fi
n=$(wc -l < "$G/sampler.csv" 2>/dev/null || echo 0)
[ "$n" -gt 5 ] || { log "ERROR: sampler.csv has $n lines"; exit 1; }
log "smoke gate PASS (SAPSQ_M enabled, sampler lines=$n)"
grep -m3 -E "SAPSQ_M enabled|init start" "$G/spdk_tgt.log"

for i in $(seq 1 20); do pgrep -f "spdk_tgt[.a-z]* -m 0x10 -r /var/tmp/spdk_realapp.sock" >/dev/null || break; sleep 1; done
if pgrep -f "spdk_tgt[.a-z]* -m 0x10 -r /var/tmp/spdk_realapp.sock" >/dev/null; then log "ERROR: smoke initiator still running"; exit 1; fi
log "=== full run (saps) ==="
MODES=saps OUTDIR="$OUT/full" bash "$D/run_rocksdb_fault.sh"
log "ALL DONE results=$OUT/full/e2_results.csv"
