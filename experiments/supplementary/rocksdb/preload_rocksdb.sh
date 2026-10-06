#!/bin/bash
# e2_build_and_preload.sh — build the RocksDB DB once, then make all 3 per-path mallocs
# byte-identical so the 3-path multipath device reads consistently under READ-ONLY mount.
# variant: target on arm-2 itself via setup_rocksdb_targets.sh (10.0.0.2).
#
# Strategy (no SPDK source change):
#   Phase BUILD  : attach ONLY path A (single-path, no multipath merge) as /dev/nbd0,
#                  mkfs.ext4, mount rw, run db_bench fillrandom (num/value from env),
#                  umount. Path A's malloc now holds a valid ext4 + RocksDB DB.
#   Phase IMAGE  : dd the FULL 12 GiB path-A device → $IMG (one contiguous device image,
#                  so ext4 superblock/journal/group-descriptors are captured verbatim).
#   Phase PRELOAD: for path B then path C — attach single-path, dd $IMG → device, detach.
#                  After this, malloc_A == malloc_B == malloc_C byte-for-byte.
#
# Why full-device image (not file copy): a later read routed to path B by SAPS/round-robin
# must hit the SAME on-disk bytes path A wrote, including ext4 metadata. Only a raw device
# image guarantees that. Mount is ro,noatime in the run phase so no path ever desyncs.
#
# Each path is driven by a throwaway spdk_tgt WITHOUT the DPA plugin (role unset) — this is
# pure preload plumbing, not a measured run. The measured run uses run_realapp_nbd_m1.sh.

set -uo pipefail

SPDK=/home/aiden/spdk
SPDK_TGT="$SPDK/build/bin/spdk_tgt"
RPC="$SPDK/scripts/rpc.py"
SOCK=/var/tmp/spdk_e2_preload.sock
TARGET_IP=10.0.0.2
NQN=nqn.2024-01.io.spdk:tenant0
NBD_DEV=/dev/nbd0
MNT="${MNT:-/mnt/realapp}"

DB_BENCH="${DB_BENCH:-/home/aiden/rocksdb/db_bench}"
NUM="${NUM:-2000000}"
VALUE_SIZE="${VALUE_SIZE:-1024}"
IMG="${IMG:-/mnt/nvme0n1p1/aiden/e2_rocksdb_img_12g.bin}"
# 12 GiB device = 12288 MiB; dd with 1 MiB block.
IMG_MB="${IMG_MB:-12288}"

declare -A PORT=( [A]=4500 [B]=4600 [C]=4700 )

log() { echo "[$(date +%H:%M:%S)] [e2-preload] $*"; }
rpc() { sudo "$RPC" -s "$SOCK" "$@"; }

kill_tgt() {
    sudo "$RPC" -s "$SOCK" nbd_stop_disk "$NBD_DEV" 2>/dev/null || true
    ( sudo pkill -9 -f "spdk_tgt -m 0x10 -r $SOCK" 2>/dev/null; exit 0 ) || true
    sleep 1
    sudo rm -f "$SOCK" /var/tmp/spdk_e2_preload*.lock 2>/dev/null || true
}

start_tgt() {
    kill_tgt
    sudo modprobe nbd nbds_max=16 2>/dev/null || true
    # No DPA plugin env → plain spdk_tgt (preload plumbing only).
    sudo nohup "$SPDK_TGT" -m 0x10 -r "$SOCK" > /tmp/e2_preload_tgt.log 2>&1 &
    for i in $(seq 1 30); do
        [ -S "$SOCK" ] && sudo "$RPC" -s "$SOCK" rpc_get_methods >/dev/null 2>&1 && { log "preload spdk_tgt ready (${i}s)"; return 0; }
        sleep 1
    done
    log "ERROR: preload spdk_tgt RPC not ready"; tail -30 /tmp/e2_preload_tgt.log; return 1
}

# attach exactly ONE path as single-path bdev "pl0n1" → export /dev/nbd0
attach_single() {
    local P=$1
    rpc bdev_nvme_attach_controller -b pl0 -t rdma -a "$TARGET_IP" -s "${PORT[$P]}" -f ipv4 -n "$NQN" 2>&1 \
        | sed 's/^/    /'
    # single attach → bdev name pl0n1
    rpc nbd_start_disk pl0n1 "$NBD_DEV" 2>&1 | sed 's/^/    /'
    sleep 1
}

detach_single() {
    rpc nbd_stop_disk "$NBD_DEV" 2>/dev/null || true
    rpc bdev_nvme_detach_controller pl0 2>/dev/null || true
    sleep 1
}

# Reuse an existing image (built earlier today on this host): preload A, B and C from it.
if [ -f "$IMG" ] && [ "${REUSE_IMG:-1}" = 1 ]; then
    IMG_SHA=$(sha256sum "$IMG" | awk '{print $1}')
    log "REUSE: $IMG sha256=$IMG_SHA; preloading A, B and C"
    start_tgt || exit 1
    for P in A B C; do
        log "PRELOAD: path $P — dd $IMG → device"
        attach_single "$P"
        sudo dd if="$IMG" of="$NBD_DEV" bs=1M count="$IMG_MB" iflag=fullblock oflag=direct status=progress 2>&1 | tail -3
        sync
        DEV_SHA=$(sudo dd if="$NBD_DEV" bs=1M count="$IMG_MB" iflag=fullblock 2>/dev/null | sha256sum | awk '{print $1}')
        if [ "$DEV_SHA" = "$IMG_SHA" ]; then log "PRELOAD: path $P sha256 MATCH"; else log "ERROR: path $P sha256 MISMATCH"; detach_single; kill_tgt; exit 1; fi
        detach_single
    done
    kill_tgt
    log "DONE — malloc_A/B/C byte-identical (sha256=$IMG_SHA), image at $IMG"
    exit 0
fi

# ── Phase BUILD on path A ──────────────────────────────────────────────────────
log "BUILD: path A — mkfs.ext4 + db_bench fillrandom (num=$NUM value=$VALUE_SIZE)"
start_tgt || exit 1
attach_single A
sudo blockdev --getsize64 "$NBD_DEV" 2>&1 | sed 's/^/    dev_bytes=/'
sudo mkfs.ext4 -F -q "$NBD_DEV" || { log "ERROR mkfs.ext4 failed"; exit 1; }
sudo mkdir -p "$MNT"
sudo mount -o noatime "$NBD_DEV" "$MNT" || { log "ERROR mount rw failed"; exit 1; }
sudo rm -rf "$MNT/rocksdb"; sudo mkdir -p "$MNT/rocksdb"; sudo chmod 777 "$MNT/rocksdb"
log "BUILD: fillrandom"
timeout 1200 "$DB_BENCH" --db="$MNT/rocksdb" --value_size="$VALUE_SIZE" --compression_type=none \
    --benchmarks=fillrandom --num="$NUM" --threads=1 > /tmp/e2_fillrandom.log 2>&1
rc=$?; log "  fillrandom rc=$rc"
[ "$rc" = 0 ] || { log "ERROR fillrandom failed"; tr '\r' '\n' < /tmp/e2_fillrandom.log | tail -15; exit 1; }
sync
sudo umount "$MNT" || { log "ERROR umount failed"; exit 1; }
log "BUILD: DB on path A done"

# ── Phase IMAGE: dd full path-A device → IMG ────────────────────────────────────
log "IMAGE: dd path-A device ($NBD_DEV) → $IMG (${IMG_MB} MiB)"
sudo dd if="$NBD_DEV" of="$IMG" bs=1M count="$IMG_MB" iflag=fullblock status=progress 2>&1 | tail -3
sudo chown "$(id -un)":"$(id -gn)" "$IMG" 2>/dev/null || true
ls -la "$IMG" | sed 's/^/    /'
IMG_SHA=$(sudo sha256sum "$IMG" | awk '{print $1}')
log "IMAGE: sha256=$IMG_SHA"
detach_single

# ── Phase PRELOAD: dd IMG → path B, path C ──────────────────────────────────────
for P in B C; do
    log "PRELOAD: path $P — dd $IMG → device"
    attach_single "$P"
    sudo dd if="$IMG" of="$NBD_DEV" bs=1M count="$IMG_MB" iflag=fullblock oflag=direct status=progress 2>&1 | tail -3
    sync
    # verify: read device back, sha256 must match IMG
    DEV_SHA=$(sudo dd if="$NBD_DEV" bs=1M count="$IMG_MB" iflag=fullblock 2>/dev/null | sha256sum | awk '{print $1}')
    if [ "$DEV_SHA" = "$IMG_SHA" ]; then log "PRELOAD: path $P sha256 MATCH ($DEV_SHA)"; else log "ERROR: path $P sha256 MISMATCH dev=$DEV_SHA img=$IMG_SHA"; detach_single; kill_tgt; exit 1; fi
    detach_single
done

kill_tgt
log "DONE — malloc_A/B/C byte-identical (sha256=$IMG_SHA), image at $IMG"
echo "IMG_SHA256=$IMG_SHA"
