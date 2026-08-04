#!/bin/bash
# Build 2--8 independently provisioned paths to one read-only logical namespace.
#
# Every target process opens the same SSD-backed file and exports the same NQN,
# UUID, and NGUID.  The initiator therefore sees one multipath namespace, while
# each target process can enforce an independent path capacity K_p.

set -euo pipefail

SPDK="${SPDK:-/home/aiden/spdk}"
RPC="$SPDK/scripts/rpc.py"
NVMF_TGT="$SPDK/build/bin/nvmf_tgt"

NUM_PATHS="${NUM_PATHS:?set NUM_PATHS to 2, 4, or 8}"
PATH_CAP_IOPS="${PATH_CAP_IOPS:?set PATH_CAP_IOPS to the per-path capacity}"
SHARED_FILE="${SHARED_FILE:-/mnt/kioxia/saps_scale_shared.img}"
FILE_SIZE_MB="${FILE_SIZE_MB:-4096}"
TARGET_IFACE="${TARGET_IFACE:-enP4p3s0f0np0}"
TARGET_IP="${TARGET_IP:-10.0.0.1}"
TARGET_PREFIX="${TARGET_PREFIX:-24}"
PORT_BASE="${PORT_BASE:-4800}"
SOCKET_PREFIX="${SOCKET_PREFIX:-/var/tmp/spdk_saps_scale_p}"
SHM_BASE="${SHM_BASE:-20}"
MANIFEST="${MANIFEST:-/tmp/saps_scale_shared_file_topology.json}"

NQN="nqn.2024-01.io.spdk:saps-shared-ns0"
UUID="5a505300-0000-0000-0000-000000000000"
NGUID="00000000000000000000000000000001"

if [[ "$NUM_PATHS" != "2" && "$NUM_PATHS" != "4" && "$NUM_PATHS" != "8" ]]; then
    echo "[ERROR] NUM_PATHS must be 2, 4, or 8" >&2
    exit 1
fi
if ! [[ "$PATH_CAP_IOPS" =~ ^[0-9]+$ ]] || (( PATH_CAP_IOPS < 1000 )); then
    echo "[ERROR] PATH_CAP_IOPS must be an integer of at least 1000" >&2
    exit 1
fi
if ! [[ "$FILE_SIZE_MB" =~ ^[0-9]+$ ]] || (( FILE_SIZE_MB < 256 )); then
    echo "[ERROR] FILE_SIZE_MB must be an integer of at least 256" >&2
    exit 1
fi
if [[ ! -x "$NVMF_TGT" ]]; then
    echo "[ERROR] missing $NVMF_TGT" >&2
    exit 1
fi

cleanup_targets() {
    sudo pkill -9 -f "/home/aiden/spdk/build/bin/[n]vmf_tgt" 2>/dev/null || true
    sleep 1
    sudo rm -f /var/tmp/spdk_saps_scale_p*.sock \
        /var/tmp/spdk_saps_scale_p*.pid \
        /var/tmp/spdk_saps_shared_ns.sock \
        /var/tmp/spdk_saps_shared_ns.pid \
        /var/tmp/spdk_a.sock /var/tmp/spdk_b.sock /var/tmp/spdk_c.sock \
        /var/tmp/spdk*.lock /var/tmp/spdk_cpu_lock_* /dev/shm/nvmf_trace.*
    for path in $(seq 0 7); do
        shm_id=$((SHM_BASE + path))
        sudo rm -f "/dev/hugepages/spdk${shm_id}map_"* \
            "/dev/shm/spdk${shm_id}_"* 2>/dev/null || true
    done
}

if [[ "${1:-}" == "--cleanup" ]]; then
    cleanup_targets
    echo "[scale-target] removed scale target processes"
    exit 0
fi

cleanup_targets

sudo ip link set dev "$TARGET_IFACE" up
if ! ip -o -4 addr show dev "$TARGET_IFACE" |
    awk '{print $4}' |
    grep -Fxq "$TARGET_IP/$TARGET_PREFIX"; then
    sudo ip addr add "$TARGET_IP/$TARGET_PREFIX" dev "$TARGET_IFACE"
fi

sudo mkdir -p "$(dirname "$SHARED_FILE")"
sudo fallocate -l "${FILE_SIZE_MB}M" "$SHARED_FILE"

for path in $(seq 0 $((NUM_PATHS - 1))); do
    socket="${SOCKET_PREFIX}${path}.sock"
    pidfile="${SOCKET_PREFIX}${path}.pid"
    logfile="/tmp/nvmf_saps_scale_p${path}.log"
    shm_id=$((SHM_BASE + path))
    core_mask=$(printf '0x%x' $((3 << (path * 2))))
    sudo rm -f "$socket" "$pidfile" "$logfile"
    sudo bash -c \
        "nohup '$NVMF_TGT' -m '$core_mask' -r '$socket' -i '$shm_id' -s 256 --no-pci > '$logfile' 2>&1 & echo \$! > '$pidfile'"
done

for attempt in $(seq 1 300); do
    ready=0
    for path in $(seq 0 $((NUM_PATHS - 1))); do
        [[ -S "${SOCKET_PREFIX}${path}.sock" ]] && ready=$((ready + 1))
    done
    (( ready == NUM_PATHS )) && break
    sleep 0.1
done

for path in $(seq 0 $((NUM_PATHS - 1))); do
    socket="${SOCKET_PREFIX}${path}.sock"
    [[ -S "$socket" ]] || {
        echo "[ERROR] target socket did not appear: $socket" >&2
        exit 1
    }

    rpc=(sudo "$RPC" -s "$socket")
    base="scale_file_p${path}"
    delayed="scale_delay_p${path}"
    port=$((PORT_BASE + path))

    "${rpc[@]}" nvmf_create_transport -t RDMA
    "${rpc[@]}" bdev_aio_create "$SHARED_FILE" "$base" 4096 -r
    "${rpc[@]}" bdev_set_qos_limit "$base" --rw-ios-per-sec "$PATH_CAP_IOPS"
    "${rpc[@]}" bdev_delay_create -b "$base" -d "$delayed" -r 0 -t 0 -w 0 -n 0
    min_cntlid=$((path * 2048 + 1))
    max_cntlid=$((min_cntlid + 2047))
    "${rpc[@]}" nvmf_create_subsystem "$NQN" -s SAPSSCALE00 -a -m 2048 \
        -i "$min_cntlid" -I "$max_cntlid"
    "${rpc[@]}" nvmf_subsystem_add_ns "$NQN" "$delayed" \
        -n 1 -u "$UUID" --nguid "$NGUID"
    "${rpc[@]}" nvmf_subsystem_add_listener "$NQN" \
        -t rdma -a "$TARGET_IP" -s "$port"

    assigned=$("${rpc[@]}" bdev_get_bdevs -b "$base" |
        python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["assigned_rate_limits"]["rw_ios_per_sec"])')
    if [[ "$assigned" != "$PATH_CAP_IOPS" ]]; then
        echo "[ERROR] path $path expected $PATH_CAP_IOPS IOPS, observed $assigned" >&2
        exit 1
    fi
done

NUM_PATHS="$NUM_PATHS" \
PATH_CAP_IOPS="$PATH_CAP_IOPS" \
SHARED_FILE="$SHARED_FILE" \
FILE_SIZE_MB="$FILE_SIZE_MB" \
TARGET_IP="$TARGET_IP" \
PORT_BASE="$PORT_BASE" \
SOCKET_PREFIX="$SOCKET_PREFIX" \
NQN="$NQN" \
UUID="$UUID" \
NGUID="$NGUID" \
MANIFEST="$MANIFEST" \
python3 - <<'PY'
import json
import os
from pathlib import Path

path_count = int(os.environ["NUM_PATHS"])
socket_prefix = os.environ["SOCKET_PREFIX"]
shared_file = Path(os.environ["SHARED_FILE"])
manifest = {
    "schema": "saps-scale-shared-file-v1",
    "num_paths": path_count,
    "path_capacity_iops": int(os.environ["PATH_CAP_IOPS"]),
    "shared_file": str(shared_file),
    "shared_file_size_bytes": shared_file.stat().st_size,
    "target_ip": os.environ["TARGET_IP"],
    "port_base": int(os.environ["PORT_BASE"]),
    "nqn": os.environ["NQN"],
    "uuid": os.environ["UUID"],
    "nguid": os.environ["NGUID"],
    "paths": [
        {
            "path": path,
            "port": int(os.environ["PORT_BASE"]) + path,
            "rpc_socket": f"{socket_prefix}{path}.sock",
            "base_bdev": f"scale_file_p{path}",
            "exported_bdev": f"scale_delay_p{path}",
            "controller_id_range": [path * 2048 + 1, path * 2048 + 2048],
        }
        for path in range(path_count)
    ],
}
Path(os.environ["MANIFEST"]).write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n"
)
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

echo "[scale-target] ready: paths=$NUM_PATHS K_p=$PATH_CAP_IOPS file=$SHARED_FILE"
echo "[scale-target] manifest: $MANIFEST"
