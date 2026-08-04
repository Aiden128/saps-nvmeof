#!/bin/bash
# setup_arm1_sapsq_Nt3p.sh — SAPS-Q N-tenant × 3-path NVMe-oF topology
# Run on arm-1.
#
# Parameterized version of setup_arm1_sapsq_4t3p.sh.
# NUM_TENANTS overridable by env var or positional arg $1 (default 4).
#
# Topology:
#   Tgt A: core 0x3,  socket spdk_a.sock, ports 4500..(4500+N-1)
#   Tgt B: core 0xc,  socket spdk_b.sock, ports 4600..(4600+N-1)
#   Tgt C: core 0x30, socket spdk_c.sock, ports 4700..(4700+N-1)
#
# Port layout (stride 100 between paths, no overlap up to N≤99):
#   tenant_i path_A port = 4500 + i
#   tenant_i path_B port = 4600 + i
#   tenant_i path_C port = 4700 + i
#
# cntlid windows (stride = NUM_TENANTS*32 per path):
#   path A: [1 .. N*32],          path B: [N*32+1 .. 2*N*32],  path C: [2*N*32+1 .. 3*N*32]
#   per-tenant within each path: 32 cntlids, non-overlapping across paths and tenants.
#   N=4:  A[1..128], B[129..256], C[257..384]   — matches original 4t3p exactly.
#   N=8:  A[1..256], B[257..512], C[513..768]
#   N=16: A[1..512], B[513..1024],C[1025..1536] — well under 65535 cap.
#
# Each tenant NQN (nqn.2024-01.io.spdk:tenant{0..N-1}) is advertised on all 3 tgts
# with identical UUID + NGUID per tenant → SPDK initiator merges 3 listeners as
# multipath for that tenant. Result: N multipath bdev_nvme entries on arm-2,
# each with 3 underlying paths.
#
# IMPORTANT: initiator-side runner must use matching port formula:
#   PATH_PORT_OFFSETS = [0, 100, 200]   TENANT_BASE_PORT = 4500
#   port = 4500 + tenant_id + path_offset
#
# Flags:
#   (no arg, or numeric N)   setup with N tenants
#   --cleanup                kill all 3 tgts, remove sockets

set -euo pipefail

SPDK=/home/aiden/spdk
RPC="$SPDK/scripts/rpc.py"
if [ -x "$SPDK/build/bin/nvmf_tgt" ]; then
    NVMF_TGT="$SPDK/build/bin/nvmf_tgt"
elif [ -x "$SPDK/build/examples/nvmf" ]; then
    NVMF_TGT="$SPDK/build/examples/nvmf"
else
    echo "[ERROR] cannot find nvmf_tgt binary under $SPDK/build/bin or $SPDK/build/examples"; exit 1
fi

# ── --cleanup mode ────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--cleanup" ]]; then
    echo "[setup_sapsq_Nt3p] cleanup: kill all nvmf_tgts, remove sockets"
    sudo pkill -9 -f "build/bin/nvmf_tgt" 2>/dev/null || true
    sudo pkill -9 -f "build/examples/nvmf" 2>/dev/null || true
    sleep 1
    sudo rm -f /var/tmp/spdk_a.sock /var/tmp/spdk_b.sock /var/tmp/spdk_c.sock \
               /var/tmp/spdk_tenants.sock /var/tmp/spdk*.lock \
               /var/tmp/spdk_cpu_lock_* /dev/shm/nvmf_trace.*
    echo "[setup_sapsq_Nt3p] cleanup done"
    exit 0
fi

# ── NUM_TENANTS resolution: env var > positional arg > default 4 ─────────────
# Positional arg must be a number (not --cleanup, already handled above)
if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
    NUM_TENANTS="${1}"
fi
NUM_TENANTS="${NUM_TENANTS:-4}"
echo "[setup_sapsq_Nt3p] NUM_TENANTS=$NUM_TENANTS"

if [ "$NUM_TENANTS" -lt 1 ] || [ "$NUM_TENANTS" -gt 99 ]; then
    echo "[ERROR] NUM_TENANTS must be 1..99 (port stride=100 supports up to 99)"; exit 1
fi

TARGET_IP="10.0.0.1"
BDEV_SIZE_MB=64
BDEV_BLOCK=4096
# DELAY_US=0: baseline latency floor removed so arm-1 target is not the bottleneck.
# Fault injection (bdev_delay_update_latency to 5000us on path B) still works
# because the bdev_delay stack is preserved — only the healthy-state baseline is 0.
DELAY_US="${DELAY_US:-0}"

# Port bases — env-overridable.  Defaults (4430/4440/4450, stride 10) match
# run_sapsq.py (TENANT_BASE_PORT=4430, PATH_PORT_OFFSETS=[0,10,20]); max N=10.
# The coordinator harness (run_saps_q_e1_coordinator.py, stride 100) exports
# these as 4500/4600/4700 so its 4500+i+offset attach ports line up with the
# listeners.  tenant_i path_A port = PATH_A_PORT_BASE + i, etc.
PATH_A_PORT_BASE="${PATH_A_PORT_BASE:-4430}"
PATH_B_PORT_BASE="${PATH_B_PORT_BASE:-4440}"
PATH_C_PORT_BASE="${PATH_C_PORT_BASE:-4450}"

declare -A SOCKS=( [A]=/var/tmp/spdk_a.sock [B]=/var/tmp/spdk_b.sock [C]=/var/tmp/spdk_c.sock )
declare -A CORES=( [A]=0x3 [B]=0xc [C]=0x30 )
declare -A SHM_IDX=( [A]=1 [B]=2 [C]=3 )
declare -A PORT_BASE=( [A]=$PATH_A_PORT_BASE [B]=$PATH_B_PORT_BASE [C]=$PATH_C_PORT_BASE )

echo "[setup_sapsq_Nt3p] using nvmf binary: $NVMF_TGT"

# ── Idempotent teardown of any prior nvmf_tgt ─────────────────────────────────
# Run pkill in a subshell so SIGKILL delivered to nvmf_tgt cannot propagate to
# this script's bash process via sudo's process group.  The subshell always
# exits 0 regardless of whether any process was found.
echo "[setup_sapsq_Nt3p] kill prior nvmf_tgt + clean sockets"
# Match by cmdline (-f), not exact comm (-x): SPDK renames the process comm, so
# pkill -x nvmf_tgt matches nothing and stale targets survive, colliding on the
# same -i shm-idx and failing the RDMA transport of the next launch.
( sudo pkill -9 -f "build/bin/nvmf_tgt" 2>/dev/null; sudo pkill -9 -f "build/examples/nvmf" 2>/dev/null; exit 0 ) || true
sleep 2
sudo rm -f "${SOCKS[A]}" "${SOCKS[B]}" "${SOCKS[C]}" /var/tmp/spdk_tenants.sock /var/tmp/spdk*.lock /var/tmp/spdk_cpu_lock_* /dev/shm/nvmf_trace.*
# SIGKILL leaves the hugepage map files (file-prefix spdk1/2/3) behind; remove
# them so the next -i 1/2/3 launch starts from a clean hugepage state.
sudo rm -f /dev/hugepages/spdk[123]map_* /dev/shm/spdk[123]_* 2>/dev/null || true
# Remove stale log files (may be root-owned from prior run; redirection below runs
# as the invoking user so permission denied if root owns the file).
sudo rm -f /tmp/nvmf_sapsq_a.log /tmp/nvmf_sapsq_b.log /tmp/nvmf_sapsq_c.log

# ── Start 3 nvmf_tgts (one per path) ──────────────────────────────────────────
for P in A B C; do
    echo "[setup_sapsq_Nt3p] start nvmf_tgt path=$P (core ${CORES[$P]}, socket ${SOCKS[$P]})"
    sudo nohup "$NVMF_TGT" -m "${CORES[$P]}" -r "${SOCKS[$P]}" -i "${SHM_IDX[$P]}" \
        > "/tmp/nvmf_sapsq_${P,,}.log" 2>&1 &
done

echo "[setup_sapsq_Nt3p] wait for sockets (up to 30s)"
for i in $(seq 1 30); do
    COUNT=0
    [ -S "${SOCKS[A]}" ] && COUNT=$((COUNT+1))
    [ -S "${SOCKS[B]}" ] && COUNT=$((COUNT+1))
    [ -S "${SOCKS[C]}" ] && COUNT=$((COUNT+1))
    if [ "$COUNT" -eq 3 ]; then
        echo "[setup_sapsq_Nt3p] all 3 sockets ready after ${i}s"
        break
    fi
    sleep 1
done
[ -S "${SOCKS[A]}" ] && [ -S "${SOCKS[B]}" ] && [ -S "${SOCKS[C]}" ] || { echo "[ERROR] sockets missing — see /tmp/nvmf_sapsq_*.log"; exit 1; }

# ── Configure each path's nvmf_tgt with N tenant subsystems ───────────────────
configure_path() {
    local P=$1                 # A, B, or C
    local SOCK="${SOCKS[$P]}"
    local BASE_PORT="${PORT_BASE[$P]}"
    local RPC_CMD="sudo $RPC -s $SOCK"

    # Path index for cntlid offsetting: A=0, B=1, C=2
    # cntlid window per (tenant_i, path_P):
    #   CNTLID_MIN = P_IDX * (NUM_TENANTS*32) + i * 32 + 1
    #   CNTLID_MAX = P_IDX * (NUM_TENANTS*32) + i * 32 + 32
    #
    # Path stride = NUM_TENANTS*32 ensures no overlap across paths.
    # N=4:  stride=128  → A[1..128], B[129..256], C[257..384]  (matches 4t3p)
    # N=8:  stride=256  → A[1..256], B[257..512], C[513..768]
    # N=16: stride=512  → A[1..512], B[513..1024],C[1025..1536]
    local PATH_STRIDE=$(( NUM_TENANTS * 32 ))
    local P_IDX
    case "$P" in
        A) P_IDX=0 ;;
        B) P_IDX=1 ;;
        C) P_IDX=2 ;;
    esac

    echo "[setup_sapsq_Nt3p] path=$P: create RDMA transport"
    $RPC_CMD nvmf_create_transport -t RDMA

    for i in $(seq 0 $((NUM_TENANTS - 1))); do
        local NQN="nqn.2024-01.io.spdk:tenant${i}"
        local MALLOC="malloc_t${i}_${P}"
        local DELAY="delay_t${i}_${P}"
        # Note: bdev_error_create produces EE_<input> name; we feed EE_<delay> to subsystem
        local ERR_BDEV="EE_${DELAY}"
        local PORT=$((BASE_PORT + i))
        local SERIAL="T${i}P${P}SN00"
        local CNTLID_MIN=$(( P_IDX * PATH_STRIDE + i * 32 + 1 ))
        local CNTLID_MAX=$(( P_IDX * PATH_STRIDE + i * 32 + 32 ))
        # IMPORTANT: UUID + NGUID identical across paths A/B/C for same tenant
        # → SPDK initiator merges 3 listeners as multipath bdev_nvme entry.
        # Differ across tenants so they remain distinct bdevs.
        # Pad tenant index to 2 digits in UUID/NGUID to stay well-formed for N>9
        local UUID_SUFFIX=$(printf "%02d" $i)
        local UUID="11111111-2222-3333-4444-0000000000${UUID_SUFFIX}"
        local NGUID_SUFFIX=$(printf "%02d" $i)
        local NGUID="0000000000000000111111111111${NGUID_SUFFIX}${NGUID_SUFFIX}"

        echo "[setup_sapsq_Nt3p] path=$P tenant=${i} → port=$PORT cntlid=[$CNTLID_MIN,$CNTLID_MAX] bdev=$MALLOC"

        $RPC_CMD bdev_malloc_create -b "$MALLOC" "$BDEV_SIZE_MB" "$BDEV_BLOCK"
        $RPC_CMD bdev_delay_create \
            -b "$MALLOC" \
            -d "$DELAY" \
            -r "$DELAY_US" -t "$DELAY_US" -w "$DELAY_US" -n "$DELAY_US"
        $RPC_CMD bdev_error_create "$DELAY"

        $RPC_CMD nvmf_create_subsystem "$NQN" \
            -s "$SERIAL" \
            -a \
            -m 32 \
            -i "$CNTLID_MIN" \
            -I "$CNTLID_MAX"

        $RPC_CMD nvmf_subsystem_add_ns "$NQN" "$ERR_BDEV" \
            -n 1 \
            -u "$UUID" \
            --nguid "$NGUID"

        $RPC_CMD nvmf_subsystem_add_listener "$NQN" \
            -t rdma \
            -a "$TARGET_IP" \
            -s "$PORT"
    done
}

for P in A B C; do
    configure_path "$P"
done

# ── Verify ─────────────────────────────────────────────────────────────────────
echo ""
echo "[verify] tenant listeners across all 3 paths:"
for P in A B C; do
    echo "  path=$P (socket=${SOCKS[$P]}):"
    sudo "$RPC" -s "${SOCKS[$P]}" nvmf_get_subsystems | python3 -c "
import json, sys
data = json.load(sys.stdin)
for ss in data:
    nqn = ss.get('nqn', '')
    if 'tenant' in nqn:
        ports = [l.get('trsvcid') for l in ss.get('listen_addresses', [])]
        ns_bdevs = [ns.get('bdev_name') for ns in ss.get('namespaces', [])]
        nguid = (ss.get('namespaces') or [{}])[0].get('nguid', '')
        print(f'    {nqn}  bdev={ns_bdevs}  ports={ports}  nguid={nguid}')
"
done

echo ""
echo "[setup_sapsq_Nt3p] DONE — ${NUM_TENANTS} tenants × 3 paths ready"
echo "  tenant_i path_A port = $PATH_A_PORT_BASE + i"
echo "  tenant_i path_B port = $PATH_B_PORT_BASE + i"
echo "  tenant_i path_C port = $PATH_C_PORT_BASE + i"
echo "  Initiator should see ${NUM_TENANTS} multipath bdev_nvme entries, each with 3 paths"
echo "  Cleanup: $0 --cleanup"
