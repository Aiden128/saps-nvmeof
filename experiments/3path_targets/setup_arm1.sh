#!/bin/bash
# setup_arm1.sh — Start 3 nvmf_tgt processes and configure 3-path multipath target
# Idempotent: kills existing processes and re-creates sockets if they exist
# Topology:
#   Tgt A: core 0x3,  port 4430, bdev malloc_A -> delay_A -> EE_delay_A
#   Tgt B: core 0xc,  port 4431, bdev malloc_B -> delay_B -> EE_delay_B
#   Tgt C: core 0x30, port 4432, bdev malloc_C -> delay_C -> EE_delay_C
# All share NQN nqn.2024-01.io.spdk:mptest, same UUID for multipath recognition

set -e

SPDK=/home/aiden/spdk
RPC="$SPDK/scripts/rpc.py"
NQN="nqn.2024-01.io.spdk:mptest"
UUID="12345678-1234-1234-1234-123456789012"
# NGUID must also be identical across 3 tgt for SPDK multipath merge
# (UUID alone不夠,SPDK 的 spdk_nvme_ns_cmp() 要 NGUID 也一致才接受為同一 bdev 的 path)
NGUID="00000000000000001234567812345678"
TARGET_IP="10.0.0.1"

# ── Idempotent teardown ────────────────────────────────────────────────────────
echo "[setup] Killing any existing nvmf_tgt processes..."
sudo pkill -f nvmf_tgt 2>/dev/null || true
sleep 2
sudo rm -f /var/tmp/spdk_a.sock /var/tmp/spdk_b.sock /var/tmp/spdk_c.sock

# ── Phase 1: Start 3 nvmf_tgt processes ───────────────────────────────────────
echo "[setup] Starting nvmf_tgt A (cores 0x3, port 4430)..."
sudo nohup "$SPDK/build/bin/nvmf_tgt" -m 0x3 -r /var/tmp/spdk_a.sock -i 1 --no-pci \
    > /tmp/nvmf_a.log 2>&1 &
PID_A=$!

echo "[setup] Starting nvmf_tgt B (cores 0xc, port 4431)..."
sudo nohup "$SPDK/build/bin/nvmf_tgt" -m 0xc -r /var/tmp/spdk_b.sock -i 2 --no-pci \
    > /tmp/nvmf_b.log 2>&1 &
PID_B=$!

echo "[setup] Starting nvmf_tgt C (cores 0x30, port 4432)..."
sudo nohup "$SPDK/build/bin/nvmf_tgt" -m 0x30 -r /var/tmp/spdk_c.sock -i 3 --no-pci \
    > /tmp/nvmf_c.log 2>&1 &
PID_C=$!

echo "[setup] Waiting for sockets to appear (up to 30s)..."
for i in $(seq 1 30); do
    COUNT=$(ls /var/tmp/spdk_a.sock /var/tmp/spdk_b.sock /var/tmp/spdk_c.sock 2>/dev/null | wc -l)
    if [ "$COUNT" -eq 3 ]; then
        echo "[setup] All 3 sockets ready after ${i}s"
        break
    fi
    sleep 1
done
ls -la /var/tmp/spdk_*.sock || { echo "[ERROR] Sockets not created. Check /tmp/nvmf_*.log"; exit 1; }

# ── Phase 2: Configure each target ────────────────────────────────────────────
configure_tgt() {
    local SOCK=$1
    local BDEV_SUFFIX=$2   # A, B, or C
    local PORT=$3
    local CNTLID_MIN=$4
    local CNTLID_MAX=$5
    local SERIAL=$6
    local SUBSYSTEM_ARGS=(
        -s "$SERIAL"
        -a
        -m 32
        -i "$CNTLID_MIN"
        -I "$CNTLID_MAX"
    )

    # The competitor/detection harness opts in so it can inject a real ANA
    # state transition without disconnecting a controller. Default behavior
    # stays unchanged for every existing caller of setup_arm1.sh.
    if [ "${SAPS_ANA_REPORTING:-0}" = "1" ]; then
        SUBSYSTEM_ARGS+=(-r)
    fi

    RPC_CMD="sudo $RPC -s $SOCK"
    echo "[setup] Configuring tgt $BDEV_SUFFIX (socket $SOCK, port $PORT)..."

    # Create transport
    $RPC_CMD nvmf_create_transport -t RDMA

    # Create bdev chain: malloc -> delay -> error
    $RPC_CMD bdev_malloc_create -b "malloc_${BDEV_SUFFIX}" 64 4096
    $RPC_CMD bdev_delay_create \
        -b "malloc_${BDEV_SUFFIX}" \
        -d "delay_${BDEV_SUFFIX}" \
        -r 27 -t 27 -w 27 -n 27
    $RPC_CMD bdev_error_create "delay_${BDEV_SUFFIX}"
    # bdev_error_create produces "EE_delay_${BDEV_SUFFIX}"

    # Create subsystem
    $RPC_CMD nvmf_create_subsystem "$NQN" "${SUBSYSTEM_ARGS[@]}"

    # Add namespace with shared UUID + NGUID (SPDK multipath merge requires both)
    $RPC_CMD nvmf_subsystem_add_ns "$NQN" "EE_delay_${BDEV_SUFFIX}" \
        -n 1 \
        -u "$UUID" \
        --nguid "$NGUID"

    # Add listener
    $RPC_CMD nvmf_subsystem_add_listener "$NQN" \
        -t rdma \
        -a "$TARGET_IP" \
        -s "$PORT"

    echo "[setup] Tgt $BDEV_SUFFIX configured OK"
}

configure_tgt /var/tmp/spdk_a.sock A 4430 1  32 MPTESTSN_A
configure_tgt /var/tmp/spdk_b.sock B 4431 33 64 MPTESTSN_B
configure_tgt /var/tmp/spdk_c.sock C 4432 65 96 MPTESTSN_C

# ── Verify ────────────────────────────────────────────────────────────────────
echo ""
echo "[verify] Subsystem state on each tgt:"
for SOCK in a b c; do
    echo "--- spdk_${SOCK} ---"
    sudo "$RPC" -s "/var/tmp/spdk_${SOCK}.sock" nvmf_get_subsystems | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
for ss in data:
    if 'mptest' in ss.get('nqn', ''):
        print('  NQN:', ss['nqn'])
        for ns in ss.get('namespaces', []):
            print('  NS uuid:', ns.get('uuid'))
        for l in ss.get('listen_addresses', []):
            print('  Listener:', l.get('trtype'), l.get('traddr'), l.get('trsvcid'))
"
done

echo ""
echo "[setup] DONE — 3-path target ready on ports 4430/4431/4432"
