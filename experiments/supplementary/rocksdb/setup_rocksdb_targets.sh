#!/bin/bash
# setup_arm1_e2_3malloc.sh — E2 target: 1 tenant × 3-path, per-path malloc+delay+error stack.
# Variant (2026-10-06): targets listen on 10.0.0.2 (mlx5_0). Cores 40-45 avoid the
# initiator reactor (core 4).
#
# WHY 3-malloc per-path (NOT the single shared malloc of setup_arm1_realapp_3path.sh):
#   E2 needs PER-PATH fault injection (D1 5ms delay on path B only; D3 sct=2/sc=0x81 on
#   path B only). On the single-shared-malloc topology a delay/error vbdev sits on the one
#   backing store and degrades ALL three listeners at once — cannot isolate path B. And
#   per-namespace fault vbdevs on that shared topology hit SPDK bdev-claim walls
#   (nvmf/delay/part EXCL_WRITE). So we go back to the D-series 3-tgt / 3-malloc layout
#   where each path = its own nvmf_tgt + its own malloc→delay→error vbdev chain, exactly
#   like setup_arm1_sapsq_4t3p_mlx5_0.sh but trimmed to ONE tenant.
#
#   Consistency for a real filesystem (read-after-write) is handled OUTSIDE this script:
#   the three mallocs are dd-preloaded with the SAME 12 GiB RocksDB image, and the run is
#   mounted READ-ONLY (ro,noatime) so no path ever desyncs from another. See
#   e2_build_and_preload.sh + run_e2_fault.sh.
#
# Topology:
#   Tgt A: core 0x3,  socket spdk_a.sock, shm 1, malloc_A→delay_A→EE_delay_A, port 4500
#   Tgt B: core 0xc,  socket spdk_b.sock, shm 2, malloc_B→delay_B→EE_delay_B, port 4600
#   Tgt C: core 0x30, socket spdk_c.sock, shm 3, malloc_C→delay_C→EE_delay_C, port 4700
#   All three subsystems = nqn.2024-01.io.spdk:tenant0, SAME UUID + SAME NGUID
#     → SPDK initiator merges the 3 listeners into ONE multipath bdev (mpn1).
#   Ports 4500/4600/4700 match run_realapp_nbd_m1.sh DPA_PLUGIN_PORT_MAP → path_id 0/1/2.
#
# Flags:  (no arg) setup;  --cleanup  kill all 3 tgts + remove sockets.

set -euo pipefail

SPDK=/home/aiden/spdk
RPC="$SPDK/scripts/rpc.py"
if   [ -x "$SPDK/build/bin/nvmf_tgt" ];  then NVMF_TGT="$SPDK/build/bin/nvmf_tgt"
elif [ -x "$SPDK/build/examples/nvmf" ]; then NVMF_TGT="$SPDK/build/examples/nvmf"
else echo "[ERROR] no nvmf_tgt binary under $SPDK/build"; exit 1; fi

TARGET_IP="10.0.0.2"
NQN="nqn.2024-01.io.spdk:tenant0"
# 12 GiB per malloc: matches setup_arm1_realapp_3path.sh M2 sizing (num=2M × 1KB DB image).
# 16 GiB fails (no single contiguous heap chunk); 12 GiB is the largest single malloc here.
BDEV_SIZE_MB="${BDEV_SIZE_MB:-12288}"
BDEV_BLOCK=4096
DELAY_US="${DELAY_US:-27}"          # healthy per-path latency (D-series baseline)
# Identical UUID + NGUID across A/B/C → one multipath bdev on the initiator.
UUID="11111111-2222-3333-4444-000000000000"
NGUID="00000000000000001111111111110000"
# malloc bdev = one contiguous spdk_zmalloc; reserve BDEV_SIZE_MB + 2 GiB headroom.
MEM_MB=$(( BDEV_SIZE_MB + 2048 ))

declare -A SOCKS=( [A]=/var/tmp/spdk_e2s_a.sock [B]=/var/tmp/spdk_e2s_b.sock [C]=/var/tmp/spdk_e2s_c.sock )
declare -A CORES=( [A]=0x30000000000 [B]=0xC0000000000 [C]=0x300000000000 )
declare -A SHM=(   [A]=11  [B]=12  [C]=13 )
declare -A PORT=(  [A]=4500 [B]=4600 [C]=4700 )

cleanup() {
    echo "[e2-setup] cleanup: kill 3 nvmf_tgts + remove sockets"
    ( sudo pkill -9 -f "nvmf_tgt -m 0x[0-9A-Fa-f]* -r /var/tmp/spdk_e2s_" 2>/dev/null; exit 0 ) || true
    sleep 1
    sudo rm -f "${SOCKS[A]}" "${SOCKS[B]}" "${SOCKS[C]}" /var/tmp/spdk_e2s_*.lock /var/tmp/spdk_cpu_lock_04[0-5] /dev/shm/nvmf_trace.1[123] 2>/dev/null || true
    # -i keeps hugepage files after the process exits; remove only ours (shm ids 11-13).
    sudo find /dev/hugepages -maxdepth 1 -regex ".*/spdk1[123]map_[0-9]+" -delete 2>/dev/null || true
    echo "[e2-setup] cleanup done"
}

if [[ "${1:-}" == "--cleanup" ]]; then cleanup; exit 0; fi

echo "[e2-setup] kill prior tgts + clean"
cleanup

for P in A B C; do
    echo "[e2-setup] start nvmf_tgt path=$P (core ${CORES[$P]}, sock ${SOCKS[$P]}, shm ${SHM[$P]}, -s ${MEM_MB}MB)"
    # -g (--single-file-segments): a few hugepage files per process instead of one per
    # 2 MiB page. With ~21,500 per-page files, each holding a flock, another user's lsof
    # polling stalled arm-2 on the file-lock rwsem (2026-10-06).
    sudo nohup "$NVMF_TGT" -m "${CORES[$P]}" -r "${SOCKS[$P]}" -i "${SHM[$P]}" -s "$MEM_MB" -g \
        > "/tmp/nvmf_e2s_${P,,}.log" 2>&1 &
done

echo "[e2-setup] wait for 3 sockets (up to 40s)"
for i in $(seq 1 40); do
    c=0; for P in A B C; do [ -S "${SOCKS[$P]}" ] && c=$((c+1)); done
    [ "$c" -eq 3 ] && { echo "[e2-setup] all 3 sockets ready (${i}s)"; break; }
    sleep 1
done
for P in A B C; do [ -S "${SOCKS[$P]}" ] || { echo "[ERROR] socket $P missing — see /tmp/nvmf_e2s_*.log"; tail -20 "/tmp/nvmf_e2s_${P,,}.log"; exit 1; }; done

configure_path() {
    local P=$1
    local SOCK="${SOCKS[$P]}"
    local R="sudo $RPC -s $SOCK"
    local MALLOC="malloc_${P}" DELAY="delay_${P}" ERR="EE_delay_${P}"
    # Non-overlapping cntlid window per path (A=0,B=1,C=2) so the initiator does not
    # reject "cntlid duplicated" when merging the 3 listeners into one multipath bdev.
    local PIDX; case "$P" in A) PIDX=0;; B) PIDX=1;; C) PIDX=2;; esac
    local CMIN=$((PIDX * 128 + 1)) CMAX=$((PIDX * 128 + 32))

    echo "[e2-setup] path=$P: RDMA transport + malloc→delay→error stack"
    $R nvmf_create_transport -t RDMA
    $R bdev_malloc_create -b "$MALLOC" "$BDEV_SIZE_MB" "$BDEV_BLOCK"
    $R bdev_delay_create -b "$MALLOC" -d "$DELAY" -r "$DELAY_US" -t "$DELAY_US" -w "$DELAY_US" -n "$DELAY_US"
    $R bdev_error_create "$DELAY"               # produces EE_<delay> = EE_delay_<P>
    $R nvmf_create_subsystem "$NQN" -s "T0P${P}SN00" -a -m 32 -i "$CMIN" -I "$CMAX"
    $R nvmf_subsystem_add_ns "$NQN" "$ERR" -n 1 -u "$UUID" --nguid "$NGUID"
    $R nvmf_subsystem_add_listener "$NQN" -t rdma -a "$TARGET_IP" -s "${PORT[$P]}"
}

for P in A B C; do configure_path "$P"; done

echo ""
echo "[verify] listeners (expect tenant0, same nguid, ports 4500/4600/4700):"
for P in A B C; do
    echo "  path=$P (${SOCKS[$P]}):"
    sudo "$RPC" -s "${SOCKS[$P]}" nvmf_get_subsystems | python3 -c "
import json,sys
for ss in json.load(sys.stdin):
    if 'tenant' in ss.get('nqn',''):
        ports=[l.get('trsvcid') for l in ss.get('listen_addresses',[])]
        ns=[n.get('bdev_name') for n in ss.get('namespaces',[])]
        ng=(ss.get('namespaces') or [{}])[0].get('nguid','')
        print(f'    {ss[\"nqn\"]} bdev={ns} ports={ports} nguid={ng}')
"
done
echo ""
echo "[e2-setup] DONE — 1 tenant × 3-path, per-path malloc(12GiB)+delay+error, ports 4500/4600/4700"
echo "  D1 (path-B 5ms): rpc.py -s ${SOCKS[B]} bdev_delay_update_latency delay_B {avg,p99}_{read,write} 5000"
echo "  D3 (path-B sct=2/sc=0x81): rpc.py -s ${SOCKS[B]} bdev_error_inject_nvme_error EE_delay_B read --sct 2 --sc 0x81 -n 100000000"
echo "  Cleanup: $0 --cleanup"
