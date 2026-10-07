#!/bin/bash
# run_e2_fault.sh — E2: RocksDB readrandom over 3-path NVMe-oF multipath, READ-ONLY,
# with mid-run per-path fault on path B. Three modes × two faults × N reps.
# variant (2026-10-06): initiator and target both on arm-2; target at
# 10.0.0.2 (mlx5_0 RDMA, cores 40-45); db_bench pinned to cores 8-39.
#
# Preconditions:
#   - arm-1: setup_arm1_e2_3malloc.sh already up (1 tenant × 3-path, per-path malloc+
#     delay+error, ports 4500/4600/4700, same UUID/NGUID).
#   - The 3 mallocs already dd-preloaded byte-identical via e2_build_and_preload.sh.
#
# Three modes (canonical mapping from scripts/run_saps_q_e2_idle.py + run_sapsq.py):
#   dpa_full : DPA SAPS_Q offload. DISABLE_INIT=0 HOST_SAPS_ENABLED=0 SAPS_Q_ENABLED=1,
#              multipath policy=plugin (DPA picks healthy path).
#   host     : host-resident SAPS. DISABLE_INIT=1 HOST_SAPS_ENABLED=1 SAPS_Q_ENABLED=0,
#              multipath policy=plugin (same algorithm, runs on host reactor core).
#   stock    : no plugin. DISABLE_INIT=1, multipath policy=active_active+round_robin.
#
# Three regimes (path B, via arm-1 per-path socket spdk_b.sock):
#   healthy : no injection
#   D1 : bdev_delay_update_latency delay_B {avg,p99}_{read,write} 5000  (5ms fail-slow)
#   D3 : bdev_error_inject_nvme_error EE_delay_B read --sct 2 --sc 129 -n 100000000
#
# HONESTY NOTE: workload is READ-ONLY (mount -o ro,noatime, db_bench readrandom). This is
# required so the 3 independent mallocs never desync. App-level numbers therefore measure
# read-path resilience only; no write path is exercised. Reported as such.
#
# Output: $OUTDIR/{mode}_{fault}/rep_N/readrandom.log + e2_results.csv + summary.

set -uo pipefail

ROOT=/mnt/nvme0n1p1/aiden/DPA/nvme-of-controller
SPDK=/home/aiden/spdk
SPDK_TGT="${SPDK_TGT:-$SPDK/build/bin/spdk_tgt}"
RPC="$SPDK/scripts/rpc.py"
DB_BENCH="${DB_BENCH:-/home/aiden/rocksdb/db_bench}"

SOCK=/var/tmp/spdk_realapp.sock
TARGET_IP=10.0.0.2
NQN=nqn.2024-01.io.spdk:tenant0
PORTS=(4500 4600 4700)            # path A/B/C → path_id 0/1/2
BDEV_BASE=mp
MP_BDEV=mpn1
NBD_DEV=/dev/nbd0
MNT=/mnt/realapp
DB="$MNT/rocksdb"

# arm-1 per-path socket for path B fault injection
SOCK_B=/var/tmp/spdk_e2s_b.sock

NUM="${NUM:-2000000}"
VALUE_SIZE="${VALUE_SIZE:-1024}"
DURATION="${DURATION:-60}"        # readrandom run length (s); fault injected at FAULT_AT
FAULT_AT="${FAULT_AT:-20}"        # inject path-B fault this many seconds into the run
THREADS="${THREADS:-8}"
# CACHE DEFEAT (load-bearing for E2 validity): arm-2 has 250 GiB RAM and the DB is ~2.5 GiB,
# so without this readrandom serves entirely from OS page cache + RocksDB block cache and NEVER
# touches the NVMe-oF paths — the path-B fault would be invisible and the app numbers fake.
# --use_direct_reads=1 → O_DIRECT bypasses the OS page cache (reads hit ext4→nbd→NVMe-oF RDMA).
# --cache_size small → minimise RocksDB block-cache hits. We also drop_caches before each run.
CACHE_MB="${CACHE_MB:-8}"
DIRECT_READS="${DIRECT_READS:-1}"
REPS="${REPS:-3}"
MODES="${MODES:-stock dpa_full}"
FAULTS="${FAULTS:-healthy D1 P3}"
OUTDIR="${OUTDIR:-$ROOT/experiments/realapp_nbd/e2_fault/run_$(date +%Y%m%d_%H%M%S)}"
SMOKE="${SMOKE:-0}"               # SMOKE=1 → single rep, short duration, gate checks only

CSV="$OUTDIR/e2_results.csv"

log() { echo "[$(date +%H:%M:%S)] [e2-run] $*"; }
rpc() { sudo "$RPC" -s "$SOCK" "$@"; }
rpcb(){ sudo $RPC -s $SOCK_B $*; }   # path-B target RPC socket

stop_stack() {
    sudo "$RPC" -s "$SOCK" nbd_stop_disk "$NBD_DEV" 2>/dev/null || true
    ( sudo pkill -9 -f "spdk_tgt[.a-z]* -m 0x10 -r $SOCK" 2>/dev/null; exit 0 ) || true
    sleep 1
    sudo rm -f "$SOCK" /var/tmp/spdk_realapp*.lock 2>/dev/null || true
}

# Restore path B to healthy (clear delay + clear error injection) on arm-1.
restore_pathB() {
    log "restore path B healthy (delay=27us, clear errors)"
    for M in avg_read p99_read avg_write p99_write; do
        rpcb "bdev_delay_update_latency delay_B $M 27" >/dev/null 2>&1 || true
    done
    # NOTE: rpc.py argparse parses --sc as base-10 int; 0x81 is REJECTED ("invalid int value").
    # sct=2/sc=0x81 (Unrecovered Read Error) → decimal sc=129. This silently broke D3 for every
    # prior run (inject rc=2, swallowed by || true) → no errors injected → SAPS saw nothing.
    rpcb "bdev_error_inject_nvme_error EE_delay_B clear --sct 2 --sc 129 -n 1" >/dev/null 2>&1 || true
    rpcb "bdev_error_inject_nvme_error EE_delay_B clear --sct 3 --sc 0 -n 1" >/dev/null 2>&1 || true
}

# Inject the named fault on path B (arm-1).
inject_fault() {
    local f=$1
    if [ "$f" = "healthy" ]; then
        log "HEALTHY: no path-B fault injected"
    elif [ "$f" = "D1" ]; then
        log "INJECT D1: path B → 5ms (avg+p99 read+write)"
        for M in avg_read p99_read avg_write p99_write; do
            rpcb "bdev_delay_update_latency delay_B $M 5000" >/dev/null 2>&1 || true
        done
    elif [ "$f" = "D3" ]; then
        log "INJECT D3: path B → sct=2/sc=0x81 read errors (100M budget)"
        # --sc must be decimal (129 = 0x81); rpc.py argparse rejects hex. Fail LOUD: if the
        # inject errors out, D3 is meaningless (no fault), so surface it instead of swallowing.
        local inj_out
        inj_out=$(rpcb "bdev_error_inject_nvme_error EE_delay_B read --sct 2 --sc 129 -n 100000000" 2>&1)
        if [ $? -ne 0 ]; then
            log "ERROR: D3 inject FAILED — $inj_out"; return 1
        fi
        log "  D3 inject armed OK"
    elif [ "$f" = "P3" ]; then
        # Path Related Status (SCT 3h), Internal Path Error (SC 00h) on path B reads.
        log "INJECT P3: path B -> sct=3/sc=0x00 path errors on reads"
        local inj_out
        inj_out=$(rpcb "bdev_error_inject_nvme_error EE_delay_B read --sct 3 --sc 0 -n 100000000" 2>&1)
        if [ $? -ne 0 ]; then log "ERROR: P3 inject FAILED - $inj_out"; return 1; fi
        log "  P3 inject armed OK"
    else
        log "ERROR: unknown fault $f"; return 1
    fi
}

# Persist target-side evidence independently of the application output.  In
# particular, bdev_get_iostat.io_error.nvme_error is the number of D3 statuses
# actually served by EE_delay_B, not merely the requested injection budget.
capture_pathB_state() {
    local outdir=$1 phase=$2
    rpcb "bdev_get_iostat -b EE_delay_B" > "$outdir/target_pathB_iostat_${phase}.json" 2> "$outdir/target_pathB_iostat_${phase}.err" || true
    rpcb "bdev_get_bdevs -b delay_B" > "$outdir/target_pathB_delay_${phase}.json" 2> "$outdir/target_pathB_delay_${phase}.err" || true
}

# Bring up the m1 multipath stack in the given mode, export nbd, mount ro.
start_mode() {
    local mode=$1 outdir=$2
    stop_stack
    sudo modprobe nbd nbds_max=16 2>/dev/null || true
    mkdir -p "$outdir"
    local TGT_LOG="$outdir/spdk_tgt.log"

    # Per-mode env (canonical mapping).
    local ENVV=()
    case "$mode" in
        dpa_full) ENVV=( DPA_PLUGIN_ROLE=standalone DPA_PLUGIN_DISABLE_INIT=0 HOST_SAPS_ENABLED=0
                         SAPS_Q_ENABLED=1 SAPS_Q_MY_TENANT_ID=0 SAPS_Q_WEIGHTS=65536
                         SAPS_Q_DEMAND_Q32=429497 SAPS_Q_PATH_BASE_IOPS_Q32=858993,858993,858993
                         SAPS_Q_PROBE_RATE_Q32=429,429,429
                         DPA_PLUGIN_PATH_MAP_PORTS=4500:0,4600:1,4700:2
                         DPA_PLUGIN_SAMPLE_RATE=1 SAPS_TRACE=0
                         SAPS_M4_ENABLED=0 SAPS_M5_DRR_ENABLED=0
                         DPA_PLUGIN_DEV=mlx5_0 DPA_PLUGIN_SAMPLER_CSV="$outdir/sampler.csv" ) ;;
        # Evaluated (v1.1) controller profile: the M-series HCAA path used by the paper's
        # service experiments (coordinator role, SAPSQ_ENABLED=1, classifier and FSM on,
        # recovery probes, sample rate 32), reduced to one tenant. The dpa_full mode above
        # is the legacy SAPS_Q path that the July 19 RocksDB runs used.
        saps)     ENVV=( DPA_PLUGIN_ROLE=coordinator DPA_PLUGIN_SOCK=/tmp/dpa_plugin_e2s.sock
                         DPA_PLUGIN_DISABLE_INIT=0 HOST_SAPS_ENABLED=0 DPA_PLUGIN_SAMPLE_RATE=32
                         SAPS_TRACE=0 SAPSQ_ENABLED=1 SAPSQ_MY_TENANT_ID=0 SAPSQ_NUM_TENANTS=1
                         SAPSQ_NUM_PATHS=3 SAPSQ_LINK_CAP_IOPS=900000
                         SAPSQ_PATH_CAP_IOPS=500000,500000,500000 SAPSQ_EPOCH_PERIOD_US=1000
                         SAPSQ_WEIGHTS=1 SAPSQ_PROBE_RATE_IOPS=1000 SAPSQ_HOST_TSC_FREQ=1000000000
                         SAPSQ_BYPASS_D_CLASSIFIER=0 SAPSQ_BYPASS_SAPS_FSM=0
                         SAPSQ_BYPASS_HEALTH_COUPLING=0 SAPSQ_BYPASS_BUDGET_SELECTION=0
                         SAPS_SELECTOR_TRACE=0 SAPS_M2_ENABLED=1 SAPS_M2_V2_CLASSIFIER=0
                         SAPS_Q_ENABLED=0 SAPS_M4_ENABLED=0 SAPS_M5_DRR_ENABLED=1
                         SAPS_M5_WEIGHTS=1 SAPS_M5_LINK_IOPS=900000 SAPS_M5_TENANT_ID=0
                         SAPS_ACTIVE_PROBE=1 DPA_PLUGIN_DEV=mlx5_0
                         DPA_PLUGIN_PATH_MAP_PORTS=4500:0,4600:1,4700:2
                         DPA_PLUGIN_SAMPLER_CSV="$outdir/sampler.csv" ) ;;
        host)     ENVV=( DPA_PLUGIN_ROLE=standalone DPA_PLUGIN_DISABLE_INIT=1 HOST_SAPS_ENABLED=1
                         SAPS_TRACE=0 SAPS_Q_ENABLED=0 SAPS_M4_ENABLED=0 SAPS_M5_DRR_ENABLED=0
                         DPA_PLUGIN_PATH_MAP_PORTS=4500:0,4600:1,4700:2 ) ;;
        stock)    ENVV=( DPA_PLUGIN_DISABLE_INIT=1 SAPS_Q_ENABLED=0 SAPS_M4_ENABLED=0 SAPS_M5_DRR_ENABLED=0 ) ;;
        *) log "ERROR unknown mode $mode"; return 1 ;;
    esac

    log "launch spdk_tgt mode=$mode"
    sudo env "${ENVV[@]}" nohup "$SPDK_TGT" -m 0x10 -r "$SOCK" > "$TGT_LOG" 2>&1 &

    local ok=0
    for i in $(seq 1 30); do
        [ -S "$SOCK" ] && sudo "$RPC" -s "$SOCK" rpc_get_methods >/dev/null 2>&1 && { ok=1; break; }
        sleep 1
    done
    [ "$ok" = 1 ] || { log "ERROR spdk_tgt RPC not ready"; tail -30 "$TGT_LOG"; return 1; }

    rpc bdev_nvme_set_options --io-path-stat 2>/dev/null || true
    for port in "${PORTS[@]}"; do
        rpc bdev_nvme_attach_controller -b "$BDEV_BASE" -t rdma -a "$TARGET_IP" -s "$port" \
            -f ipv4 -n "$NQN" --multipath multipath >> "$outdir/attach.log" 2>&1 || true
    done
    sleep 2

    # multipath policy: dpa_full/host → plugin; stock → active_active round_robin
    if [ "$mode" = "stock" ]; then
        rpc bdev_nvme_set_multipath_policy -b "$MP_BDEV" -p active_active -s round_robin >> "$outdir/attach.log" 2>&1 || true
    else
        rpc bdev_nvme_set_multipath_policy -b "$MP_BDEV" -p plugin >> "$outdir/attach.log" 2>&1 || true
    fi

    # verify multipath bdev exists
    local nb
    nb=$(rpc bdev_get_bdevs 2>/dev/null | python3 -c "
import json,sys
try:
    for b in json.load(sys.stdin):
        if b.get('name')=='$MP_BDEV': print(b['num_blocks']); break
except Exception: pass")
    [ -n "$nb" ] || { log "ERROR multipath bdev $MP_BDEV missing"; tail -20 "$outdir/attach.log"; return 1; }
    log "multipath bdev $MP_BDEV blocks=$nb"

    rpc nbd_start_disk "$MP_BDEV" "$NBD_DEV" >> "$outdir/attach.log" 2>&1 || { log "ERROR nbd_start_disk"; return 1; }
    sleep 1
    # READ-ONLY mount (no path desync). ext4 was created by e2_build_and_preload.sh.
    sudo mount -o ro,noatime "$NBD_DEV" "$MNT" 2>> "$outdir/attach.log" || { log "ERROR mount -o ro failed"; sudo dmesg | tail -5; return 1; }
    mountpoint -q "$MNT" || { log "ERROR $MNT not mounted"; return 1; }
    [ -d "$DB" ] || { log "ERROR $DB not present on mounted fs"; ls -la "$MNT" | sed 's/^/    /'; return 1; }
    log "mounted ro, DB present at $DB"
}

stop_mode() {
    sudo umount "$MNT" 2>/dev/null || true
    stop_stack
}

# Run readrandom for DURATION, inject fault at FAULT_AT seconds, parse ops/s + percentiles.
run_one() {
    local mode=$1 fault=$2 outdir=$3
    mkdir -p "$outdir"
    local rlog="$outdir/readrandom.log"

    # Drop OS page cache so reads actually traverse the NVMe-oF path (see CACHE DEFEAT note).
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true

    capture_pathB_state "$outdir" pre

    # Background fault injector with a durable rc/timestamp artifact.
    (
        sleep "$FAULT_AT"
        echo "inject_start_unix=$(date +%s.%N)"
        inject_fault "$fault"
        irc=$?
        echo "inject_rc=$irc"
        echo "inject_end_unix=$(date +%s.%N)"
        exit "$irc"
    ) > "$outdir/fault_injection.log" 2>&1 &
    local inj_pid=$!

    local CACHE_BYTES=$(( CACHE_MB * 1024 * 1024 ))
    log "readrandom mode=$mode fault=$fault dur=${DURATION}s fault_at=${FAULT_AT}s threads=$THREADS direct=$DIRECT_READS cache=${CACHE_MB}MB"
    timeout $((DURATION + 120)) taskset -c 8-39 "$DB_BENCH" \
        --db="$DB" --value_size="$VALUE_SIZE" --compression_type=none --histogram=1 \
        --benchmarks=readrandom --use_existing_db=1 --readonly=1 --num="$NUM" --reads=-1 \
        --duration="$DURATION" --threads="$THREADS" \
        --use_direct_reads="$DIRECT_READS" --cache_size="$CACHE_BYTES" \
        > "$rlog" 2>&1
    local rc=$?
    wait "$inj_pid" 2>/dev/null
    local inj_rc=$?
    capture_pathB_state "$outdir" post
    log "  readrandom rc=$rc inject_rc=$inj_rc"

    # parse: 'readrandom : X micros/op Y ops/sec ...' and 'Percentiles: P50 ... P99 ... P99.9 ...'
    python3 - "$rlog" "$mode" "$fault" "$CSV" "$outdir" "$rc" "$inj_rc" <<'PY'
import json,re,sys,time
rlog,mode,fault,csv,outdir,db_rc,inj_rc=sys.argv[1:8]
txt=open(rlog,errors="replace").read().replace("\r","\n")
ops=micros=mbs=None
m=re.search(r"readrandom\s*:\s*([\d.]+)\s*micros/op\s+(\d+)\s*ops/sec.*?([\d.]+)\s*MB/s",txt)
if m: micros=float(m.group(1)); ops=int(m.group(2)); mbs=float(m.group(3))
p={}
mp=re.search(r"Percentiles:\s*(.*)",txt)
if mp:
    for k,v in re.findall(r"(P[\d.]+):\s*([\d.]+)",mp.group(1)):
        p[k]=float(v)
import os
def load(name):
    try:
        with open(os.path.join(outdir,name)) as f: return json.load(f)
    except Exception: return {}
def bdev0(d):
    xs=d.get('bdevs') or []
    return xs[0] if xs else {}
pre=bdev0(load('target_pathB_iostat_pre.json'))
post=bdev0(load('target_pathB_iostat_post.json'))
pre_errors=sum((pre.get('io_error') or {}).values())
post_errors=sum((post.get('io_error') or {}).values())
served=max(0,post_errors-pre_errors)
delay=(load('target_pathB_delay_post.json') or [{}])[0].get('driver_specific',{}).get('delay',{})
if fault=='healthy': manifested=True; evidence='healthy_no_fault'
elif fault=='D1':
    fields=('avg_read_latency','p99_read_latency','avg_write_latency','p99_write_latency')
    manifested=all(delay.get(k)==5000 for k in fields)
    evidence='delay_fields_5000us' if manifested else 'delay_fields_missing'
else:
    manifested=served>0
    evidence=f'target_io_errors_served={served}'
manifest={
    'schema_version':1,'mode':mode,'fault':fault,'db_bench_rc':int(db_rc),
    'fault_inject_rc':int(inj_rc),'fault_manifested':manifested,
    'fault_evidence':evidence,'target_error_served_count':served,
    'ops_per_sec':ops,'p99_us':p.get('P99'),'p999_us':p.get('P99.9'),
    'generated_unix':time.time(),
}
with open(os.path.join(outdir,'manifest.json'),'w') as f: json.dump(manifest,f,indent=2); f.write('\n')
row=f"{mode},{fault},{ops},{micros},{mbs},{p.get('P50','')},{p.get('P99','')},{p.get('P99.9','')},{p.get('P99.99','')},{db_rc},{inj_rc},{served},{int(manifested)}"
new=not os.path.exists(csv)
with open(csv,"a") as f:
    if new: f.write("mode,fault,ops_per_sec,micros_per_op,mb_per_s,p50_us,p99_us,p999_us,p9999_us,db_bench_rc,fault_inject_rc,target_error_served_count,fault_manifested\n")
    f.write(row+"\n")
print(f"[e2-run]   parsed mode={mode} fault={fault} ops={ops} p99={p.get('P99')} p99.9={p.get('P99.9')} manifested={manifested} served={served}")
PY
    [ "$inj_rc" -eq 0 ] || return 2
}

# ── SMOKE GATE ───────────────────────────────────────────────────────────────
smoke() {
    log "=== SMOKE GATE (single rep, short) ==="
    local sd="$OUTDIR/smoke"; mkdir -p "$sd"
    DURATION=15 FAULT_AT=5

    # Gate 1: 3-path same-NGUID multipath device + ext4 ro mount + readrandom runs
    log "[gate1] start dpa_full stack, mount ro, short readrandom"
    local sm="${SMOKE_MODE:-dpa_full}"
    start_mode "$sm" "$sd/g1" || { log "GATE1 FAIL: stack/mount"; return 1; }
    run_one "$sm" D1 "$sd/g1"
    grep -q "ops/sec" <(tr '\r' '\n' < "$sd/g1/readrandom.log") || { log "GATE1 FAIL: no ops/sec"; stop_mode; return 1; }
    log "[gate1] PASS (multipath ro mount + readrandom produced ops/sec)"

    # Gate 2: SAPS saw IO (sampler.csv sample events > 0)
    local nev=0
    [ -f "$sd/g1/sampler.csv" ] && nev=$(wc -l < "$sd/g1/sampler.csv" 2>/dev/null || echo 0)
    if [ "$nev" -gt 1 ]; then log "[gate2] PASS (sampler.csv lines=$nev)"; else
        log "[gate2] WARN sampler.csv lines=$nev — checking spdk_tgt.log for sample_events"
        grep -iE "sample_events|samples|SAPS" "$sd/g1/spdk_tgt.log" | tail -3 | sed 's/^/    /' || true
    fi

    # Gate 3: after path-B fault, dpa_full health[B] drops (sampler) — fault already injected in g1.
    log "[gate3] check path-B health change in dpa_full"
    grep -iE "health|HEALTH|DEGRAD|fault_type|path.*1" "$sd/g1/sampler.csv" 2>/dev/null | tail -5 | sed 's/^/    /' || true
    stop_mode; restore_pathB

    # Gate 4: db_bench readrandom numbers (ops/s + p99/p99.9) captured — already in CSV from g1.
    log "[gate4] CSV rows so far:"
    [ -f "$CSV" ] && sed 's/^/    /' "$CSV" || log "GATE4 FAIL: no CSV"
    log "=== SMOKE GATE DONE — review gate1/4 PASS + gate2/3 evidence above ==="
}

# ── MAIN ─────────────────────────────────────────────────────────────────────
mkdir -p "$OUTDIR"
[ -x "$DB_BENCH" ] || { log "ERROR db_bench missing at $DB_BENCH"; exit 1; }
log "OUTDIR=$OUTDIR modes=[$MODES] faults=[$FAULTS] reps=$REPS dur=${DURATION}s fault_at=${FAULT_AT}s SMOKE=$SMOKE"

trap 'log "trap: teardown"; stop_mode 2>/dev/null; restore_pathB 2>/dev/null' EXIT

if [ "$SMOKE" = "1" ]; then
    smoke
    exit 0
fi

restore_pathB
for fault in $FAULTS; do
    for mode in $MODES; do
        for r in $(seq 1 "$REPS"); do
            od="$OUTDIR/${mode}_${fault}/rep_$r"
            log "--- mode=$mode fault=$fault rep=$r ---"
            restore_pathB
            if ! start_mode "$mode" "$od"; then log "WARN start_mode failed mode=$mode fault=$fault rep=$r"; stop_mode; continue; fi
            run_one "$mode" "$fault" "$od" || log "WARN run_one failed"
            stop_mode
            restore_pathB
        done
    done
done

echo ""
echo "================ E2 READ-ONLY FAULT SUMMARY (app-level RocksDB readrandom) ================"
[ -f "$CSV" ] && column -s, -t "$CSV"
echo "=========================================================================================="
log "DONE. results in $OUTDIR ; csv=$CSV"
