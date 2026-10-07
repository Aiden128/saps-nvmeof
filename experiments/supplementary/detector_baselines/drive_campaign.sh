#!/usr/bin/env bash
# Driver for c1/c2/c3/c4; legacy detector mode remains available.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPDK_ROOT="${SPDK_ROOT:-/home/aiden/spdk}"
HARNESS="$SCRIPT_DIR/run_competitor_fault_experiment.py"
CAMPAIGN=""
EXPERIMENT="legacy"
REFERENCE=""
HOST_PROOF_PATTERN=""
CAPS_SET=0
OUT=""
PATH_CAPS="300000,300000,300000"
SERVICE_LIMIT="900000"
FIXED_THRESHOLD_US="500"
POLL_INTERVAL_S="1"
COUNTER_HZ=""
RUNTIME_DIR=""
RUNTIME_OWNED=0
REGISTRY=""
HP_BEFORE=""
LOCK_FILE="/var/tmp/sapsbs_single.lock"
LOCK_FD=9

usage() {
    cat <<'USAGE'
Usage: ./drive_campaign.sh --campaign c1|c2|c3|c4 --smoke|--full --out OUTPUT [options]
Omit --campaign to use the original detector matrix.

Options:
  --campaign c1|c2|c3|c4      Campaign matrix (default: legacy)
  --reference-capacity-iops C  c1 fixed reference (default: 899983.2185078033)
  --host-proof-pattern REGEX  Optional extra host activation log requirement
  --path-capacities-iops CSV   c2/c4: 400000 x3; other modes: 300000 x3
  --service-limit-iops IOPS    Default: 900000
  --fixed-threshold-us USEC    Default: 500
  --poll-interval-s SEC        Default: 1
  --counter-hz HZ              Forwarded only when set
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke) CAMPAIGN="smoke"; shift ;;
        --full) CAMPAIGN="full"; shift ;;
        --out) OUT="${2:?missing --out value}"; shift 2 ;;
        --campaign) EXPERIMENT="${2:?missing campaign}"; shift 2 ;;
        --reference-capacity-iops) REFERENCE="${2:?missing value}"; shift 2 ;;
        --host-proof-pattern) HOST_PROOF_PATTERN="${2:?missing value}"; shift 2 ;;
        --path-capacities-iops) PATH_CAPS="${2:?missing value}"; CAPS_SET=1; shift 2 ;;
        --service-limit-iops) SERVICE_LIMIT="${2:?missing value}"; shift 2 ;;
        --fixed-threshold-us) FIXED_THRESHOLD_US="${2:?missing value}"; shift 2 ;;
        --poll-interval-s) POLL_INTERVAL_S="${2:?missing value}"; shift 2 ;;
        --counter-hz) COUNTER_HZ="${2:?missing value}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

case "$EXPERIMENT" in
    legacy|c1|c2|c3|c4) ;;
    *) echo "unknown campaign: $EXPERIMENT" >&2; exit 2 ;;
esac
if [[ "$CAPS_SET" == 0 && ( "$EXPERIMENT" == c2 || "$EXPERIMENT" == c4 ) ]]; then
    PATH_CAPS="400000,400000,400000"
fi
if [[ -z "$CAMPAIGN" || -z "$OUT" ]]; then
    usage >&2
    exit 2
fi
# Validate arguments before locking/reserving pages or starting any target.
python3 -B - "$PATH_CAPS" "$SERVICE_LIMIT" "$REFERENCE" "$EXPERIMENT" "$COUNTER_HZ" "$POLL_INTERVAL_S" "$FIXED_THRESHOLD_US" "$HOST_PROOF_PATTERN" <<'PYARGS'
import math, re, sys
caps, limit, reference, experiment, counter, poll, threshold, proof = sys.argv[1:]
values = [int(value) for value in caps.split(",")]
if len(values) != 3 or any(value <= 0 or value % 1000 for value in values):
    raise SystemExit("path capacities require three positive multiples of 1000")
if int(limit) <= 0:
    raise SystemExit("service limit must be positive")
if experiment == "c3" and any(value * 3 != int(limit) for value in values):
    raise SystemExit("c3 requires equal path capacities K_p=C/3 for the host reference")
if experiment == "c1" and not reference and (values != [300000] * 3 or int(limit) != 900000):
    raise SystemExit("custom c1 capacities require an explicit matching reference")
if reference:
    value = float(reference)
    if not math.isfinite(value) or value <= 0 or experiment not in ("legacy", "c1"):
        raise SystemExit("positive external reference is allowed only for c1/legacy")
if counter and int(counter) <= 0:
    raise SystemExit("counter frequency must be positive")
if not (0 < float(poll) <= 1) or not math.isfinite(float(threshold)) or float(threshold) <= 0:
    raise SystemExit("invalid poll interval/fixed threshold")
if proof:
    re.compile(proof)
PYARGS
if (( EUID != 0 )); then
    echo "drive_campaign.sh must run as root" >&2
    exit 1
fi
if [[ ! -f "$HARNESS" ]]; then
    echo "missing harness: $HARNESS" >&2
    exit 1
fi
if [[ -e "$OUT" && ! -d "$OUT" ]]; then
    echo "refusing --out path that is not a directory: $OUT" >&2
    exit 1
fi
if [[ -e "$OUT" ]] && find "$OUT" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
    echo "refusing nonempty --out directory: $OUT" >&2
    exit 1
fi

log() { echo "[$(date -u +%FT%TZ)] [drive] $*"; }

read_hugepage_kib() {
    awk '/Hugepagesize:/ {print $2}' /proc/meminfo
}

ceil_div() {
    local n="$1" d="$2"
    echo $(( (n + d - 1) / d ))
}

capture_runtime_artifacts() {
    if [[ "$RUNTIME_OWNED" != 1 || -z "${OUT:-}" || ! -d "$RUNTIME_DIR" ]]; then
        return 0
    fi
    local artifact_dir="$OUT/runtime_artifacts"
    mkdir -p "$artifact_dir"
    for f in target_A.log target_B.log target_C.log target_ready.json process_registry.json; do
        if [[ -f "$RUNTIME_DIR/$f" ]]; then
            cp "$RUNTIME_DIR/$f" "$artifact_dir/$f" 2>/dev/null || return 1
        fi
    done
}

cleanup() {
    local rc=$?
    local cleanup_rc=0
    set +e
    capture_runtime_artifacts || cleanup_rc=1
    if [[ "$RUNTIME_OWNED" == 1 && -n "${REGISTRY:-}" && -f "$REGISTRY" ]]; then
        log "stopping recorded processes"
        PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}" python3 -B - "$REGISTRY" <<'PY'
import sys
from runtime_common import stop_recorded
stop_recorded(sys.argv[1])
PY
        if [[ $? -ne 0 ]]; then
            cleanup_rc=1
        fi
    fi
    if [[ "$RUNTIME_OWNED" == 1 && "$RUNTIME_DIR" == /var/tmp/sapsbs_* ]]; then
        rm -f "$RUNTIME_DIR"/target_*.sock "$RUNTIME_DIR"/*.lock 2>/dev/null || true
        rmdir "$RUNTIME_DIR" 2>/dev/null || true
    fi
    if [[ -n "$HP_BEFORE" ]]; then
        log "restoring nr_hugepages=$HP_BEFORE"
        echo "$HP_BEFORE" > /proc/sys/vm/nr_hugepages || cleanup_rc=1
        if [[ "$(cat /proc/sys/vm/nr_hugepages)" != "$HP_BEFORE" ]]; then
            log "ERROR: hugepage count did not return to $HP_BEFORE"
            cleanup_rc=1
        fi
    fi
    flock -u "$LOCK_FD" 2>/dev/null || true
    if (( rc == 0 && cleanup_rc != 0 )); then
        exit "$cleanup_rc"
    fi
    exit "$rc"
}

exec 9>"$LOCK_FILE"
if ! flock -n "$LOCK_FD"; then
    echo "another saps baseline job holds $LOCK_FILE" >&2
    exit 1
fi
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

HP_BEFORE="$(cat /proc/sys/vm/nr_hugepages)"
HP_SIZE_KIB="$(read_hugepage_kib)"
NEEDED_MIB=$((3 * 1536 + 4 * 384 + 256))
NEEDED_PAGES="$(ceil_div "$((NEEDED_MIB * 1024))" "$HP_SIZE_KIB")"

RUNTIME_DIR="/var/tmp/sapsbs_$$"
if [[ ${#RUNTIME_DIR} -gt 80 ]]; then
    echo "runtime dir unexpectedly long: $RUNTIME_DIR" >&2
    exit 1
fi
if ! mkdir "$RUNTIME_DIR"; then
    echo "refusing existing runtime dir: $RUNTIME_DIR" >&2
    exit 1
fi
RUNTIME_OWNED=1
REGISTRY="$RUNTIME_DIR/process_registry.json"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
exec > >(tee -a "$OUT/driver.log") 2>&1

log "OUT=$OUT"
log "runtime_dir=$RUNTIME_DIR"
log "hugepages before=$HP_BEFORE size=${HP_SIZE_KIB}KiB need_pages=$NEEDED_PAGES"
if (( HP_BEFORE < NEEDED_PAGES )); then
    echo "$NEEDED_PAGES" > /proc/sys/vm/nr_hugepages
fi
HP_NOW="$(cat /proc/sys/vm/nr_hugepages)"
HP_FREE="$(awk '/HugePages_Free:/ {print $2}' /proc/meminfo)"
if (( HP_NOW < NEEDED_PAGES || HP_FREE < NEEDED_PAGES )); then
    echo "insufficient hugepages: total=$HP_NOW free=$HP_FREE required=$NEEDED_PAGES" >&2
    exit 1
fi

{
    echo "date_utc=$(date -u +%FT%TZ)"
    echo "host=$(hostname)"
    echo "spdk_root=$SPDK_ROOT"
    echo "runtime_dir=$RUNTIME_DIR"
    echo "experiment=$EXPERIMENT mode=$CAMPAIGN"
    echo "reference_capacity_iops=${REFERENCE:-campaign-default}"
    echo "path_capacities_iops=$PATH_CAPS"
    echo "service_limit_iops=$SERVICE_LIMIT"
    echo "hugepages_before=$HP_BEFORE hugepage_size_kib=$HP_SIZE_KIB reserved_pages=$HP_NOW"
    echo "topology=target 10.0.0.2 mlx5_0; nvmf_tgt paths A/B/C ports 4430/4431/4432 cores 40-45"
} > "$OUT/driver_provenance.txt"

log "starting 3 local nvmf_tgt targets"
SPDK_ROOT="$SPDK_ROOT" python3 -B "$SCRIPT_DIR/setup_targets.py" \
    --runtime-dir "$RUNTIME_DIR" \
    --registry "$REGISTRY" \
    --path-capacities-iops "$PATH_CAPS"

run_harness() {
    local campaign="$1"
    local out_dir="$2"
    local cmd=(
        python3 -B "$HARNESS"
        --campaign "$campaign"
        --experiment "$EXPERIMENT"
        --out "$out_dir"
        --runtime-dir "$RUNTIME_DIR"
        --registry "$REGISTRY"
        --path-capacities-iops "$PATH_CAPS"
        --service-limit-iops "$SERVICE_LIMIT"
        --fixed-threshold-us "$FIXED_THRESHOLD_US"
        --poll-interval-s "$POLL_INTERVAL_S"
    )
    if [[ -n "$REFERENCE" ]]; then
        cmd+=(--reference-capacity-iops "$REFERENCE")
    fi
    if [[ -n "$HOST_PROOF_PATTERN" ]]; then
        cmd+=(--host-proof-pattern "$HOST_PROOF_PATTERN")
    fi
    if [[ -n "$COUNTER_HZ" ]]; then
        cmd+=(--counter-hz "$COUNTER_HZ")
    fi
    log "running harness campaign=$campaign out=$out_dir"
    SPDK_ROOT="$SPDK_ROOT" "${cmd[@]}"
}

if [[ "$CAMPAIGN" == "smoke" ]]; then
    run_harness smoke "$OUT/smoke"
else
    run_harness full "$OUT/full"
fi
log "complete"
