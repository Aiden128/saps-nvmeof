#!/usr/bin/env python3
"""
run_saps_q_e1_coordinator.py — SAPS static fairness via coordinator/tenant mode

Root cause of standalone-mode failure (Lane B/H/I/J):
  每個 bdevperf 進程各自持有獨立的 g_ctx.ring (posix_memalign),
  SAPSQ_* 2D fields 變成 4 份 disjoint copy → multi-tenant 設計完全失效。

Option X 修正路徑 (此 script 實作):
  - tenant 0: DPA_PLUGIN_ROLE=coordinator — 執行完整 dpa_plugin_init()
    + alloc_ring_memfd() + UDS server + DPA FlexIO scheduler
  - tenant 1/2/3: DPA_PLUGIN_ROLE=tenant — uds_client_get_memfd() attach 同一 memfd
    + SAPSQ_MY_TENANT_ID={1,2,3} → g_sapsq_local_tenant_id (per-process local)
  共享同一 ring → 4 份 SAPSQ_* 2D fields 真正 cross-process shared state

E4 per-process local tenant id 修正 (Lane K, dpa_plugin.c):
  tenant 進程 attach 後讀 SAPSQ_MY_TENANT_ID env → g_sapsq_local_tenant_id,
  admission_check() 優先用此值而非 ring->sapsq_my_tenant_id (coordinator 設的 tid=0)。

PASS criteria:
  - per-tenant IOPS ≈ [100K, 33K, 33K, 33K] ± 15%
  - weighted Jain ≥ 0.95
  - total ≈ 200K
  - io_failed = 0
  - sapsq_epoch_commit_seq > 0 (DPA scheduler 有 publish)
  - sapsq_tenant_path_admit_count[t][p] 4×3=12 cells 全 non-zero
  - sapsq_stale_epoch_fallback / total_event < 10%
"""

import argparse
import json
import math
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# ── 常數 ──────────────────────────────────────────────────────────────────────

BDEVPERF = "/home/aiden/spdk/build/examples/bdevperf"
RPCPY = "/home/aiden/spdk/scripts/rpc.py"
BDEVPERF_PY = "/home/aiden/spdk/examples/bdev/bdevperf/bdevperf.py"

NQN_PREFIX = "nqn.2024-01.io.spdk:tenant"
TARGET_RPC = "/home/aiden/spdk/scripts/rpc.py"
DEFAULT_TARGET_RPC_SOCKET = "/var/tmp/spdk_saps_shared_ns.sock"
TARGET_IP = "10.0.1.1"

# 每 tenant 有 3 個 path: port = TENANT_BASE_PORT + tenant_id + PATH_PORT_OFFSETS[path_idx]
# 新方案 (setup_arm1_sapsq_Nt3p.sh): base 4500/4600/4700, stride 100 between paths.
# N=16: A 4500-4515, B 4600-4615, C 4700-4715 — 無重疊。
# 舊 4t3p 腳本用 4430/4440/4450 + stride 10；已改用新腳本，此處同步。
TENANT_BASE_PORT = 4500
PATH_PORT_OFFSETS = [0, 100, 200]  # path A/B/C (stride 100, N≤99 safe)

SETUP_ARM1_DEFAULT = (
    "/home/aiden/DPA/nvme-of-controller/experiments/"
    "3path_targets/setup_arm1_sapsq_Nt3p.sh"
)
TGT_SOCK = "/var/tmp/spdk_tenants.sock"

BASE_CORE = 4
CORES_PER_PROC = 2
SOCK_TMPL = "/var/tmp/bdevperf_sapsq_proc{i}.sock"

# UDS socket path — coordinator 監聽,tenant 連接
DPA_PLUGIN_SOCK = "/tmp/dpa_plugin_e1.sock"
DPA_PLUGIN_RING_SIZE = 1 << 16
SAPSQ_DUMP = Path(__file__).resolve().parent / "sapsq_dump"
HEALTH_COUPLING_MODE_IDS = {
    "continuous": 0,
    "fixed": 1,
    "binary": 2,
}


# ── 工具函數 ─────────────────────────────────────────────────────────────────

def ts_str():
    return time.strftime("%H:%M:%S")


def log(msg, file=None):
    line = f"[{ts_str()}] [e1_coord] {msg}"
    print(line, flush=True)
    if file:
        print(line, file=file, flush=True)


def find_coordinator_memfd() -> tuple[int, str]:
    marker = SOCK_TMPL.format(i=0)
    result = subprocess.run(
        ["sudo", "pgrep", "-f", marker],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    pids = sorted(
        {int(token) for token in result.stdout.split() if token.isdigit()}
    )
    for pid in pids:
        found = subprocess.run(
            [
                "sudo", "find", f"/proc/{pid}/fd", "-maxdepth", "1",
                "-type", "l", "-lname", "*dpa_plugin_ring*", "-print",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        paths = [
            line.strip() for line in found.stdout.splitlines() if line.strip()
        ]
        if paths:
            return pid, paths[0]
    raise RuntimeError("coordinator dpa_plugin_ring memfd not found")


def capture_live_snapshot(path: Path, elapsed_ms: int) -> dict:
    pid, fd_path = find_coordinator_memfd()
    result = subprocess.run(
        ["sudo", str(SAPSQ_DUMP), fd_path],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"sapsq_dump rc={result.returncode}: "
            f"{result.stderr.strip()[:200]}"
        )
    payload = json.loads(result.stdout)
    payload["_meta"] = {
        "elapsed_ms": elapsed_ms,
        "captured_unix_s": time.time(),
        "source_pid": pid,
        "source_fd": fd_path,
    }
    path.write_text(json.dumps(payload, indent=2))
    return payload


class LiveSnapshotter:
    def __init__(self, directory: Path, interval_ms: int):
        self.directory = directory
        self.interval_s = interval_ms / 1000.0
        self.started = 0.0
        self.stop_event = threading.Event()
        self.thread = None
        self.samples = []
        self.errors = []

    def start(self):
        self.directory.mkdir(parents=True, exist_ok=False)
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        index = 0
        while not self.stop_event.is_set():
            elapsed_ms = int((time.monotonic() - self.started) * 1000)
            try:
                sample = capture_live_snapshot(
                    self.directory / f"snap_{elapsed_ms:06d}ms.json",
                    elapsed_ms,
                )
                self.samples.append(sample)
            except Exception as error:
                self.errors.append(
                    {"elapsed_ms": elapsed_ms, "error": str(error)}
                )
            index += 1
            self.stop_event.wait(self.interval_s)

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(10.0, 2 * self.interval_s))


def capture_tenant_iostat(proc, elapsed_s: float) -> dict:
    result = subprocess.run(
        [
            "sudo", RPCPY, "-s", proc.sock,
            "bdev_get_iostat", "-b", "mpn1",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"tenant {proc.tid} bdev_get_iostat rc={result.returncode}: "
            f"{result.stderr.strip()[:200]}"
        )
    payload = json.loads(result.stdout)
    bdevs = payload.get("bdevs") or []
    stat = next(
        (item for item in bdevs if item.get("name") == "mpn1"),
        None,
    )
    if stat is None:
        raise RuntimeError(f"tenant {proc.tid} iostat lacks mpn1")
    return {
        "tenant_id": proc.tid,
        "elapsed_s": elapsed_s,
        "captured_unix_s": time.time(),
        "num_read_ops": int(stat.get("num_read_ops", 0)),
        "bytes_read": int(stat.get("bytes_read", 0)),
    }


class TenantIostatSnapshotter:
    """Collect delivered I/O independently of allocator state."""

    def __init__(self, procs: list, directory: Path, interval_ms: int):
        self.procs = procs
        self.directory = directory
        self.interval_s = interval_ms / 1000.0
        self.started = 0.0
        self.stop_event = threading.Event()
        self.thread = None
        self.samples = []
        self.errors = []

    def start(self):
        self.directory.mkdir(parents=True, exist_ok=False)
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        round_index = 0
        while not self.stop_event.is_set():
            observations = []
            for proc in self.procs:
                elapsed_s = time.monotonic() - self.started
                try:
                    observations.append(
                        capture_tenant_iostat(proc, elapsed_s)
                    )
                except Exception as error:
                    self.errors.append(
                        {
                            "tenant_id": proc.tid,
                            "elapsed_s": elapsed_s,
                            "error": str(error),
                        }
                    )
            if observations:
                sample = {
                    "round": round_index,
                    "observations": observations,
                }
                self.samples.append(sample)
                (self.directory / f"round_{round_index:04d}.json").write_text(
                    json.dumps(sample, indent=2) + "\n"
                )
            round_index += 1
            self.stop_event.wait(self.interval_s)

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(10.0, 2 * self.interval_s))


def analyze_fault_window_iostat(samples: list, errors: list, args,
                                weights: list[int]) -> dict:
    """Compute delivered service over the stable portion of the fault."""
    if args.fault_path is None or not args.fault_clear_s:
        return {
            "pass": False,
            "errors": errors,
            "reason": "fault-window iostat requires a bounded fault interval",
        }

    settle_s = max(2.0, 2.0 * args.iostat_interval_ms / 1000.0)
    start_s = args.fault_onset_s + settle_s
    end_s = args.fault_clear_s - 1.0
    if end_s <= start_s:
        return {
            "pass": False,
            "errors": errors,
            "reason": "fault interval is too short for steady-state iostat",
        }

    by_tenant = {tenant: [] for tenant in range(args.num_tenants)}
    for sample in samples:
        for observation in sample.get("observations", []):
            tenant = int(observation["tenant_id"])
            elapsed_s = float(observation["elapsed_s"])
            if tenant in by_tenant and start_s <= elapsed_s <= end_s:
                by_tenant[tenant].append(observation)

    per_tenant = []
    sum_w = sum(weights[:args.num_tenants])
    for tenant in range(args.num_tenants):
        observations = sorted(
            by_tenant[tenant], key=lambda item: float(item["elapsed_s"])
        )
        if len(observations) < 2:
            per_tenant.append(
                {
                    "tenant_id": tenant,
                    "pass": False,
                    "samples": len(observations),
                }
            )
            continue
        first = observations[0]
        last = observations[-1]
        elapsed = float(last["elapsed_s"]) - float(first["elapsed_s"])
        reads = int(last["num_read_ops"]) - int(first["num_read_ops"])
        iops = reads / elapsed if elapsed > 0 and reads >= 0 else 0.0
        entitlement = args.link_cap * weights[tenant] / sum_w
        per_tenant.append(
            {
                "tenant_id": tenant,
                "pass": elapsed > 0 and reads >= 0 and iops > 0,
                "samples": len(observations),
                "first_elapsed_s": float(first["elapsed_s"]),
                "last_elapsed_s": float(last["elapsed_s"]),
                "read_ops": reads,
                "iops": iops,
                "configured_entitlement_iops": entitlement,
                "entitlement_delivered": (
                    iops / entitlement if entitlement > 0 else 0.0
                ),
            }
        )

    valid = [item for item in per_tenant if item.get("pass")]
    return {
        "window_start_s": start_s,
        "window_end_s": end_s,
        "errors": errors,
        "per_tenant": per_tenant,
        "total_iops": sum(float(item["iops"]) for item in valid),
        "minimum_entitlement_delivered": min(
            (float(item["entitlement_delivered"]) for item in valid),
            default=0.0,
        ),
        "pass": len(valid) == args.num_tenants and not errors,
    }


def analyze_live_evidence(samples: list, errors: list, args) -> dict:
    valid = [
        sample for sample in samples
        if len(sample.get("path_health_factor_q16") or []) >= args.num_paths
    ]
    if not valid:
        return {
            "pass": False,
            "errors": errors,
            "reason": "no valid live snapshots",
        }

    last = valid[-1]
    host_counts = last.get("host_submit_published_by_tenant") or []
    dpa_counts = last.get("dpa_submit_consumed_by_tenant") or []
    attribution = []
    for tenant in range(args.num_tenants):
        host = host_counts[tenant] if tenant < len(host_counts) else 0
        dpa = dpa_counts[tenant] if tenant < len(dpa_counts) else 0
        attribution.append(
            {
                "tenant": tenant,
                "host_published": host,
                "dpa_consumed": dpa,
                "consumed_fraction": dpa / host if host else 0.0,
                "pass": host > 0 and dpa > 0 and dpa >= 0.9 * host,
            }
        )
    attribution_ok = all(item["pass"] for item in attribution)

    demand_seen = []
    for tenant in range(args.num_tenants):
        peak = max(
            (
                (sample.get("demand_iops") or [])[tenant]
                for sample in valid
                if tenant < len(sample.get("demand_iops") or [])
            ),
            default=0,
        )
        demand_seen.append({"tenant": tenant, "peak_iops": peak, "pass": peak > 0})
    demand_ok = all(item["pass"] for item in demand_seen)

    healthy_last = all(
        value >= int(0.95 * 65536)
        for value in last["path_health_factor_q16"][:args.num_paths]
    )
    path_response = None
    response_ok = healthy_last
    if args.fault_path is not None:
        fault_samples = [
            sample for sample in valid
            if args.fault_onset_s + 0.5
            <= sample["_meta"]["elapsed_ms"] / 1000.0
            <= args.fault_clear_s
        ]
        recovery_samples = [
            sample for sample in valid
            if sample["_meta"]["elapsed_ms"] / 1000.0
            >= args.fault_clear_s + 1.0
        ]
        nonhealthy = [
            sample for sample in fault_samples
            if sample["path_health_factor_q16"][args.fault_path]
            < int(0.95 * 65536)
        ]
        recovered = [
            sample for sample in recovery_samples
            if all(
                value >= int(0.95 * 65536)
                for value in sample["path_health_factor_q16"][:args.num_paths]
            )
        ]
        recovered_live = [
            sample for sample in recovered
            if len(sample.get("demand_iops") or []) >= args.num_tenants
            and all(
                value > 0
                for value in sample["demand_iops"][:args.num_tenants]
            )
        ]
        final_recovery = recovered_live[-1] if recovered_live else None
        budget_restored = final_recovery is not None
        restored_budgets = []
        if final_recovery is not None:
            rows = final_recovery.get("tenant_path_rate_budget_q32") or []
            for tenant in range(args.num_tenants):
                row = rows[tenant][:args.num_paths] if tenant < len(rows) else []
                positive = [value for value in row if value > 0]
                balanced = (
                    len(positive) == args.num_paths
                    and max(positive) - min(positive)
                    <= max(1, int(max(positive) * 0.05))
                )
                restored_budgets.append(
                    {"tenant": tenant, "budgets_q32": row, "pass": balanced}
                )
                budget_restored &= balanced
        response_ok = (
            bool(nonhealthy)
            and len(recovered_live) >= 2
            and budget_restored
        )
        path_response = {
            "detected": bool(nonhealthy),
            "nonhealthy_samples": len(nonhealthy),
            "recovered_samples": len(recovered_live),
            "budget_restored": budget_restored,
            "restored_budgets": restored_budgets,
            "pass": response_ok,
        }

    return {
        "samples": len(valid),
        "errors": errors,
        "attribution": attribution,
        "demand_seen": demand_seen,
        "healthy_at_end": healthy_last,
        "path_response": path_response,
        "pass": (
            not errors
            and attribution_ok
            and demand_ok
            and response_ok
        ),
    }


def detect_tsc_hz() -> int:
    """arm64 TSC 頻率偵測:讀 /proc/cpuinfo 的 CPU MHz。BF3 fallback 1 GHz。"""
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("CPU MHz") or line.startswith("cpu MHz"):
                    mhz = float(line.split(":")[1].strip())
                    return int(mhz * 1_000_000)
    except Exception:
        pass
    return 1_000_000_000


def cpu_mask(proc_idx: int) -> str:
    start = BASE_CORE + proc_idx * CORES_PER_PROC
    end = start + CORES_PER_PROC - 1
    mask = 0
    for c in range(start, end + 1):
        mask |= (1 << c)
    return hex(mask)


def taskset_cpus(proc_idx: int) -> str:
    start = BASE_CORE + proc_idx * CORES_PER_PROC
    return f"{start}-{start + CORES_PER_PROC - 1}"


def csv_str(xs) -> str:
    return ",".join(str(x) for x in xs)


def listener_delay_rpc_command(args, port: int, delay_us: int) -> list[str]:
    socket_path = target_rpc_socket_for_fault(args)
    return [
        "ssh",
        args.target_rpc_host,
        "sudo",
        TARGET_RPC,
        "-s",
        socket_path,
        "nvmf_rdma_saps_set_delay",
        str(port),
        str(delay_us),
    ]


def listener_delay_get_command(args) -> list[str]:
    socket_path = target_rpc_socket_for_fault(args)
    return [
        "ssh",
        args.target_rpc_host,
        "sudo",
        TARGET_RPC,
        "-s",
        socket_path,
        "nvmf_rdma_saps_get_delay",
    ]


def target_rpc_socket_for_fault(args) -> str:
    template = getattr(args, "target_rpc_socket_template", None)
    if not template:
        return args.target_rpc_socket
    if args.fault_path is None:
        raise ValueError("a path-indexed target socket requires --fault-path")
    try:
        return template.format(path=args.fault_path)
    except (IndexError, KeyError, ValueError) as error:
        raise ValueError(
            "target RPC socket template must contain a valid {path} field"
        ) from error


def _run_target_delay_rpc(command: list[str]) -> dict:
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"target delay RPC failed: {detail}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"target delay RPC returned invalid JSON: {result.stdout!r}"
        ) from error


def set_listener_delay(args, port: int, delay_us: int, logfile) -> dict:
    state = _run_target_delay_rpc(
        listener_delay_rpc_command(args, port, delay_us)
    )
    if logfile is not None:
        log(
            f"[TARGET_DELAY] port={port} delay_us={delay_us} "
            f"generation={state.get('generation')}",
            logfile,
        )
    return state


def get_listener_delay(args) -> dict:
    return _run_target_delay_rpc(listener_delay_get_command(args))


def listener_delay_evidence(start: dict | None, end: dict | None) -> dict:
    if start is None or end is None:
        return {
            "delayed_requests": None,
            "expired_requests": None,
            "reconfigured_requests": None,
            "disconnected_requests": None,
            "pending_requests": None,
            "pass": False,
        }

    delayed = end.get("delayed_requests", 0)
    expired = end.get("expired_requests", 0)
    reconfigured = end.get("reconfigured_requests", 0)
    disconnected = end.get("disconnected_requests", 0)
    pending = end.get("pending_requests", 0)
    generation_advanced = end.get("generation", 0) > start.get("generation", 0)
    return {
        "delayed_requests": delayed,
        "expired_requests": expired,
        "reconfigured_requests": reconfigured,
        "disconnected_requests": disconnected,
        "pending_requests": pending,
        "pass": (
            generation_advanced
            and delayed > 0
            and expired > 0
            and disconnected == 0
            and pending == 0
        ),
    }


def validate_listener_delay_args(args) -> None:
    if args.fault_path is None:
        return
    if not args.shared_nqn:
        raise ValueError(
            "listener-scoped delay requires a shared namespace topology"
        )
    if args.fault_path < 0 or args.fault_path >= args.num_paths:
        raise ValueError(
            f"fault path must be in 0..{args.num_paths - 1}"
        )
    if args.port_base + args.fault_path > 65535:
        raise ValueError("fault listener port exceeds 65535")
    template = getattr(args, "target_rpc_socket_template", None)
    if template and "{path}" not in template:
        raise ValueError(
            "target RPC socket template must contain the {path} field"
        )
    if args.fault_delay_us < 1 or args.fault_delay_us > 1_000_000:
        raise ValueError("fault delay must be in 1..1000000 microseconds")
    if args.fault_onset_s < 0 or args.fault_onset_s >= args.duration:
        raise ValueError("fault onset must occur within the workload")
    if args.fault_clear_s:
        if args.fault_clear_s <= args.fault_onset_s:
            raise ValueError("fault clear time must follow fault onset")
        if args.fault_clear_s > args.duration:
            raise ValueError("fault clear time must not exceed the workload")


def run_listener_delay_schedule(
    args,
    logfile,
    sleep_fn=time.sleep,
    set_delay_fn=set_listener_delay,
) -> None:
    port = args.port_base + args.fault_path
    sleep_fn(args.fault_onset_s)
    set_delay_fn(args, port, args.fault_delay_us, logfile)
    if args.fault_clear_s:
        sleep_fn(args.fault_clear_s - args.fault_onset_s)
        set_delay_fn(args, port, 0, logfile)


def schedule_listener_delay(args, logfile):
    if args.fault_path is None:
        return None, []
    errors = []

    def _run():
        try:
            run_listener_delay_schedule(args, logfile)
        except Exception as error:
            errors.append(str(error))
            log(f"[TARGET_DELAY] ERROR: {error}", logfile)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread, errors


# ── env var 組裝 ─────────────────────────────────────────────────────────────

def build_env_coordinator(tenant_id: int, n_tenants: int, n_paths: int,
                           link_cap: int, weights: list, tsc_hz: int,
                           bypass_d: str, path_caps: list[int],
                           sample_rate: int,
                           bypass_saps_fsm: str = "1",
                           health_coupling_mode: str = "continuous") -> list:
    """
    Coordinator (tenant 0) env:
      - DPA_PLUGIN_ROLE=coordinator → dpa_plugin_init() 完整 path
        (ibv_open_device + flexio_process_create + alloc_ring_memfd + UDS server)
      - SAPSQ_ENABLED=1 + 完整 SAPSQ_* config → sapsq_m_init() 寫入 ring
      - DPA_PLUGIN_SOCK=DPA_PLUGIN_SOCK → UDS 監聽路徑 (tenant 連這裡)
      - SAPS_Q_ENABLED=0 → 關掉舊 1D sapsq_epoch_commit path
      - HOST_SAPS_ENABLED=0 → 不能設 1,否則 host_saps.c 會 skip dpa_plugin_init()
    """
    return [
        f"DPA_PLUGIN_ROLE=coordinator",
        f"DPA_PLUGIN_SOCK={DPA_PLUGIN_SOCK}",
        "DPA_PLUGIN_DISABLE_INIT=0",
        # Only the coordinator opens the ibv device. Default mlx5_0 (mlx5_1 port
        # is DOWN on this testbed); honour an explicit env override if present.
        f"DPA_PLUGIN_DEV={os.environ.get('DPA_PLUGIN_DEV', 'mlx5_0')}",
        "HOST_SAPS_ENABLED=0",
        f"DPA_PLUGIN_SAMPLE_RATE={sample_rate}",
        "SAPS_TRACE=0",
        # SAPS-Q M-series 2D path (新)
        "SAPSQ_ENABLED=1",
        f"SAPSQ_MY_TENANT_ID={tenant_id}",
        f"SAPSQ_NUM_TENANTS={n_tenants}",
        f"SAPSQ_NUM_PATHS={n_paths}",
        f"SAPSQ_LINK_CAP_IOPS={link_cap}",
        f"SAPSQ_PATH_CAP_IOPS={csv_str(path_caps)}",
        "SAPSQ_EPOCH_PERIOD_US=1000",
        f"SAPSQ_WEIGHTS={csv_str(weights)}",
        # probe_rate: must be << min per-path rate_budget for any tenant.
        # With demand floor=link_cap and weight 3:1:1:1, weakest tenant
        # (weight=1) gets link_cap/6/num_paths per path ≈ 11K IO/s
        # → rate_budget_q32 ≈ 47K.  Setting probe to 1000 IO/s keeps
        # probe_q32 ≈ 4300 well below any real budget so sub-probe bypass
        # does not fire once DPA publishes valid (floor-boosted) rates.
        "SAPSQ_PROBE_RATE_IOPS=1000",
        f"SAPSQ_HOST_TSC_FREQ={tsc_hz}",
        f"SAPSQ_BYPASS_D_CLASSIFIER={bypass_d}",
        f"SAPSQ_BYPASS_SAPS_FSM={bypass_saps_fsm}",
        f"SAPSQ_HEALTH_COUPLING_MODE={health_coupling_mode}",
        # DPA_PLUGIN_ADMISSION intentionally omitted (default 0):
        # SAPSQ 2D admission (line 2826 in dpa_plugin.c) runs BEFORE the
        # g_admission_enabled gate (line 3186) and is independent of E3 CUSUM.
        # Enabling ADMISSION=1 triggers E3 per-QP CUSUM which fires on
        # cold-start high-latency, blocking all IO before DPA can see any
        # events — a deadlock. SAPSQ 2D handles E1 rate limiting on its own.
        # 舊 SAPS_Q_* 1D path 關閉
        "SAPS_Q_ENABLED=0",
        # 其他路徑關閉
        "SAPS_M4_ENABLED=0",
        "SAPS_M5_DRR_ENABLED=0",
    ]


def build_env_tenant(tenant_id: int, n_tenants: int, n_paths: int,
                      link_cap: int, weights: list, tsc_hz: int,
                      bypass_d: str, sample_rate: int,
                      health_coupling_mode: str = "continuous") -> list:
    """
    Tenant (1/2/3) env:
      - DPA_PLUGIN_ROLE=tenant → uds_client_get_memfd() attach coordinator ring
        不開 ibv device,不建 DPA process — 純 producer on shared ring
      - SAPSQ_MY_TENANT_ID={1,2,3} → g_sapsq_local_tenant_id (per-process local)
        避免所有 tenant 讀 ring->sapsq_my_tenant_id (coordinator 設的 tid=0)
      - SAPSQ_ENABLED=1 → admission_check 走 2D path (用 g_sapsq_local_tenant_id)
      - SAPS_Q_ENABLED=0 → 關掉舊 1D path
      - HOST_SAPS_ENABLED=0 → 不能設 1,否則 host_saps.c skip dpa_plugin_init()
    """
    return [
        f"DPA_PLUGIN_ROLE=tenant",
        f"DPA_PLUGIN_SOCK={DPA_PLUGIN_SOCK}",
        "DPA_PLUGIN_DISABLE_INIT=0",
        # Tenants do not open the ibv device (they attach the coordinator ring
        # over UDS), but keep the device selection consistent for any codepath
        # that reads it. Default mlx5_0; honour an explicit env override.
        f"DPA_PLUGIN_DEV={os.environ.get('DPA_PLUGIN_DEV', 'mlx5_0')}",
        "HOST_SAPS_ENABLED=0",
        f"DPA_PLUGIN_SAMPLE_RATE={sample_rate}",
        "SAPS_TRACE=0",
        # SAPS-Q M-series — tenant 不呼叫 sapsq_m_init(),只 attach ring
        # SAPSQ_ENABLED=1 確保 admission_check 走 sapsq path
        "SAPSQ_ENABLED=1",
        f"SAPSQ_MY_TENANT_ID={tenant_id}",
        # tenant 不需要 SAPSQ_NUM_TENANTS / SAPSQ_LINK_CAP_IOPS 等 (init 不執行)
        # 但 SAPSQ_BYPASS_D_CLASSIFIER 傳給 ring 是 coordinator 的 SAPSQ_BYPASS_D_CLASSIFIER
        # 此處設也無妨 (tenant path 不跑 sapsq_m_init)
        f"SAPSQ_BYPASS_D_CLASSIFIER={bypass_d}",
        f"SAPSQ_HEALTH_COUPLING_MODE={health_coupling_mode}",
        # DPA_PLUGIN_ADMISSION intentionally omitted (see coordinator comment above)
        # 舊 1D path 關閉
        "SAPS_Q_ENABLED=0",
        "SAPS_M4_ENABLED=0",
        "SAPS_M5_DRR_ENABLED=0",
    ]


def enable_recovery_probe(environment: list, bypass_saps_fsm: str) -> list:
    """Enable bounded path probes whenever the path-state estimator is active."""
    if bypass_saps_fsm == "0":
        environment.append("SAPS_ACTIVE_PROBE=1")
    return environment


def parse_path_response_trace(content: str, path_id: int) -> dict:
    """Parse the shutdown trace for one path."""
    prefix = rf"D4_TRACE path={path_id} "
    state_match = re.search(
        prefix + r"state_count H=(\d+) D=(\d+) E=(\d+) R=(\d+)",
        content,
    )
    fault_match = re.search(prefix + r"fault_count ([^\n]+)", content)
    transitions = {}
    for match in re.finditer(
        prefix + r"transitions from=(\d+) to H=(\d+) D=(\d+) E=(\d+) R=(\d+)",
        content,
    ):
        transitions[match.group(1)] = {
            "H": int(match.group(2)),
            "D": int(match.group(3)),
            "E": int(match.group(4)),
            "R": int(match.group(5)),
        }

    states = None
    if state_match:
        states = {
            "H": int(state_match.group(1)),
            "D": int(state_match.group(2)),
            "E": int(state_match.group(3)),
            "R": int(state_match.group(4)),
        }

    faults = {}
    if fault_match:
        faults = {
            match.group(1): int(match.group(2))
            for match in re.finditer(r"(\d+)=(\d+)", fault_match.group(1))
        }

    return {
        "states": states,
        "faults": faults,
        "transitions": transitions,
    }


def assess_path_response(trace: dict, rate_budgets: dict, fault_path: int,
                         num_tenants: int) -> dict:
    """Require detection and a restored final allocation after fault removal."""
    states = trace.get("states") or {}
    faults = trace.get("faults") or {}
    transitions = trace.get("transitions") or {}
    nonhealthy_samples = sum(
        count for fault, count in faults.items() if fault != "0"
    )
    hard_recovery = (
        states.get("E", 0) > 0
        and states.get("R", 0) > 0
        and transitions.get("3", {}).get("H", 0) > 0
    )
    proportional_recovery = faults.get("9", 0) > 0

    tenant_budgets = {}
    budget_restored = True
    for tenant_id in range(num_tenants):
        values = []
        for key, value in rate_budgets.items():
            match = re.fullmatch(rf"t{tenant_id}_p(\d+)", key)
            if match:
                values.append((int(match.group(1)), int(value)))
        values.sort()
        tenant_budgets[f"t{tenant_id}"] = {
            f"p{path_id}": value for path_id, value in values
        }
        if not values or not any(path_id == fault_path for path_id, _ in values):
            budget_restored = False
            continue
        positive = [value for _, value in values if value > 0]
        if not positive:
            budget_restored = False
            continue
        high = max(positive)
        low = min(positive)
        if high - low > max(1, int(high * 0.05)):
            budget_restored = False

    detected = nonhealthy_samples > 0
    recovery_mode = None
    if hard_recovery:
        recovery_mode = "fsm"
    elif proportional_recovery:
        recovery_mode = "proportional"
    passed = detected and budget_restored and recovery_mode is not None
    return {
        "detected": detected,
        "nonhealthy_samples": nonhealthy_samples,
        "recovery_mode": recovery_mode,
        "hard_recovery": hard_recovery,
        "proportional_recovery": proportional_recovery,
        "budget_restored": budget_restored,
        "final_rate_budget_q32": tenant_budgets,
        "trace": trace,
        "pass": passed,
    }


# ── TenantProc 管理 ──────────────────────────────────────────────────────────

class TenantProc:
    """單一 bdevperf process (coordinator 或 tenant)。"""

    def __init__(self, tenant_id: int, is_coordinator: bool, args, out_dir: Path,
                 logfile, tsc_hz: int):
        self.tid = tenant_id
        self.is_coordinator = is_coordinator
        self.args = args
        self.out_dir = out_dir / f"tenant_{tenant_id:02d}"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.logpath = self.out_dir / "bdevperf.log"
        self.result_json = self.out_dir / "result.json"
        self.sock = SOCK_TMPL.format(i=tenant_id)
        self.proc = None
        self.logfp = None
        self.orch_log = logfile
        self._mask = cpu_mask(tenant_id)
        self._taskset_cpus = taskset_cpus(tenant_id)
        if args.shared_nqn:
            self._nqn = args.shared_nqn
            self._base_port = args.port_base
            self._path_offsets = list(range(args.num_paths))
        else:
            self._nqn = f"{NQN_PREFIX}{tenant_id}"
            self._base_port = TENANT_BASE_PORT + tenant_id
            self._path_offsets = PATH_PORT_OFFSETS
        self._tsc_hz = tsc_hz

    def _log(self, msg):
        log(f"[t{self.tid:02d}{'(coord)' if self.is_coordinator else ''}] {msg}",
            self.orch_log)

    def env_list(self) -> list:
        weights = [int(w) for w in self.args.weights.split(",")]
        bypass_d = "1" if self.args.bypass_d else "0"
        bypass_saps_fsm = (
            "1" if getattr(self.args, "bypass_fsm", True) else "0"
        )
        if self.is_coordinator:
            env = build_env_coordinator(
                self.tid, self.args.num_tenants, self.args.num_paths,
                self.args.link_cap, weights, self._tsc_hz, bypass_d,
                self.args.path_caps, self.args.sample_rate,
                bypass_saps_fsm,
                getattr(self.args, "health_coupling_mode", "continuous"))
        else:
            env = build_env_tenant(
                self.tid, self.args.num_tenants, self.args.num_paths,
                self.args.link_cap, weights, self._tsc_hz, bypass_d,
                self.args.sample_rate,
                getattr(self.args, "health_coupling_mode", "continuous"))
        enable_recovery_probe(
            env,
            bypass_saps_fsm,
        )
        if self.args.shared_nqn:
            port_map = ",".join(
                f"{self._base_port + offset}:{path_id}"
                for path_id, offset in enumerate(self._path_offsets)
            )
            env.append(f"DPA_PLUGIN_PATH_MAP_PORTS={port_map}")
        return env

    def cmd_list(self) -> list:
        env_vars = self.env_list()
        return (
            ["sudo", "taskset", "-c", self._taskset_cpus]
            + ["env"] + env_vars
            + [BDEVPERF,
               "-m", self._mask,
               "-r", self.sock,
               "--wait-for-rpc",
               "-g", "-s", "384",
               "-q", str(self.args.qd),
               "-o", "4096",
               "-w", "randread",
               "-t", str(self.args.duration),
               "-z", "-l"]
        )

    def attach_cmds(self) -> list:
        """3-path multipath attach commands (path A/B/C)"""
        cmds = []
        for p_idx in range(self.args.num_paths):
            port = self._base_port + self._path_offsets[p_idx]
            cmds.append([
                "sudo", RPCPY, "-s", self.sock,
                "bdev_nvme_attach_controller",
                "-b", "mp", "-t", "rdma", "-a", TARGET_IP,
                "-s", str(port), "-f", "ipv4",
                "-n", self._nqn, "--multipath", "multipath",
            ])
        return cmds

    def start(self):
        subprocess.run(["sudo", "rm", "-f", self.sock], capture_output=True)
        cmd = self.cmd_list()
        role = "coordinator" if self.is_coordinator else "tenant"
        self._log(
            f"launching: role={role} nqn={self._nqn} "
            f"ports={[self._base_port + o for o in self._path_offsets[:self.args.num_paths]]}"
        )
        self._log(f"cmd: {' '.join(cmd[:12])}...")
        self.logfp = open(self.logpath, "w")
        self.proc = subprocess.Popen(cmd, stdout=self.logfp, stderr=self.logfp)
        self._log(f"PID={self.proc.pid}")

    def wait_socket(self, timeout=120) -> bool:
        for _ in range(timeout):
            if os.path.exists(self.sock):
                return True
            time.sleep(1)
        return False

    def setup_controllers(self) -> bool:
        try:
            r = subprocess.run(
                ["sudo", RPCPY, "-s", self.sock,
                 "bdev_nvme_set_options", "--io-path-stat"],
                capture_output=True, text=True, timeout=60
            )
        except subprocess.TimeoutExpired:
            self._log("bdev_nvme_set_options timed out after 60s")
            return False
        if r.returncode != 0:
            self._log(f"bdev_nvme_set_options failed: {r.stderr[:120]}")
            return False

        try:
            r = subprocess.run(
                ["sudo", RPCPY, "-s", self.sock, "framework_start_init"],
                capture_output=True, text=True, timeout=60
            )
        except subprocess.TimeoutExpired:
            self._log("framework_start_init timed out after 60s")
            return False
        if r.returncode != 0:
            self._log(f"framework_start_init failed: {r.stderr[:120]}")
            return False
        time.sleep(1)

        for cmd in self.attach_cmds():
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
            self._log(f"attach: rc={r.returncode} "
                      f"{(r.stdout + r.stderr).strip()[:100]}")
            if r.returncode != 0:
                return False
        time.sleep(2)

        r = subprocess.run(
            ["sudo", RPCPY, "-s", self.sock, "bdev_get_bdevs"],
            capture_output=True, text=True, timeout=30
        )
        try:
            bdevs = json.loads(r.stdout)
            controller_count = max(
                (
                    len(item.get("driver_specific", {}).get("nvme", []))
                    for item in bdevs
                ),
                default=0,
            )
        except (TypeError, ValueError) as error:
            self._log(f"could not parse bdev_get_bdevs: {error}")
            return False
        self._log(f"NVMe controller count={controller_count}")
        if controller_count < self.args.num_paths:
            return False

        cmd = ["sudo", RPCPY, "-s", self.sock,
               "bdev_nvme_set_multipath_policy", "-b", "mpn1",
               "-p", "plugin"]
        try:
            r = subprocess.run(
                cmd, capture_output=True, text=True, timeout=60
            )
        except subprocess.TimeoutExpired:
            self._log("multipath policy=plugin timed out after 60s")
            return False
        self._log(f"multipath policy=plugin: rc={r.returncode}")
        if r.returncode != 0:
            self._log(f"multipath policy stderr={r.stderr.strip()[:200]}")
            return False
        return True

    def run_perform_tests(self):
        cmd = [
            "sudo", "timeout", str(self.args.duration + 30),
            "python3", BDEVPERF_PY,
            "-s", self.sock,
            "-t", str(self.args.duration + 15),
            "perform_tests",
        ]
        return subprocess.Popen(cmd, stdout=self.logfp, stderr=self.logfp)

    def kill(self):
        if self.proc:
            try:
                subprocess.run(["sudo", "kill", "-9", str(self.proc.pid)],
                               capture_output=True)
            except Exception:
                pass
            try:
                self.proc.wait(timeout=5)
            except Exception:
                pass
        if self.logfp:
            try:
                self.logfp.close()
            except Exception:
                pass
        subprocess.run(["sudo", "rm", "-f", self.sock], capture_output=True)

    def parse_result(self) -> dict:
        try:
            content = self.logpath.read_text()
        except Exception:
            content = ""
        iops = None
        io_failed = None
        for idx in range(len(content)):
            if '"results"' not in content[idx:idx + 50]:
                continue
            i = content.find('"results"', idx)
            if i < 0:
                break
            start = content.rfind("{", 0, i)
            if start < 0:
                break
            try:
                j, _ = json.JSONDecoder().raw_decode(content, start)
                if isinstance(j, dict) and "results" in j:
                    for res in j["results"]:
                        v = res.get("iops", 0)
                        if v and float(v) > 0:
                            iops = round(float(v), 0)
                        f = res.get("io_error", 0)
                        io_failed = int(f) if f else 0
                        break
            except Exception:
                pass
            break
        if not iops:
            periodic = [float(x) for x in
                        re.findall(r"\s+([\d.]+)\s+IOPS,", content)]
            if len(periodic) >= 3:
                iops = round(sum(periodic) / len(periodic), 0)
        result = {
            "tenant_id": self.tid,
            "role": "coordinator" if self.is_coordinator else "tenant",
            "iops": iops,
            "io_failed": io_failed,
        }
        self.result_json.write_text(json.dumps(result, indent=2))
        self._log(f"result: IOPS={iops} io_failed={io_failed}")
        return result


# ── sapsq dump (coordinator ring snapshot) ───────────────────────────────────

def read_sapsq_dump(out_dir: Path, procs: list, logfile) -> dict:
    """
    從 coordinator process 的 bdevperf log 抓 SAPS-Q exit stats。
    coordinator 在 dpa_plugin_shutdown() 時印 per-tenant/path admit/reject/rate。
    同時讀 dpa_plugin_ring memfd 透過 /proc/<pid>/fd (若 coordinator 還活著)。
    """
    coord_proc = next((p for p in procs if p.is_coordinator), None)
    if not coord_proc:
        return {}
    try:
        content = coord_proc.logpath.read_text()
    except Exception:
        return {}

    dump = {
        "sapsq_epoch_commit_seq": None,
        "sapsq_stale_epoch_fallback": None,
        "admit_count": {},
        "reject_count": {},
        "rate_budget_q32": {},
        "path_health": {},
        "path_capacity_iops": {},
        "path_effective_capacity_iops": {},
        "demand_iops": {},
        "ring": {},
        "health_coupling_mode": None,
    }

    # 從 coordinator log 解析 SAPS-Q exit stats
    # dpa_plugin.c 在 shutdown 時印:
    #   "dpa_plugin: SAPS-Q exit stats my_tid=N stale_epoch_fallback=X ..."
    m = re.search(r"stale_epoch_fallback=(\d+)", content)
    if m:
        dump["sapsq_stale_epoch_fallback"] = int(m.group(1))

    m = re.search(r"health_coupling_mode=(\d+)", content)
    if m:
        dump["health_coupling_mode"] = int(m.group(1))

    # epoch_commit_seq 從 DPA scheduler tick log (若有)
    m = re.search(r"epoch_commit_seq[=:](\d+)", content)
    if m:
        dump["sapsq_epoch_commit_seq"] = int(m.group(1))

    m = re.search(
        r"SAPS-Q ring producer=(\d+) consumer=(\d+) "
        r"dpa_consumed=(\d+) overrun=(\d+) max_lag=(\d+)",
        content,
    )
    if m:
        dump["ring"] = {
            "producer": int(m.group(1)),
            "consumer": int(m.group(2)),
            "dpa_consumed": int(m.group(3)),
            "overrun": int(m.group(4)),
            "max_lag": int(m.group(5)),
        }

    for m in re.finditer(
        r"SAPS-Q path (\d+) capacity_iops=(\d+) "
        r"health_factor_q16=(\d+) effective_capacity_iops=(\d+)",
        content,
    ):
        path = f"p{int(m.group(1))}"
        dump["path_capacity_iops"][path] = int(m.group(2))
        dump["path_health"][path] = int(m.group(3))
        dump["path_effective_capacity_iops"][path] = int(m.group(4))

    # per-tenant demand_iops from coordinator exit stats (new M-series format):
    # "dpa_plugin: SAPS-Q tenant T weight=W demand_iops=D"
    for m in re.finditer(
        r"SAPS-Q tenant (\d+) weight=(\d+) demand_iops=(\d+)",
        content
    ):
        t = int(m.group(1))
        dump["demand_iops"][f"t{t}"] = int(m.group(3))

    # per-tenant-path rate_budget and admit/reject from coordinator exit stats:
    # "dpa_plugin: SAPS-Q tenant T path P rate_budget_q32=Q admit=A reject=R"
    tenant_admit = {}
    for m in re.finditer(
        r"SAPS-Q tenant (\d+) path (\d+) rate_budget_q32=(\d+) admit=(\d+) reject=(\d+)",
        content
    ):
        t, p = int(m.group(1)), int(m.group(2))
        key = f"t{t}_p{p}"
        tenant_admit[key] = {
            "rate_budget_q32": int(m.group(3)),
            "admit": int(m.group(4)),
            "reject": int(m.group(5)),
        }
        dump["rate_budget_q32"][key] = int(m.group(3))
        dump["admit_count"][key] = int(m.group(4))
        dump["reject_count"][key] = int(m.group(5))

    # Also parse old-format per-path from tenant logs (fallback):
    # "dpa_plugin: SAPS-Q exit path[P] admit=A reject=R probe=B rate_q32_last=Q"
    for proc in procs:
        if proc.is_coordinator:
            continue
        try:
            tc = proc.logpath.read_text()
        except Exception:
            continue
        for m in re.finditer(
            r"SAPS-Q exit path\[(\d+)\] admit=(\d+) reject=(\d+).*?rate_q32_last=(\d+)",
            tc
        ):
            key = f"t{proc.tid}_p{m.group(1)}"
            if key not in tenant_admit:
                tenant_admit[key] = {
                    "admit": int(m.group(2)),
                    "reject": int(m.group(3)),
                    "rate_q32": int(m.group(4)),
                }
    dump["per_tenant_path"] = tenant_admit

    dump_path = out_dir / "coordinator_dump.json"
    dump_path.write_text(json.dumps(dump, indent=2))
    log(f"sapsq dump → {dump_path}", logfile)
    return dump


# ── Jain 指數計算 ─────────────────────────────────────────────────────────────

def weighted_jain(iops_list: list, weights: list) -> float:
    """
    加權 Jain index:
      - 每 tenant 的 share_ratio = iops[t] / (weight[t] / sum_w × link_cap)
      - weighted_jain = (Σ share_ratio)² / (N × Σ share_ratio²)
    """
    n = len(iops_list)
    if n == 0:
        return 0.0
    sum_w = sum(weights[:n])
    if sum_w == 0:
        return 0.0
    link_cap = sum(iops_list)
    if link_cap == 0:
        return 0.0
    shares = []
    for t in range(n):
        desired = (weights[t] / sum_w) * link_cap
        if desired > 0:
            shares.append(iops_list[t] / desired)
        else:
            shares.append(0.0)
    num = sum(shares) ** 2
    den = n * sum(s * s for s in shares)
    return num / den if den > 0 else 0.0


# ── cleanup ───────────────────────────────────────────────────────────────────

def cleanup_all(procs: list):
    for p in procs:
        try:
            p.kill()
        except Exception:
            pass
    subprocess.run(
        ["sudo", "bash", "-c",
         f"pkill -9 -f '{BDEVPERF}' 2>/dev/null; "
         "pkill -9 -f 'reactor_[0-9]' 2>/dev/null; "
         "rm -f /var/tmp/bdevperf_sapsq_proc*.sock; "
         "rm -f /var/tmp/spdk_cpu_lock_*; "
         f"rm -f {DPA_PLUGIN_SOCK}; "
         # Remove stale SPDK hugepage files so hugepages are returned to the
         # kernel pool before the next run.  kill -9 bypasses SPDK shutdown,
         # leaving /dev/hugepages/spdk_pid*map_* files that pin the pages.
         "rm -f /dev/hugepages/spdk_pid*map_* 2>/dev/null; "
         "true"],
        capture_output=True, timeout=15
    )
    time.sleep(2)


# ── arm-1 setup ───────────────────────────────────────────────────────────────

def setup_arm1(script_path: str, logfile):
    log(f"arm-1 setup: {script_path}", logfile)
    # Forward this harness's port layout (4500/4600/4700, stride 100) to the
    # shared Nt3p setup script, whose defaults (4430/4440/4450) target
    # run_sapsq.py.  Without this the target listens on 4430+ while the
    # coordinator attaches to 4500+ and every attach fails.
    port_env = (
        f"PATH_A_PORT_BASE={TENANT_BASE_PORT + PATH_PORT_OFFSETS[0]} "
        f"PATH_B_PORT_BASE={TENANT_BASE_PORT + PATH_PORT_OFFSETS[1]} "
        f"PATH_C_PORT_BASE={TENANT_BASE_PORT + PATH_PORT_OFFSETS[2]}"
    )
    r = subprocess.run(["ssh", "arm-1", f"{port_env} bash {script_path}"],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        log(f"ERROR: arm-1 setup rc={r.returncode}: "
            f"stderr={r.stderr[-300:]} stdout={r.stdout[-300:]}", logfile)
        raise RuntimeError(f"arm-1 setup failed: {script_path}")
    log("arm-1 setup ok", logfile)


# ── argument parsing ──────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="SAPS coordinator/tenant mode static fairness testbed"
    )
    p.add_argument("--num-tenants", type=int, default=4)
    p.add_argument("--num-paths", type=int, default=3)
    p.add_argument("--weights", type=str, default="3,1,1,1")
    p.add_argument("--link-cap", type=int, default=200000)
    p.add_argument(
        "--path-caps",
        type=str,
        required=True,
        help="required comma-separated deliverable capacities K_p",
    )
    p.add_argument(
        "--sample-rate",
        type=int,
        default=32,
        help="publish one completion observation per N commands",
    )
    p.add_argument(
        "--dump-interval-ms",
        type=int,
        default=500,
        help="interval for live shared-state snapshots",
    )
    p.add_argument(
        "--iostat-interval-ms",
        type=int,
        default=1000,
        help="interval for independent per-tenant delivered-I/O snapshots",
    )
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--qd", type=int, default=32)
    p.add_argument(
        "--bypass-d",
        action="store_true",
        default=False,
        help="disable the path-health classifier",
    )
    p.add_argument("--no-bypass-d", dest="bypass_d", action="store_false")
    p.add_argument(
        "--bypass-fsm",
        dest="bypass_fsm",
        action="store_true",
        help="Skip path-state transitions for a static allocation run",
    )
    p.add_argument(
        "--no-bypass-fsm",
        dest="bypass_fsm",
        action="store_false",
        help="Enable the full path-state estimator",
    )
    p.set_defaults(bypass_fsm=None)
    p.add_argument("--output-dir", type=str, required=True)
    p.add_argument("--no-setup-arm1", action="store_true")
    p.add_argument("--setup-arm1-script", type=str, default=SETUP_ARM1_DEFAULT)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--target-ip", type=str, default=None,
                   help="Override TARGET_IP (default: 10.0.1.1). Use 10.0.0.1 for mlx5_0 lane.")
    p.add_argument(
        "--shared-nqn",
        type=str,
        default=None,
        help="Use one shared namespace NQN for every tenant process",
    )
    p.add_argument(
        "--port-base",
        type=int,
        default=4800,
        help="First direct listener port when --shared-nqn is set",
    )
    p.add_argument(
        "--fault-path",
        type=int,
        default=None,
        help="Zero-based shared-namespace path to delay during the workload",
    )
    p.add_argument(
        "--fault-delay-us",
        type=int,
        default=5000,
        help="Added target-side completion delay in microseconds",
    )
    p.add_argument(
        "--fault-onset-s",
        type=float,
        default=10.0,
        help="Seconds after workload start to enable the listener delay",
    )
    p.add_argument(
        "--fault-clear-s",
        type=float,
        default=0.0,
        help="Seconds after workload start to clear the delay, or zero",
    )
    p.add_argument(
        "--target-rpc-host",
        default="arm-1",
        help="Host running the shared-namespace NVMe-oF target",
    )
    p.add_argument(
        "--target-rpc-socket",
        default=DEFAULT_TARGET_RPC_SOCKET,
        help="RPC socket of the shared-namespace NVMe-oF target",
    )
    p.add_argument(
        "--target-rpc-socket-template",
        default=None,
        help=(
            "Path-indexed target RPC socket, for example "
            "/var/tmp/spdk_saps_scale_p{path}.sock"
        ),
    )
    p.add_argument(
        "--health-coupling-mode",
        choices=tuple(HEALTH_COUPLING_MODE_IDS),
        default="continuous",
        help=(
            "continuous applies HCAA; fixed keeps admission at the namespace "
            "envelope while retaining the same estimator and path placement"
        ),
    )
    return p.parse_args()


# ── dry-run ───────────────────────────────────────────────────────────────────

def print_dry_run(args):
    weights = [int(w) for w in args.weights.split(",")]
    tsc_hz = detect_tsc_hz()
    bypass_d = "1" if args.bypass_d else "0"
    bypass_saps_fsm = "1" if args.bypass_fsm else "0"
    print("\n=== DRY RUN — SAPS coordinator/tenant harness ===")
    print(f"  mode=coordinator/tenant  tenants={args.num_tenants}  "
          f"paths={args.num_paths}  weights={args.weights}")
    print(f"  link_cap={args.link_cap}  qd={args.qd}  duration={args.duration}s")
    print(f"  bypass_d_classifier={bypass_d}")
    print(f"  bypass_path_estimator={bypass_saps_fsm}")
    print(f"  UDS socket: {DPA_PLUGIN_SOCK}")
    if args.fault_path is not None:
        print(
            f"  target fault: path={args.fault_path} "
            f"port={args.port_base + args.fault_path} "
            f"delay={args.fault_delay_us}us "
            f"onset={args.fault_onset_s}s clear={args.fault_clear_s}s"
        )
    print()
    print("--- Per-process env (summary) ---")
    for t in range(args.num_tenants):
        is_coord = (t == 0)
        if is_coord:
            env = build_env_coordinator(t, args.num_tenants, args.num_paths,
                                         args.link_cap, weights, tsc_hz,
                                         bypass_d, args.path_caps,
                                         args.sample_rate, bypass_saps_fsm,
                                         args.health_coupling_mode)
        else:
            env = build_env_tenant(t, args.num_tenants, args.num_paths,
                                    args.link_cap, weights, tsc_hz, bypass_d,
                                    args.sample_rate,
                                    args.health_coupling_mode)
        enable_recovery_probe(env, bypass_saps_fsm)
        role = "COORDINATOR" if is_coord else "tenant"
        summary = (
            e for e in env
            if "ROLE" in e
            or "TENANT_ID" in e
            or "ENABLED" in e
            or "BYPASS" in e
            or "ACTIVE_PROBE" in e
        )
        print(f"  t{t} [{role}]: {' '.join(summary)}")
    sum_w = sum(weights)
    expected = [round(args.link_cap * w / sum_w) for w in weights]
    print(f"\n--- Expected IOPS (weights {args.weights}) ---")
    for t, e in enumerate(expected):
        print(f"  t{t}: {e:,} IOPS  (±15% = [{round(e*0.85):,}, {round(e*1.15):,}])")
    print(f"\n=== end dry run ===\n")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    global TARGET_IP
    args = parse_args()
    if args.bypass_fsm is None:
        args.bypass_fsm = args.fault_path is None
    try:
        validate_listener_delay_args(args)
    except ValueError as error:
        raise SystemExit(f"ERROR: {error}") from error
    if args.target_ip:
        TARGET_IP = args.target_ip
    weights = [int(w) for w in args.weights.split(",")]
    args.path_caps = [
        int(value.strip()) for value in args.path_caps.split(",")
        if value.strip()
    ]
    if len(args.path_caps) != args.num_paths or any(
        value <= 0 for value in args.path_caps
    ):
        raise SystemExit(
            "ERROR: --path-caps must contain one positive value per path"
        )
    if args.sample_rate < 1:
        raise SystemExit("ERROR: --sample-rate must be positive")
    if args.dump_interval_ms < 100:
        raise SystemExit("ERROR: --dump-interval-ms must be at least 100")
    if args.iostat_interval_ms < 250:
        raise SystemExit("ERROR: --iostat-interval-ms must be at least 250")
    if not SAPSQ_DUMP.is_file():
        raise SystemExit(f"ERROR: missing live snapshot helper {SAPSQ_DUMP}")
    if len(weights) < args.num_tenants:
        print(f"ERROR: --weights has {len(weights)} entries, need {args.num_tenants}",
              file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print_dry_run(args)
        return

    tsc_hz = detect_tsc_hz()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    orch_log_path = out_dir / "orchestrator.log"
    orch_log = open(orch_log_path, "w")

    log("=== E1 coordinator/tenant orchestrator start ===", orch_log)
    log(f"tenants={args.num_tenants} paths={args.num_paths} weights={args.weights} "
        f"link_cap={args.link_cap} duration={args.duration}s qd={args.qd} "
        f"path_caps={args.path_caps} sample_rate={args.sample_rate} "
        f"bypass_d={args.bypass_d} bypass_fsm={args.bypass_fsm} "
        f"health_coupling_mode={args.health_coupling_mode} "
        f"tsc_hz={tsc_hz}",
        orch_log)
    log(f"UDS socket: {DPA_PLUGIN_SOCK}", orch_log)
    fault_port = (
        args.port_base + args.fault_path
        if args.fault_path is not None
        else None
    )
    fault_thread = None
    fault_errors = []
    fault_start_state = None
    fault_end_state = None

    max_core = BASE_CORE + args.num_tenants * CORES_PER_PROC - 1
    if max_core >= 96:
        log(f"ERROR: need core {max_core} but max is 95", orch_log)
        sys.exit(1)

    procs = [
        TenantProc(
            tenant_id=i,
            is_coordinator=(i == 0),
            args=args,
            out_dir=out_dir,
            logfile=orch_log,
            tsc_hz=tsc_hz,
        )
        for i in range(args.num_tenants)
    ]

    interrupted = [False]

    def sigint_handler(sig, frame):
        log("SIGINT — cleanup", orch_log)
        interrupted[0] = True
        if fault_port is not None:
            try:
                set_listener_delay(args, fault_port, 0, orch_log)
            except Exception as error:
                log(f"[TARGET_DELAY] cleanup failed: {error}", orch_log)
        cleanup_all(procs)
        sys.exit(130)

    signal.signal(signal.SIGINT, sigint_handler)

    # ── Step 1: arm-1 setup ──────────────────────────────────────────────────
    if not args.no_setup_arm1:
        setup_arm1(args.setup_arm1_script, orch_log)
    else:
        log("skip arm-1 setup (--no-setup-arm1)", orch_log)

    # ── Step 2: cleanup stale state ──────────────────────────────────────────
    log("cleanup stale bdevperf + UDS socket", orch_log)
    cleanup_all([])
    # Wait for arm-1 NVMe-oF connections to drain (keepalive timeout ~10-15s).
    # run5 showed 8s insufficient: cntlid:12/139/268 still occupied, RDMA addr
    # resolution error on reconnect. Raise to 20s to ensure all NQN slots freed.
    log("waiting 20s for arm-1 connections to drain", orch_log)
    time.sleep(20)

    # ── Step 3: initialize coordinator (tenant 0) FIRST ───────────────────────
    # The SPDK RPC socket appears before framework_start_init creates the DPA
    # plugin's UDS server.  Complete coordinator setup before launching tenant
    # processes so tenant memfd attachment has a live control-plane endpoint.
    coord = procs[0]
    log("launching coordinator (tenant 0)", orch_log)
    coord.start()

    # Wait for coordinator RPC socket
    log("waiting for coordinator RPC socket", orch_log)
    if not coord.wait_socket(timeout=60):
        log("ERROR: coordinator socket timeout — abort", orch_log)
        cleanup_all(procs)
        sys.exit(1)
    log("coordinator socket ready", orch_log)

    log("setup coordinator controller", orch_log)
    setup_results = {coord.tid: coord.setup_controllers()}
    if not setup_results[coord.tid]:
        log("ERROR: coordinator controller setup failed", orch_log)
        cleanup_all(procs)
        sys.exit(1)

    # ── Step 4: launch tenant processes (1..N-1) ─────────────────────────────
    log("launching tenant processes (1..N-1)", orch_log)
    for p in procs[1:]:
        p.start()
        time.sleep(0.2)  # stagger launches to avoid UDS accept thundering-herd

    # ── Step 5: wait for all RPC sockets ─────────────────────────────────────
    log("waiting for tenant RPC sockets", orch_log)
    for p in procs[1:]:
        if not p.wait_socket(timeout=120):
            log(f"ERROR: t{p.tid} socket timeout — abort", orch_log)
            cleanup_all(procs)
            sys.exit(1)
    log("all tenant sockets ready", orch_log)

    # ── Step 6: initialize tenant controllers ────────────────────────────────
    def do_setup(p):
        try:
            setup_results[p.tid] = p.setup_controllers()
        except Exception as error:
            p._log(f"controller setup raised {type(error).__name__}: {error}")
            setup_results[p.tid] = False

    log("setup tenant controllers (parallel)", orch_log)
    threads = [threading.Thread(target=do_setup, args=(p,), daemon=True)
               for p in procs[1:]]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=200)

    failed = [p for p in procs if not setup_results.get(p.tid, False)]
    if failed:
        log(f"ERROR: setup failed for t={[p.tid for p in failed]}", orch_log)
        cleanup_all(procs)
        sys.exit(1)
    log("all tenant controllers ready", orch_log)

    # ── Step 7: perform_tests (parallel) ─────────────────────────────────────
    if fault_port is not None:
        set_listener_delay(args, fault_port, 0, orch_log)
        fault_start_state = get_listener_delay(args)
        (out_dir / "target_delay_start.json").write_text(
            json.dumps(fault_start_state, indent=2)
        )
        fault_thread, fault_errors = schedule_listener_delay(args, orch_log)

    log(f"perform_tests ({args.duration}s) on {args.num_tenants} procs", orch_log)
    test_procs = []
    for p in procs:
        tp = p.run_perform_tests()
        test_procs.append((p, tp))

    snapshotter = LiveSnapshotter(
        out_dir / "sapsq_periodic",
        args.dump_interval_ms,
    )
    iostat_snapshotter = TenantIostatSnapshotter(
        procs,
        out_dir / "tenant_iostat_periodic",
        args.iostat_interval_ms,
    )
    snapshotter.start()
    iostat_snapshotter.start()

    for p, tp in test_procs:
        try:
            tp.wait(timeout=args.duration + 60)
        except subprocess.TimeoutExpired:
            log(f"WARN: t{p.tid} perform_tests timeout", orch_log)
            tp.kill()
        log(f"t{p.tid} perform_tests done", orch_log)
    snapshotter.stop()
    iostat_snapshotter.stop()
    live_evidence = analyze_live_evidence(
        snapshotter.samples,
        snapshotter.errors,
        args,
    )
    fault_window_iostat = analyze_fault_window_iostat(
        iostat_snapshotter.samples,
        iostat_snapshotter.errors,
        args,
        weights,
    )

    if fault_thread is not None:
        fault_thread.join(timeout=5)
        if fault_thread.is_alive():
            fault_errors.append("target delay schedule did not finish")
        try:
            fault_end_state = get_listener_delay(args)
            (out_dir / "target_delay_end.json").write_text(
                json.dumps(fault_end_state, indent=2)
            )
        except Exception as error:
            fault_errors.append(str(error))
            log(f"[TARGET_DELAY] final snapshot failed: {error}", orch_log)
        try:
            set_listener_delay(args, fault_port, 0, orch_log)
        except Exception as error:
            fault_errors.append(str(error))
            log(f"[TARGET_DELAY] final cleanup failed: {error}", orch_log)

    # ── Step 8: graceful shutdown ─────────────────────────────────────────────
    # 先送 SIGTERM 給所有 proc，讓 dpa_plugin_shutdown() 有機會執行並 print exit stats
    log("graceful shutdown: sending SIGTERM to all procs", orch_log)
    for p in procs:
        if p.proc:
            try:
                subprocess.run(["sudo", "kill", "-15", str(p.proc.pid)],
                               capture_output=True)
            except Exception:
                pass
    # 等 5s 讓 coordinator dpa_plugin_shutdown() 執行並 flush log
    time.sleep(5)

    # Read sapsq dump BEFORE killing processes (exit stats in logs)
    dump = read_sapsq_dump(out_dir, procs, orch_log)

    cleanup_all(procs)

    # ── Step 9: parse per-tenant results ─────────────────────────────────────
    log("parsing results", orch_log)
    results = [p.parse_result() for p in procs]

    # ── Step 10: compute metrics ──────────────────────────────────────────────
    iops_list = [r.get("iops") or 0.0 for r in results]
    total_iops = sum(iops_list)
    wj = weighted_jain(iops_list, weights[:args.num_tenants])

    sum_w = sum(weights[:args.num_tenants])
    expected = [round(args.link_cap * w / sum_w) for w in weights[:args.num_tenants]]

    # PASS criteria
    pass_iops = True
    for t, (actual, exp) in enumerate(zip(iops_list, expected)):
        if actual < exp * 0.85 or actual > exp * 1.15:
            pass_iops = False

    pass_jain = wj >= 0.95
    pass_io_failed = all((r.get("io_failed") or 0) == 0 for r in results)
    fault_evidence = (
        listener_delay_evidence(fault_start_state, fault_end_state)
        if fault_port is not None
        else None
    )
    path_response = None
    if fault_port is not None:
        try:
            coordinator_log = next(
                proc.logpath for proc in procs if proc.is_coordinator
            ).read_text()
            path_trace = parse_path_response_trace(
                coordinator_log,
                args.fault_path,
            )
            path_response = dict(live_evidence.get("path_response") or {})
            path_response["trace"] = path_trace
        except Exception as error:
            fault_errors.append(f"path response validation failed: {error}")
    fault_ok = (
        not fault_errors
        and (fault_evidence["pass"] if fault_evidence is not None else True)
        and (path_response.get("pass", False) if path_response is not None else True)
    )
    epoch_ok = (dump.get("sapsq_epoch_commit_seq") or 0) > 0
    stale_fb = dump.get("sapsq_stale_epoch_fallback") or 0
    ring = dump.get("ring") or {}
    ring_ok = (
        ring.get("overrun") == 0
        and ring.get("consumer") == ring.get("dpa_consumed")
        and 0 <= ring.get("max_lag", -1) < DPA_PLUGIN_RING_SIZE
    )
    capacity_ok = all(
        dump.get("path_capacity_iops", {}).get(f"p{path}")
        == args.path_caps[path]
        and dump.get("path_effective_capacity_iops", {}).get(f"p{path}")
        == (
            args.path_caps[path]
            * dump.get("path_health", {}).get(f"p{path}", -1)
        ) >> 16
        for path in range(args.num_paths)
    )
    expected_coupling_mode = HEALTH_COUPLING_MODE_IDS[
        args.health_coupling_mode
    ]
    coupling_ok = (
        dump.get("health_coupling_mode") == expected_coupling_mode
    )
    overall_pass = (
        pass_iops
        and pass_jain
        and pass_io_failed
        and live_evidence.get("pass", False)
        and fault_ok
        and epoch_ok
        and ring_ok
        and capacity_ok
        and coupling_ok
    )

    fault_summary = None
    if fault_port is not None:
        fault_summary = {
            "path": args.fault_path,
            "listener_port": fault_port,
            "delay_us": args.fault_delay_us,
            "onset_s": args.fault_onset_s,
            "clear_s": args.fault_clear_s,
            "start": fault_start_state,
            "end": fault_end_state,
            "activation_counters": fault_evidence,
            "path_response": path_response,
            "errors": fault_errors,
            "pass": fault_ok,
        }

    summary = {
        "mode": "coordinator_tenant",
        "num_tenants": args.num_tenants,
        "num_paths": args.num_paths,
        "weights": args.weights,
        "link_cap": args.link_cap,
        "path_capacity_iops": args.path_caps,
        "sample_rate": args.sample_rate,
        "iostat_interval_ms": args.iostat_interval_ms,
        "health_coupling_mode": args.health_coupling_mode,
        "duration_s": args.duration,
        "bypass_d": args.bypass_d,
        "bypass_fsm": args.bypass_fsm,
        "per_tenant": results,
        "metrics": {
            "total_iops": total_iops,
            "weighted_jain": round(wj, 4),
            "expected_iops": expected,
            "pass_iops": pass_iops,
            "pass_jain": pass_jain,
            "pass_io_failed": pass_io_failed,
            "pass_live_evidence": live_evidence.get("pass", False),
            "pass_fault_injection": fault_ok,
            "pass_epoch_commit": epoch_ok,
            "pass_ring_integrity": ring_ok,
            "pass_capacity_formula": capacity_ok,
            "pass_health_coupling_mode": coupling_ok,
        },
        "verdict": {
            "pass": overall_pass,
            "notes": (
                f"iops={'OK' if pass_iops else 'FAIL'} "
                f"jain={'OK' if pass_jain else 'FAIL'}({wj:.4f}) "
                f"io_failed={'OK' if pass_io_failed else 'FAIL'} "
                f"live={'OK' if live_evidence.get('pass') else 'FAIL'} "
                f"fault={'OK' if fault_ok else 'FAIL'} "
                f"epoch={'OK' if epoch_ok else 'FAIL'} "
                f"ring={'OK' if ring_ok else 'FAIL'} "
                f"capacity={'OK' if capacity_ok else 'FAIL'} "
                f"coupling={'OK' if coupling_ok else 'FAIL'}"
            ),
        },
        "sapsq_dump": dump,
        "live_evidence": live_evidence,
        "fault_window_iostat": fault_window_iostat,
        "target_listener_fault": fault_summary,
    }
    summary_path = out_dir / "e1_coordinator_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    log(f"summary → {summary_path}", orch_log)

    # ── Step 11: print report ─────────────────────────────────────────────────
    log("=== E1 coordinator/tenant run complete ===", orch_log)
    orch_log.close()

    print(f"\n{'='*60}")
    print(f"SAPS Coordinator/Tenant Run — {args.output_dir}")
    print(f"{'='*60}")
    print(
        f"weights={args.weights}  link_cap={args.link_cap} "
        f"bypass_d={args.bypass_d}  bypass_fsm={args.bypass_fsm}"
    )
    print()
    print(f"{'Tenant':<10} {'Role':<14} {'IOPS':>10} {'Expected':>10} {'io_failed':>10}")
    print(f"{'-'*56}")
    for r, exp in zip(results, expected):
        role = "coordinator" if r["role"] == "coordinator" else "tenant"
        iops_s = f"{r.get('iops') or 0:.0f}"
        print(f"  t{r['tenant_id']:<7} {role:<14} {iops_s:>10} {exp:>10,}  {r.get('io_failed') or 0:>9}")
    print(f"{'-'*56}")
    print(f"  {'TOTAL':<21} {total_iops:>10.0f} {args.link_cap:>10,}")
    print()
    print(f"  Weighted Jain: {wj:.4f}  (target ≥ 0.95)")
    print(f"  epoch_commit_seq: {dump.get('sapsq_epoch_commit_seq')}")
    print(f"  stale_epoch_fallback: {stale_fb}")
    print()
    verdict_str = "PASS" if overall_pass else "FAIL"
    print(f"  Verdict: {verdict_str}")
    print(f"  {summary['verdict']['notes']}")
    print(f"{'='*60}\n")

    sys.exit(0 if overall_pass else 1)


if __name__ == "__main__":
    main()
