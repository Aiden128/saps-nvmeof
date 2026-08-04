#!/usr/bin/env python3
"""Sustained single-path fault harness for the competitor / SAPS / stock arms.

WHY THIS EXISTS
---------------
The E2 idle harness (`run_saps_q_e2_idle.py`) injects its Phase-2 "idle" fault by
firing `bdev_delay_update_latency` on *three* paths (A/B/C) of one tenant
serially over SSH, and it only restores the fault after the workload has already
ended.  That gives a fault whose onset skews across ~t=23/26/28s and which is
never stable long enough for an external competitor arm to observe a sustained
single-path fault and react.  The competitor experiment therefore cannot reuse
`idle_inject`.  This harness is the additive replacement:

  1. It uses the D-series single-NQN topology (one namespace, three RDMA paths;
     `experiments/3path_targets/setup_arm1.sh`), so a path-selection arm can
     steer among paths.
  2. It injects ONE sustained single-path fault on ONE path at a known onset
     wall-clock, in a single SSH round-trip, and holds it for the whole
     measurement window (fail-slow via `bdev_delay_update_latency delay_B`, a
     media status via `bdev_error_inject_nvme_error --sct 2 --sc 129
     EE_delay_B`, or a workload-preserving ANA listener transition to
     `inaccessible`).  The fault is restored by the harness only after the
     window closes.
  3. It runs the arm's monitor DURING the sustained fault and records the reroute
     timestamp (detection latency) plus per-path / per-tenant delivered IOPS
     (worst-tenant fair-share):
       * competitor arms (`external_*.py`) run as the real mutating sidecar and
         emit their own `metrics.json` (disable-event detection + fair-share);
       * `saps_q` and `stock_qd` have no sidecar, so a passive `--observe-only`
         collector (the per-path-adaptive entry point, which never mutates)
         records per-path / per-tenant delivered IOPS + fair-share, and this
         harness derives a traffic-reroute detection latency from the faulted
         path's delivered-IOPS collapse.
  4. It emits a unified per-run `metrics.json` + `manifest.json` into the run
     directory so a downstream step can populate fig:detection and coverage
     panels a/b/d.

This module NEVER modifies `run_saps_q_e2_idle.py`; it only imports the SAPS-Q
plugin env builder from it.  The target-side layout is brought up by the existing
`setup_arm1.sh`; no new target script is introduced.

SAFETY / SCOPE
--------------
Run on arm-2 (initiator).  Drives arm-1 (target) over SSH exactly like the
existing D-series scripts (`sudo -u aiden ssh arm-1 "sudo rpc.py ..."`).  Use
`--dry-run` to print the full plan (commands, timeline, injection RPCs, sidecar
cmdline) without touching the testbed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── constants: D-series single-NQN topology (mirror setup_arm1.sh) ─────────────

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

BDEVPERF = os.environ.get(
    "COMPETITOR_BDEVPERF",
    "/home/aiden/spdk/build/examples/bdevperf",
)
RPCPY = "/home/aiden/spdk/scripts/rpc.py"
BDEVPERF_PY = "/home/aiden/spdk/examples/bdev/bdevperf/bdevperf.py"
DEFAULT_SPDK_ROOT = "/home/aiden/spdk"

NQN = "nqn.2024-01.io.spdk:mptest"
E2_NQN_PREFIX = "nqn.2024-01.io.spdk:tenant"
DEFAULT_TARGET_IP = "10.0.0.1"

# path label -> (target port, arm-1 rpc socket, delay bdev, error bdev)
# ports/sockets/bdevs are exactly those created by setup_arm1.sh.
PATHS: Dict[str, Tuple[int, str, str, str]] = {
    "A": (4430, "/var/tmp/spdk_a.sock", "delay_A", "EE_delay_A"),
    "B": (4431, "/var/tmp/spdk_b.sock", "delay_B", "EE_delay_B"),
    "C": (4432, "/var/tmp/spdk_c.sock", "delay_C", "EE_delay_C"),
}
PATH_ORDER = ["A", "B", "C"]
BASE_LATENCY_US = 27          # setup_arm1.sh bdev_delay_create -r/-t/-w/-n 27


def sha256_file(path: Path) -> Optional[str]:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def git_provenance() -> Dict[str, object]:
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT, text=True, stderr=subprocess.DEVNULL,
        ).strip()
        dirty = bool(subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=REPO_ROOT, text=True, stderr=subprocess.DEVNULL,
        ).strip())
    except (OSError, subprocess.CalledProcessError):
        commit = None
        dirty = None
    return {"commit": commit, "dirty": dirty}

SOCK_TMPL = "/var/tmp/bdevperf_cfx_proc{i}.sock"   # cfx = competitor-fault-experiment
BASE_CORE = 4
CORES_PER_PROC = 2

DEFAULT_SETUP_ARM1 = str(
    REPO_ROOT / "experiments" / "3path_targets" / "setup_arm1.sh"
)
DEFAULT_E2_SETUP_ARM1 = str(
    REPO_ROOT / "experiments" / "3path_targets" / "setup_arm1_sapsq_Nt3p.sh"
)
E2_PORT_BASE = {"A": 4500, "B": 4600, "C": 4700}


def topology_name(args) -> str:
    return getattr(args, "topology", "single_nqn")


def tenant_nqn(args, tenant_id: int) -> str:
    if topology_name(args) == "e2":
        return f"{E2_NQN_PREFIX}{tenant_id}"
    return NQN


def tenant_path(args, tenant_id: int, path: str) -> Tuple[int, str, str, str]:
    """Return port/socket/delay/error names for one tenant/path in the selected
    target topology.  E2 uses a distinct NQN and bdev chain per tenant while
    retaining the same three target processes and path labels."""
    if topology_name(args) == "e2":
        socket = PATHS[path][1]
        return (
            E2_PORT_BASE[path] + tenant_id,
            socket,
            f"delay_t{tenant_id}_{path}",
            f"EE_delay_t{tenant_id}_{path}",
        )
    return PATHS[path]

# arm -> sidecar entry point (competitor arms only)
SIDECAR_ENTRY = {
    "fixed_threshold":   "external_fixed_threshold.py",
    "per_path_adaptive": "external_per_path_adaptive.py",
    "kernel_ana":        "external_kernel_ana.py",
    "cross_stream":      "external_cross_stream.py",
}
SIDECAR_ARMS = set(SIDECAR_ENTRY)
SIGNAL_ISOLATION_ARMS = {
    "saps_completion": "completion",
    "saps_reachability": "reachability",
    "saps_qd": "queue_depth",
    "saps_rtt": "request_rtt",
}
NATIVE_PLUGIN_ARMS = {"saps_q", *SIGNAL_ISOLATION_ARMS}
NATIVE_ARMS = NATIVE_PLUGIN_ARMS | {"stock_qd"}
ALL_ARMS = sorted(SIDECAR_ARMS | NATIVE_ARMS)

# Competitors must observe all paths before they can diagnose one.  Start from
# neutral active-active RR; BaselineRunner switches atomically to
# active-passive only when it issues bdev_nvme_set_preferred_path after a
# detection.  Starting fixed/ANA in active-passive starves non-preferred paths
# of completions and makes their detector blind.
SIDECAR_POLICY = {
    "fixed_threshold":   ("active_active", "round_robin"),
    "kernel_ana":        ("active_active", "round_robin"),
    "per_path_adaptive": ("active_active", "round_robin"),
    "cross_stream":      ("active_active", "round_robin"),
}

# passive collector for native arms: the per-path-adaptive entry point run
# --observe-only (never mutates), used purely to record delivered IOPS + the
# fair-share summary with the SAME OutputWriter code the competitor arms use.
NATIVE_COLLECTOR_ENTRY = "external_per_path_adaptive.py"


# ── logging ────────────────────────────────────────────────────────────────────


def ts_str() -> str:
    return time.strftime("%H:%M:%S")


def log(msg: str, fp=None) -> None:
    line = f"[{ts_str()}] [cfx] {msg}"
    print(line, flush=True)
    if fp:
        print(line, file=fp, flush=True)


def cpu_mask(proc_idx: int) -> str:
    start = BASE_CORE + proc_idx * CORES_PER_PROC
    mask = 0
    for c in range(start, start + CORES_PER_PROC):
        mask |= (1 << c)
    return hex(mask)


def taskset_cpus(proc_idx: int) -> str:
    start = BASE_CORE + proc_idx * CORES_PER_PROC
    return f"{start}-{start + CORES_PER_PROC - 1}"


def path_label(path: str) -> str:
    """Sidecar pN label: paths are labelled in ascending transport-port order,
    and setup_arm1.sh assigns A<B<C => 4430<4431<4432 => p0<p1<p2."""
    return f"p{PATH_ORDER.index(path)}"


# ── SAPS-Q plugin env (imported from the validated E2 harness) ─────────────────


def saps_q_env(
    tenant_id: int,
    n_tenants: int,
    link_cap: int,
    weights: List[int],
    path_capacities_iops: List[int],
    sample_rate: int,
    health_source: str = "completion",
) -> List[str]:
    """Reuse the exact plugin env vars the validated E2 harness uses so saps_q
    behaviour here is identical (coordinator=tenant 0, tenants=1..N-1, shared
    UDS ring).  Imported additively; run_saps_q_e2_idle.py is never modified."""
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from run_saps_q_e2_idle import build_env  # noqa: E402  (lazy, saps_q only)

    # Detection/coverage evaluates health-aware steering, so the D classifier
    # must be live.  The imported E2 harness defaults to bypass=1 because its
    # fairness-only experiment intentionally isolates scheduling.
    reachability_only = health_source == "reachability"
    runtime_health_source = (
        "completion" if reachability_only else health_source
    )
    env = build_env(
        "saps_q",
        tenant_id,
        n_tenants,
        3,
        link_cap,
        weights,
        path_capacities_iops,
        "1" if reachability_only else "0",
    )
    # build_env() intentionally defaults to the E2 fairness-only profile
    # (health FSM bypassed and both M2/M5 disabled).  Detection/coverage needs
    # the same full-SAPS profile used by the validated summit-v2 arm.  Apply the
    # profile to the returned argv entries instead of mutating process-global
    # os.environ or the shared E2 harness.
    full_saps = {
        "DPA_PLUGIN_SAMPLE_RATE": str(sample_rate),
        "SAPSQ_BYPASS_D_CLASSIFIER": (
            "1" if reachability_only else "0"
        ),
        "SAPSQ_BYPASS_SAPS_FSM": (
            "0" if health_source == "completion" else "1"
        ),
        "SAPS_M2_ENABLED": "1",
        "SAPS_M2_V2_CLASSIFIER": "0",
        "SAPS_M5_DRR_ENABLED": "1",
        "SAPSQ_HEALTH_SOURCE": runtime_health_source,
    }
    patched = []
    seen = set()
    for entry in env:
        key, separator, _value = entry.partition("=")
        if separator and key in full_saps:
            patched.append(f"{key}={full_saps[key]}")
            seen.add(key)
        else:
            patched.append(entry)
    patched.extend(f"{key}={value}" for key, value in full_saps.items()
                   if key not in seen)
    # sudo starts bdevperf from an explicit env list, so opt-in selector
    # diagnostics must be forwarded deliberately.  These variables only emit
    # trace records and do not change selection behavior.
    for key in ("SAPS_SELECTOR_TRACE", "SAPS_SELECTOR_TRACE_STRIDE"):
        if key in os.environ:
            patched.append(f"{key}={os.environ[key]}")
    return patched


def stock_env() -> List[str]:
    """stock_qd and the competitor arms run bdevperf with the DPA plugin fully
    disabled (competitor steering is the host-side sidecar's job)."""
    return [
        "DPA_PLUGIN_DISABLE_INIT=1",
        "SAPS_Q_ENABLED=0",
        "SAPS_M4_ENABLED=0",
        "SAPS_M5_DRR_ENABLED=0",
    ]


# ── one bdevperf tenant on the single-NQN 3-path topology ──────────────────────


class TenantProc:
    """A bdevperf initiator process attaching the 3 mptest paths as one
    multipath bdev (mpn1, controller prefix mp)."""

    def __init__(self, tenant_id: int, args, out_dir: Path, orch_log):
        self.tid = tenant_id
        self.args = args
        self.out_dir = out_dir / f"tenant_{tenant_id:02d}"
        self.logpath = self.out_dir / "bdevperf.log"
        self.result_json = self.out_dir / "result.json"
        self.sock = SOCK_TMPL.format(i=tenant_id)
        self.proc = None
        self.logfp = None
        self.orch_log = orch_log
        self._mask = cpu_mask(tenant_id)
        self._taskset = taskset_cpus(tenant_id)
        self._nqn = tenant_nqn(args, tenant_id)

    def _log(self, msg: str) -> None:
        log(f"[t{self.tid:02d}] {msg}", self.orch_log)

    def env_list(self) -> List[str]:
        if self.args.arm in NATIVE_PLUGIN_ARMS:
            return saps_q_env(self.tid, self.args.num_tenants,
                              self.args.link_cap, self.args.weights_list,
                              self.args.path_capacities_iops_list,
                              self.args.sample_rate,
                              SIGNAL_ISOLATION_ARMS.get(
                                  self.args.arm,
                                  "completion",
                              ))
        return stock_env()

    def cmd_list(self) -> List[str]:
        cmd = (
            ["sudo", "taskset", "-c", self._taskset, "env"] + self.env_list()
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
        # bdevperf normally exits on the first failed I/O.  Media-error cells
        # need the host policy to observe and react while the workload stays
        # alive, so use bdevperf's native continue-on-failure mode.  Suppress
        # per-completion NOTICE logging because a sustained injected status can
        # otherwise generate a multi-gigabyte log before a latency/ANA-blind
        # competitor times out without rerouting.
        if self.args.fault_type == "media_error":
            cmd.extend(["-f", "--silence-noticelog"])
        return cmd

    def attach_cmds(self) -> List[List[str]]:
        cmds = []
        for path in PATH_ORDER:
            port = tenant_path(self.args, self.tid, path)[0]
            cmds.append([
                "sudo", RPCPY, "-s", self.sock,
                "bdev_nvme_attach_controller",
                "-b", "mp", "-t", "rdma", "-a", self.args.target_ip,
                "-s", str(port), "-f", "ipv4",
                "-n", self._nqn, "--multipath", "multipath",
            ])
        return cmds

    def policy_cmd(self) -> Tuple[List[str], str]:
        if self.args.arm in NATIVE_PLUGIN_ARMS:
            cmd = ["sudo", RPCPY, "-s", self.sock,
                   "bdev_nvme_set_multipath_policy", "-b", "mpn1", "-p", "plugin"]
            return cmd, "plugin"
        # stock_qd + competitor arms: neutral active_active RR baseline.  A
        # competitor sidecar re-sets and restores its own policy on top of this.
        cmd = ["sudo", RPCPY, "-s", self.sock,
               "bdev_nvme_set_multipath_policy", "-b", "mpn1",
               "-p", "active_active", "-s", "round_robin"]
        return cmd, "active_active+round_robin"

    def start(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(["sudo", "rm", "-f", self.sock], capture_output=True)
        self._log(f"launch arm={self.args.arm} nqn={self._nqn} "
                  f"ports={[tenant_path(self.args, self.tid, p)[0] for p in PATH_ORDER]}")
        self.logfp = open(self.logpath, "w")
        self.proc = subprocess.Popen(self.cmd_list(), stdout=self.logfp,
                                     stderr=self.logfp)
        self._log(f"PID={self.proc.pid}")

    def wait_socket(self, timeout: int = 120) -> bool:
        for _ in range(timeout):
            if os.path.exists(self.sock):
                return True
            time.sleep(1)
        return False

    def make_socket_readable(self) -> bool:
        """Allow the unprivileged measurement sidecar to poll this root-owned
        bdevperf RPC socket.  The socket is ephemeral and removed by cleanup."""
        result = subprocess.run(
            ["sudo", "chmod", "0666", self.sock],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            self._log(f"RPC socket chmod failed: {result.stderr.strip()[:120]}")
            return False
        return True

    def setup_controllers(self) -> bool:
        try:
            r = subprocess.run(
                ["sudo", RPCPY, "-s", self.sock,
                 "bdev_nvme_set_options", "--io-path-stat"],
                capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            self._log("HUNG at bdev_nvme_set_options — INVALID")
            return False
        if r.returncode != 0:
            self._log(f"bdev_nvme_set_options failed: {r.stderr[:120]}")
            return False
        try:
            r = subprocess.run(["sudo", RPCPY, "-s", self.sock,
                                "framework_start_init"],
                               capture_output=True, text=True, timeout=90)
        except subprocess.TimeoutExpired:
            self._log("HUNG at framework_start_init — INVALID")
            return False
        if r.returncode != 0:
            self._log(f"framework_start_init failed: {r.stderr[:120]}")
            return False
        time.sleep(1)

        for path, cmd in zip(PATH_ORDER, self.attach_cmds()):
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
            except subprocess.TimeoutExpired:
                self._log(f"HUNG attaching path {path} — INVALID")
                return False
            self._log(f"attach {path}: rc={r.returncode} "
                      f"{(r.stdout + r.stderr).strip()[:100]}")
            if r.returncode != 0:
                self._log(f"attach {path} failed — INVALID")
                return False
        time.sleep(2)

        cmd, label = self.policy_cmd()
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            self._log(f"HUNG at multipath-policy ({label}) — INVALID")
            return False
        self._log(f"multipath policy={label}: rc={r.returncode}")
        if r.returncode != 0:
            self._log(f"multipath-policy ({label}) failed — INVALID")
            return False
        return True

    def run_perform_tests(self):
        cmd = ["sudo", "timeout", str(self.args.duration + 30),
               "python3", BDEVPERF_PY, "-s", self.sock,
               "-t", str(self.args.duration + 15), "perform_tests"]
        return subprocess.Popen(cmd, stdout=self.logfp, stderr=self.logfp)

    def parse_result(self) -> dict:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        try:
            content = self.logpath.read_text()
        except Exception:
            content = ""
        iops = None
        i = content.find('"results"')
        if i >= 0:
            start = content.rfind("{", 0, i)
            if start >= 0:
                try:
                    j, _ = json.JSONDecoder().raw_decode(content, start)
                    if isinstance(j, dict) and "results" in j:
                        for res in j["results"]:
                            v = res.get("iops", 0)
                            if v and float(v) > 0:
                                iops = round(float(v), 0)
                                break
                except Exception:
                    pass
        result = {"tenant_id": self.tid, "arm": self.args.arm, "iops": iops}
        self.result_json.write_text(json.dumps(result, indent=2))
        self._log(f"result: IOPS={iops}")
        return result

    def kill(self) -> None:
        if self.proc:
            try:
                subprocess.run(["sudo", "kill", "-9", str(self.proc.pid)],
                               capture_output=True)
                self.proc.wait(timeout=5)
            except Exception:
                pass
        if self.logfp:
            try:
                self.logfp.close()
            except Exception:
                pass
        subprocess.run(["sudo", "rm", "-f", self.sock], capture_output=True)


def cleanup_all(procs: List[TenantProc]) -> None:
    for p in procs:
        try:
            p.kill()
        except Exception:
            pass
    subprocess.run(
        ["sudo", "bash", "-c",
         f"pkill -9 -f '{BDEVPERF}' 2>/dev/null; "
         "pkill -9 -f 'reactor_[0-9]' 2>/dev/null; "
         "rm -f /var/tmp/bdevperf_cfx_proc*.sock; "
         "rm -f /tmp/dpa_plugin_e1.sock; true"],
        capture_output=True, timeout=15)
    time.sleep(2)


# ── sustained single-path fault injector ───────────────────────────────────────


class SustainedFault:
    """ONE clean-onset, held-until-restore single-path fault, injected in a single
    SSH round-trip (no serial per-path skew, no early restore).  Reuses the exact
    D-series RPCs: fail-slow via bdev_delay_update_latency delay_<P>; media
    status via bdev_error_inject_nvme_error --sct 2 --sc 129 EE_delay_<P>; or a
    hard path fault via nvmf_subsystem_listener_set_ana_state."""

    def __init__(self, args, orch_log):
        self.args = args
        self.orch_log = orch_log
        self.path = args.fault_path
        self.restore_latency_us = (
            args.restore_lat_us
            if args.restore_lat_us is not None
            else (0 if topology_name(args) == "e2" else BASE_LATENCY_US)
        )
        selected_paths = (
            args.common_mode_paths_list
            if args.fault_type == "common_mode"
            else [self.path]
        )
        self.specs = [
            (tenant_id, path, *tenant_path(args, tenant_id, path))
            for tenant_id in range(args.num_tenants)
            for path in selected_paths
        ]
        _tid, _path, self.port, self.sock, self.delay_bdev, self.err_bdev = self.specs[0]
        self.onset_unix: Optional[float] = None
        self.inject_ok: Optional[bool] = None
        self._stop_topup = threading.Event()
        self._topup_thread: Optional[threading.Thread] = None

    def _ssh(self, remote_cmd: str, timeout: int = 20) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["sudo", "-u", "aiden", "ssh", self.args.arm1_host, remote_cmd],
            capture_output=True, text=True, timeout=timeout)

    @staticmethod
    def _parallel_remote(commands: List[str]) -> str:
        if len(commands) == 1:
            return commands[0]
        return "set -e; " + " ".join(f"({cmd}) &" for cmd in commands) + " wait"

    def _failslow_remote(self, latency_us: int) -> str:
        # All four metrics MUST move together, else p99 stays low and masks the
        # signal (see D1_inconsistency_forensic.md).  One SSH round-trip => the
        # onset is a single instant, not three serial RPCs.
        commands = []
        for _tid, _path, _port, sock, delay_bdev, _err_bdev in self.specs:
            commands.append(
                f"for m in avg_read p99_read avg_write p99_write; do "
                f"sudo {RPCPY} -s {sock} bdev_delay_update_latency "
                f"{delay_bdev} $m {latency_us}; done"
            )
        return self._parallel_remote(commands)

    def _media_remote(self, count: int) -> str:
        commands = []
        for _tid, _path, _port, sock, _delay_bdev, err_bdev in self.specs:
            commands.append(
                f"sudo {RPCPY} -s {sock} bdev_error_inject_nvme_error "
                f"--sct {self.args.media_sct} --sc {self.args.media_sc} "
                f"{err_bdev} read -n {count}"
            )
        return self._parallel_remote(commands)

    def _ana_remote(self, state: str) -> str:
        commands = []
        for tenant_id, _path, port, sock, _delay_bdev, _err_bdev in self.specs:
            commands.append(
                f"sudo {RPCPY} -s {sock} "
                f"nvmf_subsystem_listener_set_ana_state {tenant_nqn(self.args, tenant_id)} "
                f"-n {state} -t rdma -a {self.args.target_ip} "
                f"-s {port} -f ipv4"
            )
        return self._parallel_remote(commands)

    def dry_run_commands(self) -> List[str]:
        if self.args.fault_type in ("failslow", "common_mode"):
            return [f"ssh {self.args.arm1_host} '{self._failslow_remote(self.args.slow_lat_us)}'"]
        if self.args.fault_type == "media_error":
            cmds = [f"ssh {self.args.arm1_host} '{self._media_remote(self.args.media_count)}'"]
            cmds.append(f"  (+ topup every {self.args.media_topup_interval_s}s until restore)")
            return cmds
        if self.args.fault_type == "ana_inaccessible":
            return [f"ssh {self.args.arm1_host} '{self._ana_remote('inaccessible')}'"]
        return ["(fault_type=none: calibration run, no injection)"]

    def inject(self) -> None:
        self.onset_unix = time.time()
        if self.args.fault_type in ("failslow", "common_mode"):
            scope = (
                f"paths {','.join(self.args.common_mode_paths_list)} across "
                f"{self.args.num_tenants} tenant(s)"
                if self.args.fault_type == "common_mode"
                else f"path {self.path} across {self.args.num_tenants} tenant(s)"
            )
            log(f"INJECT {self.args.fault_type}: {scope} -> {self.args.slow_lat_us}us "
                f"onset_unix={self.onset_unix:.3f}", self.orch_log)
            r = self._ssh(self._failslow_remote(self.args.slow_lat_us))
            self.inject_ok = r.returncode == 0
            if r.returncode != 0:
                log(f"[ERROR] fail-slow inject rc={r.returncode}: "
                    f"{(r.stdout + r.stderr).strip()[:160]}", self.orch_log)
        elif self.args.fault_type == "media_error":
            log(f"INJECT media/hard error: {self.err_bdev} sct={self.args.media_sct} "
                f"sc={self.args.media_sc} n={self.args.media_count} (path {self.path}) "
                f"onset_unix={self.onset_unix:.3f}", self.orch_log)
            r = self._ssh(self._media_remote(self.args.media_count))
            self.inject_ok = r.returncode == 0
            if r.returncode != 0:
                log(f"[ERROR] media inject rc={r.returncode}: "
                    f"{(r.stdout + r.stderr).strip()[:160]}", self.orch_log)
            self._topup_thread = threading.Thread(target=self._topup_loop, daemon=True)
            self._topup_thread.start()
        elif self.args.fault_type == "ana_inaccessible":
            log(f"INJECT ANA hard-state: path {self.path} listener {self.port} "
                f"-> inaccessible onset_unix={self.onset_unix:.3f}", self.orch_log)
            r = self._ssh(self._ana_remote("inaccessible"))
            self.inject_ok = r.returncode == 0
            if r.returncode != 0:
                log(f"[ERROR] ANA inject rc={r.returncode}: "
                    f"{(r.stdout + r.stderr).strip()[:160]}", self.orch_log)

    def _topup_loop(self) -> None:
        # bdev_error_inject_nvme_error -n N is a countdown budget; top it up so the
        # fault is truly SUSTAINED for the whole window regardless of path B IOPS.
        while not self._stop_topup.wait(self.args.media_topup_interval_s):
            try:
                self._ssh(self._media_remote(self.args.media_count), timeout=15)
            except Exception:
                pass

    def restore(self) -> None:
        if self.args.fault_type in ("failslow", "common_mode"):
            log(f"RESTORE {self.args.fault_type}: {len(self.specs)} bdev path(s) "
                f"-> {self.restore_latency_us}us",
                self.orch_log)
            r = self._ssh(self._failslow_remote(self.restore_latency_us))
            if r.returncode != 0:
                log(f"[WARN] fail-slow restore rc={r.returncode}: "
                    f"{(r.stdout + r.stderr).strip()[:160]}", self.orch_log)
        elif self.args.fault_type == "media_error":
            self._stop_topup.set()
            if self._topup_thread:
                self._topup_thread.join(timeout=5)
            log(f"RESTORE media error: clear {len(self.specs)} error bdev(s)", self.orch_log)
            commands = [
                f"sudo {RPCPY} -s {sock} bdev_error_inject_error "
                f"{err_bdev} clear failure"
                for _tid, _path, _port, sock, _delay_bdev, err_bdev in self.specs
            ]
            r = self._ssh(self._parallel_remote(commands))
            if r.returncode != 0:
                log(f"[WARN] media clear rc={r.returncode}: "
                    f"{(r.stdout + r.stderr).strip()[:160]}", self.orch_log)
        elif self.args.fault_type == "ana_inaccessible":
            log(f"RESTORE ANA hard-state: path {self.path} listener {self.port} "
                "-> optimized", self.orch_log)
            r = self._ssh(self._ana_remote("optimized"))
            if r.returncode != 0:
                log(f"[WARN] ANA restore rc={r.returncode}: "
                    f"{(r.stdout + r.stderr).strip()[:160]}", self.orch_log)


# ── measurement arm (competitor sidecar OR native passive collector) ───────────


def is_passive_collector(args) -> bool:
    """The measurement process is a passive --observe-only collector for the
    native arms (saps_q/stock_qd), and for ANY arm on a fault-free calibration
    run (so a competitor detector cannot reroute a path and depress the measured
    fault-free aggregate C)."""
    return args.arm not in SIDECAR_ARMS or args.fault_type == "none"


def build_arm_cmd(args, out_dir: Path, arm_out: Path, fault_unix: float,
                  duration_s: float, measure_start_s: float,
                  measure_end_s: float) -> List[str]:
    """Command line for the measurement process.  Competitor arms under a fault
    run the real mutating sidecar; native arms and all calibration runs use the
    per-path-adaptive entry point --observe-only as a passive collector."""
    passive = is_passive_collector(args)
    entry = NATIVE_COLLECTOR_ENTRY if passive else SIDECAR_ENTRY[args.arm]

    cmd = [
        "python3", str(SCRIPT_DIR / entry),
        "--num-tenants", str(args.num_tenants),
        "--weights", args.weights,
        "--bdev-name", "mpn1",
        "--controller-name", "mp",
        "--spdk-root", args.spdk_root,
        "--duration-s", f"{duration_s:.3f}",
        "--interval-s", str(args.interval_s),
        "--io-kind", "read",
        "--workload-label", args.workload_label,
        "--qd", str(args.qd),
        "--measure-start-s", f"{measure_start_s:.3f}",
        "--measure-end-s", f"{measure_end_s:.3f}",
        "--output-dir", str(arm_out),
    ]
    # explicit per-tenant socket mapping (t0..t{N-1})
    for i in range(args.num_tenants):
        cmd += ["--tenant", f"t{i}={SOCK_TMPL.format(i=i)}"]
    if args.reference_capacity_iops > 0:
        cmd += ["--reference-capacity-iops", str(args.reference_capacity_iops)]
    if args.fault_type != "none":
        cmd += ["--fault-at-unix", f"{fault_unix:.3f}",
                "--fault-path-id", path_label(args.fault_path)]

    if passive:
        # passive collector: never mutate anything under SAPS/stock/calibration.
        cmd += ["--observe-only", "--multipath-policy", "keep"]
    else:
        policy, selector = SIDECAR_POLICY[args.arm]
        cmd += ["--multipath-policy", policy]
        if selector:
            cmd += ["--selector", selector]
        # Keep the preferred path stable throughout the sustained-fault
        # measurement window; any probe happens only after the fault restore.
        cmd += ["--hold-down-s", str(args.hold_s + 5.0)]
        if args.arm == "fixed_threshold":
            cmd += ["--threshold-us", str(args.fixed_threshold_us)]
    return cmd


# ── native-arm reroute detection (derived from per_path_samples.csv) ───────────


def reroute_detection_from_csv(csv_path: Path, fault_path_id: str,
                               onset_unix: float, reroute_frac: float
                               ) -> dict:
    """Traffic-reroute detection for the native arms.  For each tenant, the
    reroute instant is the first post-onset sample where the faulted path's
    delivered IOPS falls below `reroute_frac` * its pre-onset mean.  This is the
    only host-observable steering signal for saps_q (per-IO steering, no disable
    RPC) and it reads back "never" for stock_qd (traffic stays on the bad path).

    NOTE (needs testbed calibration): for a fail-slow fault the faulted path's
    delivered IOPS also drops WITHOUT any reroute (each I/O just takes longer),
    so `reroute_frac` must be set below the natural fail-slow throughput floor to
    separate steering from mere slow-down.  The raw per-path timeseries is kept
    so the threshold can be recalibrated offline; this function reports the
    threshold it used.
    """
    pre: Dict[str, List[float]] = {}
    post: Dict[str, List[Tuple[float, float]]] = {}   # tenant -> [(t_unix, iops)]
    tenants: set = set()
    if csv_path.exists():
        with csv_path.open(newline="") as f:
            for row in csv.DictReader(f):
                if row.get("path_id") != fault_path_id:
                    continue
                tenant = row.get("tenant", "")
                tenants.add(tenant)
                try:
                    t_unix = float(row["timestamp_unix"])
                except (TypeError, ValueError, KeyError):
                    continue
                iops_raw = row.get("interval_iops")
                if iops_raw in (None, ""):
                    continue
                try:
                    iops = float(iops_raw)
                except ValueError:
                    continue
                if t_unix < onset_unix:
                    pre.setdefault(tenant, []).append(iops)
                else:
                    post.setdefault(tenant, []).append((t_unix, iops))

    per_tenant: Dict[str, dict] = {}
    latencies: List[float] = []
    for tenant in sorted(tenants):
        baseline_samples = pre.get(tenant, [])
        baseline = (sum(baseline_samples) / len(baseline_samples)
                    if baseline_samples else None)
        detected = None
        if baseline and baseline > 0:
            floor = reroute_frac * baseline
            for t_unix, iops in sorted(post.get(tenant, [])):
                if iops < floor:
                    detected = (t_unix - onset_unix) * 1000.0
                    break
        per_tenant[tenant] = {
            "detected": detected is not None,
            "detection_latency_ms": detected,
            "pre_fault_mean_iops": baseline,
            "reroute_floor_iops": (reroute_frac * baseline) if baseline else None,
        }
        if detected is not None:
            latencies.append(detected)

    never = [t for t, v in per_tenant.items() if not v["detected"]]
    return {
        "method": "traffic_reroute_delivered_iops_collapse",
        "reroute_frac": reroute_frac,
        "fault_path_id": fault_path_id,
        "per_tenant": per_tenant,
        "never_detected_tenants": never,
        "first_detection_latency_ms": min(latencies) if latencies else None,
        "worst_detection_latency_ms": (max(latencies)
                                       if latencies and not never else None),
        "calibration_note": (
            "fail-slow: set reroute_frac below the natural slow-down floor; "
            "raw per_path_samples.csv retained for offline recalibration"),
    }


# ── metrics harvest ────────────────────────────────────────────────────────────


def harvest_metrics(args, arm_out: Path, fault: SustainedFault,
                    planned_onset_unix: float) -> dict:
    """Unify detection + fair-share into one metrics object per run."""
    arm_metrics_path = arm_out / "metrics.json"
    arm_metrics = {}
    if arm_metrics_path.exists():
        try:
            arm_metrics = json.loads(arm_metrics_path.read_text())
        except Exception:
            arm_metrics = {}

    measurement = arm_metrics.get("measurement", {})
    fair_share = {
        "valid": measurement.get("valid"),
        "worst_tenant": measurement.get("worst_tenant"),
        "worst_tenant_fair_share": measurement.get("worst_tenant_fair_share"),
        "per_tenant": measurement.get("per_tenant"),
        "per_path": measurement.get("per_path"),
        "reference_capacity_iops": args.reference_capacity_iops or None,
    }

    onset_unix = fault.onset_unix if fault.onset_unix else planned_onset_unix
    if args.fault_type == "none":
        detection = {"source": "calibration", "note": "no fault injected"}
    elif args.arm in SIDECAR_ARMS:
        # Native preferred-path event produced by the mutating sidecar.
        detection = arm_metrics.get("detection", {})
        detection = {
            "source": "competitor_sidecar_preferred_path_event",
            "first_detection_latency_ms": detection.get("first_detection_latency_ms"),
            "worst_detection_latency_ms": detection.get("worst_detection_latency_ms"),
            "never_detected_tenants": detection.get("never_detected_tenants"),
            "false_positive_reroute_count_before_fault": detection.get(
                "false_positive_reroute_count_before_fault",
                detection.get("false_positive_disable_count_before_fault"),
            ),
            "per_tenant": detection.get("per_tenant"),
        }
    else:
        detection = reroute_detection_from_csv(
            arm_out / "per_path_samples.csv",
            path_label(args.fault_path), onset_unix, args.reroute_frac)
        detection["source"] = "harness_traffic_reroute"

    return {
        "arm": args.arm,
        "fault_type": args.fault_type,
        "fault_path": args.fault_path,
        "fault_path_id": path_label(args.fault_path),
        "onset_unix": onset_unix,
        "planned_onset_unix": planned_onset_unix,
        "onset_skew_s": (onset_unix - planned_onset_unix),
        "detection": detection,
        "fair_share": fair_share,
        "arm_output_dir": str(arm_out),
    }


def verify_preflight(arm_out: Path, args, orch_log) -> Optional[str]:
    """Confirm the pN<->port mapping the sidecar discovered matches the faulted
    port, so --fault-path-id points at the real bad path."""
    pf = arm_out / "preflight.json"
    if not pf.exists():
        return "preflight.json not written (collector failed before first poll)"
    try:
        data = json.loads(pf.read_text())
    except Exception as exc:
        return f"preflight.json unreadable: {exc}"
    want_label = path_label(args.fault_path)
    seen = {}
    for entry in data.get("paths", []):
        tenant = entry.get("tenant", "")
        pid = entry.get("path_id")
        key = entry.get("path_key", "")   # trtype|traddr|trsvcid
        port = key.rsplit("|", 1)[-1] if "|" in key else ""
        seen[(tenant, pid)] = port
    checked = []
    for tenant_id in range(args.num_tenants):
        tenant = f"t{tenant_id}"
        want_port = str(tenant_path(args, tenant_id, args.fault_path)[0])
        got_port = seen.get((tenant, want_label))
        if got_port and got_port != want_port:
            return (f"path-label mismatch: {tenant} fault path {args.fault_path} "
                    f"expects {want_label}=port {want_port} but preflight has "
                    f"{want_label}=port {got_port}")
        checked.append(got_port or want_port)
    log(f"preflight path mapping ok: {want_label} -> ports {checked}", orch_log)
    return None


# ── target setup ───────────────────────────────────────────────────────────────


def setup_arm1(args, orch_log) -> None:
    if args.topology == "e2":
        remote = (
            "sudo env PATH_A_PORT_BASE=4500 PATH_B_PORT_BASE=4600 "
            f"PATH_C_PORT_BASE=4700 bash {args.setup_arm1_script} {args.num_tenants}"
        )
    else:
        remote = f"sudo env SAPS_ANA_REPORTING=1 bash {args.setup_arm1_script}"
    log(f"arm-1 setup topology={args.topology}: {remote}", orch_log)
    r = subprocess.run(
        ["sudo", "-u", "aiden", "ssh", args.arm1_host,
         remote],
        capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        log(f"ERROR: arm-1 setup rc={r.returncode}: {r.stderr[:200]}", orch_log)
        raise RuntimeError("arm-1 setup failed")
    log("arm-1 setup ok", orch_log)


def enforce_target_path_capacities(args, orch_log) -> None:
    """Apply each aggregate path capacity to the corresponding target bdevs."""
    rpc = "/home/aiden/spdk/scripts/rpc.py"
    sockets = {
        "A": "/var/tmp/spdk_a.sock",
        "B": "/var/tmp/spdk_b.sock",
        "C": "/var/tmp/spdk_c.sock",
    }
    commands = []
    if args.topology == "single_nqn":
        for path, path_cap in zip(PATH_ORDER, args.path_capacities_iops_list):
            commands.append(
                (
                    path,
                    "shared",
                    [
                        "sudo", rpc, "-s", sockets[path],
                        "bdev_set_qos_limit", f"delay_{path}",
                        "--rw-ios-per-sec", str(path_cap),
                    ],
                )
            )
    else:
        total_weight = sum(args.weights_list)
        for path, path_cap in zip(PATH_ORDER, args.path_capacities_iops_list):
            assigned = 0
            for tenant_id, weight in enumerate(args.weights_list):
                if tenant_id == args.num_tenants - 1:
                    tenant_cap = path_cap - assigned
                else:
                    tenant_cap = max(
                        1,
                        int(round(path_cap * weight / total_weight)),
                    )
                    assigned += tenant_cap
                commands.append(
                    (
                        path,
                        f"t{tenant_id}",
                        [
                            "sudo", rpc, "-s", sockets[path],
                            "bdev_set_qos_limit", f"delay_t{tenant_id}_{path}",
                            "--rw-ios-per-sec", str(tenant_cap),
                        ],
                    )
                )

    remote_script = " && ".join(
        " ".join(remote_cmd) for _path, _owner, remote_cmd in commands
    )
    result = subprocess.run(
        [
            "sudo", "-u", "aiden", "ssh", args.arm1_host,
            remote_script,
        ],
        capture_output=True,
        text=True,
        timeout=max(30, len(commands) * 10),
    )
    if result.returncode != 0:
        detail = (result.stdout + result.stderr).strip()[-400:]
        raise RuntimeError(f"target QoS setup failed: {detail}")
    log(
        "target path capacities enforced: "
        + ",".join(
            f"{path}={cap}"
            for path, cap in zip(PATH_ORDER, args.path_capacities_iops_list)
        )
        + " aggregate IOPS",
        orch_log,
    )


# ── CLI ────────────────────────────────────────────────────────────────────────


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Sustained single-path fault harness for competitor / SAPS / "
                    "stock path-selection arms")
    p.add_argument("--arm", choices=ALL_ARMS, required=True,
                   help="measurement arm")
    p.add_argument("--fault-type",
                   choices=["failslow", "media_error", "ana_inaccessible",
                            "common_mode", "none"],
                   required=True,
                   help="failslow=bdev_delay_update_latency; "
                        "media_error=bdev_error_inject_nvme_error --sc 129; "
                        "ana_inaccessible=listener ANA hard-state transition; "
                        "common_mode=all three paths receive the same delay; "
                        "none=fault-free calibration run")
    p.add_argument("--fault-path", choices=PATH_ORDER, default="B",
                   help="which of the 3 paths carries the fault (default B)")
    p.add_argument(
        "--common-mode-paths",
        default=",".join(PATH_ORDER),
        help=(
            "comma-separated path subset degraded by common_mode; default A,B,C. "
            "Ignored for other fault types"
        ),
    )

    p.add_argument("--num-tenants", type=int, default=4)
    p.add_argument("--weights", type=str, default="1,1,1,1",
                   help="tenant weight CSV (one value => equal weights)")
    p.add_argument("--qd", type=int, default=32)
    p.add_argument(
        "--sample-rate",
        type=int,
        default=32,
        help="publish one completion observation per N commands",
    )
    p.add_argument("--duration", type=int, default=60,
                   help="total workload duration seconds")
    p.add_argument("--link-cap", type=int, default=200000,
                   help="SAPSQ_LINK_CAP_IOPS for saps_q plugin env")
    p.add_argument(
        "--path-capacities-iops",
        required=True,
        help=(
            "comma-separated provisioned capacities K_p for paths A/B/C; "
            "required so the SAPS arm does not infer capacity from the envelope"
        ),
    )
    p.add_argument(
        "--enforce-path-capacities",
        action="store_true",
        help="apply the provisioned aggregate K_p values as target-side QoS caps",
    )
    p.add_argument(
        "--fixed-threshold-us",
        type=float,
        default=500.0,
        help="absolute latency threshold for the fixed_threshold arm",
    )

    # timeline (all relative to measurement-clock start = collector launch)
    p.add_argument("--fault-at-s", type=float, default=20.0,
                   help="fault onset, seconds after collector start")
    p.add_argument("--hold-s", type=float, default=30.0,
                   help="seconds the fault is held before restore")
    p.add_argument("--settle-s", type=float, default=3.0,
                   help="skip this many seconds after onset before the "
                        "fair-share measurement window opens")
    p.add_argument("--measure-start-s", type=float, default=None,
                   help="override fair-share window start (rel. collector start)")
    p.add_argument("--measure-end-s", type=float, default=None,
                   help="override fair-share window end (rel. collector start)")

    p.add_argument("--reference-capacity-iops", type=float, default=0.0,
                   help="fault-free all-tenant aggregate C shared by every arm "
                        "in the panel; required for a certified fair-share")
    p.add_argument("--reroute-frac", type=float, default=0.25,
                   help="native-arm reroute threshold: faulted-path IOPS below "
                        "this fraction of its pre-onset mean = rerouted")

    p.add_argument("--slow-lat-us", type=int, default=5000,
                   help="fail-slow injected latency (default 5000us = D1)")
    p.add_argument(
        "--restore-lat-us",
        type=int,
        default=None,
        help=(
            "delay value restored after fail-slow/common-mode; default is "
            "topology-specific (single_nqn=27us, e2=0us)"
        ),
    )
    p.add_argument("--media-sct", type=int, default=2)
    p.add_argument("--media-sc", type=int, default=129,
                   help="NVMe status code (129=0x81 UNRECOVERED READ ERROR)")
    p.add_argument("--media-count", type=int, default=5_000_000,
                   help="error budget per top-up injection")
    p.add_argument("--media-topup-interval-s", type=float, default=1.0)

    p.add_argument("--interval-s", type=float, default=1.0,
                   help="collector poll interval")
    p.add_argument("--workload-label", type=str, default="randread_4k")
    p.add_argument("--target-ip", type=str, default=DEFAULT_TARGET_IP)
    p.add_argument("--arm1-host", type=str, default="arm-1")
    p.add_argument("--topology", choices=["single_nqn", "e2"],
                   default="single_nqn",
                   help="single_nqn=mptest ports 4430/31/32; e2=per-tenant "
                        "NQNs on port bases 4500/4600/4700")
    p.add_argument("--setup-arm1-script", type=str, default=None)
    p.add_argument("--no-setup-arm1", action="store_true",
                   help="assume the target is already in the mptest topology")
    p.add_argument("--spdk-root", type=str, default=DEFAULT_SPDK_ROOT)
    p.add_argument("--output-dir", type=str, required=True)
    p.add_argument("--dry-run", action="store_true",
                   help="print the plan; do not touch the testbed")

    args = p.parse_args(argv)

    if args.setup_arm1_script is None:
        args.setup_arm1_script = (
            DEFAULT_E2_SETUP_ARM1 if args.topology == "e2" else DEFAULT_SETUP_ARM1
        )

    args.weights_list = [int(w) for w in args.weights.split(",") if w.strip()]
    if len(args.weights_list) == 1:
        args.weights_list = args.weights_list * args.num_tenants
    if len(args.weights_list) != args.num_tenants:
        p.error(f"--weights needs 1 or {args.num_tenants} values")
    try:
        args.path_capacities_iops_list = [
            int(value)
            for value in args.path_capacities_iops.split(",")
            if value.strip()
        ]
    except ValueError:
        p.error("--path-capacities-iops must contain three integers")
    if (
        len(args.path_capacities_iops_list) != len(PATH_ORDER)
        or any(value <= 0 for value in args.path_capacities_iops_list)
    ):
        p.error("--path-capacities-iops must contain three positive values")
    if args.sample_rate <= 0:
        p.error("--sample-rate must be positive")
    if args.fixed_threshold_us <= 0:
        p.error("--fixed-threshold-us must be positive")

    args.common_mode_paths_list = [
        path.strip().upper()
        for path in args.common_mode_paths.split(",")
        if path.strip()
    ]
    if not args.common_mode_paths_list:
        p.error("--common-mode-paths must name at least one path")
    invalid_common_paths = [
        path for path in args.common_mode_paths_list if path not in PATH_ORDER
    ]
    if invalid_common_paths:
        p.error(
            f"--common-mode-paths contains unknown paths {invalid_common_paths}; "
            f"choices={PATH_ORDER}"
        )
    if len(set(args.common_mode_paths_list)) != len(args.common_mode_paths_list):
        p.error("--common-mode-paths must not contain duplicates")
    if args.restore_lat_us is not None and args.restore_lat_us < 0:
        p.error("--restore-lat-us must be non-negative")

    # default fair-share window: [onset+settle, restore-1s]
    if args.measure_start_s is None:
        args.measure_start_s = args.fault_at_s + args.settle_s
    if args.measure_end_s is None:
        args.measure_end_s = args.fault_at_s + args.hold_s - 1.0
    if args.fault_type == "none":
        # calibration: no fault; measure the same window position on a clean run.
        args.measure_end_s = min(args.measure_end_s, args.duration - 2.0)
    if args.measure_end_s <= args.measure_start_s:
        p.error("measurement window is empty; check --fault-at-s/--hold-s/--settle-s")
    if args.fault_at_s + args.hold_s > args.duration:
        p.error(f"--fault-at-s + --hold-s ({args.fault_at_s + args.hold_s}) "
                f"exceeds --duration ({args.duration})")
    max_core = BASE_CORE + args.num_tenants * CORES_PER_PROC - 1
    if max_core >= 96:
        p.error(f"need core {max_core}, max is 95; reduce --num-tenants")
    return args


def print_dry_run(args) -> None:
    print("\n=== DRY RUN — run_competitor_fault_experiment.py ===")
    print(f"  topology={args.topology}  arm={args.arm}  fault_type={args.fault_type}  "
          f"fault_path={args.fault_path} ({path_label(args.fault_path)}, "
          f"tenant0 port {tenant_path(args, 0, args.fault_path)[0]})")
    if args.fault_type == "common_mode":
        print(f"  common_mode_paths={args.common_mode_paths_list}")
    print(f"  tenants={args.num_tenants}  weights={args.weights}  qd={args.qd}  "
          f"duration={args.duration}s  target_ip={args.target_ip}")
    print(f"  timeline (rel. collector start):")
    print(f"    t=0            collector/sidecar start (healthy warmup)")
    print(f"    t={args.fault_at_s:g}s        fault onset (sustained)")
    print(f"    window        [{args.measure_start_s:g}s, {args.measure_end_s:g}s]  fair-share")
    print(f"    t={args.fault_at_s + args.hold_s:g}s        fault restore")
    print(f"  reference_capacity_iops={args.reference_capacity_iops or '(unset -> fair-share invalid)'}")
    print(
        "  target_path_capacities="
        f"{args.path_capacities_iops_list}"
        f" ({'enforced' if args.enforce_path_capacities else 'declared only'})"
    )

    print("\n  arm-1 setup:")
    print(f"    {'(skipped)' if args.no_setup_arm1 else args.setup_arm1_script}")

    print("\n  bdevperf tenants (single-NQN mptest, 3 paths):")
    for i in range(min(args.num_tenants, 2)):
        tp = TenantProc(i, args, Path(args.output_dir), None)
        print(f"    t{i}: {' '.join(tp.cmd_list())}")
        print(f"        attach: nqn={tp._nqn} ports "
              f"{[tenant_path(args, i, p)[0] for p in PATH_ORDER]} -> mpn1 (mp)")
        print(f"        policy: {tp.policy_cmd()[1]}")
    if args.num_tenants > 2:
        print(f"    ... ({args.num_tenants} tenants total)")

    print("\n  fault injection RPCs:")
    for c in SustainedFault(args, None).dry_run_commands():
        print(f"    {c}")

    fake_onset = 0.0
    cmd = build_arm_cmd(args, Path(args.output_dir),
                        Path(args.output_dir) / args.arm, fake_onset,
                        args.duration - 1, args.measure_start_s, args.measure_end_s)
    kind = "passive --observe-only collector (non-mutating)" \
        if is_passive_collector(args) \
        else "competitor sidecar (mutating)"
    print(f"\n  measurement arm — {kind}:")
    print(f"    {' '.join(cmd)}")
    print("\n=== end dry run ===\n")


# ── main orchestration ─────────────────────────────────────────────────────────


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.dry_run:
        print_dry_run(args)
        return 0

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    arm_out = out_dir / args.arm
    arm_out.mkdir(parents=True, exist_ok=True)
    orch_log = open(out_dir / "orchestrator.log", "w")

    log(f"=== competitor-fault harness start: arm={args.arm} "
        f"fault={args.fault_type} path={args.fault_path} ===", orch_log)

    procs = [TenantProc(i, args, out_dir, orch_log)
             for i in range(args.num_tenants)]
    fault = SustainedFault(args, orch_log)
    arm_proc = None
    planned_onset_unix = None
    completed = False
    stop_reason = "completed"

    def sigint(_sig, _frame):
        log("SIGINT — teardown", orch_log)
        try:
            fault.restore()
        except Exception:
            pass
        cleanup_all(procs)
        sys.exit(130)
    signal.signal(signal.SIGINT, sigint)

    try:
        if not args.no_setup_arm1:
            setup_arm1(args, orch_log)
        else:
            log("skip arm-1 setup (--no-setup-arm1)", orch_log)
        if args.enforce_path_capacities:
            enforce_target_path_capacities(args, orch_log)

        cleanup_all([])   # clear stale bdevperf state

        # launch tenants (coordinator first for saps_q's UDS ring)
        if args.arm in NATIVE_PLUGIN_ARMS:
            log("launch coordinator (t0)", orch_log)
            procs[0].start()
            if not procs[0].wait_socket(60):
                raise RuntimeError("coordinator socket timeout")
            time.sleep(3)
            for p in procs[1:]:
                p.start()
                time.sleep(0.2)
        else:
            for p in procs:
                p.start()
                time.sleep(0.2)

        for p in procs:
            if not p.wait_socket(120):
                raise RuntimeError(f"t{p.tid} RPC socket timeout")
            if not p.make_socket_readable():
                raise RuntimeError(f"t{p.tid} RPC socket permission setup failed")

        # setup controllers in parallel
        log("setup controllers", orch_log)
        setup_ok: Dict[int, bool] = {}

        def _do_setup(pp):
            setup_ok[pp.tid] = pp.setup_controllers()
        threads = [threading.Thread(target=_do_setup, args=(p,), daemon=True)
                   for p in procs]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=200)
        failed = [tid for tid in range(args.num_tenants) if not setup_ok.get(tid)]
        if failed:
            raise RuntimeError(f"controller setup failed for t={failed}")
        log("all tenant controllers ready", orch_log)

        # start the workload
        log(f"perform_tests ({args.duration}s) x {args.num_tenants}", orch_log)
        test_procs = [(p, p.run_perform_tests()) for p in procs]

        # measurement clock starts now; schedule fault + restore relative to it
        clock0 = time.time()
        planned_onset_unix = clock0 + args.fault_at_s
        log(f"measurement clock start unix={clock0:.3f}; "
            f"planned onset unix={planned_onset_unix:.3f}", orch_log)

        # launch the measurement arm (needs healthy warmup before onset)
        arm_cmd = build_arm_cmd(args, out_dir, arm_out, planned_onset_unix,
                                duration_s=args.duration - args.settle_s,
                                measure_start_s=args.measure_start_s,
                                measure_end_s=args.measure_end_s)
        arm_log = open(arm_out / "arm_stdout.log", "w")
        log(f"launch measurement arm: {' '.join(arm_cmd)}", orch_log)
        arm_proc = subprocess.Popen(arm_cmd, stdout=arm_log, stderr=arm_log)

        # preflight path-mapping verification (after collector's first poll)
        time.sleep(max(3.0, args.interval_s * 2 + 1))
        preflight_path = arm_out / "preflight.json"
        preflight_deadline = time.time() + 15.0
        while (
            not preflight_path.exists()
            and time.time() < preflight_deadline
            and arm_proc.poll() is None
        ):
            time.sleep(0.5)
        mismatch = verify_preflight(arm_out, args, orch_log)
        if mismatch:
            raise RuntimeError(mismatch)

        # fault onset + restore timers
        if args.fault_type != "none":
            def _inject():
                delay = planned_onset_unix - time.time()
                if delay > 0:
                    time.sleep(delay)
                fault.inject()

            def _restore():
                delay = (clock0 + args.fault_at_s + args.hold_s) - time.time()
                if delay > 0:
                    time.sleep(delay)
                fault.restore()
            it = threading.Thread(target=_inject, daemon=True)
            rt = threading.Thread(target=_restore, daemon=True)
            it.start()
            rt.start()
        else:
            log("calibration run: no fault injected", orch_log)
            it = rt = None

        # wait for the workload to finish
        for p, tp in test_procs:
            try:
                tp.wait(timeout=args.duration + 90)
            except subprocess.TimeoutExpired:
                log(f"WARN: t{p.tid} perform_tests timeout", orch_log)
                tp.kill()
        log("perform_tests done", orch_log)

        if args.fault_type != "none":
            if it:
                it.join(timeout=25)
            if fault.inject_ok is not True:
                raise RuntimeError("fault injection RPC did not complete successfully")

        # ensure fault restored even if the timer thread lagged
        if args.fault_type != "none":
            if rt:
                rt.join(timeout=10)
            fault.restore()

        # let the arm finish its own duration and flush metrics.json
        if arm_proc:
            try:
                arm_returncode = arm_proc.wait(timeout=args.duration + 60)
            except subprocess.TimeoutExpired:
                log("WARN: measurement arm did not exit; terminating", orch_log)
                arm_proc.terminate()
                try:
                    arm_returncode = arm_proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    arm_proc.kill()
                    arm_returncode = arm_proc.wait(timeout=15)
            if arm_returncode != 0:
                raise RuntimeError(
                    f"measurement arm exited rc={arm_returncode}; "
                    f"see {arm_out / 'arm_stdout.log'}"
                )
        arm_log.close()
        completed = True

    except Exception as exc:
        stop_reason = f"error: {exc}"
        log(f"ERROR: {exc}", orch_log)
        try:
            fault.restore()
        except Exception:
            pass
    finally:
        # perform_tests has already returned (or timed out) above.  The
        # bdevperf apps themselves intentionally remain resident because they
        # were launched with --wait-for-rpc, so waiting 20 seconds per tenant
        # here can never add validation and makes a T=8 cell idle for 160s.
        # Parse the completed RPC result, then let cleanup_all terminate the
        # resident apps immediately.
        results = [p.parse_result() for p in procs]
        cleanup_all(procs)
        if args.arm in NATIVE_PLUGIN_ARMS:
            sampler_path = Path("/tmp/e2_sampler.csv")
            if sampler_path.exists():
                shutil.copy2(sampler_path, out_dir / "controller_sampler.csv")

    onset_ref = planned_onset_unix if planned_onset_unix else time.time()
    run_metrics = harvest_metrics(args, arm_out, fault, onset_ref)
    (out_dir / "metrics.json").write_text(json.dumps(run_metrics, indent=2) + "\n")

    manifest = {
        "schema_version": 1,
        "harness": "run_competitor_fault_experiment.py",
        "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "completed": completed,
        "stop_reason": stop_reason,
        "arm": args.arm,
        "arm_kind": ("competitor_sidecar" if args.arm in SIDECAR_ARMS
                     else "native_passive_collector"),
        "fault_type": args.fault_type,
        "fault_path": args.fault_path,
        "fault_path_id": path_label(args.fault_path),
        "common_mode_paths": (
            args.common_mode_paths_list if args.fault_type == "common_mode" else None
        ),
        "restore_latency_us": fault.restore_latency_us,
        "topology": {
            "mode": args.topology,
            "nqn": (NQN if args.topology == "single_nqn"
                    else f"{E2_NQN_PREFIX}{{tenant_id}}"),
            "target_ip": args.target_ip,
            "paths_tenant0": {
                p: {
                    "port": tenant_path(args, 0, p)[0],
                    "delay_bdev": tenant_path(args, 0, p)[2],
                    "error_bdev": tenant_path(args, 0, p)[3],
                }
                for p in PATH_ORDER
            },
            "bdev_name": "mpn1",
            "controller_name": "mp",
            "ana_reporting": args.topology == "single_nqn",
        },
        "num_tenants": args.num_tenants,
        "weights": args.weights_list,
        "path_capacities_iops": args.path_capacities_iops_list,
        "path_capacities_enforced": args.enforce_path_capacities,
        "fixed_threshold_us": (
            args.fixed_threshold_us if args.arm == "fixed_threshold" else None
        ),
        "qd": args.qd,
        "sample_rate": args.sample_rate,
        "duration_s": args.duration,
        "experiment_contract": ({
            "health_source": SIGNAL_ISOLATION_ARMS[args.arm],
            "runtime_health_source": (
                "completion"
                if args.arm == "saps_reachability"
                else SIGNAL_ISOLATION_ARMS[args.arm]
            ),
            "allocator": "HCAA",
            "selector": "committed_budget",
            "completion_fsm_enabled": args.arm == "saps_completion",
            "classifier_bypassed": args.arm == "saps_reachability",
        } if args.arm in SIGNAL_ISOLATION_ARMS else None),
        "provenance": {
            "git": git_provenance(),
            "bdevperf_sha256": sha256_file(Path(BDEVPERF)),
            "dpa_firmware_sha256": sha256_file(
                REPO_ROOT /
                "dpa-smart-initiator/flexio_build/samples/build/"
                "dpa_plugin/dev/dpa_plugin_app.a"
            ),
            "harness_sha256": sha256_file(Path(__file__).resolve()),
        },
        "timeline": {
            "fault_at_s": args.fault_at_s,
            "hold_s": args.hold_s,
            "restore_at_s": args.fault_at_s + args.hold_s,
            "measure_start_s": args.measure_start_s,
            "measure_end_s": args.measure_end_s,
            "planned_onset_unix": planned_onset_unix,
            "actual_onset_unix": fault.onset_unix,
            "onset_skew_s": ((fault.onset_unix - planned_onset_unix)
                             if fault.onset_unix and planned_onset_unix else None),
        },
        "reference_capacity_iops": args.reference_capacity_iops or None,
        "reroute_frac": args.reroute_frac,
        "per_tenant_bdevperf_iops": results,
        "arm_output_dir": str(arm_out),
        "cmdline": " ".join([sys.executable, *sys.argv]),
        "flags_for_testbed_verification": [
            "socket permissions / live SPDK RPC schema on the 3-path topology",
            "preferred-path reroute + preferred-path/policy restoration (competitor arms)",
            "saps_q coordinator/tenant UDS ring on the single-NQN topology (unverified here)",
            "media_error: whether the injected NVMe status causes a target-reported "
            "ANA transition (kernel-ANA only reacts to ANA, not to a bare media status)",
            "ana_inaccessible: listener must report optimized->inaccessible and restore optimized",
            "fail-slow reroute_frac calibration for native-arm detection",
            "onset_skew_s should be < 0.5s; larger => investigate scheduling",
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    log(f"=== done: metrics.json + manifest.json in {out_dir} ===", orch_log)
    orch_log.close()

    print(f"\nRun complete. Out dir: {out_dir}")
    print(f"  arm={args.arm}  fault={args.fault_type}/{args.fault_path}")
    det = run_metrics["detection"]
    print(f"  detection: {det.get('first_detection_latency_ms')} ms (first) "
          f"[{det.get('source')}]")
    fs = run_metrics["fair_share"]
    print(f"  worst-tenant fair-share: {fs.get('worst_tenant_fair_share')} "
          f"(valid={fs.get('valid')})")
    if args.fault_type == "none" and fs.get("per_tenant"):
        agg = sum(v.get("mean_iops") or 0.0 for v in fs["per_tenant"].values())
        print(f"  CALIBRATION aggregate delivered IOPS (window) = {agg:.0f}  "
              f"=> pass as --reference-capacity-iops")
    return 0 if completed else 2


if __name__ == "__main__":
    raise SystemExit(main())
