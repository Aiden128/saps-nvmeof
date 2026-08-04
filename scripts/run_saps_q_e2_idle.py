#!/usr/bin/env python3
"""
run_saps_q_e2_idle.py — SAPS-Q E2 work-conserving idle tenant orchestrator

E2 設計(specs/dm-research-redesign-20260522.md §5 E2):
  4 tenants weight=[3,1,1,1], 3 paths, link_cap=200K IOPS
  Phase 1 (t=0-20s) : 所有 4 tenants active → expected [100K, 33K, 33K, 33K]
  Phase 2 (t=20-40s): tenant 1 idle → saps_q 應 redistribute → [120K, 0, 40K, 40K]
  Phase 3 (t=40-60s): tenant 1 復工 → 應回到 [100K, 33K, 33K, 33K]

比較組:
  saps_q      — SAPSQ_ENABLED=1, 動態分配;work conservation 應讓 active 三人
                充分利用 link_cap
  host_saps   — HOST_SAPS_ENABLED=1, DPA init disabled;host-side classifier
                standalone per bdevperf process, no coordinator/tenant UDS ring
  m3_v2_static — 舊 M4 host token bucket (static rates),沒 work conservation,
                Phase 2 只剩 ~60% utilization
  stock_qd     — 無 QoS,pure bdevperf saturation;用作 sanity baseline

Idle 機制:
  SIGSTOP/SIGCONT via os.kill()。
  bdevperf 以 root 啟動故需 sudo kill;本 orchestrator 用
  subprocess.run(["sudo","kill","-STOP",str(pid)]) 發送。

  SIGSTOP 選擇理由:
    - SIGSTOP pause → bdevperf IO 停止,DPA scheduler epoch 偵測 demand 下降
    - 比 cgroup freeze (需 systemd cgroup v2 + root setup) 更輕量
    - 比 taskset --cpu-list 99 更確定(bdevperf core 已 bind,mid-run taskset -p
      race condition)
    - pending IO 在 SIGSTOP 期間被 RDMA NIC 緩衝;SIGCONT 後 bdevperf 繼續 inflight

  Fallback 記錄(如果 SIGSTOP 在 bdevperf 上不 work):
    症狀: bdevperf log 顯示 IO 繼續完成 / DPA 端 demand_iops[idle_tid] 沒降
    備案 1: spdk RPC `bdev_set_qos_limit rw_ios_per_sec=0` on bdevperf local socket
            → 限制 IO submission rate 到 0;恢復用 `bdev_set_qos_limit rw_ios_per_sec=-1`
    備案 2: taskset -p <cpu_mask_invalid> <pid> 給 bdevperf 分配無 IO 的 CPU
            (適合 SIGSTOP 因 ptrace 被 block 的情境)

用法:
  python3 scripts/run_saps_q_e2_idle.py \\
      --mode saps_q --weights 3,1,1,1 --link-cap 200000 \\
      --idle-tenant 1 --idle-start 20 --idle-end 40 --duration 60 \\
      --output-dir experiments/sapsq/e2_idle_test/rep0

  # dry-run(印 commands + envs,不跑 testbed):
  python3 scripts/run_saps_q_e2_idle.py \\
      --mode saps_q --dry-run \\
      --output-dir /tmp/e2_dry

Refactored from: run_m0_multiclient_tenants.py (N=4 framework) +
                 run_sapsq.py (SAPS-Q env assembly + idle SIGSTOP logic)
"""

import argparse
import csv
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
RAW_RPC = str(Path(__file__).with_name("spdk_rpc_raw.py"))

NQN_PREFIX = "nqn.2024-01.io.spdk:tenant"
TARGET_IP = "10.0.0.1"

# 每 tenant 有 3 個 path:port = TENANT_BASE_PORT + tenant_id + PATH_PORT_OFFSETS[path_idx]
# 新方案 (setup_arm1_sapsq_Nt3p.sh): base 4500/4600/4700, stride 100 between paths.
TENANT_BASE_PORT = 4500
PATH_PORT_OFFSETS = [0, 100, 200]   # path A/B/C (stride 100, N≤99 safe)

# arm-1 multipath setup script (N tenant × 3 path topology, parameterized)
SETUP_ARM1_DEFAULT = (
    "/home/aiden/DPA/nvme-of-controller/experiments/"
    "3path_targets/setup_arm1_sapsq_Nt3p.sh"
)
TGT_SOCK = "/var/tmp/spdk_tenants.sock"

BASE_CORE = 4
CORES_PER_PROC = 2
SOCK_TMPL = "/var/tmp/bdevperf_sapsq_proc{i}.sock"

# ── launch-race 防護常數 ───────────────────────────────────────────────────────
# rep7 (N=10) 觀察到的 launch-race:perform_tests RPC 在啟動瞬間 (~3s) 即退出,
# 不產任何 IO/result,但 orchestrator 仍把該 run 記成 complete 帶 None IOPS。
# 偵測條件 (任一成立 → INVALID):
#   (a) 任一 tenant parse 出的 IOPS 為 None 或 0
#   (b) perform_tests phase wall-time < duration * MIN_WALLTIME_FRACTION
#       (正常應跑滿 duration;<50% 代表 RPC 在 launch 即退出)
# INVALID 時 tear down 整個 rep 並重跑,最多 MAX_RUN_ATTEMPTS 次。
# 設 MAX_RUN_ATTEMPTS=1 即可完全還原舊行為 (不重試),故對既有 caller 相容。
MAX_RUN_ATTEMPTS = 3            # 1 次正常 + 2 次重試
MIN_WALLTIME_FRACTION = 0.5     # perform_tests wall-time 低於 duration*此值 → launch-race

# UDS socket path — coordinator 監聽,tenant 連接 (與 e1_coordinator 共用路徑)
DPA_PLUGIN_SOCK = "/tmp/dpa_plugin_e1.sock"
ARM1_SSH_CONTROL = "/tmp/sapsq_arm1_rpc_%r_%h_%p"

# Mode 名稱 mapping: 本 orchestrator CLI 名稱 → run_sapsq 內部 mode
_MODE_MAP = {
    "saps_q":       "sapsq",
    "host_saps":    "host_saps",
    "m3_v2_static": "m4_static",
    "stock_qd":     "stock",
}

# ── 工具函數 ─────────────────────────────────────────────────────────────────


def ts_str():
    return time.strftime("%H:%M:%S")


def log(msg, file=None):
    line = f"[{ts_str()}] [e2_orch] {msg}"
    print(line, flush=True)
    if file:
        print(line, file=file, flush=True)


def raw_rpc_command(sock: str, method: str, params=None,
                    timeout_s: float = 60.0) -> list[str]:
    """Build a root RPC command without importing the SPDK Python package."""
    cmd = [
        "sudo", "-n", "python3", RAW_RPC,
        "-s", sock,
        "--timeout", str(timeout_s),
        method,
    ]
    if params is not None:
        cmd.extend([
            "--params-json",
            json.dumps(params, separators=(",", ":"), sort_keys=True),
        ])
    return cmd


def detect_tsc_hz() -> int:
    """arm64 TSC 頻率偵測:讀 /proc/cpuinfo 的 CPU MHz 或 BogoMIPS 推算。
    BF3 aarch64 cntfrq_el0 = 1 GHz;若讀不到 fallback 1e9。
    host-side 無法直接 mrs cntfrq_el0,改讀 /proc/cpuinfo 的 CPU MHz。
    """
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("CPU MHz") or line.startswith("cpu MHz"):
                    mhz = float(line.split(":")[1].strip())
                    return int(mhz * 1_000_000)
    except Exception:
        pass
    return 1_000_000_000  # BF3 aarch64 fallback


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


# ── env var 組裝 ─────────────────────────────────────────────────────────────


def build_env_saps_q(tenant_id: int, n_tenants: int, n_paths: int,
                      link_cap: int, weights: list,
                      path_caps: list[int] | None = None,
                      bypass_d: str = "0") -> list:
    """SAPS-Q mode:SAPSQ_ENABLED=1 → 走 sapsq_m_init() 新 2D enforcement path。

    E2 coordinator/tenant 修正 (Lane AA):
      tenant_id == 0: DPA_PLUGIN_ROLE=coordinator
        → dpa_plugin_init() 完整 path + alloc_ring_memfd + UDS server
      tenant_id > 0:  DPA_PLUGIN_ROLE=tenant
        → uds_client_get_memfd() attach coordinator ring,不建 DPA process

    修正內容 (vs Lane W broken version):
      1. DPA_PLUGIN_ROLE: coordinator(proc 0) / tenant(proc 1-3);非 standalone
      2. DPA_PLUGIN_SAMPLE_RATE: hardcode 1;非 env default 64 (event density 差 64×)
      3. SAPSQ_BYPASS_SAPS_FSM=1: E2 skip SAPS B7 FSM cold-start degrade (同 e1)
      4. DPA_PLUGIN_SOCK: 所有 proc 設同一 UDS path → coordinator 監聽 tenant 連接
      5. DPA_PLUGIN_DEV: 從外部 DPA_PLUGIN_DEV env var 繼承(mlx5_0 / mlx5_1 lane)
         sudo taskset ... env ... 命令行傳遞確保 sudo 不丟失此 var

    env 對應 dpa_plugin.c:sapsq_m_init() (line 1250-1371):
      SAPSQ_ENABLED          → gates sapsq_m_init()
      SAPSQ_MY_TENANT_ID     → per-proc tenant id (0/1/2/3)
      SAPSQ_NUM_TENANTS      → sapsq_num_tenants
      SAPSQ_NUM_PATHS        → sapsq_num_paths
      SAPSQ_LINK_CAP_IOPS    → namespace service envelope
      SAPSQ_PATH_CAP_IOPS    → per-path deliverable capacities
      SAPSQ_EPOCH_PERIOD_US  → sapsq_epoch_period_us (1ms scheduler tick)
      SAPSQ_WEIGHTS          → sapsq_tenant_weight[] (plain int list,不做 Q16.16)
      SAPSQ_PROBE_RATE_IOPS  → probe_rate_budget_q32 換算用(host plugin 內部算 Q32)
      SAPSQ_HOST_TSC_FREQ    → sapsq_host_tsc_freq(DPA scheduler tick period 換算必須)

    舊 SAPS_Q_* envs(Q16/Q32 encoded)屬於舊 sapsq_epoch_commit 1D path,
    sapsq_m_init() 完全不讀它們,已移除。
    """
    tsc_hz = detect_tsc_hz()
    fsm_bypass = os.environ.get("SAPSQ_BYPASS_SAPS_FSM", "1")
    coupling_mode = os.environ.get("SAPSQ_HEALTH_COUPLING_MODE", "")
    active_probe = bypass_d == "0" and fsm_bypass == "0"
    # DPA_PLUGIN_DEV: 從外部 env 繼承(DPA_PLUGIN_DEV=mlx5_0 sudo -E python3 ...)
    # sudo 不自動傳遞自訂 env var,必須顯式加入 env list 確保 bdevperf 能拿到
    dpa_dev = os.environ.get("DPA_PLUGIN_DEV", "")
    if path_caps is None:
        raise ValueError(
            "SAPS-Q requires explicit per-path deliverable capacities K_p"
        )
    if len(path_caps) < n_paths or any(cap <= 0 for cap in path_caps[:n_paths]):
        raise ValueError(
            f"SAPS-Q requires {n_paths} positive path capacities, got {path_caps}"
        )

    if tenant_id == 0:
        # Coordinator: 完整 dpa_plugin_init() + alloc_ring_memfd + UDS server
        env = [
            "DPA_PLUGIN_ROLE=coordinator",
            f"DPA_PLUGIN_SOCK={DPA_PLUGIN_SOCK}",
            "DPA_PLUGIN_DISABLE_INIT=0",
            "HOST_SAPS_ENABLED=0",
            f"DPA_PLUGIN_SAMPLE_RATE={os.environ.get('SAPS_SAMPLE_RATE', '1')}",
            "SAPS_TRACE=0",
            "SAPSQ_ENABLED=1",
            f"SAPSQ_MY_TENANT_ID={tenant_id}",
            f"SAPSQ_NUM_TENANTS={n_tenants}",
            f"SAPSQ_NUM_PATHS={n_paths}",
            f"SAPSQ_LINK_CAP_IOPS={link_cap}",
            f"SAPSQ_PATH_CAP_IOPS={csv_str(path_caps)}",
            "SAPSQ_EPOCH_PERIOD_US=1000",
            f"SAPSQ_WEIGHTS={csv_str(weights)}",
            "SAPSQ_PROBE_RATE_IOPS=1000",
            f"SAPSQ_HOST_TSC_FREQ={tsc_hz}",
            f"SAPSQ_BYPASS_D_CLASSIFIER={bypass_d}",
            # FSM bypass is env-configurable (default 1 = bypass, the fairness-test
            # behaviour). The integrated smart-QoS demo sets SAPSQ_BYPASS_SAPS_FSM=0
            # so the health FSM is LIVE during multi-tenant QoS — health_factor then
            # shrinks a degraded path's effective capacity and max-min reallocates.
            f"SAPSQ_BYPASS_SAPS_FSM={fsm_bypass}",
            (
                "SAPSQ_BYPASS_HEALTH_COUPLING="
                f"{os.environ.get('SAPSQ_BYPASS_HEALTH_COUPLING', '0')}"
            ),
            (
                "SAPSQ_BYPASS_BUDGET_SELECTION="
                f"{os.environ.get('SAPSQ_BYPASS_BUDGET_SELECTION', '0')}"
            ),
            f"SAPS_SELECTOR_TRACE={os.environ.get('SAPS_SELECTOR_TRACE', '0')}",
            (
                "SAPS_SELECTOR_TRACE_STRIDE="
                f"{os.environ.get('SAPS_SELECTOR_TRACE_STRIDE', '100000')}"
            ),
            f"SAPS_M2_ENABLED={os.environ.get('SAPS_M2_ENABLED', '0')}",
            f"SAPS_M2_V2_CLASSIFIER={os.environ.get('SAPS_M2_V2_CLASSIFIER', '0')}",
            "SAPS_Q_ENABLED=0",
            "SAPS_M4_ENABLED=0",
            f"SAPS_M5_DRR_ENABLED={os.environ.get('SAPS_M5_DRR_ENABLED', '0')}",
            # M5 DRR weighted rates: feed the SAME weights + link cap as the SAPS-Q
            # 2D bucket so M5 enforces the identical 3:1:1:1 split (benign co-enforce,
            # not a conflicting second schedule). Without these M5 falls back to equal
            # weights → equal grants [128,128,128,128] (the bug seen in the first demo).
            f"SAPS_M5_WEIGHTS={csv_str(weights)}",
            f"SAPS_M5_LINK_IOPS={link_cap}",
            # Coordinator is tenant 0; see the tenant branch for why this must be explicit.
            f"SAPS_M5_TENANT_ID={tenant_id}",
            # Drain-stall diagnostic: enable the built-in 1 Hz sampler (no rebuild
            # needed) so we can see whether dpa_consumed keeps up with producer_idx
            # or the DPA ring drain stalls (ring_lag grows) during steady state.
            "DPA_PLUGIN_SAMPLER_CSV=/tmp/e2_sampler.csv",
        ]
        if active_probe:
            env.append("SAPS_ACTIVE_PROBE=1")
        if coupling_mode:
            env.append(f"SAPSQ_HEALTH_COUPLING_MODE={coupling_mode}")
        if dpa_dev:
            env.append(f"DPA_PLUGIN_DEV={dpa_dev}")
        return env
    else:
        # Tenant (1/2/3): attach coordinator ring via UDS,不建 DPA process
        env = [
            "DPA_PLUGIN_ROLE=tenant",
            f"DPA_PLUGIN_SOCK={DPA_PLUGIN_SOCK}",
            "DPA_PLUGIN_DISABLE_INIT=0",
            "HOST_SAPS_ENABLED=0",
            f"DPA_PLUGIN_SAMPLE_RATE={os.environ.get('SAPS_SAMPLE_RATE', '1')}",
            "SAPS_TRACE=0",
            "SAPSQ_ENABLED=1",
            f"SAPSQ_MY_TENANT_ID={tenant_id}",
            f"SAPSQ_BYPASS_D_CLASSIFIER={bypass_d}",
            f"SAPSQ_BYPASS_SAPS_FSM={fsm_bypass}",
            (
                "SAPSQ_BYPASS_HEALTH_COUPLING="
                f"{os.environ.get('SAPSQ_BYPASS_HEALTH_COUPLING', '0')}"
            ),
            (
                "SAPSQ_BYPASS_BUDGET_SELECTION="
                f"{os.environ.get('SAPSQ_BYPASS_BUDGET_SELECTION', '0')}"
            ),
            f"SAPS_SELECTOR_TRACE={os.environ.get('SAPS_SELECTOR_TRACE', '0')}",
            (
                "SAPS_SELECTOR_TRACE_STRIDE="
                f"{os.environ.get('SAPS_SELECTOR_TRACE_STRIDE', '100000')}"
            ),
            f"SAPS_M2_ENABLED={os.environ.get('SAPS_M2_ENABLED', '0')}",
            f"SAPS_M2_V2_CLASSIFIER={os.environ.get('SAPS_M2_V2_CLASSIFIER', '0')}",
            "SAPS_Q_ENABLED=0",
            "SAPS_M4_ENABLED=0",
            f"SAPS_M5_DRR_ENABLED={os.environ.get('SAPS_M5_DRR_ENABLED', '0')}",
            # Each proc must tell M5 which tenant it is, or every proc reads tenant 0's
            # grant and the weighted split collapses to equal shares. Measured 2026-07-16:
            # without this, worst-tenant fair-share is 0.638 with the high-weight tenant
            # at 0.64 (a baseline-shaped result) instead of the weighted split.
            f"SAPS_M5_TENANT_ID={tenant_id}",
        ]
        if active_probe:
            env.append("SAPS_ACTIVE_PROBE=1")
        if coupling_mode:
            env.append(f"SAPSQ_HEALTH_COUPLING_MODE={coupling_mode}")
        return env


def effective_health_coupling_mode() -> str:
    mode = os.environ.get("SAPSQ_HEALTH_COUPLING_MODE", "")
    if mode:
        return mode
    if os.environ.get("SAPSQ_BYPASS_HEALTH_COUPLING", "0") == "1":
        return "fixed"
    return "continuous"


def build_env_m3_v2_static(tenant_id: int, link_cap: int, weights: list) -> list:
    """M3-v2 static 模式:SAPS_M4_ENABLED=1(舊 M4 host token bucket),靜態 per-tenant rate"""
    sample_rate = os.environ.get("DPA_PLUGIN_SAMPLE_RATE", "64")
    return [
        "DPA_PLUGIN_ROLE=standalone",
        "DPA_PLUGIN_DISABLE_INIT=0",
        "HOST_SAPS_ENABLED=0",
        f"DPA_PLUGIN_SAMPLE_RATE={sample_rate}",
        "SAPS_TRACE=0",
        "SAPS_Q_ENABLED=0",
        "SAPS_M4_ENABLED=1",
        f"SAPS_M4_LINK_IOPS={link_cap}",
        f"SAPS_M4_WEIGHTS={csv_str(weights)}",
        f"SAPS_M4_TENANT_ID={tenant_id}",
    ]


def build_env_host_saps(tenant_id: int) -> list:
    """host_saps: per-process host classifier; no DPA coordinator/tenant UDS ring."""
    return [
        "DPA_PLUGIN_ROLE=standalone",
        "DPA_PLUGIN_DISABLE_INIT=1",
        "HOST_SAPS_ENABLED=1",
        "SAPS_TRACE=0",
        "SAPS_Q_ENABLED=0",
        "SAPS_M4_ENABLED=0",
        "SAPS_M5_DRR_ENABLED=0",
    ]


def build_env_stock_qd() -> list:
    """stock 模式:DPA plugin 完全不 init"""
    return [
        "DPA_PLUGIN_DISABLE_INIT=1",
        "SAPS_Q_ENABLED=0",
        "SAPS_M4_ENABLED=0",
        "SAPS_M5_DRR_ENABLED=0",
    ]


def build_env(mode_cli: str, tenant_id: int, n_tenants: int, n_paths: int,
              link_cap: int, weights: list,
              path_caps: list[int] | None = None,
              bypass_d: str = "0") -> list:
    if mode_cli == "saps_q":
        return build_env_saps_q(tenant_id, n_tenants, n_paths,
                                 link_cap, weights, path_caps, bypass_d)
    elif mode_cli == "host_saps":
        return build_env_host_saps(tenant_id)
    elif mode_cli == "m3_v2_static":
        return build_env_m3_v2_static(tenant_id, link_cap, weights)
    elif mode_cli == "stock_qd":
        return build_env_stock_qd()
    else:
        raise ValueError(f"unknown mode: {mode_cli}")


# ── 單一 bdevperf process 管理 ───────────────────────────────────────────────


class TenantProcE2:
    """E2-專用 bdevperf process:4-tenant × 3-path multipath topology。"""

    def __init__(self, tenant_id: int, args, out_dir: Path, logfile):
        self.tid = tenant_id
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
        self._nqn = f"{NQN_PREFIX}{tenant_id}"
        self._cgroup_path = f"/sys/fs/cgroup/bdevperf_sapsq_t{tenant_id}"
        self._base_port = TENANT_BASE_PORT + tenant_id

    def _log(self, msg):
        log(f"[t{self.tid:02d}] {msg}", self.orch_log)

    def env_list(self) -> list:
        bypass_d = "1" if getattr(self.args, "bypass_d", False) else "0"
        return build_env(
            self.args.mode,
            self.tid,
            self.args.num_tenants,
            3,  # n_paths fixed at 3 for E2
            self.args.link_cap,
            [int(w) for w in self.args.weights.split(",")],
            self.args.path_caps,
            bypass_d,
        )

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
        """3 path attach commands (path A/B/C)"""
        cmds = []
        for p_idx in range(3):
            port = self._base_port + PATH_PORT_OFFSETS[p_idx]
            cmds.append(raw_rpc_command(
                self.sock,
                "bdev_nvme_attach_controller",
                {
                    "name": "mp",
                    "trtype": "rdma",
                    "traddr": TARGET_IP,
                    "trsvcid": str(port),
                    "adrfam": "ipv4",
                    "subnqn": self._nqn,
                    "multipath": "multipath",
                },
                timeout_s=90,
            ))
        return cmds

    def _cgroup_setup(self, pid: int):
        """cgroup v2 freeze cgroup を作成して bdevperf PID を登録する。
        SPDK reactor は SCHED_FIFO で動くため SIGSTOP が効かない。
        cgroup freeze は kernel レベルで全スレッドを確実に停止する。
        """
        try:
            os.makedirs(self._cgroup_path, exist_ok=True)
            # cgroup.procs に TGID を書く(all threads が同じ cgroup に入る)
            with open(f"{self._cgroup_path}/cgroup.procs", "w") as f:
                f.write(str(pid))
            self._log(f"cgroup setup: {self._cgroup_path} PID={pid}")
        except Exception as e:
            self._log(f"[WARN] cgroup setup failed: {e} — freeze will fallback to SIGSTOP")

    def _cgroup_teardown(self):
        """cgroup を削除する。プロセスを先に kill してから呼ぶこと。"""
        try:
            # まず freeze を解除してからでないと rmdir できない
            freeze_path = f"{self._cgroup_path}/cgroup.freeze"
            if os.path.exists(freeze_path):
                with open(freeze_path, "w") as f:
                    f.write("0")
            os.rmdir(self._cgroup_path)
        except Exception:
            pass

    def start(self):
        subprocess.run(["sudo", "rm", "-f", self.sock], capture_output=True)
        cmd = self.cmd_list()
        self._log(
            f"launching: mode={self.args.mode} nqn={self._nqn} "
            f"ports={[self._base_port + o for o in PATH_PORT_OFFSETS]}"
        )
        self.logfp = open(self.logpath, "w")
        self.proc = subprocess.Popen(cmd, stdout=self.logfp, stderr=self.logfp)
        self._log(f"PID={self.proc.pid}")
        self._cgroup_setup(self.proc.pid)

    def wait_socket(self, timeout=120) -> bool:
        for _ in range(timeout):
            if os.path.exists(self.sock):
                return True
            time.sleep(1)
        return False

    def setup_controllers(self) -> bool:
        try:
            r = subprocess.run(
                raw_rpc_command(
                    self.sock,
                    "bdev_nvme_set_options",
                    {"io_path_stat": True},
                    timeout_s=15,
                ),
                capture_output=True, text=True, timeout=20
            )
        except subprocess.TimeoutExpired:
            self._log("setup HUNG at step bdev_nvme_set_options (timeout=15s) "
                      "— attempt INVALID")
            return False
        if r.returncode != 0:
            self._log(
                "bdev_nvme_set_options failed: "
                f"{(r.stdout + r.stderr).strip()[:160]}"
            )
            return False
        try:
            r = subprocess.run(
                raw_rpc_command(
                    self.sock, "framework_start_init", timeout_s=30
                ),
                capture_output=True, text=True, timeout=35
            )
        except subprocess.TimeoutExpired:
            self._log("setup HUNG at step framework_start_init (timeout=30s) "
                      "— attempt INVALID")
            return False
        if r.returncode != 0:
            self._log(f"framework_start_init failed: {r.stderr[:120]}")
            return False
        time.sleep(1)

        for idx, cmd in enumerate(self.attach_cmds()):
            path = "ABC"[idx] if idx < 3 else str(idx)
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
            except subprocess.TimeoutExpired:
                self._log(f"setup HUNG at step attach path {path} (timeout=90s) "
                          "— attempt INVALID")
                return False
            self._log(f"attach: rc={r.returncode} "
                      f"{(r.stdout + r.stderr).strip()[:100]}")
        time.sleep(2)

        # multipath policy: saps_q/host_saps → plugin; others → active_active+round_robin
        # DIAG override (2026-05-29): SAPSQ_FORCE_RR=1 keeps admission token
        # bucket but swaps the per-IO plugin path-selection for stock RR, to
        # isolate whether the throughput ceiling is in path-selection or admission.
        use_plugin_policy = (
            self.args.mode == "host_saps"
            or (
                self.args.mode == "saps_q"
                and os.environ.get("SAPSQ_FORCE_RR") != "1"
            )
        )
        if use_plugin_policy:
            cmd = raw_rpc_command(
                self.sock,
                "bdev_nvme_set_multipath_policy",
                {"name": "mpn1", "policy": "plugin"},
                timeout_s=15,
            )
            label = "plugin"
        else:
            cmd = raw_rpc_command(
                self.sock,
                "bdev_nvme_set_multipath_policy",
                {
                    "name": "mpn1",
                    "policy": "active_active",
                    "selector": "round_robin",
                },
                timeout_s=15,
            )
            label = "active_active+round_robin"
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        except subprocess.TimeoutExpired:
            self._log(f"setup HUNG at step multipath-policy ({label}) "
                      "(timeout=15s) — attempt INVALID")
            return False
        self._log(f"multipath policy={label}: rc={r.returncode}")
        return True

    def run_perform_tests(self):
        cmd = [
            "sudo", "-n", "timeout", str(self.args.duration + 30),
            "python3", RAW_RPC,
            "-s", self.sock,
            "--timeout", str(self.args.duration + 15),
            "perform_tests",
        ]
        return subprocess.Popen(cmd, stdout=self.logfp, stderr=self.logfp)

    def _find_bdevperf_tgid(self) -> int:
        """bdevperf の TGID(= スレッドグループ leader の PID = 真の process PID)を返す。

        SPDK bdevperf は多スレッド。self.proc.pid は Popen が返した TID であり、
        bdevperf 起動後は reactor thread の TID になっている場合がある。
        SIGSTOP を TID に送っても 1 スレッドしか止まらない。

        正しい手順:
        1. /proc/<self.proc.pid>/status から Tgid を読む
           → Tgid = スレッドグループ leader PID = bdevperf の "真の" PID
        2. SIGSTOP を Tgid に送る → 全スレッドが停止する

        Fallback: /proc が読めない場合は self.proc.pid をそのまま使う。
        """
        try:
            status_path = f"/proc/{self.proc.pid}/status"
            with open(status_path) as f:
                for line in f:
                    if line.startswith("Tgid:"):
                        tgid = int(line.split()[1])
                        if tgid != self.proc.pid:
                            self._log(f"_find_bdevperf_tgid: TID={self.proc.pid} → Tgid={tgid}")
                        return tgid
        except Exception as e:
            self._log(f"_find_bdevperf_tgid: cannot read /proc/{self.proc.pid}/status: {e}")
        return self.proc.pid

    def sigstop(self):
        """Idle the tenant for Phase 2.  Prefer cgroup v2 freeze when the per-tenant
        cgroup is writable; otherwise fall back to SIGSTOP on the whole thread group
        (Tgid).  kill -STOP to the Tgid stops every reactor thread at the kernel
        level regardless of SCHED_FIFO — the earlier "SIGSTOP doesn't work" note only
        applied to single-TID signals, not to a Tgid-wide stop."""
        if not self.proc:
            return
        freeze_path = f"{self._cgroup_path}/cgroup.freeze"
        if os.path.exists(freeze_path):
            try:
                with open(freeze_path, "w") as f:
                    f.write("1")
                self._log(f"cgroup freeze=1 → bdevperf PID={self.proc.pid} frozen")
                return
            except Exception as e:
                self._log(f"[WARN] cgroup freeze failed: {e} — falling back to SIGSTOP")
        pids = self._real_bdevperf_pids()
        for pid in pids:
            subprocess.run(["sudo", "kill", "-STOP", str(pid)], capture_output=True)
        self._log(f"SIGSTOP → bdevperf pids={pids} stopped (Phase 2 idle)")

    def sigcont(self):
        """Resume the tenant (Phase 3): cgroup unfreeze, or kill -CONT on the Tgid."""
        if not self.proc:
            return
        freeze_path = f"{self._cgroup_path}/cgroup.freeze"
        if os.path.exists(freeze_path):
            try:
                with open(freeze_path, "w") as f:
                    f.write("0")
                self._log(f"cgroup freeze=0 → bdevperf PID={self.proc.pid} resumed")
                return
            except Exception as e:
                self._log(f"[WARN] cgroup unfreeze failed: {e} — falling back to SIGCONT")
        pids = self._real_bdevperf_pids()
        for pid in pids:
            subprocess.run(["sudo", "kill", "-CONT", str(pid)], capture_output=True)
        self._log(f"SIGCONT → bdevperf pids={pids} resumed (Phase 3)")

    def _real_bdevperf_pids(self) -> list:
        """Find the actual bdevperf process(es) for this tenant.  Launched under
        'sudo taskset env bdevperf -r <sock>', so self.proc.pid is the sudo wrapper,
        not bdevperf.  Identify by the unique -r socket path on the cmdline, then keep
        only processes whose comm is 'bdevperf' (excludes the sudo/taskset/env wrappers
        and any transient rpc.py touching the same socket)."""
        r = subprocess.run(["pgrep", "-f", self.sock], capture_output=True, text=True)
        pids = []
        for tok in r.stdout.split():
            try:
                pid = int(tok)
            except ValueError:
                continue
            try:
                comm = open(f"/proc/{pid}/comm").read().strip()
            except OSError:
                continue
            # SPDK renames the main thread, so don't match on comm=="bdevperf";
            # instead exclude the shell/sudo wrappers and rpc helpers.  taskset/env
            # exec-replace into bdevperf (same pid), so only 'sudo' lingers as a
            # separate parent matching the socket on its cmdline.
            if comm not in ("sudo", "taskset", "env", "rpc.py", "python3",
                            "python", "sh", "bash", "ssh"):
                pids.append(pid)
        return pids

    def delay_inject(self, arm1_host: str, latency_us: int):
        """Phase 2 idle simulation via bdev_delay_update_latency on arm-1.

        Injects a large read latency (default 10s) into delay_t{tid}_{P} for all 3
        paths (A/B/C) on arm-1 target side.  bdevperf outstanding IOs stall → no new
        IO submitted → host_submit_count[tid] stops incrementing → demand_iops[tid]
        EWMA decays to 0 within 1-2 scheduler epochs → saps_q progressive_fill skips
        this tenant → work-conserving redistribution to active tenants.

        Sockets: /var/tmp/spdk_a.sock, spdk_b.sock, spdk_c.sock on arm-1.
        bdev naming: delay_t{tid}_{P}  (P in A,B,C)
        RPC: bdev_delay_update_latency delay_t{tid}_{P} avg_read <latency_us>
        """
        RPC = "/home/aiden/spdk/scripts/rpc.py"
        socks = {
            "A": "/var/tmp/spdk_a.sock",
            "B": "/var/tmp/spdk_b.sock",
            "C": "/var/tmp/spdk_c.sock",
        }
        self._log(f"delay_inject: setting avg_read={latency_us}us on all 3 paths "
                  f"for delay_t{self.tid}_{{A,B,C}} via ssh {arm1_host}")
        errors = []
        for path, sock in socks.items():
            bdev = f"delay_t{self.tid}_{path}"
            cmd = (
                f"sudo {RPC} -s {sock} "
                f"bdev_delay_update_latency {bdev} avg_read {latency_us}"
            )
            r = subprocess.run(
                ["sudo", "-u", "aiden", "ssh", arm1_host, cmd],
                capture_output=True, text=True, timeout=15
            )
            if r.returncode != 0:
                errors.append(f"path={path} rc={r.returncode} "
                               f"{(r.stdout + r.stderr).strip()[:80]}")
            else:
                self._log(f"  path={path} {bdev} avg_read={latency_us}us ok")
        if errors:
            self._log(f"[ERROR] delay_inject partial failure: {errors}")
        else:
            self._log(f"delay_inject done — t{self.tid} IO will stall on all 3 paths")

    def delay_restore(self, arm1_host: str, restore_us: int):
        """Phase 3 restore — set delay_t{tid}_{P} avg_read back to original latency_us.

        restore_us should match DELAY_US in setup_arm1_sapsq_4t3p_mlx5_0.sh (default 27).
        """
        RPC = "/home/aiden/spdk/scripts/rpc.py"
        socks = {
            "A": "/var/tmp/spdk_a.sock",
            "B": "/var/tmp/spdk_b.sock",
            "C": "/var/tmp/spdk_c.sock",
        }
        self._log(f"delay_restore: restoring avg_read={restore_us}us on all 3 paths "
                  f"for delay_t{self.tid}_{{A,B,C}} via ssh {arm1_host}")
        errors = []
        for path, sock in socks.items():
            bdev = f"delay_t{self.tid}_{path}"
            cmd = (
                f"sudo {RPC} -s {sock} "
                f"bdev_delay_update_latency {bdev} avg_read {restore_us}"
            )
            r = subprocess.run(
                ["sudo", "-u", "aiden", "ssh", arm1_host, cmd],
                capture_output=True, text=True, timeout=15
            )
            if r.returncode != 0:
                errors.append(f"path={path} rc={r.returncode} "
                               f"{(r.stdout + r.stderr).strip()[:80]}")
            else:
                self._log(f"  path={path} {bdev} avg_read={restore_us}us ok")
        if errors:
            self._log(f"[ERROR] delay_restore partial failure: {errors}")
        else:
            self._log(f"delay_restore done — t{self.tid} IO latency back to {restore_us}us")

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
        self._cgroup_teardown()

    def parse_result(self) -> dict:
        try:
            content = self.logpath.read_text()
        except Exception:
            content = ""
        iops = None
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
                            break
            except Exception:
                pass
            break
        if not iops:
            periodic = [float(x) for x in
                        re.findall(r"\s+([\d.]+)\s+IOPS,", content)]
            if len(periodic) >= 3:
                iops = round(sum(periodic) / len(periodic), 0)
        result = {"tenant_id": self.tid, "mode": self.args.mode, "iops": iops}
        self.result_json.write_text(json.dumps(result, indent=2))
        self._log(f"result: IOPS={iops}")
        return result


# ── per-tenant timeseries collector ─────────────────────────────────────────


class PerTenantTimeseries(threading.Thread):
    """
    每秒 poll arm-1 spdk_tenants.sock 的 per-tenant bdev iostat,
    計算 1-second delta,寫到 out_dir/per_tenant_iops_timeseries.csv。

    CSV 格式:
      ts,t0,t1,t2,t3
      0.0,100000,33000,33000,33000
      1.0,...
    其中 t0..t3 = 每秒 IOPS delta。
    """

    def __init__(self, out_dir: Path, duration: int, logfile, n_tenants: int = 4,
                 num_paths: int = 3, collect_latency: bool = False):
        super().__init__(daemon=True)
        self.out_file = out_dir / "per_tenant_iops_timeseries.csv"
        self.duration = duration
        self.logfile = logfile
        self.n_tenants = n_tenants
        if num_paths < 1 or num_paths > len(PATH_PORT_OFFSETS):
            raise ValueError(f"invalid runtime path count: {num_paths}")
        self.paths = ["A", "B", "C"][:num_paths]
        self._stop_evt = threading.Event()
        self._rows = []   # list of (elapsed_s, [t0, t1, t2, t3])
        self._initial_full = None
        # 可選逐秒延遲 (latency) + per-path 收集 (--collect-latency)。
        # 預設 False → 完全還原舊行為 (只收 path A IOPS),既有 caller 不受影響。
        self.collect_latency = collect_latency
        # per-path IOPS timeseries (聚合 / 偵測導離壞路用):cols=[elapsed_s, A_t0..,B_t0..,C_t0..]
        self.perpath_file = out_dir / "per_path_iops_timeseries.csv"
        # per-tenant 每秒延遲 (ns):avg = read_latency_ticks delta / read_ops delta,
        # worst = max_read_latency_ticks (累計最大,非 per-second 重置;當 P99 proxy)
        self.latency_file = out_dir / "per_tenant_latency_timeseries.csv"

    def start(self):
        """Prime target counters before workload launch, then start sampling."""
        self._initial_full = self._get_arm1_full()
        super().start()

    def _get_arm1_reads(self):
        """從 arm-1 spdk_a.sock 讀 malloc_t0_A..t(N-1)_A 的 num_read_ops。
        4t3p topology 下 path A (spdk_a.sock) 各 tenant bdev 名為 malloc_t{i}_A。
        """
        import base64
        n = self.n_tenants
        script = (
            "import json,socket,time\n"
            "def rpc(sock,method):\n"
            "    req={'jsonrpc':'2.0','id':int(time.time_ns() & 0x7fffffff),'method':method}\n"
            "    s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); s.settimeout(3); s.connect(sock)\n"
            "    s.sendall((json.dumps(req,separators=(',',':'))+'\\n').encode())\n"
            "    data=b''\n"
            "    while True:\n"
            "        chunk=s.recv(65536)\n"
            "        if not chunk: break\n"
            "        data+=chunk\n"
            "        try: response=json.loads(data); break\n"
            "        except json.JSONDecodeError: pass\n"
            "    s.close()\n"
            "    if 'error' in response: raise RuntimeError(response['error'])\n"
            "    return response['result']\n"
            "results={}\n"
            "try:\n"
            "    d=rpc('/var/tmp/spdk_a.sock','bdev_get_iostat')\n"
            "    bdevs=d.get('bdevs',d) if isinstance(d,dict) else d\n"
            f"    for i in range({n}):\n"
            "        nm=f'malloc_t{i}_A'\n"
            "        val=0\n"
            "        for b in bdevs:\n"
            "            if b.get('name','').lower()==nm.lower():\n"
            "                val=b.get('num_read_ops',0); break\n"
            "        results[f't{i}']=val\n"
            "except Exception as e:\n"
            f"    for i in range({n}): results[f't{{i}}']=-1\n"
            "print(json.dumps(results))\n"
        )
        b64 = base64.b64encode(script.encode()).decode()
        # root 下 ssh arm-1 key 不通;用 sudo -u aiden ssh 借 aiden 的 key
        r = subprocess.run(
            [
             "sudo", "-u", "aiden", "ssh",
             "-o", "ControlMaster=auto",
             "-o", "ControlPersist=120",
             "-o", f"ControlPath={ARM1_SSH_CONTROL}",
             "arm-1",
             f"echo {b64} | base64 -d | sudo -n python3"],
            capture_output=True, text=True, timeout=20
        )
        if r.returncode != 0 or not r.stdout.strip():
            return None
        try:
            return json.loads(r.stdout.strip())
        except Exception:
            return None

    def _get_arm1_full(self):
        """收集全部 3 path × N tenant 的 num_read_ops + read_latency_ticks +
        max_read_latency_ticks。一次 SSH 撈 spdk_a/b/c.sock 三個 bdev_get_iostat,
        bdev 名為 delay_t{i}_{P}(延遲注入點,既含 read_ops 也含 latency_ticks)。

        回傳 dict:
          tsc_hz                         — arm-1 報告的 tsc_rate (ticks→ns 換算用)
          reads[P][i]                    — 累計 num_read_ops
          lat_ticks[P][i]                — 累計 read_latency_ticks
          max_lat_ticks[P][i]            — max_read_latency_ticks(累計最大值)
        撈失敗回 None。
        """
        import base64
        n = self.n_tenants
        socks = {
            path: f"/var/tmp/spdk_{path.lower()}.sock"
            for path in self.paths
        }
        script = (
            "import concurrent.futures,json,socket,time\n"
            "def rpc(sock,method):\n"
            "    req={'jsonrpc':'2.0','id':int(time.time_ns() & 0x7fffffff),'method':method}\n"
            "    s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); s.settimeout(3); s.connect(sock)\n"
            "    s.sendall((json.dumps(req,separators=(',',':'))+'\\n').encode())\n"
            "    data=b''\n"
            "    while True:\n"
            "        chunk=s.recv(65536)\n"
            "        if not chunk: break\n"
            "        data+=chunk\n"
            "        try: response=json.loads(data); break\n"
            "        except json.JSONDecodeError: pass\n"
            "    s.close()\n"
            "    if 'error' in response: raise RuntimeError(response['error'])\n"
            "    return response['result']\n"
            f"socks={socks!r}\n"
            "out={'tsc_hz':0,'reads':{},'lat_ticks':{},'max_lat_ticks':{}}\n"
            f"N={n}\n"
            "def fetch(item):\n"
            "    P,sock=item\n"
            "    try: return P,rpc(sock,'bdev_get_iostat'),None\n"
            "    except Exception as exc: return P,None,str(exc)\n"
            "with concurrent.futures.ThreadPoolExecutor(max_workers=len(socks)) as pool:\n"
            "    fetched={P:(data,error) for P,data,error in pool.map(fetch,socks.items())}\n"
            "for P in socks:\n"
            "    out['reads'][P]={}; out['lat_ticks'][P]={}; out['max_lat_ticks'][P]={}\n"
            "    try:\n"
            "        d,error=fetched[P]\n"
            "        if error is not None: raise RuntimeError(error)\n"
            "        if out['tsc_hz']==0 and isinstance(d,dict):\n"
            "            out['tsc_hz']=d.get('tick_rate',0)\n"
            "        bdevs=d.get('bdevs',d) if isinstance(d,dict) else d\n"
            "        idx={}\n"
            "        for b in bdevs:\n"
            "            idx[b.get('name','').lower()]=b\n"
            "        for i in range(N):\n"
            "            b=idx.get(f'delay_t{i}_{P}'.lower(),{})\n"
            "            out['reads'][P][str(i)]=b.get('num_read_ops',0)\n"
            "            out['lat_ticks'][P][str(i)]=b.get('read_latency_ticks',0)\n"
            "            out['max_lat_ticks'][P][str(i)]=b.get('max_read_latency_ticks',0)\n"
            "    except Exception:\n"
            "        for i in range(N):\n"
            "            out['reads'][P][str(i)]=-1; out['lat_ticks'][P][str(i)]=0; out['max_lat_ticks'][P][str(i)]=0\n"
            "print(json.dumps(out))\n"
        )
        b64 = base64.b64encode(script.encode()).decode()
        r = subprocess.run(
            [
             "sudo", "-u", "aiden", "ssh",
             "-o", "ControlMaster=auto",
             "-o", "ControlPersist=120",
             "-o", f"ControlPath={ARM1_SSH_CONTROL}",
             "arm-1",
             f"echo {b64} | base64 -d | sudo -n python3"],
            capture_output=True, text=True, timeout=30
        )
        if r.returncode != 0 or not r.stdout.strip():
            return None
        try:
            return json.loads(r.stdout.strip())
        except Exception:
            return None

    def run(self):
        if self.collect_latency:
            self._run_with_latency()
            return
        # BUG2 fix: the per-tenant IOPS timeseries must sum reads across all 3
        # paths (A+B+C). The old default sampled only path A (_get_arm1_reads),
        # which undercounts any tenant whose SAPS-steered traffic is not evenly
        # split across paths. Reuse the existing 3-path collector (_get_arm1_full)
        # and the same per-path delta+(-1)-guard summing as _run_with_latency,
        # WITHOUT enabling the heavier latency path (no extra CSVs / side files).
        n = self.n_tenants
        paths = self.paths
        start = time.time()
        prev = self._initial_full
        prev_elapsed = 0.0 if prev is not None else None
        with open(self.out_file, "w", newline="") as f:
            writer = csv.writer(f)
            cols = ["elapsed_s"] + [f"t{i}" for i in range(n)]
            writer.writerow(cols)
            while not self._stop_evt.is_set():
                full = self._get_arm1_full()
                elapsed = round(time.time() - start, 1)
                if elapsed > self.duration + 5:
                    break
                if full is not None and prev is not None:
                    sample_dt = max(elapsed - prev_elapsed, 1e-6)
                    deltas = [0] * n
                    for P in paths:
                        for i in range(n):
                            cur = full["reads"][P].get(str(i), 0)
                            pv = prev["reads"][P].get(str(i), 0)
                            delta = (max(0, cur - pv)
                                     if cur >= 0 and pv >= 0 else 0)
                            deltas[i] += delta / sample_dt
                    deltas = [round(value, 1) for value in deltas]
                    writer.writerow([elapsed] + deltas)
                    f.flush()
                    self._rows.append((elapsed, deltas))
                prev = full
                prev_elapsed = elapsed
                time.sleep(1.0)
        log(f"timeseries written to {self.out_file}", self.logfile)

    def _run_with_latency(self):
        """--collect-latency 模式:每秒收 3 path × N tenant 的 IOPS + latency。

        同時寫三個 CSV:
          per_tenant_iops_timeseries.csv  — [elapsed_s, t0..t{N-1}]
              每 tenant = 三 path read_ops delta 之和(聚合每秒 IOPS,軌跡主訊號)
          per_path_iops_timeseries.csv    — [elapsed_s, A_t0..A_t{N-1}, B_*, C_*]
              per (path, tenant) read_ops delta(看 SAPS 多快把流量導離壞路 B)
          per_tenant_latency_timeseries.csv — [elapsed_s,
              t0_avg_ns..,t{N-1}_avg_ns, t0_worst_ns.., t{N-1}_worst_ns]
              avg = Σ_P Δread_latency_ticks / Σ_P Δread_ops × (1e9/tsc_hz)
              worst = max_P(max_read_latency_ticks) × (1e9/tsc_hz)
                      (累計 worst-case，P99 的便宜 proxy)
        """
        n = self.n_tenants
        paths = self.paths
        start = time.time()
        # 寫絕對 wall-clock 起點 (ms) 供 analyze_ftdyn.py 把 elapsed_s 軌跡對齊到
        # d2_marks.log 的 D2_MARK *_ms 時戳 (兩者都是 epoch-ms 基準)。
        try:
            (self.out_file.parent / "collector_start_ms.txt").write_text(
                str(int(start * 1000)))
        except Exception:
            pass
        prev = self._initial_full
        prev_elapsed = 0.0 if prev is not None else None
        f_iops = open(self.out_file, "w", newline="")
        f_path = open(self.perpath_file, "w", newline="")
        f_lat = open(self.latency_file, "w", newline="")
        w_iops = csv.writer(f_iops)
        w_path = csv.writer(f_path)
        w_lat = csv.writer(f_lat)
        w_iops.writerow(["elapsed_s"] + [f"t{i}" for i in range(n)])
        w_path.writerow(["elapsed_s"] +
                        [f"{P}_t{i}" for P in paths for i in range(n)])
        w_lat.writerow(["elapsed_s"] +
                       [f"t{i}_avg_ns" for i in range(n)] +
                       [f"t{i}_worst_ns" for i in range(n)])
        try:
            while not self._stop_evt.is_set():
                full = self._get_arm1_full()
                elapsed = round(time.time() - start, 1)
                if elapsed > self.duration + 5:
                    break
                if full is not None and prev is not None:
                    sample_dt = max(elapsed - prev_elapsed, 1e-6)
                    tsc_hz = full.get("tsc_hz") or prev.get("tsc_hz") or 1_000_000_000
                    ns_per_tick = 1e9 / tsc_hz if tsc_hz else 1.0
                    # per-path delta + per-tenant 聚合
                    per_tenant_iops = [0] * n
                    path_row = []
                    for P in paths:
                        for i in range(n):
                            cur = full["reads"][P].get(str(i), 0)
                            pv = prev["reads"][P].get(str(i), 0)
                            d = max(0, cur - pv) if cur >= 0 and pv >= 0 else 0
                            rate = d / sample_dt
                            path_row.append(round(rate, 1))
                            per_tenant_iops[i] += rate
                    # per-tenant latency
                    avg_row, worst_row = [], []
                    for i in range(n):
                        d_ticks = 0
                        d_ops = 0
                        worst_ticks = 0
                        for P in paths:
                            ct = full["lat_ticks"][P].get(str(i), 0)
                            pt = prev["lat_ticks"][P].get(str(i), 0)
                            d_ticks += max(0, ct - pt)
                            co = full["reads"][P].get(str(i), 0)
                            po = prev["reads"][P].get(str(i), 0)
                            d_ops += max(0, co - po) if co >= 0 and po >= 0 else 0
                            worst_ticks = max(worst_ticks,
                                              full["max_lat_ticks"][P].get(str(i), 0))
                        avg_ns = round((d_ticks / d_ops) * ns_per_tick, 1) if d_ops > 0 else 0.0
                        worst_ns = round(worst_ticks * ns_per_tick, 1)
                        avg_row.append(avg_ns)
                        worst_row.append(worst_ns)
                    per_tenant_iops = [
                        round(value, 1) for value in per_tenant_iops
                    ]
                    w_iops.writerow([elapsed] + per_tenant_iops)
                    w_path.writerow([elapsed] + path_row)
                    w_lat.writerow([elapsed] + avg_row + worst_row)
                    f_iops.flush(); f_path.flush(); f_lat.flush()
                    self._rows.append((elapsed, per_tenant_iops))
                prev = full
                prev_elapsed = elapsed
                time.sleep(1.0)
        finally:
            f_iops.close(); f_path.close(); f_lat.close()
        log(f"timeseries (iops+path+latency) written to {self.out_file.parent}",
            self.logfile)

    def stop(self):
        self._stop_evt.set()


# ── E2 idle phase controller ─────────────────────────────────────────────────


def schedule_idle_phase(procs: list, idle_tid: int, idle_start: int,
                        idle_end: int, logfile,
                        method: str = "delay_inject",
                        arm1_host: str = "arm-1",
                        delay_inject_us: int = 10_000_000,
                        delay_restore_us: int = 27) -> threading.Thread:
    """
    Background thread implementing Phase 2 idle simulation.

    method='cgroup':
      sleep(idle_start) → cgroup freeze (echo 1 > cgroup.freeze)
      sleep(idle_end - idle_start) → cgroup unfreeze (echo 0 > cgroup.freeze)
      Note: ineffective for SCHED_FIFO SPDK reactors — use delay_inject instead.

    method='delay_inject':
      sleep(idle_start) → bdev_delay_update_latency delay_t{tid}_{A,B,C} avg_read
                          <delay_inject_us> on arm-1 via SSH
                          → bdevperf IOs stall → demand_iops[tid] decays to 0
                          → saps_q work-conserving redistribution to active tenants
      sleep(idle_end - idle_start) → restore latency to <delay_restore_us>
                          → bdevperf IOs complete → demand_iops[tid] recovers
    """
    target = next((p for p in procs if p.tid == idle_tid), None)
    if target is None:
        log(f"WARN: idle_tenant={idle_tid} not in proc list", logfile)
        return None

    def _do():
        log(f"[IDLE] t{idle_tid}: sleep until t={idle_start}s (method={method})",
            logfile)
        time.sleep(idle_start)
        if method == "delay_inject":
            log(f"[IDLE] t{idle_tid}: delay_inject {delay_inject_us}us (Phase 2 start)",
                logfile)
            target.delay_inject(arm1_host, delay_inject_us)
        else:
            log(f"[IDLE] t{idle_tid}: cgroup freeze (Phase 2 start)", logfile)
            target.sigstop()
        time.sleep(max(0, idle_end - idle_start))
        if method == "delay_inject":
            log(f"[IDLE] t{idle_tid}: delay_restore {delay_restore_us}us (Phase 3 start)",
                logfile)
            target.delay_restore(arm1_host, delay_restore_us)
        else:
            log(f"[IDLE] t{idle_tid}: cgroup unfreeze (Phase 3 start)", logfile)
            target.sigcont()

    th = threading.Thread(target=_do, daemon=True)
    th.start()
    return th


# ── cleanup ──────────────────────────────────────────────────────────────────


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
         f"rm -f {DPA_PLUGIN_SOCK}; true"],
        capture_output=True, timeout=15
    )
    time.sleep(2)


# ── arm-1 setup ───────────────────────────────────────────────────────────────


def setup_arm1(script_path: str, logfile, arm1_host: str = "arm-1"):
    log(f"arm-1 setup: {script_path}", logfile)
    r = subprocess.run(["sudo", "-u", "aiden", "ssh", arm1_host,
                        f"sudo bash {script_path}"],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        log(f"ERROR: arm-1 setup rc={r.returncode}: {r.stderr[:200]}", logfile)
        raise RuntimeError(f"arm-1 setup failed: {script_path}")
    log("arm-1 setup ok", logfile)


# ── argument parsing ─────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(
        description="SAPS-Q E2 work-conserving idle tenant orchestrator"
    )
    p.add_argument("--num-tenants", type=int, default=4,
                   help="number of tenants (fixed 4 for E2; default 4)")
    p.add_argument("--weights", type=str, default="3,1,1,1",
                   help='weight CSV (default "3,1,1,1")')
    p.add_argument("--mode",
                   choices=["stock_qd", "host_saps", "m3_v2_static", "saps_q"],
                   default="saps_q",
                   help="scheduler mode (default saps_q)")
    p.add_argument("--idle-tenant", type=int, default=1,
                   help="which tenant becomes idle in Phase 2 (default 1)")
    p.add_argument("--idle-start", type=int, default=20,
                   help="seconds at which idle tenant pauses (default 20)")
    p.add_argument("--idle-end", type=int, default=40,
                   help="seconds at which idle tenant resumes (default 40)")
    p.add_argument("--duration", type=int, default=60,
                   help="total run duration seconds (default 60)")
    p.add_argument("--output-dir", type=str, required=True,
                   help="output directory for results")
    p.add_argument("--link-cap", type=int, default=200000,
                   help="namespace service envelope in IOPS (default 200000)")
    p.add_argument("--path-caps", type=str, default=None,
                   help="required SAPS-Q per-path deliverable capacities K_p, "
                        "as three comma-separated positive IOPS values")
    p.add_argument("--qd", type=int, default=32,
                   help="queue depth per tenant (default 32)")
    p.add_argument("--no-setup-arm1", action="store_true",
                   help="skip arm-1 setup (assume already configured)")
    p.add_argument("--setup-arm1-script", type=str, default=SETUP_ARM1_DEFAULT,
                   help=f"arm-1 setup script (default: {SETUP_ARM1_DEFAULT})")
    p.add_argument("--bypass-d", action="store_true", default=False,
                   help="disable health classification for an explicit ablation")
    p.add_argument("--no-bypass-d", dest="bypass_d", action="store_false")
    p.add_argument("--collect-latency", action="store_true", default=False,
                   help="除每秒 IOPS 外，額外收 per-path IOPS + per-tenant avg/worst "
                        "latency timeseries (ftdyn onset→recovery 軌跡用)。"
                        "預設關閉，不影響既有 caller。")
    p.add_argument("--dry-run", action="store_true",
                   help="print commands + envs, do not launch testbed")
    p.add_argument("--target-ip", type=str, default=None,
                   help="Override TARGET_IP (default: 10.0.1.1). Use 10.0.0.1 for mlx5_0 lane.")
    p.add_argument("--idle-method",
                   choices=["cgroup", "delay_inject"],
                   default="delay_inject",
                   help=(
                       "Mechanism to simulate idle tenant in Phase 2. "
                       "'cgroup': cgroup v2 freeze (fails on SCHED_FIFO SPDK reactor). "
                       "'delay_inject': set bdev_delay avg_read latency=10s on arm-1 target "
                       "for all 3 paths of idle tenant → bdevperf IO stalls → demand drops to 0 "
                       "(default: delay_inject)"
                   ))
    p.add_argument("--arm1-host", type=str, default="arm-1",
                   help="SSH hostname for arm-1 (default: arm-1)")
    p.add_argument("--delay-inject-us", type=int, default=10_000_000,
                   help="Latency in microseconds injected on idle tenant bdevs in Phase 2 "
                        "(default: 10000000 = 10s)")
    p.add_argument("--delay-restore-us", type=int, default=27,
                   help="Latency in microseconds restored on idle tenant bdevs in Phase 3 "
                        "(matches DELAY_US=27 in setup_arm1_sapsq_4t3p_mlx5_0.sh, default: 27)")
    return p.parse_args()


# ── dry-run helper ────────────────────────────────────────────────────────────


def print_dry_run(args):
    """
    --dry-run 模式:印出所有 mode 的 invocation 差異 + phase timing。
    不跑 testbed,不 SSH。
    """
    weights = [int(w) for w in args.weights.split(",")]
    n = args.num_tenants

    print("\n=== DRY RUN — run_saps_q_e2_idle.py E2 orchestrator ===")
    print(f"  mode={args.mode}  tenants={n}  weights={args.weights}")
    print(f"  service_envelope={args.link_cap} IOPS  "
          f"path_caps={csv_str(args.path_caps)}  qd={args.qd}")
    print(f"  duration={args.duration}s  idle_tenant={args.idle_tenant}")
    print(f"  Phase 1: t=0..{args.idle_start}s  [all active]")
    idle_method = getattr(args, "idle_method", "delay_inject")
    if idle_method == "delay_inject":
        inject_us = getattr(args, "delay_inject_us", 10_000_000)
        restore_us = getattr(args, "delay_restore_us", 27)
        print(f"  Phase 2: t={args.idle_start}..{args.idle_end}s  "
              f"[tenant {args.idle_tenant} delay_inject {inject_us}us on arm-1 "
              f"delay_t{args.idle_tenant}_{{A,B,C}}]")
        print(f"  Phase 3: t={args.idle_end}..{args.duration}s  "
              f"[tenant {args.idle_tenant} delay_restore {restore_us}us]")
    else:
        print(f"  Phase 2: t={args.idle_start}..{args.idle_end}s  "
              f"[tenant {args.idle_tenant} cgroup freeze]")
        print(f"  Phase 3: t={args.idle_end}..{args.duration}s  "
              f"[tenant {args.idle_tenant} cgroup unfreeze]")
    print()

    print("--- Mode comparison (invocation env diff) ---")
    modes = ["stock_qd", "host_saps", "m3_v2_static", "saps_q"]
    for m in modes:
        print(f"\n[mode={m}]")
        for tid in range(n):
            runtime_paths = len(args.path_caps) if m == "saps_q" else 3
            envs = build_env(m, tid, n, runtime_paths, args.link_cap, weights,
                             args.path_caps)
            print(f"  tenant {tid}: env {' '.join(envs)}")

    print("\n--- Phase timing ---")
    _m = getattr(args, "idle_method", "delay_inject")
    if _m == "delay_inject":
        _inj = getattr(args, "delay_inject_us", 10_000_000)
        _rst = getattr(args, "delay_restore_us", 27)
        print(f"  t={args.idle_start}s: delay_inject({_inj}us) on arm-1 "
              f"delay_t{args.idle_tenant}_{{A,B,C}}")
        print(f"           → [IDLE] tenant {args.idle_tenant} IO stalls, "
              f"demand_iops[{args.idle_tenant}] decays to 0, saps_q redistributes")
        print(f"  t={args.idle_end}s: delay_restore({_rst}us) on arm-1 "
              f"delay_t{args.idle_tenant}_{{A,B,C}}")
        print(f"           → [IDLE] tenant {args.idle_tenant} IO resumes, "
              f"scheduler re-converges")
    else:
        print(f"  t={args.idle_start}s: cgroup freeze tenant {args.idle_tenant}")
        print(f"           → [IDLE] tenant {args.idle_tenant} pauses, "
              f"DPA scheduler redistributes")
        print(f"  t={args.idle_end}s: cgroup unfreeze tenant {args.idle_tenant}")
        print(f"           → [IDLE] tenant {args.idle_tenant} resumes, "
              f"scheduler re-converges")

    print("\n--- Expected IOPS (theoretical) ---")
    total_w = sum(weights)
    phase1 = [round(args.link_cap * w / total_w) for w in weights]
    print(f"  Phase 1 (saps_q) expected: {phase1}")
    # Phase 2: tenant idle_tenant exits; redistribute to active
    active_w = [(i, weights[i]) for i in range(n) if i != args.idle_tenant]
    active_total = sum(w for _, w in active_w)
    phase2 = [0] * n
    for i, w in active_w:
        phase2[i] = round(args.link_cap * w / active_total)
    if 0 <= args.idle_tenant < n:
        phase2[args.idle_tenant] = 0
    print(f"  Phase 2 (saps_q) expected: {phase2}  (work-conserving)")
    static_phase2 = [0] * n
    for i in range(n):
        if i != args.idle_tenant:
            static_phase2[i] = phase1[i]
    print(f"  Phase 2 (m3_v2_static) expected: {static_phase2}  (static, wastes idle share)")
    total_static_p2 = sum(static_phase2)
    print(f"    → m3_v2_static total in Phase 2: {total_static_p2} / {args.link_cap} "
          f"= {total_static_p2/args.link_cap:.0%} utilization")
    print(f"    → saps_q total in Phase 2: {sum(phase2)} / {args.link_cap} "
          f"= {sum(phase2)/args.link_cap:.0%} utilization")

    print("\n--- arm-1 setup ---")
    print(f"  script: {args.setup_arm1_script}")
    print(f"  (skipped in dry-run)")

    print("\n--- Output ---")
    print(f"  output_dir: {args.output_dir}")
    print(f"  per_tenant_iops_timeseries.csv: cols=[elapsed_s, t0, t1, t2, t3]")
    print(f"  (aggregate_saps_q_e2.py reads this CSV)")

    print("\n--- Idle mechanism (--idle-method) ---")
    print("  delay_inject (default, recommended):")
    print("    arm-1 target 端 bdev_delay_update_latency delay_t{tid}_{A,B,C} avg_read 10s")
    print("    → bdevperf outstanding IO stall → QD=32 被填滿 → 不再 submit 新 IO")
    print("    → host_submit_count[tid] 停止 increment → demand_iops[tid] EWMA 衰減到 0")
    print("    → saps_q progressive_fill skip tid → work-conserving 重分配給 t0/t2/t3")
    print("    Phase 3: restore latency to 27us → bdevperf IO 恢復 → demand 回升")
    print("    驗證: bdev_delay_update_latency avg_read 10000000 → rc=0 (2026-05-28)")
    print("  cgroup (deprecated, fails on SCHED_FIFO SPDK reactor):")
    print("    echo 1 > /sys/fs/cgroup/bdevperf_sapsq_t{tid}/cgroup.freeze")
    print("    Lane W E2 失敗原因: SPDK reactor SCHED_FIFO → cgroup freeze 無效")

    print("\n=== end dry run ===\n")


# ── launch-race 偵測 + 單次嘗試 ───────────────────────────────────────────────


def _run_is_valid(results: list, perform_walltime_s: float, duration: int,
                  idle_tenant: int = None):
    """判定一次 attempt 是否為有效 run。回傳 (valid: bool, reason: str)。

    INVALID 條件 (任一成立):
      (a) 任一 tenant 的 parsed IOPS 為 None 或 <= 0
          → perform_tests 沒產 IO/result (rep7 launch-race 症狀)
      (b) perform_tests phase wall-time < duration * MIN_WALLTIME_FRACTION
          → RPC 在 launch 瞬間即退出 (rep7: 應 60s 卻 ~3s 返回)
      (c) 任一 NON-idle tenant 的 IOPS < busy-tenant median 的 1%
          → 誤中的 delay-injection (colliding process) 把非 idle tenant 壓成
            ~個位數 IOPS,rep 仍 parse 成 valid(IOPS>0)但量測已被污染。
    """
    bad = [r["tenant_id"] for r in results
           if not r.get("iops") or float(r["iops"]) <= 0]
    if bad:
        return False, (f"tenants {bad} reported IOPS=None/0 "
                       f"(perform_tests produced no IO)")
    min_walltime = duration * MIN_WALLTIME_FRACTION
    if perform_walltime_s < min_walltime:
        return False, (f"perform_tests wall-time {perform_walltime_s:.1f}s "
                       f"< {min_walltime:.1f}s (={MIN_WALLTIME_FRACTION:g}×"
                       f"duration {duration}s) — launch-race early exit")
    # (c) contamination guard (BUG3): only per-tenant IOPS is available here.
    # A stray delay-injection landing ~10s latency on a NON-idle tenant
    # collapses it to single-digit IOPS while its peers stay in the thousands.
    # Such a rep parses as "valid" (IOPS>0) but is measurement-contaminated.
    # Flag when any non-idle tenant delivers <1% of the busy-tenant median IOPS.
    # Pure validity guard — no scheduling/allocation/env change.
    if idle_tenant is not None:
        busy = [float(r["iops"]) for r in results
                if r.get("tenant_id") != idle_tenant and r.get("iops")]
        if len(busy) >= 2:
            s = sorted(busy)
            m = len(s)
            med = s[m // 2] if m % 2 else (s[m // 2 - 1] + s[m // 2]) / 2.0
            floor = med * 0.01
            collapsed = [r["tenant_id"] for r in results
                         if r.get("tenant_id") != idle_tenant
                         and r.get("iops") and float(r["iops"]) < floor]
            if collapsed:
                return False, (f"non-idle tenants {collapsed} collapsed to "
                               f"<1% of busy-tenant median IOPS ({med:.0f}) — "
                               f"likely stray delay-injection contamination")
    return True, "ok"


def run_one_attempt(args, out_dir: Path, orch_log):
    """執行一次完整 launch→perform→parse,並判定有效性。

    回傳 (results, ts_out_file, valid, reason)。
    每次 attempt 自建 procs 並在結束 (含失敗) 時 cleanup_all,讓 retry 從乾淨
    狀態重來。arm-1 setup 不在此函式 — 它是 idempotent 的一次性步驟。
    """
    procs = [TenantProcE2(i, args, out_dir, orch_log)
             for i in range(args.num_tenants)]

    def sigint_handler(sig, frame):
        log("SIGINT — cleanup", orch_log)
        cleanup_all(procs)
        sys.exit(130)

    signal.signal(signal.SIGINT, sigint_handler)

    # ── Step 2: launch bdevperf procs ────────────────────────────────────────
    log("cleanup stale state", orch_log)
    cleanup_all([])
    subprocess.run(["sudo", "rm", "-f", DPA_PLUGIN_SOCK], capture_output=True)

    if args.mode == "host_saps":
        log("launching host_saps procs as independent peers (no DPA UDS coordinator)",
            orch_log)
        for p in procs:
            p.start()
            time.sleep(0.2)

        log("waiting for all host_saps RPC sockets", orch_log)
        for p in procs:
            if not p.wait_socket(timeout=120):
                log(f"t{p.tid} socket timeout — attempt INVALID", orch_log)
                cleanup_all(procs)
                results = [{"tenant_id": q.tid, "mode": args.mode, "iops": None}
                           for q in procs]
                return results, None, False, f"t{p.tid} RPC socket timeout"
    else:
        # coordinator (tid=0) 開 UDS server;tenant 需在 coordinator UDS ready 後才連
        coord = procs[0]
        log("launching coordinator (tenant 0)", orch_log)
        coord.start()

        # Wait for coordinator RPC socket first
        log("waiting for coordinator RPC socket", orch_log)
        if not coord.wait_socket(timeout=60):
            log("coordinator socket timeout — attempt INVALID", orch_log)
            cleanup_all(procs)
            results = [{"tenant_id": p.tid, "mode": args.mode, "iops": None}
                       for p in procs]
            return results, None, False, "coordinator RPC socket timeout"
        log("coordinator RPC socket ready", orch_log)

        # Give coordinator time to start UDS server (ibv_open_device + flexio_process_create
        # + uds_server_start takes ~1-2s; tenants retry-connect 5s so 3s margin is enough)
        time.sleep(3)

        log("launching tenant procs (1..N-1)", orch_log)
        for p in procs[1:]:
            p.start()
            time.sleep(0.2)  # stagger to avoid UDS accept thundering-herd

        # ── Step 3: wait for tenant sockets ──────────────────────────────────
        log("waiting for tenant RPC sockets", orch_log)
        for p in procs[1:]:
            if not p.wait_socket(timeout=120):
                log(f"t{p.tid} socket timeout — attempt INVALID", orch_log)
                cleanup_all(procs)
                results = [{"tenant_id": q.tid, "mode": args.mode, "iops": None}
                           for q in procs]
                return results, None, False, f"t{p.tid} RPC socket timeout"

    # ── Step 4: setup controllers (parallel) ──────────────────────────────────
    log("setup controllers (parallel)", orch_log)
    setup_results = {}

    def do_setup(p):
        setup_results[p.tid] = p.setup_controllers()

    threads = [threading.Thread(target=do_setup, args=(p,), daemon=True)
               for p in procs]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=200)

    failed = [p for p in procs if not setup_results.get(p.tid, False)]
    if failed:
        log(f"setup failed for t={[p.tid for p in failed]} — attempt INVALID",
            orch_log)
        cleanup_all(procs)
        results = [{"tenant_id": p.tid, "mode": args.mode, "iops": None}
                   for p in procs]
        return results, None, False, \
            f"controller setup failed for t={[p.tid for p in failed]}"
    log("all tenant controllers ready", orch_log)

    # ── Step 5: start timeseries collector ───────────────────────────────────
    log("starting per-tenant timeseries collector", orch_log)
    ts_coll = PerTenantTimeseries(out_dir, args.duration, orch_log,
                                   args.num_tenants,
                                   num_paths=len(args.path_caps),
                                   collect_latency=getattr(args, "collect_latency", False))
    ts_coll.start()

    # ── Step 6: schedule idle phase ───────────────────────────────────────────
    schedule_idle_phase(
        procs, args.idle_tenant, args.idle_start, args.idle_end, orch_log,
        method=args.idle_method,
        arm1_host=args.arm1_host,
        delay_inject_us=args.delay_inject_us,
        delay_restore_us=args.delay_restore_us,
    )

    # ── Step 7: perform_tests (parallel) ─────────────────────────────────────
    # Time the whole perform_tests phase: a launch-race makes the RPC return in
    # ~3s instead of ~duration, which _run_is_valid() flags via MIN_WALLTIME.
    log(f"perform_tests ({args.duration}s) on {args.num_tenants} procs", orch_log)
    perform_start = time.time()
    test_procs = []
    for p in procs:
        tp = p.run_perform_tests()
        test_procs.append((p, tp))

    for p, tp in test_procs:
        try:
            tp.wait(timeout=args.duration + 60)
        except subprocess.TimeoutExpired:
            log(f"WARN: t{p.tid} perform_tests timeout", orch_log)
            tp.kill()
        log(f"t{p.tid} perform_tests done", orch_log)
    perform_walltime_s = time.time() - perform_start
    log(f"perform_tests phase wall-time={perform_walltime_s:.1f}s "
        f"(expected ~{args.duration}s)", orch_log)

    # ── Step 8: stop timeseries + cleanup ─────────────────────────────────────
    ts_coll.stop()
    ts_coll.join(timeout=10)

    for p in procs:
        if p.proc:
            try:
                p.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pass
    cleanup_all(procs)

    # ── Step 9: parse per-tenant results + validity check ─────────────────────
    log("parsing results", orch_log)
    results = [p.parse_result() for p in procs]
    valid, reason = _run_is_valid(results, perform_walltime_s, args.duration,
                                  idle_tenant=args.idle_tenant)
    return results, ts_coll.out_file, valid, reason


# ── main ─────────────────────────────────────────────────────────────────────


def main():
    global TARGET_IP
    args = parse_args()
    if args.target_ip:
        TARGET_IP = args.target_ip

    weights = [int(w) for w in args.weights.split(",")]
    if len(weights) < args.num_tenants:
        print(f"ERROR: --weights has {len(weights)} entries, "
              f"need {args.num_tenants}", file=sys.stderr)
        sys.exit(1)
    if args.path_caps is None:
        if args.mode == "saps_q":
            print(
                "ERROR: saps_q requires explicit --path-caps K_A,K_B,K_C",
                file=sys.stderr,
            )
            sys.exit(1)
        args.path_caps = [args.link_cap] * 3
    else:
        try:
            args.path_caps = [int(v) for v in args.path_caps.split(",")]
        except ValueError:
            print("ERROR: --path-caps must contain comma-separated integers",
                  file=sys.stderr)
            sys.exit(1)
        if not args.path_caps or any(v <= 0 for v in args.path_caps):
            print("ERROR: --path-caps requires positive capacities",
                  file=sys.stderr)
            sys.exit(1)

    if args.idle_start >= args.idle_end:
        print(f"ERROR: --idle-start ({args.idle_start}) must be < --idle-end "
              f"({args.idle_end})", file=sys.stderr)
        sys.exit(1)
    if args.idle_end > args.duration:
        print(f"ERROR: --idle-end ({args.idle_end}) must be <= --duration "
              f"({args.duration})", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print_dry_run(args)
        return

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    orch_log_path = out_dir / "orchestrator.log"
    orch_log = open(orch_log_path, "w")

    log("=== E2 idle tenant orchestrator start ===", orch_log)
    log(f"mode={args.mode}  tenants={args.num_tenants}  weights={args.weights}  "
        f"service_envelope={args.link_cap}  path_caps={csv_str(args.path_caps)}  "
        f"duration={args.duration}s  "
        f"idle_tenant={args.idle_tenant}  idle_method={args.idle_method}  "
        f"Phase2 t={args.idle_start}s→{args.idle_end}s",
        orch_log)
    if args.idle_method == "delay_inject":
        log(f"delay_inject: arm1_host={args.arm1_host}  "
            f"inject={args.delay_inject_us}us  restore={args.delay_restore_us}us",
            orch_log)

    # core check
    max_core = BASE_CORE + args.num_tenants * CORES_PER_PROC - 1
    if max_core >= 96:
        log(f"ERROR: need core {max_core} but max is 95", orch_log)
        sys.exit(1)

    # ── Step 1: arm-1 setup (idempotent; done once, retried reps reuse it) ────
    if not args.no_setup_arm1:
        setup_arm1(args.setup_arm1_script, orch_log, arm1_host=args.arm1_host)
    else:
        log("skip arm-1 setup (--no-setup-arm1)", orch_log)

    # ── retry loop: launch+perform+parse 為一個重試單元 ───────────────────────
    # launch-race (rep collapse to None IOPS) 為暫態 harness 故障;tear down 整個
    # rep 並重跑通常即可恢復。最多 MAX_RUN_ATTEMPTS 次。
    results = None
    ts_out_file = None
    for attempt in range(1, MAX_RUN_ATTEMPTS + 1):
        log(f"=== run attempt {attempt}/{MAX_RUN_ATTEMPTS} ===", orch_log)
        results, ts_out_file, valid, reason = run_one_attempt(
            args, out_dir, orch_log)
        if valid:
            if attempt > 1:
                log(f"attempt {attempt} produced a VALID run", orch_log)
            break
        log(f"[INVALID] attempt {attempt}: {reason} — "
            f"{'retrying' if attempt < MAX_RUN_ATTEMPTS else 'giving up'}",
            orch_log)
    else:
        # all attempts exhausted; keep the last (invalid) results so the summary
        # still records what happened rather than silently passing.
        log(f"ERROR: all {MAX_RUN_ATTEMPTS} attempts INVALID (launch-race) — "
            f"recording last attempt as-is", orch_log)

    # ── Step 10: write run summary ────────────────────────────────────────────
    summary = {
        "mode": args.mode,
        "num_tenants": args.num_tenants,
        "weights": args.weights,
        "link_cap": args.link_cap,
        "service_envelope_iops": args.link_cap,
        "path_capacity_iops": args.path_caps,
        "health_classifier_bypassed": bool(args.bypass_d),
        "health_coupling_mode": effective_health_coupling_mode(),
        "duration_s": args.duration,
        "idle_tenant": args.idle_tenant,
        "idle_start_s": args.idle_start,
        "idle_end_s": args.idle_end,
        "per_tenant": results,
        "timeseries_file": str(ts_out_file) if ts_out_file else None,
        # launch-race verdict: lets downstream consumers drop runs that only
        # "completed" because retries were exhausted (don't trust None IOPS).
        "valid": valid,
        "invalid_reason": None if valid else reason,
        "attempts": attempt,
    }
    summary_path = out_dir / "e2_run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    log(f"summary → {summary_path}", orch_log)
    log("=== E2 orchestrator done ===", orch_log)
    orch_log.close()

    total_iops = sum(r["iops"] for r in results if r.get("iops"))
    print(f"\nRun complete. Out dir: {out_dir}")
    print(f"Total IOPS: {total_iops:.0f}")
    for r in results:
        print(f"  t{r['tenant_id']}: IOPS={r['iops']}")


if __name__ == "__main__":
    main()
