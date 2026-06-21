#!/usr/bin/env python3
"""
run_sapsq.py — SAPS-Q E1/E2/E3 testbed orchestrator (node2 initiator)

對應 specs/dm-research-redesign-20260522.md §5 (E1/E2/E3) + §7 step 5
(orchestrator dumps per-tenant/per-path metrics)。

骨架沿用 run_m0_multiclient_tenants.py;改動:
  1. 4 tenant × 3 path topology(per-tenant multipath,不是 single path)
     依賴 node1 setup script 提供:每個 NQN listen 在 3 個 port(path A/B/C)
  2. SAPS-Q env vars(SAPS_Q_*)由 orchestrator per-proc 計算 + 注入
  3. Mode sweep:stock / m4_static / sapsq / spdk_bdev_qos
     - stock: SAPS_Q_ENABLED=0, M4 關
     - m4_static: SAPS_M4_V2_GATE=1 + SAPS_M4_WEIGHTS(沿用舊 M4)
     - sapsq: SAPS_Q_ENABLED=1 + 完整 SAPS_Q_* 配置
     - spdk_bdev_qos: SAPS_Q_ENABLED=0;node1 須先設好 per-bdev QoS(orchestrator
       不負責,只 record mode 名稱;依賴外部 setup_arm1_bdev_qos.sh)
  4. E2 idle tenant:把指定 tenant 的 bdevperf duration 改短(或對其 proc
     SIGSTOP),其他 tenant 持續滿載
  5. E3 path degradation:t=inject_time 時 ssh node1 跑 tc-netem 加 5ms 到
     path B 介面(用法沿用 inject_D1_wallclock.sh)
  6. SAPS-Q counter snapshot:讀 /proc/<pid>/fd/<memfd> 對應的 dpa_plugin_ring
     memfd,呼叫外部 sapsq_dump helper(sapsq_dump.c 編出的 binary)dump JSON
     到 <out>/sapsq_start.json + sapsq_end.json,計算 delta 寫 sapsq_summary.json

Q-format conversion(必須正確):
  - tenant_weight: 純 ratio,轉 Q16.16 = round(w_i * 65536 / sum(w))
    例:weights=[3,1,1,1] → sum=6 → [3/6,1/6,1/6,1/6]*65536
    = [32768, 10923, 10923, 10923]
  - demand_q32 / path_base_iops_q32 / probe_rate_q32: IO/tsc Q32
    轉換:rate_q32 = round(iops_per_sec / tsc_hz * 2^32)
    例:tsc_hz=1e9, iops=200000 → 200000/1e9 * 2^32
    = 858993 ≈ 0xD1B72 (約 200K IOPS @ 1GHz TSC)

CPU binding(沿用 m0_multiclient_tenants):
  proc i → cores (BASE_CORE + 2i)..(BASE_CORE + 2i + 1)

用法:
  python3 scripts/run_sapsq.py \\
      --tenants 4 --weights "3,1,1,1" --duration 60 \\
      --path-base-iops "200000,200000,200000,200000" \\
      --demand-iops "100000,100000,100000,100000" \\
      --probe-rate-iops "100,100,100,100" \\
      --mode sapsq \\
      --out-dir experiments/sapsq/e1_static_fairness/<ts>/sapsq_rep0
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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ── 常數 ──────────────────────────────────────────────────────────────────────

BDEVPERF = "/home/user/spdk/build/examples/bdevperf"
RPCPY = "/home/user/spdk/scripts/rpc.py"
BDEVPERF_PY = "/home/user/spdk/examples/bdev/bdevperf/bdevperf.py"

# SAPS-Q topology:每 tenant 一個 NQN,multipath 跨 3 個 port
NQN_PREFIX = "nqn.2024-01.io.spdk:tenant"
TARGET_IP = "10.0.0.1"

# 3 paths per tenant — node1 須為每個 tenant NQN 開 3 個 listener
# (path A/B/C 對應 port +0/+10/+20 對 tenant base port)
TENANT_BASE_PORT = 4430
# Default stride 10 保留 N=4/8 (E1..E7) 既有 port map 不變;
# N>10 (E11) 自動 promote 成 stride 16,參見 build_config()。
PATH_PORT_OFFSETS = [0, 10, 20]   # path A=base, path B=base+10, path C=base+20
PATH_PORT_OFFSETS_STRIDE_16 = [0, 16, 32]  # E11 N=16: 4430-4445/4446-4461/4462-4477

# 對應 dpa_plugin_com.h: SAPSQ_TENANT_MAX = M3_TENANT_MAX = 16
# DPA-side 陣列 (sapsq_tenant_*, m4_tenant_*) 都 size 16,不可超過。
SAPSQ_TENANT_MAX = 16

# Port 配置上限 (port collision check) for default stride [0,10,20]:
# tenant_i path_p_port = TENANT_BASE_PORT + i + PATH_PORT_OFFSETS[p]
# 最小 path offset 間距 = min(diff(PATH_PORT_OFFSETS)) = 10
# 即 path_A 上限 port = TENANT_BASE_PORT + (N-1),需 < TENANT_BASE_PORT + 10
# → 最大 N = 10 (tenant 0..9, path A 用 4430..4439, path B 從 4440 起,不撞)
MAX_TENANTS_PORT_SAFE = min(PATH_PORT_OFFSETS[i + 1] - PATH_PORT_OFFSETS[i]
                            for i in range(len(PATH_PORT_OFFSETS) - 1))

# Stride-16 layout 對應上限 (E11 N=16):
# 4430-4445 / 4446-4461 / 4462-4477,無 collision,N 上限 = SAPSQ_TENANT_MAX。
MAX_TENANTS_PORT_SAFE_STRIDE_16 = min(
    PATH_PORT_OFFSETS_STRIDE_16[i + 1] - PATH_PORT_OFFSETS_STRIDE_16[i]
    for i in range(len(PATH_PORT_OFFSETS_STRIDE_16) - 1)
)

# node1 setup script(per-experiment-driver 透過 --setup-node1-script 覆寫)
SETUP_ARM1_DEFAULT = (
    "/home/user/DPA/nvme-of-controller/experiments/"
    "3path_targets/setup_arm1_tenants.sh"
)

TGT_SOCK_TENANTS = "/var/tmp/spdk_tenants.sock"
TGT_BDEV_TENANT_NAMES = [f"malloc_t{i}" for i in range(16)]

# E3 path degradation target — path B 在 node1 的 interface 名稱
# (node1 三個 IB/RoCE 介面;ENP_PATH_B 為 PATH B 對應的 physical iface)
ARM1_PATH_B_IFACE = "enP4p3s0f1np1"   # 沿用 inject_D1_wallclock.sh 的同樣 iface

# CPU binding
BASE_CORE = 4
CORES_PER_PROC = 2

SOCK_TMPL = "/var/tmp/bdevperf_sapsq_proc{i}.sock"

# SAPS-Q ring memfd name(同 dpa_plugin.c L861)
DPA_PLUGIN_MEMFD_NAME = "dpa_plugin_ring"

# sapsq_dump helper binary path(由 sapsq_dump.c 編出來)
SAPSQ_DUMP_BIN = (
    "/home/user/DPA/nvme-of-controller/scripts/sapsq_dump"
)


# ── 工具 ──────────────────────────────────────────────────────────────────────


def ts():
    return time.strftime("%H:%M:%S")


def log(msg, file=None):
    line = f"[{ts()}] [orch] {msg}"
    print(line, flush=True, file=sys.stderr)
    if file:
        print(line, file=file, flush=True)


def get_tsc_hz_default():
    """Linux aarch64 上 cntvct_el0 freq = `cat /proc/cpuinfo` 不準。
    用 dmesg 或 /proc/device-tree 試;否則回 fallback 1GHz。
    aarch64 cntfrq_el0 一般 = 1e9(BF3 BlueField-3 是 1000000000)。
    """
    # 用 dpa_plugin.c 同樣的 fallback;orchestrator 無權讀 cntfrq_el0(user-space
    # 不一定 trap-able),所以用 known constant for BF3 host。
    return 1_000_000_000


def iops_to_q32(iops_per_sec, tsc_hz):
    """rate_q32 = round(iops_per_sec / tsc_hz * 2^32)。"""
    if iops_per_sec <= 0:
        return 0
    val = int(round(iops_per_sec / tsc_hz * (1 << 32)))
    return min(val, 0xFFFFFFFF)


def weights_to_q16(weights):
    """純 ratio 轉 Q16.16(normalized to sum=1.0 * 65536)。
    return list of int(同長度為 weights)。
    """
    s = sum(weights)
    if s <= 0:
        raise ValueError(f"weights must sum > 0, got {weights}")
    return [int(round(w / s * 65536)) for w in weights]


def core_range(proc_idx):
    start = BASE_CORE + proc_idx * CORES_PER_PROC
    return start, start + CORES_PER_PROC - 1


def cpu_mask(proc_idx):
    start, end = core_range(proc_idx)
    mask = 0
    for c in range(start, end + 1):
        mask |= (1 << c)
    return hex(mask)


def taskset_cpus(proc_idx):
    start, end = core_range(proc_idx)
    return f"{start}-{end}"


# ── Argument parsing ─────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(
        description="SAPS-Q E1/E2/E3 orchestrator (4-tenant × multipath)"
    )
    p.add_argument("--tenants", type=int, default=4,
                   help="number of tenants (default 4)")
    p.add_argument("--weights", type=str, default="3,1,1,1",
                   help='weights csv, e.g. "3,1,1,1"')
    p.add_argument("--duration", type=int, default=60,
                   help="run duration seconds (default 60)")
    p.add_argument("--qd", type=int, default=32, help="queue depth per tenant")
    p.add_argument("--io-size", type=int, default=4096,
                   help="block size bytes (default 4096)")
    p.add_argument("--workload", type=str, default="randread",
                   help="bdevperf workload (default randread)")
    p.add_argument("--per-tenant-workload", type=str, default=None,
                   help='E8: per-tenant workload + IO size csv, format '
                        '"<rw>:<bytes>,<rw>:<bytes>,..." (one entry per '
                        'tenant). Overrides --workload / --io-size for any '
                        'tenant with an entry. Example: '
                        '"randread:4096,randwrite:65536,randread:4096,'
                        'randread:4096"')
    p.add_argument("--path-base-iops", type=str,
                   default="200000,200000,200000,200000",
                   help="per-path base IOPS csv (only first N_PATHS used)")
    p.add_argument("--demand-iops", type=str,
                   default="100000,100000,100000,100000",
                   help="per-tenant demand IOPS csv")
    p.add_argument("--probe-rate-iops", type=str, default="100,100,100,100",
                   help="per-path probe IOPS csv")
    p.add_argument("--epoch-interval-events", type=int, default=1024,
                   help="DPA scheduler trigger events (default 1024)")
    p.add_argument("--epoch-stale-us", type=int, default=500000,
                   help="host fallback threshold microseconds (default 500000; "
                        "Bug D 2026-05-24: with N standalone DPA procs, effective epoch "
                        "cadence per proc ≈ N×14ms; at N=4 that is ~56ms. Use 500ms "
                        "to cover jitter and avoid stale-fallthrough dominating admits)")
    p.add_argument("--n-paths", type=int, default=3,
                   help="paths per tenant (default 3)")
    p.add_argument("--out-dir", type=str, required=True)
    p.add_argument("--idle-tenant", type=str, default=None,
                   help='E2 idle spec, format "<tid>:<start_s>-<end_s>" '
                        '(e.g. "1:20-40"); tenant runs to start_s then exits')
    p.add_argument("--inject-path-degradation", type=int, default=None,
                   help="E3: path index (0/1/2) to inject 5ms bdev_delay at "
                        "t=<inject-time-s> on node1 (per-tenant RPC); "
                        "default no injection")
    p.add_argument("--inject-time-s", type=int, default=30,
                   help="seconds into run when to apply bdev_delay (default 30)")
    # E6 recovery: restore bdev_delay to 100us at t=<remove-time-s>
    p.add_argument("--inject-remove-time-s", type=int, default=0,
                   help="E6: seconds when to RESTORE bdev_delay to 100us "
                        "(0=keep until end)")
    # E6 recovery: periodic sapsq_dump for time-series health snapshots
    p.add_argument("--periodic-dump-interval-ms", type=int, default=0,
                   help="E6: ms between periodic sapsq_dump snapshots "
                        "(0=disabled, only start/end snapshots). Outputs to "
                        "<out>/sapsq_periodic/snap_<ms>.json")
    p.add_argument("--mode",
                   choices=["stock", "sapsq", "m4_static", "spdk_bdev_qos",
                            "stock_qd", "host_saps", "dpa_saps"],
                   default="sapsq",
                   help="which arm to enable (default sapsq); "
                        "stock_qd/host_saps/dpa_saps are N-scale sweep aliases: "
                        "stock_qd=stock, host_saps=full SAPS classifier on host "
                        "reactor (HOST_SAPS_ENABLED=1, DPA off), "
                        "dpa_saps=SAPS classifier offloaded to DPA")
    p.add_argument("--shared-cores", type=str, default=None,
                   help="taskset ALL tenant procs to this CPU list for core "
                        "oversubscription (e.g. '4-11').  Each proc still gets "
                        "its own SPDK core pair to avoid lock conflicts — this "
                        "option controls the taskset mask only when SPDK's own "
                        "mask is a subset of shared-cores. When set with "
                        "--cgroup-cores-quota, enables both oversubscription "
                        "axes simultaneously.")
    p.add_argument("--setup-node1-script", type=str, default=SETUP_ARM1_DEFAULT,
                   help=f"node1 setup script (default {SETUP_ARM1_DEFAULT})")
    p.add_argument("--no-setup-node1", action="store_true",
                   help="skip node1 setup (assume already configured)")
    p.add_argument("--tsc-hz", type=int, default=0,
                   help="override TSC hz for Q32 conversion "
                        "(default: 1e9 for BF3 aarch64)")
    p.add_argument("--dry-run", action="store_true",
                   help="print env vars + command lines, do not launch")
    return p.parse_args()


# ── Config dataclass ──────────────────────────────────────────────────────────


@dataclass
class SapsqConfig:
    tenants: int
    n_paths: int
    weights: list             # raw ratios (e.g. [3,1,1,1])
    weights_q16: list         # Q16.16 (normalized)
    demand_q32: list          # per-tenant Q32 IO/tsc
    path_base_q32: list       # per-path Q32 IO/tsc
    probe_q32: list           # per-path Q32 IO/tsc
    epoch_interval_events: int
    epoch_stale_us: int
    tsc_hz: int
    mode: str
    path_port_offsets: list = field(default_factory=lambda: list(PATH_PORT_OFFSETS))
    port_map_str: str = ""    # auto-emitted DPA_PLUGIN_PATH_MAP_PORTS (stride 16 only)

    def to_dict(self):
        return {
            "tenants": self.tenants,
            "n_paths": self.n_paths,
            "weights": self.weights,
            "weights_q16": self.weights_q16,
            "demand_q32": self.demand_q32,
            "path_base_q32": self.path_base_q32,
            "probe_q32": self.probe_q32,
            "epoch_interval_events": self.epoch_interval_events,
            "epoch_stale_us": self.epoch_stale_us,
            "tsc_hz": self.tsc_hz,
            "mode": self.mode,
            "path_port_offsets": self.path_port_offsets,
            "port_map_str": self.port_map_str,
        }


def build_config(args) -> SapsqConfig:
    tsc_hz = args.tsc_hz if args.tsc_hz > 0 else get_tsc_hz_default()

    # ── N=tenants 上限驗證 (DPA schema + port layout) ─────────────────────────
    if args.tenants <= 0:
        raise ValueError(f"--tenants must be > 0, got {args.tenants}")
    if args.tenants > SAPSQ_TENANT_MAX:
        raise ValueError(
            f"--tenants={args.tenants} exceeds SAPSQ_TENANT_MAX="
            f"{SAPSQ_TENANT_MAX} (set by dpa_plugin_com.h M3_TENANT_MAX); "
            f"DPA-side arrays cannot hold more tenants"
        )

    # ── Stride auto-promotion (N=4/8 keep stride 10; N>10 → stride 16) ───────
    # 既有 E1..E7 driver (N=4/8) 沿用 stride 10,port map 不變;
    # E11 (N=16) 自動跳到 stride 16 避免 port collision (N=11 之後在 stride 10
    # 下 4441 = tenant11 path A AND tenant1 path B)。
    port_map_str = ""
    if args.tenants <= MAX_TENANTS_PORT_SAFE:
        path_offsets = list(PATH_PORT_OFFSETS)
    elif args.tenants <= MAX_TENANTS_PORT_SAFE_STRIDE_16:
        path_offsets = list(PATH_PORT_OFFSETS_STRIDE_16)
        print(
            f"[run_sapsq] auto-promote PATH_PORT_OFFSETS to stride 16 "
            f"({path_offsets}) for N={args.tenants} (>{MAX_TENANTS_PORT_SAFE} "
            f"would collide under default stride 10)",
            file=sys.stderr,
        )
        # Stride-16 下 hashmap fallback ((port-4430)/10) 失效 → 必須顯式 emit
        # DPA_PLUGIN_PATH_MAP_PORTS。Per (tenant × path) 列每個 port → path_id。
        entries = []
        for t in range(args.tenants):
            for p_idx in range(args.n_paths):
                port = TENANT_BASE_PORT + t + path_offsets[p_idx]
                entries.append(f"{port}:{p_idx}")
        port_map_str = ",".join(entries)
        print(
            f"[run_sapsq] auto-emit DPA_PLUGIN_PATH_MAP_PORTS with "
            f"{len(entries)} entries (stride 16 disables host hashmap fallback)",
            file=sys.stderr,
        )
    else:
        raise ValueError(
            f"--tenants={args.tenants} exceeds MAX_TENANTS_PORT_SAFE_STRIDE_16="
            f"{MAX_TENANTS_PORT_SAFE_STRIDE_16}; widen PATH_PORT_OFFSETS_STRIDE_16 "
            f"or reduce --tenants"
        )

    weights_raw = [int(x) for x in args.weights.split(",")]
    if len(weights_raw) < args.tenants:
        raise ValueError(f"--weights has {len(weights_raw)} entries, "
                         f"need {args.tenants}")
    weights_raw = weights_raw[: args.tenants]
    weights_q16 = weights_to_q16(weights_raw)

    demand_iops = [int(x) for x in args.demand_iops.split(",")]
    if len(demand_iops) < args.tenants:
        raise ValueError(f"--demand-iops needs {args.tenants} values")
    demand_iops = demand_iops[: args.tenants]
    demand_q32 = [iops_to_q32(d, tsc_hz) for d in demand_iops]

    base_iops = [int(x) for x in args.path_base_iops.split(",")]
    if len(base_iops) < args.n_paths:
        raise ValueError(f"--path-base-iops needs {args.n_paths} values")
    base_iops = base_iops[: args.n_paths]
    path_base_q32 = [iops_to_q32(b, tsc_hz) for b in base_iops]

    probe_iops = [int(x) for x in args.probe_rate_iops.split(",")]
    if len(probe_iops) < args.n_paths:
        raise ValueError(f"--probe-rate-iops needs {args.n_paths} values")
    probe_iops = probe_iops[: args.n_paths]
    probe_q32 = [iops_to_q32(p, tsc_hz) for p in probe_iops]

    return SapsqConfig(
        tenants=args.tenants,
        n_paths=args.n_paths,
        weights=weights_raw,
        weights_q16=weights_q16,
        demand_q32=demand_q32,
        path_base_q32=path_base_q32,
        probe_q32=probe_q32,
        epoch_interval_events=args.epoch_interval_events,
        epoch_stale_us=args.epoch_stale_us,
        tsc_hz=tsc_hz,
        mode=args.mode,
        path_port_offsets=path_offsets,
        port_map_str=port_map_str,
    )


def parse_per_tenant_workload(spec: str, n_tenants: int):
    """parse "<rw>:<bytes>,<rw>:<bytes>,..." → list[(rw, io_size)] length n_tenants。

    Returns None if spec is empty/None (caller uses uniform --workload/--io-size).
    Raises ValueError on malformed entries or wrong count.
    """
    if not spec:
        return None
    items = []
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        m = re.match(r"^(\w+):(\d+)$", entry)
        if not m:
            raise ValueError(
                f"--per-tenant-workload entry '{entry}' must be 'rw:bytes' "
                f"(e.g. 'randread:4096')"
            )
        rw = m.group(1)
        io_size = int(m.group(2))
        if io_size <= 0:
            raise ValueError(f"io_size must be > 0, got {io_size}")
        items.append((rw, io_size))
    if len(items) != n_tenants:
        raise ValueError(
            f"--per-tenant-workload has {len(items)} entries, "
            f"need exactly {n_tenants} (one per tenant)"
        )
    return items


def parse_idle_spec(spec: str):
    """parse "<tid>:<start>-<end>" → (tid, start_s, end_s)。"""
    if not spec:
        return None
    m = re.match(r"^(\d+):(\d+)-(\d+)$", spec.strip())
    if not m:
        raise ValueError(f"--idle-tenant must be 'tid:start-end', got {spec}")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


# ── env var assembly per tenant proc ──────────────────────────────────────────


def csv(xs):
    return ",".join(str(x) for x in xs)


def build_env_for_tenant(cfg: SapsqConfig, tenant_id: int) -> list:
    """return list of 'KEY=VAL' strings to prepend to bdevperf via `env` cmd.
    Plugin init reads getenv(); 此處組成 the env list for `env K=V ...` prefix。
    """
    env = []
    # SAMPLE_RATE=64 default: at rate=1 the DPA→host ring overflows under
    # 200k IOPS × 4 tenants (Task #34), causing dropped completions and
    # noisy classifier signal. 1/64 sampling keeps signal-to-noise high
    # while preventing overflow. Override via env if needed.
    _sample_rate = os.environ.get("DPA_PLUGIN_SAMPLE_RATE", "64")

    if cfg.mode == "sapsq":
        # DPA plugin 啟動 + SAPS-Q 2D M-series 路徑開
        # SAPSQ_ENABLED=1 → sapsq_m_init() 寫入 2D budget plane (slice 1-4)
        # SAPS_Q_ENABLED=0 → 關舊 1D path,避免 double admission_check
        link_cap_iops = sum(
            int(round(q * cfg.tsc_hz / (1 << 32)))
            for q in cfg.path_base_q32
        )
        if link_cap_iops == 0:
            link_cap_iops = 200000
        probe_iops_per_path = int(round(
            cfg.probe_q32[0] * cfg.tsc_hz / (1 << 32)
        )) if cfg.probe_q32 else 1000
        env += [
            "DPA_PLUGIN_ROLE=standalone",
            "DPA_PLUGIN_DISABLE_INIT=0",
            "HOST_SAPS_ENABLED=0",
            f"DPA_PLUGIN_SAMPLE_RATE={_sample_rate}",
            "SAPS_TRACE=0",
            # 新 2D M-series SAPS-Q (slice 1-4)
            "SAPSQ_ENABLED=1",
            f"SAPSQ_MY_TENANT_ID={tenant_id}",
            f"SAPSQ_NUM_TENANTS={cfg.tenants}",
            f"SAPSQ_NUM_PATHS={cfg.n_paths}",
            f"SAPSQ_LINK_CAP_IOPS={link_cap_iops}",
            "SAPSQ_EPOCH_PERIOD_US=1000",
            f"SAPSQ_WEIGHTS={csv(cfg.weights)}",
            f"SAPSQ_PROBE_RATE_IOPS={probe_iops_per_path}",
            f"SAPSQ_HOST_TSC_FREQ={cfg.tsc_hz}",
            # 舊 1D path: enable only to set sapsq_epoch_interval_events +
            # sapsq_epoch_interval_tsc_fallback (Bug-A fix: TSC-based fallback
            # 10ms default prevents starvation when event count stays below
            # threshold). sapsq_m_init() (SAPSQ_ENABLED=1) runs AFTER and
            # overwrites my_tenant_id with the correct per-tenant value.
            # SAPS_Q_EPOCH_INTERVAL_EVENTS=64 ensures DPA commits epoch even
            # when throttled IOs reduce event count well below 1024.
            "SAPS_Q_ENABLED=1",
            f"SAPS_Q_MY_TENANT_ID={tenant_id}",
            "SAPS_Q_EPOCH_INTERVAL_EVENTS=64",
            # 顯式關 M4/M5 避免 double admission_check
            "SAPS_M4_ENABLED=0",
            "SAPS_M5_DRR_ENABLED=0",
            # Lane I: D classifier bypass — set SAPSQ_BYPASS_D_CLASSIFIER=1 in outer
            # env to force all paths HEALTHY, allowing slice 1-4 2D enforcement
            # validation without D classifier FSM interference (E1 baseline).
            # E3 path-degradation: unset or set to 0 to re-enable D classifier.
            f"SAPSQ_BYPASS_D_CLASSIFIER={os.environ.get('SAPSQ_BYPASS_D_CLASSIFIER', '0')}",
        ]
        # Stride-16 下 host hashmap fallback ((port-4430)/10) 算錯 path_id,
        # 必須顯式告訴 plugin 每個 port → path_id map。N=4/8 (stride 10) 不需要。
        if cfg.port_map_str:
            env.append(f"DPA_PLUGIN_PATH_MAP_PORTS={cfg.port_map_str}")
    elif cfg.mode == "m4_static":
        # M4 v2 host token bucket (per-tenant 靜態速率,沒 DPA dynamic)
        # M4 weights 用整數比例(不是 Q16.16),沿用舊 M4 init 慣例
        env += [
            "DPA_PLUGIN_ROLE=standalone",
            "DPA_PLUGIN_DISABLE_INIT=0",
            "HOST_SAPS_ENABLED=0",
            f"DPA_PLUGIN_SAMPLE_RATE={_sample_rate}",
            "SAPS_TRACE=0",
            "SAPS_Q_ENABLED=0",
            "SAPS_M4_ENABLED=1",
            f"SAPS_M4_LINK_IOPS={sum(int(x) for x in os.environ.get('_M4_BASE_IOPS', '600000').split(','))}"
            if "_M4_BASE_IOPS" in os.environ
            else "SAPS_M4_LINK_IOPS=600000",
            f"SAPS_M4_WEIGHTS={csv(cfg.weights)}",
            f"SAPS_M4_TENANT_ID={tenant_id}",
        ]
    elif cfg.mode in ("stock", "stock_qd"):
        # 純 stock — plugin 完全不 init
        env += [
            "DPA_PLUGIN_DISABLE_INIT=1",
            "SAPS_Q_ENABLED=0",
            "SAPS_M4_ENABLED=0",
            "SAPS_M5_DRR_ENABLED=0",
        ]
    elif cfg.mode == "host_saps":
        # Full SAPS classifier (NEWMA + Frugal-2U + multi-modal FSM + per-opcode)
        # runs on the SPDK reactor thread — consumes host CPU cycles under fault.
        # DPA firmware disabled (DPA_PLUGIN_DISABLE_INIT=1).  Only difference vs
        # dpa_saps is classifier placement: host reactor vs DPA silicon.
        _sample_rate = os.environ.get("DPA_PLUGIN_SAMPLE_RATE", "1")
        env += [
            "DPA_PLUGIN_DISABLE_INIT=1",
            "HOST_SAPS_ENABLED=1",
            f"DPA_PLUGIN_SAMPLE_RATE={_sample_rate}",
            "SAPS_Q_ENABLED=0",
            "SAPS_M4_ENABLED=0",
            "SAPS_M5_DRR_ENABLED=0",
        ]
    elif cfg.mode == "dpa_saps":
        # SAPS classifier offloaded to DPA — same algorithm as host_saps but
        # runs on DPA silicon, freeing host reactor cycles.
        _sample_rate = os.environ.get("DPA_PLUGIN_SAMPLE_RATE", "1")
        env += [
            "DPA_PLUGIN_ROLE=standalone",
            "DPA_PLUGIN_DISABLE_INIT=0",
            "HOST_SAPS_ENABLED=0",
            f"DPA_PLUGIN_SAMPLE_RATE={_sample_rate}",
            "SAPS_TRACE=0",
            "SAPS_Q_ENABLED=0",
            "SAPS_M4_ENABLED=0",
            "SAPS_M5_DRR_ENABLED=0",
        ]
    elif cfg.mode == "spdk_bdev_qos":
        # initiator 同 stock;node1 須先設好 bdev QoS
        env += [
            "DPA_PLUGIN_DISABLE_INIT=1",
            "SAPS_Q_ENABLED=0",
        ]
    else:
        raise ValueError(f"unknown mode {cfg.mode}")

    return env


# ── bdevperf process management ──────────────────────────────────────────────


class TenantProc:
    """Single bdevperf process for one tenant, attaches N_PATHS paths."""

    def __init__(self, tenant_id: int, cfg: SapsqConfig, args, out_dir: Path,
                 logfile):
        self.tid = tenant_id
        self.cfg = cfg
        self.args = args
        self.out_dir = out_dir / f"tenant_{tenant_id:02d}"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.logpath = self.out_dir / "bdevperf.log"
        self.result_json = self.out_dir / "result.json"
        self.sock = SOCK_TMPL.format(i=tenant_id)
        self.proc = None
        self.logfp = None
        self.orch_log = logfile
        # --shared-cores: when set, all procs share the same SPDK core mask.
        # Each proc's own core pair must still be inside shared-cores so that
        # SPDK's per-core lock file doesn't conflict (each proc claims a unique
        # core subset within the shared range).
        shared = getattr(args, "shared_cores", None)
        if shared:
            # Use the proc's own per-proc core pair (no lock conflict) but
            # oversubscribe by assigning all procs within the shared range.
            # The shared_cores string is stored for logging; actual mask stays
            # per-proc to satisfy SPDK lock requirements.
            self._mask = cpu_mask(tenant_id)
            self._taskset_cpus = taskset_cpus(tenant_id)
            self._shared_cores_hint = shared
        else:
            self._mask = cpu_mask(tenant_id)
            self._taskset_cpus = taskset_cpus(tenant_id)
            self._shared_cores_hint = None
        self._nqn = f"{NQN_PREFIX}{tenant_id}"
        self._tenant_base_port = TENANT_BASE_PORT + tenant_id
        self._duration = args.duration
        # E8: resolve per-tenant workload + io_size (falls back to uniform args)
        pt = parse_per_tenant_workload(
            getattr(args, "per_tenant_workload", None), cfg.tenants)
        if pt is not None:
            self._workload, self._io_size = pt[tenant_id]
        else:
            self._workload = args.workload
            self._io_size = args.io_size

    def _log(self, msg):
        log(f"[t{self.tid:02d}] {msg}", self.orch_log)

    def env_list(self) -> list:
        """env list 給 dry-run 用(同 start 用的)。"""
        return build_env_for_tenant(self.cfg, self.tid)

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
               "-o", str(self._io_size),
               "-w", self._workload,
               "-t", str(self._duration),
               "-z", "-l"]
        )

    def attach_cmds(self) -> list:
        """list of rpc.py invocations for N_PATHS attach_controller."""
        cmds = []
        for p_idx in range(self.cfg.n_paths):
            port = self._tenant_base_port + self.cfg.path_port_offsets[p_idx]
            cmds.append([
                "sudo", RPCPY, "-s", self.sock,
                "bdev_nvme_attach_controller",
                "-b", "mp", "-t", "rdma", "-a", TARGET_IP, "-s", str(port),
                "-f", "ipv4", "-n", self._nqn, "--multipath", "multipath",
            ])
        return cmds

    def start(self):
        subprocess.run(["sudo", "rm", "-f", self.sock], capture_output=True)
        cmd = self.cmd_list()
        self._log(f"launching: mask={self._mask} cores={self._taskset_cpus} "
                  f"mode={self.cfg.mode} nqn={self._nqn} "
                  f"ports={[self._tenant_base_port + o for o in self.cfg.path_port_offsets[:self.cfg.n_paths]]}")
        # show env for debuggability
        env_only = [x for x in cmd if "=" in x and not x.startswith("/")]
        self._log(f"env: {env_only}")
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
        subprocess.run(
            ["sudo", RPCPY, "-s", self.sock,
             "bdev_nvme_set_options", "--io-path-stat"],
            capture_output=True, timeout=15
        )
        r = subprocess.run(
            ["sudo", RPCPY, "-s", self.sock, "framework_start_init"],
            capture_output=True, text=True, timeout=30
        )
        if r.returncode != 0:
            self._log(f"framework_start_init failed: {r.stderr[:120]}")
            return False
        time.sleep(1)

        for cmd in self.attach_cmds():
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
            self._log(f"attach: rc={r.returncode} "
                      f"{(r.stdout + r.stderr).strip()[:100]}")
        time.sleep(2)

        r = subprocess.run(
            ["sudo", RPCPY, "-s", self.sock, "bdev_get_bdevs"],
            capture_output=True, text=True, timeout=30
        )
        try:
            bdevs = json.loads(r.stdout)
            for bdev in bdevs:
                nvme_ctls = bdev.get("driver_specific", {}).get("nvme", [])
                if nvme_ctls:
                    count = len(nvme_ctls)
                    self._log(f"bdev {bdev['name']} controllers={count}")
                    if count < self.cfg.n_paths:
                        self._log(f"WARN: expected {self.cfg.n_paths} "
                                  f"controllers, got {count}")
                    break
        except Exception:
            self._log("could not parse bdev_get_bdevs")

        # Set multipath policy per mode.
        # sapsq → plugin (DPA SAPS_Q admission_check fires).
        # All other modes → active_active + round_robin selector so they
        # actually exercise all 3 paths and observe path B degradation. Default
        # active_passive sticks to one path and invalidates the comparison.
        if self.cfg.mode == "sapsq":
            cmd = ["sudo", RPCPY, "-s", self.sock,
                   "bdev_nvme_set_multipath_policy", "-b", "mpn1",
                   "-p", "plugin"]
            label = "plugin"
        else:
            cmd = ["sudo", RPCPY, "-s", self.sock,
                   "bdev_nvme_set_multipath_policy", "-b", "mpn1",
                   "-p", "active_active", "-s", "round_robin"]
            label = "active_active+round_robin"
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        self._log(f"set multipath policy={label}: rc={r.returncode}")
        if r.returncode != 0:
            self._log(f"  stderr={r.stderr.strip()}")
        return True

    def run_perform_tests(self):
        cmd = [
            "sudo", "timeout", str(self._duration + 30),
            "python3", BDEVPERF_PY,
            "-s", self.sock,
            "-t", str(self._duration + 15),
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

    def stop(self):
        """SIGSTOP — used for E2 idle phase (pause but keep proc alive)。"""
        if self.proc:
            try:
                subprocess.run(["sudo", "kill", "-STOP", str(self.proc.pid)],
                               capture_output=True)
            except Exception:
                pass

    def cont(self):
        """SIGCONT to resume after idle phase."""
        if self.proc:
            try:
                subprocess.run(["sudo", "kill", "-CONT", str(self.proc.pid)],
                               capture_output=True)
            except Exception:
                pass

    def find_memfd_path(self) -> Optional[str]:
        # Find /proc/<pid>/fd/<n> for dpa_plugin_ring memfd. bdevperf creates
        # it shortly after fork(), so retry to avoid racing the child startup.
        if not self.proc:
            raise RuntimeError(
                f"ERROR: t{self.tid:02d} bdevperf process not started; "
                f"cannot find {DPA_PLUGIN_MEMFD_NAME} memfd"
            )
        last_candidates = []
        # Smoke test on 2026-05-25 showed bdevperf for t01/t02/t03 takes
        # > 5s to fork + run DPA plugin init that creates the memfd (only t00
        # consistently makes it under 5s). Extend to 20s budget: 100 attempts
        # × 200ms = 20.0s. Periodic dump thread re-snapshots later anyway, so
        # if a tenant misses the start snapshot, end snapshot still covers it.
        attempts = 100
        sleep_s = 0.2
        for _ in range(attempts):
            last_candidates = self._candidate_pids()
            for pid in last_candidates:
                fd_path = self._find_memfd_path_for_pid(pid)
                if fd_path:
                    return fd_path
            time.sleep(sleep_s)
        msg = (
            f"ERROR: t{self.tid:02d} {DPA_PLUGIN_MEMFD_NAME} memfd not found "
            f"after {attempts * sleep_s:.1f}s; "
            f"candidate_pids={last_candidates}"
        )
        self._log(msg)
        raise RuntimeError(msg)

    def _candidate_pids(self) -> list:
        # self.proc is the sudo/taskset launcher; bdevperf owns the memfd.
        if not self.proc:
            return []
        seen = set()
        ordered = []
        stack = [self.proc.pid]
        while stack:
            pid = stack.pop(0)
            if pid in seen:
                continue
            seen.add(pid)
            ordered.append(pid)
            stack.extend(self._child_pids(pid))

        # 2026-05-25: BFS via /proc/<pid>/children sometimes only reaches
        # sudo + taskset (depth 2) and misses the bdevperf grandchild,
        # especially for root-owned processes where /proc reads need sudo.
        # Last-resort fallback: pgrep -f for bdevperf processes owning
        # /var/tmp/bdevperf_sapsq_proc{tid}.sock — those are uniquely tied
        # to this tenant by socket name. This catches the bdevperf even if
        # /proc walk failed.
        try:
            sock_marker = SOCK_TMPL.format(i=self.tid)
            r = subprocess.run(
                ["sudo", "pgrep", "-f", sock_marker],
                capture_output=True, text=True, timeout=5
            )
            for line in r.stdout.split():
                if line.isdigit():
                    pid = int(line)
                    if pid not in seen:
                        seen.add(pid)
                        ordered.append(pid)
        except Exception:
            pass

        bdevperf_pids = [pid for pid in ordered
                         if self._proc_comm(pid) == "bdevperf"]
        other_pids = [pid for pid in ordered if pid not in bdevperf_pids]
        return bdevperf_pids + other_pids

    def _proc_comm(self, pid: int) -> str:
        try:
            return Path(f"/proc/{pid}/comm").read_text().strip()
        except Exception:
            return ""

    def _child_pids(self, pid: int) -> list:
        child_file = Path(f"/proc/{pid}/task/{pid}/children")
        try:
            text = child_file.read_text().strip()
            if text:
                return [int(x) for x in text.split() if x.isdigit()]
        except Exception:
            pass

        try:
            r = subprocess.run(
                ["sudo", "pgrep", "-P", str(pid)],
                capture_output=True, text=True, timeout=5
            )
            return [int(x) for x in r.stdout.split() if x.isdigit()]
        except Exception:
            return []

    def _find_memfd_path_for_pid(self, pid: int) -> Optional[str]:
        fd_dir = f"/proc/{pid}/fd"
        try:
            for name in os.listdir(fd_dir):
                path = f"{fd_dir}/{name}"
                try:
                    if DPA_PLUGIN_MEMFD_NAME in os.readlink(path):
                        return path
                except Exception:
                    continue
        except Exception:
            pass

        try:
            r = subprocess.run(
                ["sudo", "ls", "-l", fd_dir],
                capture_output=True, text=True, timeout=10
            )
            for line in r.stdout.splitlines():
                if DPA_PLUGIN_MEMFD_NAME in line and "->" in line:
                    left = line.split("->", 1)[0].split()
                    if left:
                        return f"{fd_dir}/{left[-1]}"
        except Exception:
            pass
        return None


    def parse_result(self):
        try:
            content = self.logpath.read_text()
        except Exception:
            content = ""

        iops = None
        avg_lat_us = None
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
                            avg_lat_us = res.get("avg_latency_us")
                            io_failed = res.get("io_failed", 0)
                            break
            except Exception:
                pass
            break

        if not iops:
            periodic = [float(x) for x in
                        re.findall(r"\s+([\d.]+)\s+IOPS,", content)]
            if len(periodic) >= 3:
                iops = round(sum(periodic) / len(periodic), 0)

        def lat_pct(text, pct):
            m = re.search(rf"\s*{re.escape(pct)}%\s*:\s*([\d.]+)us", text)
            return round(float(m.group(1)), 3) if m else None

        result = {
            "tenant_id": self.tid,
            "mode": self.cfg.mode,
            "nqn": self._nqn,
            "iops": iops,
            "io_failed": io_failed,
            "avg_lat_us": round(avg_lat_us, 3) if avg_lat_us else None,
            "lat_p50_us":  lat_pct(content, "50.00000"),
            "lat_p99_us":  lat_pct(content, "99.00000"),
            "lat_p999_us": lat_pct(content, "99.90000"),
            "lat_p9999_us": lat_pct(content, "99.99000"),
        }
        self.result_json.write_text(json.dumps(result, indent=2))
        self._log(f"result: IOPS={iops} avg_lat={avg_lat_us} "
                  f"p99={result['lat_p99_us']}")
        return result


# ── node1 helpers ────────────────────────────────────────────────────────────


def setup_arm1(script_path, logfile):
    # Audit follow-up: silent WARN previously masked E5 broken d1_on/d3_on bug
    # (E5 driver mismatched mutation-only D-series scripts against tenants
    # topology → silent garbage data). Now raises on failure to halt the run.
    log(f"node1 setup: bash {script_path}", logfile)
    r = subprocess.run(["ssh", "node1", f"bash {script_path}"],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        log(f"ERROR: node1 setup rc={r.returncode}: {r.stderr[:200]}", logfile)
        raise RuntimeError(
            f"setup_arm1 script {script_path} failed with rc={r.returncode}; "
            f"see orchestrator log"
        )
    log("node1 setup ok", logfile)


def inject_path_degradation(path_idx, n_tenants, logfile):
    """node1 bdev_delay 5ms latency 加到 path<idx> 的 delay bdev (per-tenant)。

    bdev-level injection (tc-netem is invisible to RDMA kernel-bypass; bdev_delay
    is visible to NVMe completion timestamps and triggers SAPS-Q NEWMA classifier).
    Path B socket = /var/tmp/spdk_b.sock; bdev naming = delay_t<i>_B per
    setup_arm1_sapsq_4t3p.sh.
    """
    # path index → spdk socket + bdev suffix
    sock_map = {0: "/var/tmp/spdk_a.sock",
                1: "/var/tmp/spdk_b.sock",
                2: "/var/tmp/spdk_c.sock"}
    suffix_map = {0: "A", 1: "B", 2: "C"}
    sock = sock_map.get(path_idx)
    suffix = suffix_map.get(path_idx)
    if sock is None:
        log(f"[INJECT] unknown path_idx={path_idx}", logfile)
        return
    log(f"[INJECT] path {path_idx} bdev_delay 5ms via {sock} "
        f"({n_tenants} tenants)", logfile)
    # RPC takes one latency field per call → chain 4 calls per bdev with &&
    for i in range(n_tenants):
        bdev = f"delay_t{i}_{suffix}"
        parts = [
            f"sudo /home/user/spdk/scripts/rpc.py -s {sock} "
            f"bdev_delay_update_latency {bdev} {field} 5000"
            for field in ("avg_read", "p99_read", "avg_write", "p99_write")
        ]
        cmd = " && ".join(parts)
        r = subprocess.run(["ssh", "node1", cmd],
                           capture_output=True, text=True, timeout=30)
        log(f"[INJECT] t{i} {bdev} rc={r.returncode} {r.stderr[:120]}",
            logfile)


def teardown_path_degradation(path_idx, n_tenants, logfile):
    """Restore bdev_delay latency to baseline 100us on path<idx>."""
    sock_map = {0: "/var/tmp/spdk_a.sock",
                1: "/var/tmp/spdk_b.sock",
                2: "/var/tmp/spdk_c.sock"}
    suffix_map = {0: "A", 1: "B", 2: "C"}
    sock = sock_map.get(path_idx)
    suffix = suffix_map.get(path_idx)
    if sock is None:
        log(f"[INJECT_END] unknown path_idx={path_idx}", logfile)
        return
    log(f"[INJECT_END] path {path_idx} restore 100us via {sock}", logfile)
    for i in range(n_tenants):
        bdev = f"delay_t{i}_{suffix}"
        parts = [
            f"sudo /home/user/spdk/scripts/rpc.py -s {sock} "
            f"bdev_delay_update_latency {bdev} {field} 100"
            for field in ("avg_read", "p99_read", "avg_write", "p99_write")
        ]
        cmd = " && ".join(parts)
        r = subprocess.run(["ssh", "node1", cmd],
                           capture_output=True, text=True, timeout=30)
        log(f"[INJECT_END] t{i} {bdev} rc={r.returncode} {r.stderr[:120]}",
            logfile)


# ── SAPS-Q counter dump ──────────────────────────────────────────────────────


def dump_sapsq_counters(procs, out_path: Path, logfile):
    """每個 tenant proc 透過 /proc/<pid>/fd/<memfd> 讀 ring → 呼 sapsq_dump 二進位
    binary 印 JSON;merge 成 single file。

    若 sapsq_dump 不存在(未編譯)→ skip + WARN(不 fail run)。
    """
    if not os.path.exists(SAPSQ_DUMP_BIN):
        log(f"WARN: {SAPSQ_DUMP_BIN} not found — skipping counter dump",
            logfile)
        out_path.write_text(json.dumps({
            "missing_helper": True,
            "helper_path": SAPSQ_DUMP_BIN,
        }, indent=2))
        return

    aggregate = {}
    for p in procs:
        # find_memfd_path raises RuntimeError after exhausting 5s of retries
        # (v2 fix to surface ERROR instead of silent WARN). Catch here so one
        # slow-fork bdevperf in tenant N does not abort the whole multi-tenant
        # run before cleanup. The per-tenant degradation is still surfaced via
        # observability_degraded in the aggregator's regime banner.
        try:
            fd_path = p.find_memfd_path()
        except RuntimeError as e:
            log(f"WARN: t{p.tid:02d} memfd not found after retries: {e}",
                logfile)
            continue
        if not fd_path:
            log(f"WARN: t{p.tid:02d} memfd not found — skip", logfile)
            continue
        r = subprocess.run(
            ["sudo", SAPSQ_DUMP_BIN, fd_path],
            capture_output=True, text=True, timeout=10
        )
        if r.returncode != 0:
            log(f"WARN: sapsq_dump t{p.tid} rc={r.returncode}: {r.stderr[:120]}",
                logfile)
            continue
        try:
            aggregate[f"tenant_{p.tid:02d}"] = json.loads(r.stdout)
        except Exception as e:
            log(f"WARN: parse sapsq_dump output t{p.tid}: {e}", logfile)
    out_path.write_text(json.dumps(aggregate, indent=2))


# ── E2 idle controller ───────────────────────────────────────────────────────


def schedule_idle_phase(procs, idle_spec, logfile):
    """spawn thread that SIGSTOPs target tenant at start_s, SIGCONTs at end_s。"""
    if idle_spec is None:
        return None
    tid, start_s, end_s = idle_spec
    target = next((p for p in procs if p.tid == tid), None)
    if target is None:
        log(f"WARN: idle tenant {tid} not in proc list", logfile)
        return None

    def _do_idle():
        log(f"[IDLE] t{tid} sleep until t={start_s}s", logfile)
        time.sleep(start_s)
        log(f"[IDLE] STOP t{tid}", logfile)
        target.stop()
        time.sleep(max(0, end_s - start_s))
        log(f"[IDLE] CONT t{tid}", logfile)
        target.cont()

    th = threading.Thread(target=_do_idle, daemon=True)
    th.start()
    return th


# ── E3 inject controller ─────────────────────────────────────────────────────


def schedule_path_remove(args, logfile):
    """E6 recovery: 在 t=inject_remove_time_s 還原 path<idx> bdev_delay 到 100us。"""
    if (args.inject_path_degradation is None
            or args.inject_remove_time_s <= 0):
        return None
    if args.inject_remove_time_s <= args.inject_time_s:
        log(f"WARN: --inject-remove-time-s ({args.inject_remove_time_s}) must "
            f"be > --inject-time-s ({args.inject_time_s}); skipping remove",
            logfile)
        return None
    p_idx = args.inject_path_degradation

    def _do_remove():
        time.sleep(args.inject_remove_time_s)
        log(f"[REMOVE_INJECT] t={args.inject_remove_time_s}s path={p_idx}",
            logfile)
        teardown_path_degradation(p_idx, args.tenants, logfile)

    th = threading.Thread(target=_do_remove, daemon=True)
    th.start()
    return th


def schedule_periodic_dump(procs, args, out_dir, logfile):
    """E6 recovery: periodic sapsq_dump 每 N ms 一次 → out/sapsq_periodic/。
    回傳 (thread, stop_event);main loop 跑完後 stop_event.set() + join。
    """
    if args.periodic_dump_interval_ms <= 0:
        return None, None
    if not os.path.exists(SAPSQ_DUMP_BIN):
        log(f"WARN: {SAPSQ_DUMP_BIN} not found — periodic dump disabled",
            logfile)
        return None, None
    periodic_dir = out_dir / "sapsq_periodic"
    periodic_dir.mkdir(parents=True, exist_ok=True)
    interval_s = args.periodic_dump_interval_ms / 1000.0
    stop_ev = threading.Event()

    def _do_loop():
        t0 = time.time()
        log(f"[PERIODIC_DUMP] start interval={args.periodic_dump_interval_ms}ms",
            logfile)
        idx = 0
        while not stop_ev.is_set():
            elapsed_ms = int((time.time() - t0) * 1000)
            snap_path = periodic_dir / f"snap_{elapsed_ms:06d}ms.json"
            # Wrap to prevent any per-tick exception from killing the
            # daemon thread silently — surfaces dump error per snapshot.
            try:
                dump_sapsq_counters(procs, snap_path, logfile)
            except Exception as e:
                log(f"[PERIODIC_DUMP] WARN snap_{elapsed_ms}ms: {e}",
                    logfile)
            idx += 1
            # sleep until next tick (best-effort, don't drift)
            next_target = t0 + idx * interval_s
            sleep_for = max(0, next_target - time.time())
            if stop_ev.wait(timeout=sleep_for):
                break
        log(f"[PERIODIC_DUMP] stop after {idx} snapshots", logfile)

    th = threading.Thread(target=_do_loop, daemon=True)
    th.start()
    return th, stop_ev


def schedule_path_inject(args, logfile):
    if args.inject_path_degradation is None:
        return None
    p_idx = args.inject_path_degradation
    if p_idx < 0 or p_idx > 2:
        log(f"WARN: --inject-path-degradation must be 0/1/2, got {p_idx}",
            logfile)
        return None
    # path A = path 0, path B = path 1, path C = path 2
    # bdev_delay RPC injection (tc-netem is invisible to RDMA kernel-bypass).

    def _do_inject():
        log(f"[INJECT] sleep until t={args.inject_time_s}s", logfile)
        time.sleep(args.inject_time_s)
        inject_path_degradation(p_idx, args.tenants, logfile)

    th = threading.Thread(target=_do_inject, daemon=True)
    th.start()
    return th


# ── dry-run helper ───────────────────────────────────────────────────────────


def print_dry_run(args, cfg, procs):
    print("\n=== DRY RUN — SAPS-Q orchestrator ===", file=sys.stderr)
    print(f"mode={cfg.mode} tenants={cfg.tenants} paths={cfg.n_paths} "
          f"duration={args.duration}s qd={args.qd}", file=sys.stderr)
    if args.per_tenant_workload:
        pt = parse_per_tenant_workload(args.per_tenant_workload, cfg.tenants)
        print(f"per-tenant workload (E8): {pt}", file=sys.stderr)
    else:
        print(f"workload={args.workload} io_size={args.io_size} (uniform)",
              file=sys.stderr)
    print(f"weights raw={cfg.weights} -> Q16.16={cfg.weights_q16} "
          f"(sum={sum(cfg.weights_q16)})", file=sys.stderr)
    print(f"demand_iops={args.demand_iops} -> Q32={cfg.demand_q32}",
          file=sys.stderr)
    print(f"path_base_iops={args.path_base_iops} -> Q32={cfg.path_base_q32}",
          file=sys.stderr)
    print(f"probe_rate_iops={args.probe_rate_iops} -> Q32={cfg.probe_q32}",
          file=sys.stderr)
    print(f"tsc_hz={cfg.tsc_hz}", file=sys.stderr)
    print(f"path_port_offsets={cfg.path_port_offsets} "
          f"(stride={cfg.path_port_offsets[1] - cfg.path_port_offsets[0]})",
          file=sys.stderr)
    if cfg.port_map_str:
        entries = cfg.port_map_str.split(",")
        head = ",".join(entries[:5])
        tail = ",".join(entries[-5:])
        print(f"DPA_PLUGIN_PATH_MAP_PORTS auto-emitted "
              f"({len(entries)} entries): {head}, ..., {tail}",
              file=sys.stderr)
    print(f"node1 setup: {args.setup_arm1_script}", file=sys.stderr)
    if args.idle_tenant:
        spec = parse_idle_spec(args.idle_tenant)
        print(f"idle: tenant {spec[0]} stops at t={spec[1]}s, resumes at "
              f"t={spec[2]}s", file=sys.stderr)
    if args.inject_path_degradation is not None:
        print(f"inject: path {args.inject_path_degradation} bdev_delay 5ms at "
              f"t={args.inject_time_s}s on node1 (per-tenant RPC)",
              file=sys.stderr)
    print(f"\nper-tenant launch commands ({len(procs)} procs):",
          file=sys.stderr)
    for p in procs:
        env = p.env_list()
        # split env into key=val pairs and pretty print
        env_str = " ".join(env)
        cmd = p.cmd_list()
        # strip the env list from cmd for readability(env is its own block)
        cmd_no_env = [c for c in cmd
                      if c not in env and c not in ("env",)]
        print(f"  t{p.tid:02d}: env={env_str}", file=sys.stderr)
        print(f"        cmd={' '.join(cmd_no_env)}", file=sys.stderr)
        for ac in p.attach_cmds():
            print(f"        attach={' '.join(ac)}", file=sys.stderr)
    print("=== end dry run ===\n", file=sys.stderr)


# ── cleanup ──────────────────────────────────────────────────────────────────


def cleanup_all(procs):
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
         "rm -f /var/tmp/spdk_cpu_lock_*; true"],
        capture_output=True, timeout=15
    )
    time.sleep(2)


# ── main ─────────────────────────────────────────────────────────────────────


def main():
    args = parse_args()
    cfg = build_config(args)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        # Build proc objects (no socket / no launch) just for cmd printing
        # Use temporary logfile sink that does nothing
        dummy_log = open(os.devnull, "w")
        procs = [TenantProc(i, cfg, args, out_dir, dummy_log)
                 for i in range(cfg.tenants)]
        print_dry_run(args, cfg, procs)
        dummy_log.close()
        return

    orch_log_path = out_dir / "orchestrator.log"
    orch_log = open(orch_log_path, "w")

    # persist config for reproducibility
    (out_dir / "run_config.json").write_text(json.dumps({
        "args": vars(args),
        "config": cfg.to_dict(),
    }, indent=2))

    log("=== SAPS-Q orchestrator start ===", orch_log)
    log(f"mode={cfg.mode} tenants={cfg.tenants} paths={cfg.n_paths} "
        f"duration={args.duration}s qd={args.qd}", orch_log)

    max_core = BASE_CORE + cfg.tenants * CORES_PER_PROC - 1
    if max_core >= 96:
        log(f"ERROR: max core {max_core} >= 96, reduce --tenants", orch_log)
        sys.exit(1)

    procs = [TenantProc(i, cfg, args, out_dir, orch_log)
             for i in range(cfg.tenants)]

    interrupted = [False]

    def sigint_handler(sig, frame):
        log("SIGINT — cleaning up", orch_log)
        interrupted[0] = True
        if args.inject_path_degradation is not None:
            teardown_path_degradation(args.inject_path_degradation,
                                      args.tenants, orch_log)
        cleanup_all(procs)
        sys.exit(130)

    signal.signal(signal.SIGINT, sigint_handler)

    if not args.no_setup_arm1:
        setup_arm1(args.setup_arm1_script, orch_log)
    else:
        log("skip node1 setup (--no-setup-node1)", orch_log)

    cleanup_all([])

    log("launching tenant procs", orch_log)
    for p in procs:
        p.start()

    log("waiting for sockets", orch_log)
    for p in procs:
        if not p.wait_socket(timeout=120):
            log(f"ERROR: t{p.tid} socket timeout", orch_log)
            cleanup_all(procs)
            sys.exit(1)

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
        log(f"ERROR: setup failed for {[p.tid for p in failed]}", orch_log)
        cleanup_all(procs)
        sys.exit(1)

    # ── pre-run counter snapshot ─────────────────────────────────────────────
    dump_sapsq_counters(procs, out_dir / "sapsq_start.json", orch_log)

    # ── start idle / inject schedulers ───────────────────────────────────────
    idle_spec = parse_idle_spec(args.idle_tenant) if args.idle_tenant else None
    idle_th = schedule_idle_phase(procs, idle_spec, orch_log)
    inject_th = schedule_path_inject(args, orch_log)
    # E6 recovery: remove-netem scheduler + periodic dump thread
    remove_th = schedule_path_remove(args, orch_log)
    periodic_th, periodic_stop = schedule_periodic_dump(
        procs, args, out_dir, orch_log)

    # ── run perform_tests in parallel ────────────────────────────────────────
    log(f"perform_tests ({args.duration}s) on {cfg.tenants} procs", orch_log)
    test_procs = []
    for p in procs:
        tp = p.run_perform_tests()
        test_procs.append((p, tp))
    for p, tp in test_procs:
        try:
            tp.wait(timeout=args.duration + 60)
        except subprocess.TimeoutExpired:
            log(f"WARN: perform_tests t{p.tid} timeout", orch_log)
            tp.kill()
        log(f"t{p.tid} perform_tests done", orch_log)

    # ── stop periodic dump thread (E6) ───────────────────────────────────────
    if periodic_stop is not None:
        periodic_stop.set()
    if periodic_th is not None:
        periodic_th.join(timeout=5)

    # ── post-run counter snapshot ────────────────────────────────────────────
    time.sleep(1)
    dump_sapsq_counters(procs, out_dir / "sapsq_end.json", orch_log)

    # ── teardown injection ───────────────────────────────────────────────────
    if args.inject_path_degradation is not None:
        teardown_path_degradation(args.inject_path_degradation,
                                  args.tenants, orch_log)

    # ── final wait + cleanup ─────────────────────────────────────────────────
    for p in procs:
        if p.proc:
            try:
                p.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pass
    cleanup_all(procs)

    # ── parse results ────────────────────────────────────────────────────────
    log("parsing results", orch_log)
    results = [p.parse_result() for p in procs]

    # ── compute deltas ────────────────────────────────────────────────────────
    def load_snap(path):
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}

    start_snap = load_snap(out_dir / "sapsq_start.json")
    end_snap = load_snap(out_dir / "sapsq_end.json")

    deltas = {}
    if not start_snap.get("missing_helper") and not end_snap.get("missing_helper"):
        for key in end_snap:
            if key not in start_snap or key == "missing_helper":
                continue
            s = start_snap[key]
            e = end_snap[key]
            try:
                d_admit = [[
                    e["admit_count"][t][p] - s["admit_count"][t][p]
                    for p in range(len(e["admit_count"][0]))
                ] for t in range(len(e["admit_count"]))]
                d_reject = [[
                    e["reject_count"][t][p] - s["reject_count"][t][p]
                    for p in range(len(e["reject_count"][0]))
                ] for t in range(len(e["reject_count"]))]
                d_probe = [[
                    e["probe_count"][t][p] - s["probe_count"][t][p]
                    for p in range(len(e["probe_count"][0]))
                ] for t in range(len(e["probe_count"]))]
                deltas[key] = {
                    "admit_delta": d_admit,
                    "reject_delta": d_reject,
                    "probe_delta": d_probe,
                    "stale_epoch_fallback_delta":
                        e.get("stale_epoch_fallback", 0)
                        - s.get("stale_epoch_fallback", 0),
                    "total_epochs_consumed_delta":
                        e.get("total_epochs_consumed", 0)
                        - s.get("total_epochs_consumed", 0),
                }
            except Exception as ex:
                deltas[key] = {"parse_error": str(ex)}

    summary = {
        "mode": cfg.mode,
        "tenants": cfg.tenants,
        "n_paths": cfg.n_paths,
        "duration_s": args.duration,
        "config": cfg.to_dict(),
        "per_tenant": results,
        "sapsq_deltas": deltas,
    }
    (out_dir / "sapsq_summary.json").write_text(json.dumps(summary, indent=2))
    log(f"summary written to {out_dir/'sapsq_summary.json'}", orch_log)

    log("=== orchestrator done ===", orch_log)
    orch_log.close()

    total_iops = sum(r["iops"] for r in results if r.get("iops"))
    print(f"\nRun complete. Out dir: {out_dir}")
    print(f"Total IOPS: {total_iops:.0f}")
    for r in results:
        print(f"  t{r['tenant_id']}: IOPS={r['iops']} "
              f"p99={r.get('lat_p99_us')}us")


if __name__ == "__main__":
    main()
