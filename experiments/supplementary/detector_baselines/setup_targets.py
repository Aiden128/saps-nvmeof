#!/usr/bin/env python3
"""Start and configure the 3-path NVMe-oF target topology."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from runtime_common import rpc, spawn_recorded

SPDK_ROOT = Path(os.environ.get("SPDK_ROOT", "/home/aiden/spdk"))
NVMF_TGT = SPDK_ROOT / "build" / "bin" / "nvmf_tgt"
TARGET_IP = "10.0.0.2"
NQN = "nqn.2024-01.io.spdk:mptest"
UUID = "11111111-2222-3333-4444-000000000000"
NGUID = "00000000000000001111111111110000"
PATHS = {
    "A": {"mask": "0x30000000000", "port": "4430", "ctrl_min": 1, "ctrl_max": 128},
    "B": {"mask": "0xC0000000000", "port": "4431", "ctrl_min": 129, "ctrl_max": 256},
    "C": {"mask": "0x300000000000", "port": "4432", "ctrl_min": 257, "ctrl_max": 384},
}
TARGET_ENV_DROP_PREFIXES = ("DPA_PLUGIN_", "SAPSQ_", "SAPS_", "HOST_SAPS_")
TARGET_ENV_OVERRIDES = {
    "DPA_PLUGIN_DISABLE_INIT": "1",
    "HOST_SAPS_ENABLED": "0",
    "SAPSQ_ENABLED": "0",
    "SAPS_Q_ENABLED": "0",
    "SAPS_M2_ENABLED": "0",
    "SAPS_M4_ENABLED": "0",
    "SAPS_M5_DRR_ENABLED": "0",
}


def parse_caps(raw: str) -> list[int]:
    caps = [int(part) for part in raw.split(",") if part.strip()]
    if len(caps) != 3 or any(cap <= 0 for cap in caps):
        raise argparse.ArgumentTypeError("expected three positive comma-separated IOPS values")
    return caps


def wait_ready(sock: Path, timeout_s: int = 40) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if sock.exists():
            try:
                rpc(str(sock), "rpc_get_methods", timeout=3)
                return
            except Exception:
                pass
        time.sleep(1)
    raise RuntimeError(f"RPC socket not ready: {sock}")


def target_env() -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(TARGET_ENV_DROP_PREFIXES)
    }
    env.update(TARGET_ENV_OVERRIDES)
    return env


def configure_path(label: str, spec: dict[str, object], sock: Path, malloc_mib: int, cap_iops: int) -> dict:
    malloc = f"malloc_{label}"
    delay = f"delay_{label}"
    err = f"EE_delay_{label}"
    rpc(str(sock), "nvmf_create_transport", {"trtype": "RDMA"})
    rpc(str(sock), "bdev_malloc_create", {"name": malloc, "num_blocks": (malloc_mib * 1024 * 1024) // 4096, "block_size": 4096})
    rpc(
        str(sock),
        "bdev_delay_create",
        {
            "base_bdev_name": malloc,
            "name": delay,
            "avg_read_latency": 27,
            "p99_read_latency": 27,
            "avg_write_latency": 27,
            "p99_write_latency": 27,
        },
    )
    rpc(str(sock), "bdev_error_create", {"base_name": delay})
    rpc(str(sock), "bdev_set_qos_limit", {"name": delay, "rw_ios_per_sec": cap_iops})
    rpc(
        str(sock),
        "nvmf_create_subsystem",
        {
            "nqn": NQN,
            "serial_number": f"SAPSBSP{label}000",
            "allow_any_host": True,
            "max_namespaces": 32,
            "min_cntlid": int(spec["ctrl_min"]),
            "max_cntlid": int(spec["ctrl_max"]),
        },
    )
    rpc(str(sock), "nvmf_subsystem_add_ns", {"nqn": NQN, "namespace": {"bdev_name": err, "nsid": 1, "uuid": UUID, "nguid": NGUID}})
    rpc(
        str(sock),
        "nvmf_subsystem_add_listener",
        {"nqn": NQN, "listen_address": {"trtype": "rdma", "adrfam": "ipv4", "traddr": TARGET_IP, "trsvcid": str(spec["port"])}},
    )
    bdevs = rpc(str(sock), "bdev_get_bdevs", {"name": delay})
    subsystems = rpc(str(sock), "nvmf_get_subsystems")
    if len(bdevs) != 1:
        raise RuntimeError(f"path {label}: expected one delay bdev, got {len(bdevs)}")
    limits = bdevs[0].get("assigned_rate_limits", {})
    if limits.get("rw_ios_per_sec") != cap_iops:
        raise RuntimeError(f"path {label}: QoS readback mismatch: {limits}")
    delay_state = bdevs[0].get("driver_specific", {}).get("delay", {})
    expected_delay = {
        "avg_read_latency": 27,
        "p99_read_latency": 27,
        "avg_write_latency": 27,
        "p99_write_latency": 27,
    }
    for key, expected in expected_delay.items():
        if delay_state.get(key) != expected:
            raise RuntimeError(f"path {label}: delay readback mismatch for {key}: {delay_state}")
    matching = [subsystem for subsystem in subsystems if subsystem.get("nqn") == NQN]
    if len(matching) != 1:
        raise RuntimeError(f"path {label}: expected one subsystem {NQN}, got {len(matching)}")
    listeners = matching[0].get("listen_addresses", [])
    if str(spec["port"]) not in {str(listener.get("trsvcid")) for listener in listeners}:
        raise RuntimeError(f"path {label}: listener port missing in {listeners}")
    return {"path": label, "socket": str(sock), "port": spec["port"], "delay_bdev": bdevs, "subsystems": subsystems}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", required=True)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--path-capacities-iops", required=True, type=parse_caps)
    parser.add_argument("--malloc-mib", type=int, default=512)   # 1024 failed with -12 under -s 1536 -g
    parser.add_argument("--target-mem-mib", type=int, default=1536)
    args = parser.parse_args()

    if os.geteuid() != 0:
        raise RuntimeError("setup_targets.py must run as root so recorded PIDs are nvmf_tgt")
    runtime_dir = Path(args.runtime_dir)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    if not NVMF_TGT.exists():
        raise RuntimeError(f"missing nvmf_tgt: {NVMF_TGT}")

    sockets = {label: runtime_dir / f"target_{label}.sock" for label in PATHS}
    for sock in sockets.values():
        if len(str(sock)) > 107:
            raise RuntimeError(f"UNIX socket path too long: {sock}")
        if sock.exists():
            raise RuntimeError(f"refusing to reuse existing socket path: {sock}")

    clean_target_env = target_env()
    shm_base = os.getpid() * 10
    for idx, (label, spec) in enumerate(PATHS.items(), start=1):
        spawn_recorded(
            [
                str(NVMF_TGT),
                "-m",
                str(spec["mask"]),
                "-r",
                str(sockets[label]),
                # No -i: without a shared-memory id SPDK runs with --huge-unlink, so
                # the hugepage backing files disappear when the process exits.
                "-s",
                str(args.target_mem_mib),
                "-g",
            ],
            runtime_dir / f"target_{label}.log",
            args.registry,
            env=clean_target_env,
        )

    for sock in sockets.values():
        wait_ready(sock)

    verification = {}
    for label, cap_iops in zip(("A", "B", "C"), args.path_capacities_iops):
        verification[label] = configure_path(label, PATHS[label], sockets[label], args.malloc_mib, cap_iops)

    ready = {
        "target_ip": TARGET_IP,
        "nqn": NQN,
        "uuid": UUID,
        "nguid": NGUID,
        "path_capacities_iops": dict(zip(("A", "B", "C"), args.path_capacities_iops)),
        "verification": verification,
    }
    with (runtime_dir / "target_ready.json").open("w", encoding="utf-8") as sink:
        json.dump(ready, sink, indent=2, sort_keys=True)
        sink.write("\n")
    print(json.dumps({"status": "ready", "runtime_dir": str(runtime_dir), "registry": args.registry}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"setup_targets.py: ERROR: {exc}", file=sys.stderr)
        raise
