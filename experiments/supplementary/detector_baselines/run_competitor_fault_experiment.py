#!/usr/bin/env python3
"""Local port of the competitor campaign; use drive_campaign.sh on arm-2.

Detector algorithms remain in the original helpers. Aggregate per-path QoS is
shared by all tenants. Stock and detector arms implement no weighted scheduling;
their weights define accounting entitlements only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import signal
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from external_baseline_common import parse_io_paths, positive_float
from runtime_common import SPDK_ROOT, rpc, spawn_recorded, stop_recorded
from saps_env import process_env, HOST_SAPS_PARITY_GAPS
from campaign_matrix import campaign_cells
from window_measurements import (capture_thread_stats, summarize_thread_stats,
                                 summarize_per_io, cleanup_per_io)

HERE = Path(__file__).resolve().parent
CONTROLLER = Path("/mnt/nvme0n1p1/aiden/DPA/nvme-of-controller")
BDEVPERF = SPDK_ROOT / "build/examples/bdevperf"
RPCPY = SPDK_ROOT / "scripts/rpc.py"
PERFORM = SPDK_ROOT / "examples/bdev/bdevperf/bdevperf.py"
NQN = "nqn.2024-01.io.spdk:mptest"
PORTS = {"A": "4430", "B": "4431", "C": "4432"}
FIELDS = {"avg_read": "avg_read_latency", "p99_read": "p99_read_latency",
          "avg_write": "avg_write_latency", "p99_write": "p99_write_latency"}
LEGACY_ARMS = ("saps_q", "fixed_threshold", "per_path_adaptive", "stock_round_robin")
DPA_ARMS = ("saps_q", "saps_binary", "health_only")
ARMS = LEGACY_ARMS + ("saps_binary", "health_only", "host_saps")
LABELS = {**{arm: arm for arm in ARMS}, "stock_round_robin": "stock round robin"}
TOPOLOGY = ("arm-2; RDMA mlx5_0@10.0.0.2; single NQN "
            + NQN + "; shared NSID=1 UUID/NGUID; malloc->delay->error; "
            "A/B/C=4430/4431/4432; target cores40-45; tenant cores4-11; "
            "4 tenants weights3:1:1:1 QD32 4096B randread; aggregate path QoS")


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def read_json(path):
    return json.loads(Path(path).read_text())


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def controller_head():
    """Read metadata without executing a version-control command."""
    dot = CONTROLLER / ".git"
    if dot.is_file():
        entry = dot.read_text().strip()
        if not entry.startswith("gitdir: "):
            raise RuntimeError("unrecognized controller repository metadata")
        dot = (CONTROLLER / entry[8:]).resolve()
    common = dot
    if (dot / "commondir").exists():
        common = (dot / (dot / "commondir").read_text().strip()).resolve()
    value = (dot / "HEAD").read_text().strip()
    if value.startswith("ref: "):
        ref = value[5:]
        value = ""
        for directory in (dot, common):
            if (directory / ref).is_file():
                value = (directory / ref).read_text().strip()
                break
        if not value and (common / "packed-refs").is_file():
            for line in (common / "packed-refs").read_text().splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1] == ref:
                    value = parts[0]
                    break
    if len(value) not in (40, 64) or any(char not in "0123456789abcdef" for char in value):
        raise RuntimeError("cannot resolve controller HEAD from metadata")
    return value


def provenance(args):
    files = [SPDK_ROOT / "build/bin/nvmf_tgt", BDEVPERF, RPCPY, PERFORM]
    files += sorted(HERE.glob("*.py")) + [HERE / "drive_campaign.sh"]
    result = {"controller_repo": str(CONTROLLER), "controller_HEAD": controller_head(),
              "controller_worktree_dirty": "unverified; no version-control commands used",
              "sha256": {str(path): sha256(path) for path in files},
              "uname": list(os.uname()), "topology": TOPOLOGY,
              "path_capacities_iops": args.path_capacities_iops,
              "service_limit_iops": args.service_limit_iops,
              "counter_hz": args.counter_hz or "plugin cntfrq_el0 autodetection",
              "sample_rate": "32, or 16/32/64 in c4; per-run manifest is authoritative",
              "experiment": args.experiment, "fixed_threshold_us": args.fixed_threshold_us,
              "poll_interval_s": args.poll_interval_s,
              "binary_matches_controller_HEAD": "unverified; fingerprints only"}
    firmware = CONTROLLER / "dpa-smart-initiator/flexio_build/samples/build/dpa_plugin/dev/dpa_plugin_app.a"
    result["firmware_archive"] = {"path": str(firmware),
                                  "sha256": sha256(firmware) if firmware.is_file() else None}
    return result


def target_state(args, expected_b):
    result = {}
    for index, label in enumerate(PORTS):
        socket = str(args.runtime_dir / f"target_{label}.sock")
        bdevs = rpc(socket, "bdev_get_bdevs", {"name": f"delay_{label}"}, timeout=5)
        if len(bdevs) != 1:
            raise RuntimeError(f"{label}: delay bdev missing")
        bdev = bdevs[0]
        delay = bdev.get("driver_specific", {}).get("delay", {})
        expected = expected_b if label == "B" else 27
        if any(delay.get(field) != expected for field in FIELDS.values()):
            raise RuntimeError(f"{label}: delay readback {delay}, expected {expected}us")
        cap = bdev.get("assigned_rate_limits", {}).get("rw_ios_per_sec")
        if cap != args.path_capacities_iops[index]:
            raise RuntimeError(f"{label}: aggregate QoS {cap} differs from configured capacity")
        result[label] = {"socket": socket, "delay": delay, "aggregate_qos_iops": cap}
    return {"timestamp_unix": time.time(), "paths": result}


def delay_b(args, value, run_dir, phase):
    evidence = {"phase": phase, "value_us": value, "started_unix": time.time(), "updates": []}
    path = run_dir / f"fault_{phase}.json"
    write_json(path, evidence)
    for field in FIELDS:
        cmd = [sys.executable, "-B", str(RPCPY), "-s",
               str(args.runtime_dir / "target_B.sock"),
               "bdev_delay_update_latency", "delay_B", field, str(value)]
        started = time.time()
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=True)
        evidence["updates"].append({"field": field, "started_unix": started,
                                    "completed_unix": time.time(), "cmd": cmd,
                                    "result": completed.stdout.strip()})
        write_json(path, evidence)
    evidence["completed_unix"] = time.time()
    evidence["readback"] = target_state(args, value)
    write_json(path, evidence)
    return evidence


def wait_socket(socket, proc, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"process {proc.pid} exited before RPC readiness; see log")
        if socket.exists():
            try:
                rpc(str(socket), "rpc_get_methods", timeout=2)
                return
            except Exception:
                pass
        time.sleep(0.2)
    raise RuntimeError(f"RPC readiness timeout: {socket}")


def rpc_cli(socket, *arguments, timeout=45):
    result = subprocess.run([sys.executable, "-B", str(RPCPY), "-s", str(socket), *arguments],
                            capture_output=True, text=True, timeout=timeout, check=True)
    return result.stdout.strip()


def configure_tenant(socket, arm):
    replies = [rpc_cli(socket, "bdev_nvme_set_options", "--io-path-stat"),
               rpc_cli(socket, "framework_start_init")]
    for port in PORTS.values():
        replies.append(rpc_cli(socket, "bdev_nvme_attach_controller", "-b", "mp",
                               "-t", "rdma", "-a", "10.0.0.2", "-s", port,
                               "-f", "ipv4", "-n", NQN, "--multipath", "multipath"))
    policy = (["-p", "plugin"] if arm in (*DPA_ARMS, "host_saps")
              else ["-p", "active_active", "-s", "round_robin"])
    replies.append(rpc_cli(socket, "bdev_nvme_set_multipath_policy", "-b", "mpn1", *policy))
    # bdev_nvme_get_io_paths lists only paths with an I/O channel, and bdevperf has
    # none before perform_tests, so verify the attached controllers instead.
    controllers = rpc(str(socket), "bdev_nvme_get_controllers", {"name": "mp"}, timeout=5)
    paths = {}
    for entry in controllers or []:
        for ctrlr in entry.get("ctrlrs", []):
            trid = ctrlr.get("trid", {})
            paths[str(trid.get("trsvcid"))] = {"state": ctrlr.get("state"),
                                               "cntlid": ctrlr.get("cntlid"),
                                               "transport": trid}
    if len(paths) != 3:
        raise RuntimeError(f"{socket}: expected exactly 3 distinct NVMe paths, got {paths}")
    cntlids, seen_ports = set(), set()
    for state in paths.values():
        transport = state["transport"]
        cntlid = int(state.get("cntlid") or 0)
        if state.get("state") != "enabled" or cntlid <= 0:
            raise RuntimeError(f"{socket}: unusable path/controller ID: {state}")
        if transport.get("traddr") != "10.0.0.2" or transport.get("subnqn") != NQN:
            raise RuntimeError(f"{socket}: unexpected target transport: {transport}")
        cntlids.add(cntlid)
        seen_ports.add(str(transport.get("trsvcid")))
    if len(cntlids) != 3 or seen_ports != set(PORTS.values()):
        raise RuntimeError(f"{socket}: duplicate controller IDs or unexpected ports")
    return {"rpc_replies": replies, "io_paths": paths}


def collector_command(args, arm, sockets, run_dir, epoch, duration, fault_at,
                      restore_at, window, reference, commit):
    entry = "external_fixed_threshold.py" if arm == "fixed_threshold" else "external_per_path_adaptive.py"
    passive = arm not in ("fixed_threshold", "per_path_adaptive") or fault_at is None
    hold = restore_at - fault_at + 5 if fault_at is not None else duration + 5
    cmd = [sys.executable, "-B", str(HERE / entry), "--num-tenants", "4",
           "--bdev-name", "mpn1", "--controller-name", "mp", "--spdk-root", str(SPDK_ROOT),
           "--output-dir", str(run_dir / "collector"), "--weights", "3,1,1,1",
           "--qd", "32", "--io-kind", "read", "--min-paths", "3",
           "--wait-for-sockets-s", "5", "--rpc-timeout-s", "5",
           "--max-consecutive-rpc-errors", "1", "--interval-s", str(args.poll_interval_s),
           "--duration-s", str(duration), "--clock-start-unix", str(epoch),
           "--measure-start-s", str(window[0]), "--measure-end-s", str(window[1]),
           "--reference-capacity-iops", str(reference), "--git-commit", commit,
           "--workload-label", "4tenant_3-1-1-1_QD32_4KB_randread_supplementary",
           "--hold-down-s", str(hold), "--selector", "round_robin",
           "--multipath-policy", "keep" if passive else "active_active"]
    if passive:
        cmd += ["--observe-only"]
    if arm == "fixed_threshold":
        cmd += ["--threshold-us", str(args.fixed_threshold_us)]
    if fault_at is not None:
        cmd += ["--fault-at-unix", str(epoch + fault_at), "--fault-path-id", "p1"]
    for index, socket in enumerate(sockets):
        cmd += ["--tenant", f"t{index}={socket}"]
    return cmd


def wait_until(deadline, procs):
    while time.monotonic() < deadline:
        for proc in procs:
            if proc.poll() is not None:
                raise RuntimeError(f"process {proc.pid} exited early (rc={proc.returncode})")
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))


def validate_collector_preflight(path):
    data = read_json(path)
    for tenant in (f"t{i}" for i in range(4)):
        paths = [item for item in data["paths"] if item["tenant"] == tenant]
        if len(paths) != 3 or {item["path_id"] for item in paths} != {"p0", "p1", "p2"}:
            raise RuntimeError(f"collector did not discover all three paths for {tenant}")
        if len({int(item.get("cntlid") or 0) for item in paths}) != 3 or any(int(item.get("cntlid") or 0) <= 0 for item in paths):
            raise RuntimeError(f"collector has unsafe controller IDs for {tenant}")
        b_path = next(item for item in paths if item["path_id"] == "p1")
        if "4431" not in b_path["path_key"]:
            raise RuntimeError(f"collector p1 is not target B: {b_path}")
    return data


def rows(path):
    with Path(path).open(newline="") as source:
        return list(csv.DictReader(source))


def sampler_evidence(run_dir, epoch, onset, restored):
    data = rows(run_dir / "controller_sampler.csv")
    healthy = [row for row in data if epoch + 2 <= float(row["unix_s"]) < onset - 0.5]
    faulted = [row for row in data if onset <= float(row["unix_s"]) < restored]
    if not healthy or not faulted:
        raise RuntimeError("SAPS sampler lacks healthy/fault windows")
    if max(int(row["consumed_total"]) for row in faulted) <= 0:
        raise RuntimeError("SAPS DPA ring consumed no samples")
    if max(int(row["consumed_total"]) for row in faulted) <= min(int(row["consumed_total"]) for row in faulted):
        raise RuntimeError("SAPS DPA ring made no progress during the fault window")
    if any(not any(int(row[f"dem{i}"]) > 0 and int(row[f"bud{i}"]) > 0 for row in healthy) for i in range(4)):
        raise RuntimeError("SAPS lacks demand and positive healthy budgets for all four tenants")
    base = statistics.median(float(row["chf1"]) for row in healthy)
    changed = [row for row in faulted if float(row["chf1"]) < base - max(0.05, base * 0.2)]
    return {"healthy_committed_B_health_median": base,
            "fault_committed_B_health_min": min(float(row["chf1"]) for row in faulted),
            "max_consumed_total": max(int(row["consumed_total"]) for row in faulted),
            "health_changed": bool(changed),
            "first_health_change_latency_ms": (float(changed[0]["unix_s"]) - onset) * 1000 if changed else None,
            "timing_definition": "1Hz committed-health observation; not precise classifier or reroute latency"}


def validate_result(run_dir, arm, epoch, fault, restored, window, calibrated):
    summary = read_json(run_dir / "collector/summary.json")
    metric = read_json(run_dir / "collector/metrics.json")
    if not summary["completed"] or summary["stop_reason"] != "duration":
        raise RuntimeError(f"collector did not complete: {summary['stop_reason']}")
    measurement = metric["measurement"]
    for tenant in (f"t{i}" for i in range(4)):
        value = measurement["per_tenant"].get(tenant, {})
        if value.get("samples", 0) < 3 or value.get("total_ops", 0) <= 0:
            raise RuntimeError(f"{arm}: no usable measurement samples for {tenant}")
        if value.get("observed_s", 0) < 0.6 * (window[1] - window[0]):
            raise RuntimeError(f"{arm}: insufficient measurement coverage for {tenant}")
    actions = [json.loads(line) for line in (run_dir / "collector/events.jsonl").read_text().splitlines() if line]
    real_actions = [item for item in actions if item.get("event") == "path_action"
                    and not str(item.get("rpc_result", "")).startswith("observe-only:")
                    and "no_eligible_alternate_kept_last_path" not in str(item.get("rpc_result", ""))]
    result = {"arm": arm, "label": LABELS[arm], "status": "valid",
              "measurement": measurement, "calibrated_reference": calibrated,
              "aggregate_iops": sum(item["mean_iops"] for item in measurement["per_tenant"].values()),
              "detector_actions": real_actions, "fault_manifested": None}
    if not calibrated:
        measurement["valid"] = False
        measurement["invalid_reason"] = "smoke/calibration run; no calibrated denominator yet"
        measurement["worst_tenant_fair_share"] = measurement["worst_tenant"] = None
        for item in measurement["per_tenant"].values():
            item["fair_share_ratio"] = item["entitlement_iops"] = None
    if fault is not None:
        onset = fault["updates"][0]["completed_unix"]
        end = restored["started_unix"]
        path_rows = rows(run_dir / "collector/per_path_samples.csv")
        affected = [row for row in path_rows
                    if row["path_id"] == "p1" and onset <= float(row["timestamp_unix"]) < end
                    and float(row["interval_ops"] or 0) > 0 and row["interval_latency_us"]]
        healthy = [float(row["interval_latency_us"]) for row in path_rows
                   if row["path_id"] == "p1" and epoch + 1 <= float(row["timestamp_unix"]) < onset - 0.5
                   and float(row["interval_ops"] or 0) > 0 and row["interval_latency_us"]]
        if not healthy:
            raise RuntimeError(f"{arm}: no usable healthy B samples before fault onset")
        baseline_latency = statistics.median(healthy)
        max_latency = max((float(row["interval_latency_us"]) for row in affected), default=0)
        result["fault_evidence"] = {"read_delay_onset_unix": onset,
                                    "all_fields_applied_unix": fault["completed_unix"],
                                    "restoration_started_unix": end,
                                    "healthy_B_interval_mean_latency_median_us": baseline_latency,
                                    "max_observed_B_interval_mean_latency_us": max_latency}
        # Preserve the legacy 1ms gate, but admit the new 500us sweep fault.
        latency_gate = max(min(1000, 0.8 * fault["value_us"]), 2 * baseline_latency)
        manifested = max_latency >= latency_gate
        result["fault_evidence"]["manifestation_latency_gate_us"] = latency_gate
        if arm in DPA_ARMS:
            sample = sampler_evidence(run_dir, epoch, onset, end)
            result["controller_health"] = sample
            manifested = manifested or sample["health_changed"]
        result["fault_manifested"] = manifested
        if not manifested:
            raise RuntimeError(f"{arm}: no B latency >= {latency_gate}us or SAPS health drop; fault did not manifest")
        for action in real_actions:
            action["relative_to_actual_read_delay_onset_ms"] = (float(action["timestamp_unix"]) - onset) * 1000
        result["detection"] = {"B_actions_after_onset": [item for item in real_actions
                                if item.get("path_id") == "p1" and onset <= float(item["timestamp_unix"]) < end],
                               "pre_fault_actions": [item for item in real_actions if float(item["timestamp_unix"]) < onset],
                               "native_traffic_reroute_latency": None,
                               "note": "native delivered-IOPS collapse is not proof of rerouting"}
    return result


def healthy_sampler_progress(run_dir, epoch, window):
    values = [row for row in rows(run_dir / "controller_sampler.csv")
              if epoch + window[0] <= float(row["unix_s"]) <= epoch + window[1]]
    if len(values) < 2:
        raise RuntimeError("healthy SAPS run lacks window telemetry")
    consumed = [int(row["consumed_total"]) for row in values]
    if max(consumed) <= min(consumed):
        raise RuntimeError("healthy SAPS DPA consumed no window samples")
    if any(not any(int(row[f"dem{i}"]) > 0 and int(row[f"bud{i}"]) > 0
                   for row in values) for i in range(4)):
        raise RuntimeError("healthy SAPS run lacks positive tenant demand/budgets")
    return {"window_samples": len(values), "consumed_delta": max(consumed) - min(consumed)}


def validate_profile_logs(run_dir, arm, sample_rate, args):
    import re
    proof = {}
    for index in range(4):
        text = (run_dir / f"tenant{index}.log").read_text(errors="replace")
        if arm in DPA_ARMS:
            if not re.search(rf"sample_rate={sample_rate}(?!\d)", text):
                raise RuntimeError(f"t{index}: sample rate not confirmed in plugin log")
            mode = {"saps_q": 0, "health_only": 1, "saps_binary": 2}[arm]
            if index == 0 and not re.search(rf"health_coupling_mode={mode}(?!\d)", text):
                raise RuntimeError(f"{arm}: coupling mode not confirmed in coordinator log")
            proof[f"t{index}"] = {"sample_rate_confirmed": sample_rate, "expected_mode": mode}
        elif arm == "host_saps":
            match = re.search(args.host_proof_pattern, text, re.IGNORECASE | re.MULTILINE)
            if not match:
                raise RuntimeError(f"t{index}: host activation not confirmed; inspect log and --host-proof-pattern")
            if re.search(r"dpa_plugin: (?:init start|tenant init ok)", text):
                raise RuntimeError(f"t{index}: host run also initialized the DPA")
            cp_file = run_dir / f"host_saps_cp_t{index}.log"
            cp_text = cp_file.read_text(errors="replace") if cp_file.exists() else text
            cp_rows = re.findall(r"host_saps CP epochs=(\d+).*?my_tid=(\d+)\s+rate_q32=\[([^]]+)\]", cp_text)
            active_rows = [(int(epochs), int(tid), [int(x) for x in rates.split(",")])
                           for epochs, tid, rates in cp_rows]
            if not any(epochs > 0 and tid == index and any(rate > 0 for rate in rates)
                       for epochs, tid, rates in active_rows):
                raise RuntimeError(f"t{index}: no active host CP epochs/tenant budget evidence")
            proof[f"t{index}"] = {"host_activation_log": match.group(0),
                                    "control_plane_epochs_max": max(item[0] for item in active_rows),
                                    "requested_tenants": 4, "requested_paths": 3,
                                    "requested_C": args.service_limit_iops,
                                    "parity_gaps": list(HOST_SAPS_PARITY_GAPS),
                                    "control_plane_parity": "process-local host control; not proven equivalent to shared DPA ring"}
        else:
            if re.search(r"dpa_plugin: (?:init start|tenant init ok)", text):
                raise RuntimeError(f"t{index}: stock arm initialized the DPA")
            proof[f"t{index}"] = {"DPA_initialization": "absent"}
    return proof


def perform_report(path):
    content = Path(path).read_text()
    marker = content.find('"results"')
    if marker < 0:
        raise RuntimeError(f"no perform_tests results: {path}")
    start = content.rfind("{", 0, marker)
    report, _ = json.JSONDecoder().raw_decode(content, start)
    jobs = report.get("results", [])
    if not jobs or any(float(job.get("iops", 0)) <= 0 or float(job.get("io_failed", 0)) > 0
                       or float(job.get("io_timeout", 0)) > 0 for job in jobs):
        raise RuntimeError(f"no positive successful workload results: {path}")
    return report


def run_one(args, run_id, arm, duration, fault_at, restore_at, window, reference, commit,
            fault_delay_us=5000, sample_rate=32, repeat=0):
    run_dir = args.out / run_id
    run_dir.mkdir()
    runtime = args.runtime_dir / run_id
    runtime.mkdir()
    sockets = [runtime / f"t{i}.sock" for i in range(4)]
    if any(len(str(socket)) > 107 for socket in sockets):
        raise RuntimeError("initiator UNIX socket path too long")
    started, tenants = [], []
    fault = restored = None
    busy_start = busy_end = None
    per_io_enabled = args.experiment != "legacy"
    may_be_faulted = False
    manifest = {"arm": arm, "label": LABELS[arm], "duration_s": duration,
                "fault_at_s": fault_at, "restore_at_s": restore_at,
                "fault": f"B path four delay fields 27->{fault_delay_us}us" if fault_at is not None else "none",
                "fault_delay_us": fault_delay_us if fault_at is not None else 0,
                "sample_rate": sample_rate if arm in DPA_ARMS else None,
                "requested_sample_rate": sample_rate, "repeat": repeat, "experiment": args.experiment,
                "per_io_logging": per_io_enabled,
                "measurement_window_s": window, "path_capacities_iops": args.path_capacities_iops,
                "service_limit_iops": args.service_limit_iops,
                "reference_capacity_iops": reference, "reference_is_calibrated": reference > 0,
                "weights": [3, 1, 1, 1],
                "weight_enforcement": ("DPA weighted admission" if arm in DPA_ARMS else
                                       "host parity unverified; inspect host evidence" if arm == "host_saps"
                                       else "none; accounting entitlement only"),
                "topology": TOPOLOGY, "tenant_environments": {}, "tenant_commands": {}}
    write_json(run_dir / "manifest.json", manifest)

    def launch(index):
        env = process_env(arm, index, runtime, run_dir, args.path_capacities_iops,
                          args.service_limit_iops, args.counter_hz, sample_rate=sample_rate)
        first = 4 + index * 2
        cmd = ["taskset", "-c", f"{first}-{first+1}", str(BDEVPERF),
               "-m", hex((1 << first) | (1 << (first + 1))),
               "-r", str(sockets[index]), "--wait-for-rpc", "-g", "-s", "384",
               "-q", "32", "-o", "4096", "-w", "randread", "-t", str(duration), "-z", "-l"]
        if per_io_enabled:
            per_io_dir = run_dir / f"tenant_{index:02d}" / "per_io"
            per_io_dir.mkdir(parents=True)
            cmd += ["--per-io-log", str(per_io_dir)]
        manifest["tenant_environments"][f"t{index}"] = {key: value for key, value in env.items()
            if key.startswith(("DPA_PLUGIN_", "SAPSQ_", "SAPS_", "HOST_SAPS_"))}
        manifest["tenant_commands"][f"t{index}"] = cmd
        proc = spawn_recorded(cmd, run_dir / f"tenant{index}.log", args.registry, env=env)
        started.append(proc)
        tenants.append(proc)
        wait_socket(sockets[index], proc)

    try:
        write_json(run_dir / "target_before.json", target_state(args, 27))
        launch(0)
        configurations = {"t0": configure_tenant(sockets[0], arm)}
        if arm in DPA_ARMS:
            deadline = time.monotonic() + 10
            while not (runtime / "dpa.sock").exists():
                if tenants[0].poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("SAPS coordinator did not create shared ring socket")
                time.sleep(0.2)
        for index in range(1, 4):
            launch(index)
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {index: pool.submit(configure_tenant, sockets[index], arm) for index in range(1, 4)}
            for index, future in futures.items():
                configurations[f"t{index}"] = future.result()
        write_json(run_dir / "initiator_preflight.json", configurations)
        epoch = time.time() + 10
        epoch_mono = time.monotonic() + (epoch - time.time())
        manifest["clock_start_unix"] = epoch
        cmd = collector_command(args, arm, sockets, run_dir, epoch, duration,
                                fault_at, restore_at, window, reference, commit)
        manifest["collector_command"] = cmd
        write_json(run_dir / "manifest.json", manifest)
        collector = spawn_recorded(cmd, run_dir / "collector.log", args.registry)
        started.append(collector)
        ready = run_dir / "collector/preflight.json"
        events = run_dir / "collector/events.jsonl"
        while not (ready.exists() and events.exists() and '"event": "preflight_ok"' in events.read_text()):
            if collector.poll() is not None or time.monotonic() >= epoch_mono - 1:
                raise RuntimeError("collector path/policy preflight not ready before common epoch")
            time.sleep(0.1)
        preflight = validate_collector_preflight(ready)
        if per_io_enabled:
            expected_hz = args.counter_hz or 1_000_000_000
            if any(int(hz) != expected_hz for hz in preflight["tick_rates"].values()):
                raise RuntimeError(f"per-IO counter frequency differs from iostat tick rates: {preflight['tick_rates']}")
        wait_until(epoch_mono, tenants + [collector])
        dispatch, workloads = [], []
        for index, socket in enumerate(sockets):
            timestamp = time.time()
            cmd = [sys.executable, "-B", str(PERFORM), "-s", str(socket),
                   "-t", str(duration + 15), "perform_tests"]
            proc = spawn_recorded(cmd, run_dir / f"perform_t{index}.json", args.registry)
            started.append(proc)
            workloads.append(proc)
            dispatch.append({"tenant": f"t{index}", "timestamp_unix": timestamp,
                             "pid": proc.pid, "cmd": cmd})
        manifest["workload_dispatch"] = dispatch
        manifest["dispatch_skew_s"] = dispatch[-1]["timestamp_unix"] - dispatch[0]["timestamp_unix"]
        write_json(run_dir / "manifest.json", manifest)
        if manifest["dispatch_skew_s"] > 0.5:
            raise RuntimeError("workload dispatch skew exceeds 0.5s")
        scheduled = []
        if fault_at is not None:
            scheduled += [(fault_at, "inject"), (restore_at, "restore")]
        if args.experiment == "c3":
            scheduled += [(window[0], "busy_start"), (window[1], "busy_end")]
        for at, event in sorted(scheduled):
            wait_until(epoch_mono + at, tenants + workloads + [collector])
            if event == "inject":
                may_be_faulted = True
                fault = delay_b(args, fault_delay_us, run_dir, "inject")
                if fault["updates"][0]["completed_unix"] > epoch + fault_at + 1:
                    raise RuntimeError("read-delay onset was more than 1s late")
                if fault["readback"]["timestamp_unix"] >= epoch + window[0]:
                    raise RuntimeError("fault configuration/readback did not settle before measurement")
            elif event == "restore":
                restored = delay_b(args, 27, run_dir, "restore")
                may_be_faulted = False
            else:
                snapshot = capture_thread_stats(sockets)
                write_json(run_dir / f"{event}.json", snapshot)
                if snapshot["capture_completed_unix_s"] > epoch + at + 1:
                    raise RuntimeError(f"{event}: reactor snapshot more than 1s late")
                if event == "busy_start":
                    busy_start = snapshot
                else:
                    busy_end = snapshot
        while time.monotonic() < epoch_mono + duration + 15:
            if collector.poll() is not None and all(proc.poll() is not None for proc in workloads):
                break
            if collector.poll() is None and time.monotonic() < epoch_mono + duration - 1:
                if any(proc.poll() is not None for proc in tenants):
                    raise RuntimeError("initiator exited before workload end")
            time.sleep(0.1)
        for proc in [collector, *workloads]:
            if proc.poll() is None:
                raise RuntimeError(f"process {proc.pid} exceeded completion deadline")
            if proc.wait() != 0:
                raise RuntimeError(f"process {proc.pid} failed rc={proc.returncode}; inspect logs")
        write_json(run_dir / "workload_results.json", {
            f"t{i}": perform_report(run_dir / f"perform_t{i}.json") for i in range(4)})
        write_json(run_dir / "target_after.json", target_state(args, 27))
        # Terminate only this cell's registered processes, allowing per-IO logs
        # and host/DPA shutdown diagnostics to flush before percentile analysis.
        stop_recorded(args.registry, only_pids=[proc.pid for proc in started])
        for proc in started:
            proc.wait(timeout=5)
        result = validate_result(run_dir, arm, epoch, fault, restored, window, reference > 0)
        if per_io_enabled:
            result["per_io"] = summarize_per_io(run_dir, epoch, window,
                                                args.counter_hz or 1_000_000_000)
            result["worst_tenant_p99_read_latency_us"] = result["per_io"]["worst_tenant_p99_read_latency_us"]
            write_json(run_dir / "per_io_summary.json", result["per_io"])
            result["profile_evidence"] = validate_profile_logs(run_dir, arm, sample_rate,
                                                               args)
            if arm in DPA_ARMS and fault is None:
                result["controller_progress"] = healthy_sampler_progress(run_dir, epoch, window)
        if args.experiment == "c3":
            if busy_start is None or busy_end is None:
                raise RuntimeError("placement run missing thread snapshots")
            result["reactor"] = summarize_thread_stats(busy_start, busy_end)
            result["busy_cycles_per_io"] = result["reactor"]["busy_cycles_per_io"]
        result.update(run_id=run_id, experiment=args.experiment, repeat=repeat,
                      fault_delay_us=fault_delay_us if fault_at is not None else 0,
                      sample_rate=sample_rate if arm in DPA_ARMS else None, reference_capacity_iops=reference,
                      reference_kind=("fixed calibration" if args.experiment in ("legacy", "c1")
                                      else "configured service limit C"))
        write_json(run_dir / "metrics.json", result)
        print(f"{run_id}: valid, aggregate={result['aggregate_iops']:.1f} IOPS", flush=True)
        return result
    except BaseException as exc:
        write_json(run_dir / "failure.json", {"status": "invalid", "error": str(exc),
                                              "type": type(exc).__name__, "timestamp_unix": time.time()})
        raise
    finally:
        try:
            if may_be_faulted:
                delay_b(args, 27, run_dir, "emergency_restore")
        finally:
            stop_recorded(args.registry, only_pids=[proc.pid for proc in started])
            for proc in started:
                proc.wait(timeout=5)
            if per_io_enabled:
                cleanup_per_io(run_dir)
            for socket in [*sockets, runtime / "dpa.sock"]:
                if socket.exists():
                    socket.unlink()
            try:
                runtime.rmdir()
            except OSError:
                pass


def parse_caps(value):
    try:
        caps = [int(part) for part in value.split(",")]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected three positive integer path capacities") from exc
    if len(caps) != 3 or any(cap <= 0 or cap % 1000 for cap in caps):
        raise argparse.ArgumentTypeError("three positive capacities, each a multiple of SPDK's 1000 IOPS granularity")
    return caps


def signal_stop(signum, _frame):
    raise RuntimeError(f"received signal {signum}")


SUMMARY_COLUMNS = ["run_id", "experiment", "repeat", "arm", "label", "fault_delay_us",
                   "sample_rate", "aggregate_iops", "reference_capacity_iops", "reference_kind",
                   "worst_tenant_fair_share", "worst_tenant_p99_read_latency_us",
                   "busy_cycles_per_io", "fault_manifested", "status"]
SUMMARY_COLUMNS += [f"{tenant}_{field}" for tenant in ("t0", "t1", "t2", "t3")
                    for field in ("iops", "p99_us", "busy_cycles_per_io")]


def write_summary(path, results):
    with Path(path).open("w", newline="") as sink:
        writer = csv.DictWriter(sink, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for result in results:
            row = {key: result.get(key, "") for key in SUMMARY_COLUMNS}
            row["worst_tenant_fair_share"] = result["measurement"]["worst_tenant_fair_share"]
            for tenant, value in result["measurement"]["per_tenant"].items():
                row[f"{tenant}_iops"] = value["mean_iops"]
            for tenant, value in result.get("per_io", {}).get("per_tenant", {}).items():
                row[f"{tenant}_p99_us"] = value["p99_read_latency_us"]
            for tenant, value in result.get("reactor", {}).get("per_tenant", {}).items():
                row[f"{tenant}_busy_cycles_per_io"] = value["busy_cycles_per_io"]
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", choices=("smoke", "full"), required=True)
    parser.add_argument("--experiment", choices=("legacy", "c1", "c2", "c3", "c4"), default="legacy")
    parser.add_argument("--reference-capacity-iops", type=positive_float)
    parser.add_argument("--host-proof-pattern", default=r"host_saps active.*skipping dpa_plugin_init")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--path-capacities-iops", type=parse_caps, required=True)
    parser.add_argument("--service-limit-iops", type=int, required=True)
    parser.add_argument("--fixed-threshold-us", type=positive_float, default=500)
    parser.add_argument("--poll-interval-s", type=positive_float, default=1)
    parser.add_argument("--counter-hz", type=int)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("run with sudo -n bash drive_campaign.sh")
    if args.service_limit_iops <= 0:
        parser.error("service limit must be positive; it is independent of provisioned path capacities")
    if args.experiment == "c3" and any(cap * 3 != args.service_limit_iops for cap in args.path_capacities_iops):
        parser.error("c3 host reference supports K_p=C/3 only; use matching equal path capacities")
    if (args.experiment == "c1" and args.reference_capacity_iops is None
            and (args.path_capacities_iops != [300000] * 3 or args.service_limit_iops != 900000)):
        parser.error("custom c1 capacities require an explicit matching --reference-capacity-iops")
    if args.reference_capacity_iops is not None and args.experiment not in ("legacy", "c1"):
        parser.error("c2/c3/c4 use entitlement at C; external denominator is only for c1/legacy")
    import re
    try:
        re.compile(args.host_proof_pattern)
    except re.error as exc:
        parser.error(f"invalid host proof pattern: {exc}")
    if args.poll_interval_s > 1 or (args.counter_hz is not None and args.counter_hz <= 0):
        parser.error("poll interval must be <=1s and counter frequency must be positive")
    args.out = args.out.resolve()
    args.runtime_dir = args.runtime_dir.resolve()
    if args.out.exists() and any(args.out.iterdir()):
        parser.error("output directory must be empty")
    args.out.mkdir(parents=True, exist_ok=True)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal_stop)
    campaign = {"status": "running", "campaign": args.campaign, "experiment": args.experiment, "runs": [],
                "started_unix": time.time(), "topology": TOPOLOGY}
    write_json(args.out / "campaign.json", campaign)
    try:
        prov = provenance(args)
        write_json(args.out / "provenance.json", prov)
        reference = 0.0
        calibration_runs = 0
        if args.experiment != "legacy":
            reference = (args.reference_capacity_iops or 899983.2185078033
                         if args.experiment == "c1" else float(args.service_limit_iops))
            matrix = campaign_cells(args.experiment, args.campaign == "smoke")
            write_json(args.out / "calibration.json", {
                "reference_capacity_iops": reference,
                "definition": "fixed supplied healthy calibration" if args.experiment == "c1"
                              else "configured entitlement at C",
                "source": (("explicit --reference-capacity-iops" if args.reference_capacity_iops else
                            "2026-10-06 three healthy stock calibrations")
                           if args.experiment == "c1" else "service-limit-iops"),
                "fresh_calibration_runs": 0, "weights": [3, 1, 1, 1],
                "fixed_across_all_cells": True})
        elif args.campaign == "smoke":
            matrix = [("smoke_saps_q", "saps_q", 20, 5, 18, (8, 17)),
                      ("smoke_fixed_threshold", "fixed_threshold", 20, 5, 18, (8, 17))]
        else:
            calibrations = []
            calibration_runs = 0 if args.reference_capacity_iops else 3
            for repeat in range(1, calibration_runs + 1):
                result = run_one(args, f"calibration_{repeat}", "stock_round_robin", 60,
                                 None, None, (23, 49), 0, prov["controller_HEAD"])
                calibrations.append(result["aggregate_iops"])
                campaign["runs"].append(result)
                write_json(args.out / "campaign.json", campaign)
            reference = args.reference_capacity_iops or statistics.median(calibrations)
            if not math.isfinite(reference) or reference <= 0:
                raise RuntimeError("healthy calibration produced no finite positive reference")
            write_json(args.out / "calibration.json", {
                "reference_capacity_iops": reference, "healthy_aggregate_iops": calibrations,
                "definition": ("explicit supplied fixed reference" if args.reference_capacity_iops else
                               "median of 3 healthy stock aggregates; sum of per-tenant interval-weighted mean IOPS over t=23..49s"),
                "fixed_across_all_fault_arms": True, "weights": [3, 1, 1, 1],
                "path_capacities_iops": args.path_capacities_iops,
                "service_limit_iops": args.service_limit_iops})
            matrix = [(f"r{repeat}_{arm}", arm, 60, 20, 50, (23, 49))
                      for repeat in range(1, 4) for arm in LEGACY_ARMS]
        campaign["reference_capacity_iops"] = reference or None
        campaign["expected_runs"] = len(matrix) + calibration_runs
        write_json(args.out / "campaign.json", campaign)
        for cell in matrix:
            if isinstance(cell, tuple):
                run_id, arm, duration, fault_at, restore_at, window = cell
                cell = dict(run_id=run_id, arm=arm, duration=duration, fault_at=fault_at,
                            restore_at=restore_at, window=window)
            result = run_one(args, reference=reference, commit=prov["controller_HEAD"], **cell)
            campaign["runs"].append(result)
            write_json(args.out / "campaign.json", campaign)
        campaign.update(status="complete", finished_unix=time.time())
        write_json(args.out / "campaign.json", campaign)
        write_summary(args.out / "summary.csv", campaign["runs"])
        return 0
    except BaseException as exc:
        campaign.update(status="invalid", error=str(exc), finished_unix=time.time())
        write_json(args.out / "campaign.json", campaign)
        write_summary(args.out / "summary.csv", campaign["runs"])
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1)
