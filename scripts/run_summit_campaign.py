#!/usr/bin/env python3
"""Run the internal-arm summit experiment without rebuilding the target.

This wrapper owns only campaign coordination around run_saps_q_e2_idle.py:
collision/regime gates, path-B delay injection, manifest capture, target restore,
and steady-state metric extraction.  It does not modify the existing harness.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import shlex
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path


REPO = Path("/home/aiden/DPA/nvme-of-controller")
HARNESS = REPO / "scripts/run_saps_q_e2_idle.py"
RPC = "/home/aiden/spdk/scripts/rpc.py"
RAW_RPC = "/home/aiden/DPA/nvme-of-controller/scripts/spdk_rpc_raw.py"
TARGET = "arm-1"
TARGET_SSH_CONTROL = "/tmp/sapsq_arm1_rpc_%r_%h_%p"
TARGET_SOCKS = {
    "A": "/var/tmp/spdk_a.sock",
    "B": "/var/tmp/spdk_b.sock",
    "C": "/var/tmp/spdk_c.sock",
}
TARGET_PATH_ORDER = ("A", "B", "C")
ARMS = {
    "baseline": {
        "mode": "stock_qd",
        "weights": "3,1,1,1",
        "env": {},
    },
    "drr": {
        "mode": "saps_q",
        "weights": "3,1,1,1",
        "env": {
            "SAPS_M2_ENABLED": "0",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "1",
        },
    },
    "minus_schedule": {
        "mode": "saps_q",
        "weights": "1,1,1,1",
        "env": {
            "SAPS_M2_ENABLED": "1",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "0",
        },
    },
    "saps": {
        "mode": "saps_q",
        "weights": "3,1,1,1",
        "env": {
            "SAPS_M2_ENABLED": "1",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "0",
        },
    },
}


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def run(cmd: list[str], *, timeout: int = 30, check: bool = True,
        env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=check, env=env
    )


def ssh(command: str, *, timeout: int = 60, check: bool = True) -> str:
    result = run(
        [
            "sudo", "-u", "aiden", "ssh",
            "-o", "ControlMaster=auto",
            "-o", "ControlPersist=120",
            "-o", f"ControlPath={TARGET_SSH_CONTROL}",
            TARGET,
            command,
        ],
        timeout=timeout,
        check=check,
    )
    return result.stdout


def target_rpc_command(
    socket_path: str,
    method: str,
    params: dict | None = None,
) -> str:
    command = [
        "sudo",
        "python3",
        RAW_RPC,
        "-s",
        socket_path,
        "--timeout",
        "10",
        method,
    ]
    if params is not None:
        command.extend([
            "--params-json",
            json.dumps(params, separators=(",", ":")),
        ])
    return " ".join(shlex.quote(part) for part in command)


def target_rpc(
    socket_path: str,
    method: str,
    params: dict | None = None,
    *,
    timeout: int = 30,
) -> object:
    raw = ssh(target_rpc_command(socket_path, method, params), timeout=timeout)
    return json.loads(raw)


def collision_gate() -> dict:
    local = run(
        ["pgrep", "-af", "^/home/aiden/spdk/build/examples/bdevperf"],
        check=False,
    ).stdout.strip()
    if local:
        raise RuntimeError(f"single-driver gate failed; local bdevperf:\n{local}")
    listing = ssh(
        "pgrep -af '/home/aiden/spdk/build/bin/[n]vmf_tgt'", check=False
    )
    remote_pids = []
    binary = "/home/aiden/spdk/build/bin/nvmf_tgt"
    for line in listing.splitlines():
        fields = line.split(maxsplit=2)
        if len(fields) >= 2 and fields[1] == binary:
            remote_pids.append(fields[0])
    if len(remote_pids) != 3:
        raise RuntimeError(
            f"target gate failed; expected 3 nvmf_tgt processes, got "
            f"{len(remote_pids)}:\n{listing}"
        )
    return {"local_bdevperf": [], "remote_nvmf_tgt_pids": remote_pids}


def target_regime_gate(tenants: int) -> dict:
    sockets = {}
    for path, sock in TARGET_SOCKS.items():
        subsystems = target_rpc(sock, "nvmf_get_subsystems")
        nqns = {s.get("nqn") for s in subsystems if s.get("subtype") == "NVMe"}
        expected = {f"nqn.2024-01.io.spdk:tenant{i}" for i in range(tenants)}
        missing = sorted(expected - nqns)
        if missing:
            raise RuntimeError(f"regime gate failed on {sock}; missing {missing}")
        sockets[path] = {
            "socket": sock,
            "nvme_subsystem_count": len(nqns),
            "expected_tenant_nqns_present": True,
        }
    return {
        "target": TARGET,
        "transport": "NVMe-oF/RDMA",
        "namespace_backing": "RAM-backed malloc through bdev_delay",
        "sockets": sockets,
    }


def get_aggregate_path_capacities(
    paths: tuple[str, ...] = TARGET_PATH_ORDER,
) -> dict[str, int]:
    capacities = {}
    for path in paths:
        sock = TARGET_SOCKS[path]
        bdev = f"shared_path_{path}"
        data = target_rpc(sock, "bdev_get_bdevs", {"name": bdev})
        if len(data) != 1:
            raise RuntimeError(
                f"aggregate-capacity topology requires one {bdev}, got {len(data)}"
            )
        limits = data[0].get("assigned_rate_limits", {})
        capacity = limits.get("rw_ios_per_sec")
        if not isinstance(capacity, int) or capacity <= 0:
            raise RuntimeError(
                f"aggregate-capacity topology requires positive QoS on {bdev}, "
                f"got {capacity!r}"
            )
        capacities[path] = capacity
    return capacities


def set_aggregate_path_capacities(
    capacities: list[int],
    paths: tuple[str, ...] = TARGET_PATH_ORDER,
) -> dict[str, int]:
    if len(capacities) != len(paths) or any(value < 1000 for value in capacities):
        raise ValueError(
            f"expected {len(paths)} path capacities of at least 1000 IOPS"
        )
    for path, capacity in zip(paths, capacities):
        sock = TARGET_SOCKS[path]
        bdev = f"shared_path_{path}"
        target_rpc(
            sock,
            "bdev_set_qos_limit",
            {"name": bdev, "rw_ios_per_sec": capacity},
        )
    observed = get_aggregate_path_capacities(paths)
    expected = dict(zip(paths, capacities))
    if observed != expected:
        raise RuntimeError(
            f"aggregate path capacity verification failed: "
            f"expected {expected}, observed {observed}"
        )
    return observed


def _delay_state_from_bdev(data: dict) -> dict:
    delay = data.get("driver_specific", {}).get("delay", {})
    return {
        "bdev": data.get("name"),
        "avg_read_latency_us": delay.get("avg_read_latency"),
        "p99_read_latency_us": delay.get("p99_read_latency"),
        "avg_write_latency_us": delay.get("avg_write_latency"),
        "p99_write_latency_us": delay.get("p99_write_latency"),
    }


def get_delay_state(path: str, tenant: int) -> dict:
    sock = TARGET_SOCKS[path]
    bdev = f"delay_t{tenant}_{path}"
    data = target_rpc(sock, "bdev_get_bdevs", {"name": bdev})
    if len(data) != 1:
        raise RuntimeError(f"expected one bdev for {bdev}, got {len(data)}")
    return _delay_state_from_bdev(data[0])


def get_path_states(path: str, tenants: int) -> list[dict]:
    sock = TARGET_SOCKS[path]
    data = target_rpc(sock, "bdev_get_bdevs")
    by_name = {entry.get("name"): entry for entry in data}
    states = []
    for tenant in range(tenants):
        bdev = f"delay_t{tenant}_{path}"
        if bdev not in by_name:
            raise RuntimeError(f"missing target bdev {bdev} on {sock}")
        states.append(_delay_state_from_bdev(by_name[bdev]))
    return states


def set_path_avg_read(path: str, tenants: int, latency_us: int) -> None:
    sock = TARGET_SOCKS[path]
    commands = [
        target_rpc_command(
            sock,
            "bdev_delay_update_latency",
            {
                "delay_bdev_name": f"delay_t{tenant}_{path}",
                "latency_type": "avg_read",
                "latency_us": latency_us,
            },
        )
        for tenant in range(tenants)
    ]
    ssh("set -e; " + "; ".join(commands), timeout=max(60, tenants * 20))
    states = get_path_states(path, tenants)
    wrong = [s for s in states if s["avg_read_latency_us"] != latency_us]
    if wrong:
        raise RuntimeError(
            f"path {path} latency verification failed for {latency_us}us: {wrong}"
        )


def git_metadata() -> dict:
    commit = run(["git", "rev-parse", "HEAD"], timeout=10).stdout.strip()
    dirty = bool(run(["git", "status", "--porcelain"], timeout=60).stdout.strip())
    return {"commit": commit, "dirty": dirty}


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def wait_for_log(path: Path, needle: str, timeout_s: int) -> float:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if needle in path.read_text(errors="replace"):
                return time.time()
        except FileNotFoundError:
            pass
        time.sleep(0.25)
    raise TimeoutError(f"did not observe {needle!r} in {path} within {timeout_s}s")


def stop_bdevperf_after_measurement(orch_log: Path, timeout_s: int) -> None:
    try:
        wait_for_log(orch_log, "perform_tests phase wall-time=", timeout_s)
        wait_for_log(orch_log, "timeseries (iops+path+latency) written", 30)
    except TimeoutError:
        return
    pids = run(
        ["pgrep", "-f", "^/home/aiden/spdk/build/examples/bdevperf"],
        check=False,
    ).stdout.split()
    if pids:
        run(["sudo", "kill", "-9", *pids], timeout=10, check=False)


def read_timeseries(path: Path) -> list[dict[str, float]]:
    with path.open(newline="") as handle:
        return [
            {key: float(value) for key, value in row.items()}
            for row in csv.DictReader(handle)
        ]


def normalize_counter_deltas(rows: list[dict[str, float]]) -> list[dict[str, float]]:
    """Convert target counter deltas to rates using the actual sample interval."""
    if not rows:
        return []
    normalized = []
    fallback_dt = (
        rows[1]["elapsed_s"] - rows[0]["elapsed_s"] if len(rows) > 1
        else rows[0]["elapsed_s"]
    )
    for index, row in enumerate(rows):
        interval_s = (
            row["elapsed_s"] - rows[index - 1]["elapsed_s"]
            if index > 0 else fallback_dt
        )
        if interval_s <= 0:
            raise RuntimeError(f"non-positive timeseries interval at row {index}")
        normalized.append({
            key: value if key == "elapsed_s" else value / interval_s
            for key, value in row.items()
        })
    return normalized


def median_columns(rows: list[dict[str, float]], columns: list[str]) -> dict[str, float]:
    return {
        column: statistics.median(row[column] for row in rows)
        for column in columns
    }


def compute_metrics(run_dir: Path, capacity_iops: int,
                    reference_weights: list[int], measurement_start_s: float,
                    measurement_end_s: float) -> dict:
    # The collector already converts cumulative SPDK counters to IOPS using the
    # actual interval between samples.  Applying another interval division here
    # would scale every rate a second time.
    tenant_rows = read_timeseries(run_dir / "per_tenant_iops_timeseries.csv")
    path_rows = read_timeseries(run_dir / "per_path_iops_timeseries.csv")
    selected_tenant = [
        row for row in tenant_rows
        if measurement_start_s <= row["elapsed_s"] <= measurement_end_s
    ]
    selected_path = [
        row for row in path_rows
        if measurement_start_s <= row["elapsed_s"] <= measurement_end_s
    ]
    if len(selected_tenant) < 3 or len(selected_path) < 3:
        raise RuntimeError(
            f"steady-state window too short: tenant_rows={len(selected_tenant)} "
            f"path_rows={len(selected_path)}"
        )
    tenant_columns = [f"t{i}" for i in range(len(reference_weights))]
    path_columns = [
        f"{path}_t{i}" for path in "ABC" for i in range(len(reference_weights))
    ]
    for tenant_row, path_row in zip(selected_tenant, selected_path):
        for tenant in range(len(reference_weights)):
            aggregate = sum(path_row[f"{path}_t{tenant}"] for path in "ABC")
            reported = tenant_row[f"t{tenant}"]
            tolerance = max(5.0, 0.001 * max(reported, aggregate))
            if abs(reported - aggregate) > tolerance:
                raise RuntimeError(
                    "per-tenant/path rate mismatch at "
                    f"t={tenant_row['elapsed_s']}: tenant={tenant} "
                    f"reported={reported} path_sum={aggregate}"
                )
    tenant_medians = median_columns(selected_tenant, tenant_columns)
    path_medians = median_columns(selected_path, path_columns)
    total_weight = sum(reference_weights)
    ideal = [capacity_iops * weight / total_weight for weight in reference_weights]
    ratios = [tenant_medians[f"t{i}"] / ideal[i] for i in range(len(ideal))]
    normalized = [tenant_medians[f"t{i}"] / reference_weights[i]
                  for i in range(len(reference_weights))]
    denom = len(normalized) * sum(value * value for value in normalized)
    weighted_jain = ((sum(normalized) ** 2) / denom) if denom else 0.0
    path_totals = {
        path: sum(path_medians[f"{path}_t{i}"] for i in range(len(reference_weights)))
        for path in "ABC"
    }
    return {
        "measurement_window_s": {
            "start": measurement_start_s,
            "end": measurement_end_s,
            "tenant_rows": len(selected_tenant),
            "path_rows": len(selected_path),
            "input_rows_are_rates_normalized_by_collector": True,
            "per_tenant_equals_path_sum_checked": True,
        },
        "reference_capacity_iops": capacity_iops,
        "reference_weights": reference_weights,
        "ideal_tenant_iops": ideal,
        "tenant_median_iops": tenant_medians,
        "tenant_fair_share_ratio": {f"t{i}": ratios[i] for i in range(len(ratios))},
        "worst_tenant_fair_share": min(ratios),
        "weighted_jain_normalized_by_weight": weighted_jain,
        "path_tenant_median_iops": path_medians,
        "path_total_median_iops": path_totals,
    }


def measurement_window(
    collector_start_unix_s: float,
    perform_seen_unix_s: float,
    inject_finished_unix_s: float | None,
    settle_s: float,
    duration_s: float,
    end_margin_s: float,
) -> tuple[float, float, float | None]:
    """Return collector-relative measurement bounds for the workload phase."""
    perform_elapsed = perform_seen_unix_s - collector_start_unix_s
    fault_complete_elapsed = (
        None
        if inject_finished_unix_s is None
        else inject_finished_unix_s - collector_start_unix_s
    )
    start = (
        perform_elapsed + settle_s
        if fault_complete_elapsed is None
        else fault_complete_elapsed + settle_s
    )
    end = perform_elapsed + duration_s - end_margin_s
    if end <= start:
        raise RuntimeError(
            "measurement window is empty after setup and fault injection: "
            f"start={start:.3f}s end={end:.3f}s"
        )
    return start, end, fault_complete_elapsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument("--severities-us", default="0,500,2000,5000,10000,20000")
    parser.add_argument("--arms", default=",".join(ARMS))
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration-s", type=int, default=45)
    parser.add_argument("--fault-at-s", type=float, default=8.0)
    parser.add_argument("--settle-s", type=float, default=5.0)
    parser.add_argument("--end-margin-s", type=float, default=3.0)
    parser.add_argument("--baseline-latency-us", type=int, default=27)
    parser.add_argument("--capacity-iops", type=int, default=900000)
    parser.add_argument("--tenants", type=int, default=4)
    return parser.parse_args()


def run_cell(args: argparse.Namespace, arm_name: str, severity_us: int,
             rep: int, campaign_meta: dict) -> dict:
    arm = ARMS[arm_name]
    run_dir = args.out_root / f"severity_{severity_us:06d}us" / arm_name / f"rep{rep}"
    run_dir.mkdir(parents=True, exist_ok=False)
    workload_log = run_dir / "driver.stdout.log"
    error_log = run_dir / "driver.stderr.log"
    orch_log = run_dir / "orchestrator.log"

    gate = collision_gate()
    set_path_avg_read("B", args.tenants, args.baseline_latency_us)
    pre_state = get_path_states("B", args.tenants)

    cmd = [
        sys.executable,
        str(HARNESS),
        "--num-tenants", str(args.tenants),
        "--weights", arm["weights"],
        "--mode", arm["mode"],
        "--idle-tenant", "7",
        "--idle-start", str(args.duration_s - 1),
        "--idle-end", str(args.duration_s),
        "--duration", str(args.duration_s),
        "--output-dir", str(run_dir),
        "--link-cap", str(args.capacity_iops),
        "--qd", "32",
        "--no-setup-arm1",
        "--collect-latency",
        "--target-ip", "10.0.0.1",
        "--delay-restore-us", str(args.baseline_latency_us),
    ]
    child_env = os.environ.copy()
    child_env.update({
        "DPA_PLUGIN_DEV": "mlx5_0",
        "SAPS_SAMPLE_RATE": "1",
        **arm["env"],
    })
    started_at = now_utc()
    manifest = {
        **campaign_meta,
        "experiment": "fig:summit",
        "arm": arm_name,
        "arm_config": arm,
        "severity_us": severity_us,
        "configured_fault_avg_read_us": (
            args.baseline_latency_us if severity_us == 0 else severity_us
        ),
        "rep": rep,
        "date_started_utc": started_at,
        "raw_path": str(run_dir),
        "command": cmd,
        "environment": {key: child_env[key] for key in sorted({
            "DPA_PLUGIN_DEV", "SAPS_SAMPLE_RATE", *arm["env"].keys()
        })},
        "collision_gate": gate,
        "target_path_b_before": pre_state,
        "status": "running",
    }
    write_json(run_dir / "manifest.json", manifest)

    proc = None
    inject_started = None
    inject_finished = None
    try:
        with workload_log.open("w") as stdout, error_log.open("w") as stderr:
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, env=child_env)
            perform_seen = wait_for_log(orch_log, "perform_tests (", 210)
            if severity_us > 0:
                sleep_for = max(0.0, args.fault_at_s - (time.time() - perform_seen))
                time.sleep(sleep_for)
                inject_started = time.time()
                set_path_avg_read("B", args.tenants, severity_us)
                inject_finished = time.time()
            stop_bdevperf_after_measurement(
                orch_log, timeout_s=args.duration_s + 90
            )
            rc = proc.wait(timeout=args.duration_s + 420)
        if rc != 0:
            raise RuntimeError(f"harness exited rc={rc}; see {error_log}")

        summary = json.loads((run_dir / "e2_run_summary.json").read_text())
        if not summary.get("valid"):
            raise RuntimeError(f"harness marked run invalid: {summary.get('invalid_reason')}")
        collector_start = int((run_dir / "collector_start_ms.txt").read_text()) / 1000.0
        measurement_start, measurement_end, onset_elapsed = measurement_window(
            collector_start,
            perform_seen,
            inject_finished,
            args.settle_s,
            args.duration_s,
            args.end_margin_s,
        )
        metrics = compute_metrics(
            run_dir,
            args.capacity_iops,
            [3, 1, 1, 1],
            measurement_start,
            measurement_end,
        )
        metrics["fault_timing"] = {
            "perform_seen_unix_s": perform_seen,
            "inject_started_unix_s": inject_started,
            "inject_finished_unix_s": inject_finished,
            "fault_complete_elapsed_s": onset_elapsed,
        }
        write_json(run_dir / "metrics.json", metrics)
        manifest.update({
            "status": "valid",
            "date_finished_utc": now_utc(),
            "returncode": rc,
            "fault_timing": metrics["fault_timing"],
            "metrics_path": str(run_dir / "metrics.json"),
        })
        return {
            "run_dir": str(run_dir),
            "arm": arm_name,
            "severity_us": severity_us,
            "rep": rep,
            "worst_tenant_fair_share": metrics["worst_tenant_fair_share"],
            "weighted_jain": metrics["weighted_jain_normalized_by_weight"],
            "path_total_median_iops": metrics["path_total_median_iops"],
        }
    except BaseException as exc:
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
        manifest.update({
            "status": "invalid",
            "date_finished_utc": now_utc(),
            "error": f"{type(exc).__name__}: {exc}",
        })
        raise
    finally:
        try:
            set_path_avg_read("B", args.tenants, args.baseline_latency_us)
            restored = get_path_states("B", args.tenants)
            manifest["target_path_b_after_restore"] = restored
            manifest["target_restored"] = all(
                state["avg_read_latency_us"] == args.baseline_latency_us
                for state in restored
            )
        except BaseException as restore_exc:
            manifest["target_restored"] = False
            manifest["restore_error"] = f"{type(restore_exc).__name__}: {restore_exc}"
        write_json(run_dir / "manifest.json", manifest)


def main() -> int:
    args = parse_args()
    severities = [int(value) for value in args.severities_us.split(",")]
    arms = [value.strip() for value in args.arms.split(",") if value.strip()]
    unknown = sorted(set(arms) - set(ARMS))
    if unknown:
        raise SystemExit(f"unknown arms: {unknown}; choices={sorted(ARMS)}")
    if args.reps < 1:
        raise SystemExit("--reps must be >= 1")
    if args.duration_s <= args.fault_at_s + args.settle_s + args.end_margin_s + 5:
        raise SystemExit("duration leaves fewer than five steady-state samples")

    args.out_root.mkdir(parents=True, exist_ok=False)
    campaign_meta = {
        "campaign": "testbed_campaign_20260718/05_summit",
        "git": git_metadata(),
        "campaign_command": sys.argv,
        "target_regime": target_regime_gate(args.tenants),
        "reference_capacity_iops": args.capacity_iops,
        "reference_weights": [3, 1, 1, 1],
        "fault_path": "B",
        "fault_field": "avg_read",
        "baseline_latency_us": args.baseline_latency_us,
    }
    write_json(args.out_root / "campaign_manifest.json", {
        **campaign_meta,
        "date_started_utc": now_utc(),
        "severities_us": severities,
        "arms": arms,
        "reps": args.reps,
        "duration_s": args.duration_s,
    })

    results = []
    try:
        for severity_us in severities:
            for arm in arms:
                for rep in range(args.reps):
                    print(
                        f"[summit] start severity={severity_us}us arm={arm} rep={rep}",
                        flush=True,
                    )
                    result = run_cell(args, arm, severity_us, rep, campaign_meta)
                    results.append(result)
                    write_json(args.out_root / "campaign_results.json", results)
                    print(
                        f"[summit] valid severity={severity_us}us arm={arm} rep={rep} "
                        f"worst={result['worst_tenant_fair_share']:.3f} "
                        f"paths={result['path_total_median_iops']}",
                        flush=True,
                    )
    finally:
        set_path_avg_read("B", args.tenants, args.baseline_latency_us)
    print(f"[summit] complete raw={args.out_root}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
