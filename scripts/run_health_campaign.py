#!/usr/bin/env python3
"""Run the continuous path-health probe/sweep on the existing target.

This campaign wrapper leaves run_saps_q_e2_idle.py unchanged.  It launches the
two-path adapter, injects path-B average read latency from the host, snapshots
the coordinator's shared SAPS-Q ring once per interval, writes one manifest per
cell, and restores the target to its measured baseline in a finally block.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

import run_summit_campaign as common


REPO = Path("/home/aiden/DPA/nvme-of-controller")
HARNESS = REPO / "scripts/run_saps_q_health_2p.py"
SAPSQ_DUMP = REPO / "scripts/sapsq_dump"
SOCKET_MARKER = "/var/tmp/bdevperf_sapsq_proc0.sock"
PATH_NAMES = ("A", "B")


def parse_csv_ints(raw: str, label: str) -> list[int]:
    try:
        values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    except ValueError as exc:
        raise SystemExit(f"invalid {label}: {raw!r}") from exc
    if not values or any(value < 0 for value in values):
        raise SystemExit(f"invalid {label}: {raw!r}")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument(
        "--severities-us", default="0,500,2000,5000,20000,100000"
    )
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration-s", type=int, default=25)
    parser.add_argument("--fault-at-s", type=float, default=5.0)
    parser.add_argument("--settle-s", type=float, default=5.0)
    parser.add_argument("--dump-interval-ms", type=int, default=1000)
    parser.add_argument("--baseline-latency-us", type=int, default=27)
    parser.add_argument("--capacity-iops", type=int, default=900000)
    parser.add_argument(
        "--path-capacities-iops",
        default=None,
        help="required deliverable capacities K_p for paths A/B",
    )
    parser.add_argument("--tenants", type=int, default=4)
    parser.add_argument("--weights", default="3,1,1,1")
    return parser.parse_args()


def _candidate_pids() -> list[int]:
    result = subprocess.run(
        ["sudo", "pgrep", "-f", SOCKET_MARKER],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    return sorted({int(token) for token in result.stdout.split() if token.isdigit()})


def _find_coordinator_memfd() -> tuple[int, str]:
    for pid in _candidate_pids():
        result = subprocess.run(
            [
                "sudo", "find", f"/proc/{pid}/fd", "-maxdepth", "1",
                "-type", "l", "-lname", "*dpa_plugin_ring*", "-print",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        paths = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        if paths:
            return pid, paths[0]
    raise RuntimeError(
        f"dpa_plugin_ring memfd not found for coordinator socket {SOCKET_MARKER}"
    )


def dump_health_snapshot(path: Path, elapsed_ms: int) -> dict:
    pid, fd_path = _find_coordinator_memfd()
    result = subprocess.run(
        [
            "taskset",
            "-c",
            "0",
            "sudo",
            "-n",
            str(SAPSQ_DUMP),
            fd_path,
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"sapsq_dump rc={result.returncode}: {result.stderr.strip()[:200]}"
        )
    ring = json.loads(result.stdout)
    payload = {
        "_meta": {
            "captured_unix_s": time.time(),
            "elapsed_ms": elapsed_ms,
            "source_pid": pid,
            "source_fd": fd_path,
            "sapsq_num_paths": len(ring.get("path_health_factor_q16", [])),
            "sapsq_num_tenants": len(ring.get("tenant_path_rate_budget_q32", [])),
        },
        "tenant_00": ring,
    }
    common.write_json(path, payload)
    return payload


class PeriodicDumper:
    def __init__(self, directory: Path, interval_ms: int):
        self.directory = directory
        self.interval_s = interval_ms / 1000.0
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.started_unix_s: float | None = None
        self.errors: list[dict] = []

    def start(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=False)
        self.started_unix_s = time.time()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        assert self.started_unix_s is not None
        index = 0
        while not self.stop_event.is_set():
            elapsed_ms = int((time.time() - self.started_unix_s) * 1000)
            snap = self.directory / f"snap_{elapsed_ms:06d}ms.json"
            try:
                dump_health_snapshot(snap, elapsed_ms)
            except BaseException as exc:
                # A capture can already be in flight when the driver finishes
                # its measurement window and asks the dumper to stop.  The
                # coordinator may close its memfd during that capture.  This is
                # a shutdown race, not missing in-window observability, so do
                # not turn it into a campaign failure.
                if self.stop_event.is_set():
                    break
                error = {
                    "captured_unix_s": time.time(),
                    "elapsed_ms": elapsed_ms,
                    "error": f"{type(exc).__name__}: {exc}",
                }
                self.errors.append(error)
                common.write_json(snap, {"_meta": error, "observability_degraded": True})
            index += 1
            deadline = self.started_unix_s + index * self.interval_s
            if self.stop_event.wait(max(0.0, deadline - time.time())):
                break

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(5.0, self.interval_s * 2))


def _load_samples(periodic_dir: Path) -> list[dict]:
    samples = []
    for path in sorted(periodic_dir.glob("snap_*.json")):
        payload = json.loads(path.read_text())
        ring = payload.get("tenant_00")
        if not isinstance(ring, dict):
            continue
        health = ring.get("path_health_factor_q16")
        enum = ring.get("path_health_enum")
        budgets = ring.get("tenant_path_rate_budget_q32")
        budgets_iops = ring.get("tenant_path_rate_budget_iops")
        path_capacities = ring.get("path_capacity_iops")
        effective_capacities = ring.get("path_effective_capacity_iops")
        num_paths = payload.get("_meta", {}).get("sapsq_num_paths")
        if not isinstance(num_paths, int) or num_paths <= 0:
            continue
        if not isinstance(health, list) or len(health) != num_paths:
            continue
        samples.append({
            "snapshot": str(path),
            "captured_unix_s": payload["_meta"]["captured_unix_s"],
            "elapsed_ms": payload["_meta"]["elapsed_ms"],
            "sapsq_num_paths": num_paths,
            "path_health_factor_q16": health[:num_paths],
            "path_health_enum": enum[:num_paths] if isinstance(enum, list) else None,
            "tenant_path_rate_budget_q32": budgets,
            "tenant_path_rate_budget_iops": budgets_iops,
            "path_capacity_iops": path_capacities,
            "path_effective_capacity_iops": effective_capacities,
            "available_path_capacity_iops": ring.get(
                "available_path_capacity_iops"
            ),
            "admitted_capacity_iops": ring.get("admitted_capacity_iops"),
            "service_envelope_iops": ring.get("link_cap_iops"),
        })
    return samples


def _median_vector(samples: list[dict], field: str, num_paths: int) -> list[float] | None:
    rows = [sample[field] for sample in samples if isinstance(sample.get(field), list)]
    if not rows or any(len(row) < num_paths for row in rows):
        return None
    return [statistics.median(row[path_idx] for row in rows) for path_idx in range(num_paths)]


def _median_budget_matrix(
    samples: list[dict], num_tenants: int, num_paths: int
) -> list[list[float]] | None:
    matrices = [
        sample["tenant_path_rate_budget_q32"]
        for sample in samples
        if isinstance(sample.get("tenant_path_rate_budget_q32"), list)
    ]
    if not matrices:
        return None
    if any(
        len(matrix) < num_tenants
        or any(len(row) < num_paths for row in matrix[:num_tenants])
        for matrix in matrices
    ):
        return None
    return [
        [
            statistics.median(matrix[tenant][path] for matrix in matrices)
            for path in range(num_paths)
        ]
        for tenant in range(num_tenants)
    ]


def summarize_samples(
    periodic_dir: Path,
    perform_seen: float,
    inject_started: float | None,
    inject_finished: float | None,
    settle_s: float,
) -> dict:
    samples = _load_samples(periodic_dir)
    if not samples:
        raise RuntimeError("no snapshots contained path_health_factor_q16")
    num_paths = samples[0]["sapsq_num_paths"]
    if num_paths != len(PATH_NAMES):
        raise RuntimeError(f"expected {len(PATH_NAMES)} runtime paths, got {num_paths}")
    pre_end = inject_started if inject_started is not None else perform_seen + settle_s
    pre = [sample for sample in samples if sample["captured_unix_s"] < pre_end]
    if inject_finished is None:
        post = [sample for sample in samples if sample["captured_unix_s"] >= pre_end]
    else:
        post = [
            sample for sample in samples
            if sample["captured_unix_s"] >= inject_finished + settle_s
        ]
    if not pre or not post:
        raise RuntimeError(
            f"insufficient pre/post snapshots: pre={len(pre)} post={len(post)}"
        )
    pre_health = _median_vector(pre, "path_health_factor_q16", num_paths)
    post_health = _median_vector(post, "path_health_factor_q16", num_paths)
    assert pre_health is not None and post_health is not None
    first_budgets = samples[0].get("tenant_path_rate_budget_q32")
    num_tenants = len(first_budgets) if isinstance(first_budgets, list) else 0
    post_budgets = _median_budget_matrix(post, num_tenants, num_paths)
    post_budgets_iops = [
        sample["tenant_path_rate_budget_iops"]
        for sample in post
        if isinstance(sample.get("tenant_path_rate_budget_iops"), list)
    ]
    post_budget_iops = None
    if post_budgets_iops and all(
        len(matrix) >= num_tenants
        and all(len(row) >= num_paths for row in matrix[:num_tenants])
        for matrix in post_budgets_iops
    ):
        post_budget_iops = [
            [
                statistics.median(
                    matrix[tenant][path] for matrix in post_budgets_iops
                )
                for path in range(num_paths)
            ]
            for tenant in range(num_tenants)
        ]
    formula_checks = []
    for sample in samples:
        capacities = sample.get("path_capacity_iops")
        effective = sample.get("path_effective_capacity_iops")
        envelope = sample.get("service_envelope_iops")
        admitted = sample.get("admitted_capacity_iops")
        if (
            not isinstance(capacities, list)
            or len(capacities) != num_paths
            or not isinstance(effective, list)
            or len(effective) != num_paths
            or not isinstance(envelope, int)
            or not isinstance(admitted, int)
        ):
            formula_checks.append(False)
            continue
        expected_effective = [
            (int(capacities[path]) * int(sample["path_health_factor_q16"][path]))
            >> 16
            for path in range(num_paths)
        ]
        expected_admitted = min(envelope, sum(expected_effective))
        formula_checks.append(
            effective == expected_effective
            and sample.get("available_path_capacity_iops")
            == sum(expected_effective)
            and admitted == expected_admitted
        )
    continuous_drop_observed = (
        post_health[1] < pre_health[1]
        and post_health[1] < post_health[0]
    )
    return {
        "sapsq_num_paths": num_paths,
        "sapsq_num_tenants": num_tenants,
        "path_names": list(PATH_NAMES),
        "sample_count": len(samples),
        "pre_sample_count": len(pre),
        "post_sample_count": len(post),
        "pre_health_factor_q16_median": pre_health,
        "post_health_factor_q16_median": post_health,
        "post_path_health_ratio_to_q16_one": [value / 65536.0 for value in post_health],
        "post_tenant_path_rate_budget_q32_median": post_budgets,
        "post_tenant_path_rate_budget_iops_median": post_budget_iops,
        "post_path_effective_capacity_iops_median": _median_vector(
            post, "path_effective_capacity_iops", num_paths
        ),
        "post_admitted_capacity_iops_median": statistics.median(
            sample["admitted_capacity_iops"]
            for sample in post
            if isinstance(sample.get("admitted_capacity_iops"), int)
        ),
        "allocation_formula_consistent_all_samples": (
            bool(formula_checks) and all(formula_checks)
        ),
        "continuous_health_drop_observed_on_path_b": continuous_drop_observed,
        "samples": samples,
    }


def run_cell(
    args: argparse.Namespace,
    severity_us: int,
    rep: int,
    campaign_meta: dict,
) -> dict:
    run_dir = args.out_root / f"severity_{severity_us:06d}us" / f"rep{rep}"
    run_dir.mkdir(parents=True, exist_ok=False)
    orch_log = run_dir / "orchestrator.log"
    stdout_path = run_dir / "driver.stdout.log"
    stderr_path = run_dir / "driver.stderr.log"
    periodic_dir = run_dir / "sapsq_periodic"

    gate = common.collision_gate()
    aggregate_before = common.get_aggregate_path_capacities(PATH_NAMES)
    aggregate_configured = common.set_aggregate_path_capacities(
        args.path_capacities_iops, PATH_NAMES
    )
    common.set_path_avg_read("B", args.tenants, args.baseline_latency_us)
    before = common.get_path_states("B", args.tenants)
    command = [
        sys.executable,
        str(HARNESS),
        "--num-tenants", str(args.tenants),
        "--weights", args.weights,
        "--mode", "saps_q",
        "--idle-tenant", "7",
        "--idle-start", str(args.duration_s - 1),
        "--idle-end", str(args.duration_s),
        "--duration", str(args.duration_s),
        "--output-dir", str(run_dir),
        "--link-cap", str(args.capacity_iops),
        "--path-caps", ",".join(
            str(value) for value in args.path_capacities_iops
        ),
        "--qd", "32",
        "--no-setup-arm1",
        "--no-bypass-d",
        "--collect-latency",
        "--target-ip", "10.0.0.1",
        "--delay-restore-us", str(args.baseline_latency_us),
    ]
    child_env = os.environ.copy()
    child_env.update({
        "DPA_PLUGIN_DEV": "mlx5_0",
        "SAPS_SAMPLE_RATE": "1",
        "SAPSQ_BYPASS_SAPS_FSM": "0",
        "SAPS_M2_ENABLED": "1",
        "SAPS_M2_V2_CLASSIFIER": "0",
        "SAPS_M5_DRR_ENABLED": "1",
    })
    manifest = {
        **campaign_meta,
        "experiment": "fig:health-signal/fig:health-share",
        "date_started_utc": common.now_utc(),
        "raw_path": str(run_dir),
        "severity_us": severity_us,
        "rep": rep,
        "command": command,
        "environment": {
            key: child_env[key]
            for key in (
                "DPA_PLUGIN_DEV", "SAPS_SAMPLE_RATE", "SAPSQ_BYPASS_SAPS_FSM",
                "SAPS_M2_ENABLED", "SAPS_M2_V2_CLASSIFIER", "SAPS_M5_DRR_ENABLED",
            )
        },
        "collision_gate": gate,
        "aggregate_path_capacity_before_iops": aggregate_before,
        "aggregate_path_capacity_configured_iops": aggregate_configured,
        "target_path_b_before": before,
        "status": "running",
    }
    common.write_json(run_dir / "manifest.json", manifest)

    proc: subprocess.Popen | None = None
    dumper: PeriodicDumper | None = None
    perform_seen = None
    inject_started = None
    inject_finished = None
    try:
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            proc = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=child_env)
            perform_seen = common.wait_for_log(orch_log, "perform_tests (", 210)
            dumper = PeriodicDumper(periodic_dir, args.dump_interval_ms)
            dumper.start()
            time.sleep(max(0.0, args.fault_at_s - (time.time() - perform_seen)))
            if severity_us > 0:
                inject_started = time.time()
                common.set_path_avg_read("B", args.tenants, severity_us)
                inject_finished = time.time()
            common.stop_bdevperf_after_measurement(
                orch_log, timeout_s=args.duration_s + 90
            )
            if dumper is not None:
                dumper.stop()
            returncode = proc.wait(timeout=args.duration_s + 420)
        if dumper is not None:
            dumper.stop()
        if returncode != 0:
            raise RuntimeError(f"harness exited rc={returncode}; see {stderr_path}")
        summary = json.loads((run_dir / "e2_run_summary.json").read_text())
        if not summary.get("valid"):
            raise RuntimeError(
                f"harness marked run invalid: {summary.get('invalid_reason')}"
            )
        if summary.get("health_classifier_bypassed") is not False:
            raise RuntimeError(
                "health campaign requires bypass_d=0, observed "
                f"{summary.get('health_classifier_bypassed')}"
            )
        if summary.get("path_capacity_iops") != args.path_capacities_iops:
            raise RuntimeError(
                "path-capacity mismatch: "
                f"expected {args.path_capacities_iops}, "
                f"observed {summary.get('path_capacity_iops')}"
            )
        health = summarize_samples(
            periodic_dir,
            perform_seen,
            inject_started,
            inject_finished,
            args.settle_s,
        )
        if not health["allocation_formula_consistent_all_samples"]:
            raise RuntimeError(
                "captured scheduler state violates "
                "e_p=K_p h_p or B=min(C,sum(e_p))"
            )
        if (
            severity_us > 0
            and not health["continuous_health_drop_observed_on_path_b"]
        ):
            raise RuntimeError(
                "faulted path B did not fall below its pre-fault health "
                "and healthy path A"
            )
        health.update({
            "severity_us": severity_us,
            "rep": rep,
            "fault_timing": {
                "perform_seen_unix_s": perform_seen,
                "inject_started_unix_s": inject_started,
                "inject_finished_unix_s": inject_finished,
            },
            "periodic_dump_errors": dumper.errors if dumper is not None else [],
        })
        common.write_json(run_dir / "health_metrics.json", health)
        manifest.update({
            "status": "valid",
            "date_finished_utc": common.now_utc(),
            "returncode": returncode,
            "health_metrics_path": str(run_dir / "health_metrics.json"),
            "fault_timing": health["fault_timing"],
            "health_signal_gate": {
                "passed": (
                    (
                        severity_us == 0
                        or health["continuous_health_drop_observed_on_path_b"]
                    )
                    and health["allocation_formula_consistent_all_samples"]
                ),
                "criterion": (
                    "post-injection path B health factor drops below both its "
                    "pre-injection value and healthy path A, and every snapshot "
                    "satisfies e_p=K_p h_p and B=min(C,sum(e_p))"
                ),
            },
        })
        return {
            "run_dir": str(run_dir),
            "severity_us": severity_us,
            "rep": rep,
            "post_health_factor_q16_median": health[
                "post_health_factor_q16_median"
            ],
            "post_path_health_ratio_to_q16_one": health[
                "post_path_health_ratio_to_q16_one"
            ],
        }
    except BaseException as exc:
        if dumper is not None:
            dumper.stop()
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
        manifest.update({
            "status": "invalid",
            "date_finished_utc": common.now_utc(),
            "error": f"{type(exc).__name__}: {exc}",
        })
        raise
    finally:
        try:
            common.set_path_avg_read("B", args.tenants, args.baseline_latency_us)
            restored = common.get_path_states("B", args.tenants)
            manifest["target_path_b_after_restore"] = restored
            manifest["target_restored"] = all(
                state["avg_read_latency_us"] == args.baseline_latency_us
                for state in restored
            )
        except BaseException as restore_exc:
            manifest["target_restored"] = False
            manifest["restore_error"] = (
                f"{type(restore_exc).__name__}: {restore_exc}"
            )
        try:
            restored_caps = common.set_aggregate_path_capacities(
                [aggregate_before[path] for path in PATH_NAMES], PATH_NAMES
            )
            manifest["aggregate_path_capacity_after_restore_iops"] = restored_caps
            manifest["aggregate_path_capacity_restored"] = (
                restored_caps == aggregate_before
            )
        except BaseException as cap_restore_exc:
            manifest["aggregate_path_capacity_restored"] = False
            manifest["aggregate_path_capacity_restore_error"] = (
                f"{type(cap_restore_exc).__name__}: {cap_restore_exc}"
            )
        common.write_json(run_dir / "manifest.json", manifest)


def main() -> int:
    args = parse_args()
    if args.path_capacities_iops is None:
        raise SystemExit(
            "--path-capacities-iops is required; do not infer K_p from "
            "--capacity-iops"
        )
    args.path_capacities_iops = parse_csv_ints(
        args.path_capacities_iops, "path capacities"
    )
    if len(args.path_capacities_iops) != len(PATH_NAMES) or any(
        value <= 0 for value in args.path_capacities_iops
    ):
        raise SystemExit("--path-capacities-iops must contain two positive values")
    severities = parse_csv_ints(args.severities_us, "severities")
    weights = parse_csv_ints(args.weights, "weights")
    if len(weights) != args.tenants:
        raise SystemExit(
            f"--weights has {len(weights)} entries, expected --tenants={args.tenants}"
        )
    if args.reps < 1:
        raise SystemExit("--reps must be positive")
    if args.dump_interval_ms < 100:
        raise SystemExit("--dump-interval-ms must be at least 100")
    if args.duration_s <= args.fault_at_s + args.settle_s + 5:
        raise SystemExit("duration leaves fewer than five seconds of post-settle data")
    if not SAPSQ_DUMP.exists():
        raise SystemExit(f"missing helper: {SAPSQ_DUMP}")

    args.out_root.mkdir(parents=True, exist_ok=False)
    campaign_meta = {
        "campaign": "testbed_campaign_20260718/08_health",
        "git": common.git_metadata(),
        "campaign_command": sys.argv,
        "target_regime": common.target_regime_gate(args.tenants),
        "severities_us": severities,
        "reps": args.reps,
        "path_deliverable_capacity_iops": args.path_capacities_iops,
        "fixed_conditions": {
            "tenants": args.tenants,
            "weights": weights,
            "runtime_paths": list(PATH_NAMES),
            "qd": 32,
            "workload": "4KB random read",
            "fault_path": "B",
            "baseline_path_latency_us": args.baseline_latency_us,
            "periodic_dump_interval_ms": args.dump_interval_ms,
        },
    }
    common.write_json(args.out_root / "campaign_manifest.json", {
        **campaign_meta,
        "date_started_utc": common.now_utc(),
    })

    results = []
    try:
        for severity_us in severities:
            for rep in range(args.reps):
                print(
                    f"[health] start severity={severity_us}us rep={rep}",
                    flush=True,
                )
                result = run_cell(args, severity_us, rep, campaign_meta)
                results.append(result)
                common.write_json(args.out_root / "campaign_results.json", results)
                health = result["post_path_health_ratio_to_q16_one"]
                print(
                    f"[health] valid severity={severity_us}us rep={rep} "
                    f"health_A={health[0]:.4f} health_B={health[1]:.4f}",
                    flush=True,
                )
    finally:
        common.set_path_avg_read("B", args.tenants, args.baseline_latency_us)

    common.write_json(args.out_root / "campaign_manifest.json", {
        **campaign_meta,
        "date_finished_utc": common.now_utc(),
        "status": "complete",
        "completed_cells": len(results),
    })
    print(f"[health] complete raw={args.out_root}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
