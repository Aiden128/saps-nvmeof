#!/usr/bin/env python3
"""Run fig:generality with one frozen SAPS configuration across workloads."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import run_summit_campaign as common
import run_health_campaign as health_observation
import run_summit_v2_campaign as summit_v2


REPO = Path("/home/aiden/DPA/nvme-of-controller")
WORKLOAD_RUNNER = REPO / "scripts/run_saps_q_workload.py"
WORKLOADS = {
    "4kb_rr": {"io_size": 4096, "workload": "randread", "rwmixread": None},
    "128kb_rr": {"io_size": 131072, "workload": "randread", "rwmixread": None},
    "4kb_rw70": {"io_size": 4096, "workload": "randrw", "rwmixread": 70},
}
ARMS = {
    "baseline": {
        "mode": "stock_qd",
        "bypass_d": False,
        "env": {},
    },
    "saps": {
        "mode": "saps_q",
        "bypass_d": False,
        "env": {
            "SAPS_M2_ENABLED": "1",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "0",
        },
    },
}


def set_path_b_latency(tenants: int, read_us: int, write_us: int) -> list[dict]:
    sock = common.TARGET_SOCKS["B"]
    commands = []
    for tenant in range(tenants):
        bdev = f"delay_t{tenant}_B"
        commands.extend([
            f"sudo {common.RPC} -s {sock} bdev_delay_update_latency "
            f"{bdev} avg_read {read_us}",
            f"sudo {common.RPC} -s {sock} bdev_delay_update_latency "
            f"{bdev} avg_write {write_us}",
        ])
    common.ssh("set -e; " + "; ".join(commands), timeout=max(90, tenants * 30))
    states = common.get_path_states("B", tenants)
    wrong = [
        state for state in states
        if state["avg_read_latency_us"] != read_us
        or state["avg_write_latency_us"] != write_us
    ]
    if wrong:
        raise RuntimeError(
            f"path-B latency verification failed read={read_us} write={write_us}: {wrong}"
        )
    return states


def parse_tail_latency(run_dir: Path, tenants: int) -> dict:
    percentiles = {"p99_us": "99.00000", "p999_us": "99.90000",
                   "p9999_us": "99.99000"}
    per_tenant = {}
    for tenant in range(tenants):
        text = (run_dir / f"tenant_{tenant:02d}" / "bdevperf.log").read_text(
            errors="replace"
        )
        values = {}
        for label, percentile in percentiles.items():
            matches = re.findall(
                rf"{re.escape(percentile)}%\s*:\s*([\d.]+)us", text
            )
            values[label] = float(matches[-1]) if matches else None
        per_tenant[f"t{tenant}"] = values
    return {
        "per_tenant": per_tenant,
        "worst_tenant_p99_us": max(
            values["p99_us"] for values in per_tenant.values()
            if values["p99_us"] is not None
        ),
        "worst_tenant_p999_us": max(
            values["p999_us"] for values in per_tenant.values()
            if values["p999_us"] is not None
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--arms", default=",".join(ARMS))
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration-s", type=int, default=45)
    parser.add_argument("--fault-at-s", type=float, default=5.0)
    parser.add_argument("--settle-s", type=float, default=3.0)
    parser.add_argument("--end-margin-s", type=float, default=3.0)
    parser.add_argument("--fault-latency-us", type=int, default=2000)
    parser.add_argument("--capacity-iops", type=int, default=900000)
    parser.add_argument(
        "--path-capacities-iops",
        default=None,
        help="required comma-separated deliverable capacities K_p for paths A/B/C",
    )
    parser.add_argument(
        "--sample-rate",
        type=int,
        default=16,
        help="publish one completion observation per N commands",
    )
    parser.add_argument(
        "--reference-capacity",
        action="append",
        default=[],
        metavar="WORKLOAD=IOPS",
        help=(
            "post-hoc fault-free aggregate for fair-share; repeat per workload. "
            "--capacity-iops remains the frozen SAPS link cap"
        ),
    )
    parser.add_argument(
        "--healthy-reference",
        action="store_true",
        help="run without fault injection to measure workload capacity",
    )
    parser.add_argument("--tenants", type=int, default=4)
    return parser.parse_args()


def run_cell(args: argparse.Namespace, workload_name: str, arm_name: str,
             rep: int, campaign_meta: dict) -> dict:
    workload = WORKLOADS[workload_name]
    arm = ARMS[arm_name]
    run_dir = args.out_root / workload_name / arm_name / f"rep{rep}"
    run_dir.mkdir(parents=True, exist_ok=False)
    orch_log = run_dir / "orchestrator.log"
    stdout_path = run_dir / "driver.stdout.log"
    stderr_path = run_dir / "driver.stderr.log"

    gate = common.collision_gate()
    aggregate_before = common.get_aggregate_path_capacities()
    aggregate_configured = common.set_aggregate_path_capacities(
        args.path_capacities_iops
    )
    before = set_path_b_latency(args.tenants, 27, 0)
    cmd = [
        sys.executable,
        str(WORKLOAD_RUNNER),
        "--eval-io-size", str(workload["io_size"]),
        "--eval-workload", workload["workload"],
    ]
    if workload["rwmixread"] is not None:
        cmd += ["--eval-rwmixread", str(workload["rwmixread"])]
    cmd += [
        "--num-tenants", str(args.tenants),
        "--weights", "3,1,1,1",
        "--mode", arm["mode"],
        "--idle-tenant", "7",
        "--idle-start", str(args.duration_s - 1),
        "--idle-end", str(args.duration_s),
        "--duration", str(args.duration_s),
        "--output-dir", str(run_dir),
        "--link-cap", str(args.capacity_iops),
        "--path-caps", ",".join(str(value) for value in args.path_capacities_iops),
        "--qd", "32",
        "--no-setup-arm1",
        "--collect-latency",
        "--target-ip", "10.0.0.1",
        "--delay-restore-us", "27",
    ]
    cmd.append("--bypass-d" if arm["bypass_d"] else "--no-bypass-d")
    child_env = os.environ.copy()
    child_env.update({
        "DPA_PLUGIN_DEV": "mlx5_0",
        "SAPS_SAMPLE_RATE": str(args.sample_rate),
        **arm["env"],
    })
    manifest = {
        **campaign_meta,
        "experiment": "fig:generality",
        "date_started_utc": common.now_utc(),
        "raw_path": str(run_dir),
        "workload_name": workload_name,
        "workload": workload,
        "arm": arm_name,
        "arm_config": arm,
        "rep": rep,
        "command": cmd,
        "environment": {key: child_env[key] for key in sorted({
            "DPA_PLUGIN_DEV", "SAPS_SAMPLE_RATE", *arm["env"].keys()
        })},
        "collision_gate": gate,
        "aggregate_path_capacity_before_iops": aggregate_before,
        "aggregate_path_capacity_configured_iops": aggregate_configured,
        "target_path_b_before": before,
        "status": "running",
    }
    common.write_json(run_dir / "manifest.json", manifest)

    proc = None
    inject_started = None
    inject_finished = None
    scheduler_snapshot = None
    try:
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, env=child_env)
            perform_seen = common.wait_for_log(orch_log, "perform_tests (", 210)
            if not args.healthy_reference:
                time.sleep(max(0.0, args.fault_at_s - (time.time() - perform_seen)))
                inject_started = time.time()
                set_path_b_latency(
                    args.tenants,
                    args.fault_latency_us,
                    args.fault_latency_us,
                )
                inject_finished = time.time()
            if arm_name == "saps":
                time.sleep(args.settle_s)
                scheduler_snapshot = health_observation.dump_health_snapshot(
                    run_dir / "sapsq_post_fault.json",
                    int((time.time() - perform_seen) * 1000),
                )
            common.stop_bdevperf_after_measurement(
                orch_log, timeout_s=args.duration_s + 90
            )
            rc = proc.wait(timeout=args.duration_s + 420)
        if rc != 0:
            raise RuntimeError(f"workload harness exited rc={rc}; see {stderr_path}")
        summary = json.loads((run_dir / "e2_run_summary.json").read_text())
        if not summary.get("valid"):
            raise RuntimeError(f"harness marked run invalid: {summary.get('invalid_reason')}")
        if summary.get("health_classifier_bypassed") != arm["bypass_d"]:
            raise RuntimeError(
                "classifier state mismatch: "
                f"expected bypass={arm['bypass_d']}, "
                f"observed {summary.get('health_classifier_bypassed')}"
            )
        if summary.get("path_capacity_iops") != args.path_capacities_iops:
            raise RuntimeError(
                "path-capacity mismatch: "
                f"expected {args.path_capacities_iops}, "
                f"observed {summary.get('path_capacity_iops')}"
            )
        scheduler_state_gate = None
        if arm_name == "saps":
            ring = scheduler_snapshot["tenant_00"]
            health = ring.get("path_health_factor_q16")
            capacities = ring.get("path_capacity_iops")
            effective = ring.get("path_effective_capacity_iops")
            envelope = ring.get("link_cap_iops")
            admitted = ring.get("admitted_capacity_iops")
            expected_effective = [
                (int(capacity) * int(factor)) >> 16
                for capacity, factor in zip(capacities or [], health or [])
            ]
            expected_admitted = min(
                int(envelope), sum(expected_effective)
            ) if isinstance(envelope, int) else None
            formula_ok = (
                capacities == args.path_capacities_iops
                and len(expected_effective) == 3
                and effective == expected_effective
                and ring.get("available_path_capacity_iops")
                == sum(expected_effective)
                and admitted == expected_admitted
            )
            fault_state_ok = (
                args.healthy_reference
                or (
                    isinstance(health, list)
                    and len(health) == 3
                    and health[0] == 65536
                    and 0 <= health[1] < 65536
                    and health[2] == 65536
                )
            )
            ring_safe = (
                ring.get("ring_overrun_count") == 0
                and 0 <= ring.get("ring_max_lag", -1) < (1 << 16)
            )
            scheduler_state_gate = {
                "passed": formula_ok and fault_state_ok and ring_safe,
                "path_health_factor_q16": health,
                "path_capacity_iops": capacities,
                "path_effective_capacity_iops": effective,
                "admitted_capacity_iops": admitted,
                "formula_consistent": formula_ok,
                "fault_state_consistent": fault_state_ok,
                "ring_safe": ring_safe,
                "ring_overrun_count": ring.get("ring_overrun_count"),
                "ring_max_lag": ring.get("ring_max_lag"),
                "snapshot": str(run_dir / "sapsq_post_fault.json"),
            }
            if not scheduler_state_gate["passed"]:
                raise RuntimeError(
                    f"scheduler-state gate failed: {scheduler_state_gate}"
                )
        collector_start = int((run_dir / "collector_start_ms.txt").read_text()) / 1000.0
        measurement_start, measurement_end, onset_elapsed = (
            common.measurement_window(
                collector_start,
                perform_seen,
                inject_finished,
                args.settle_s,
                args.duration_s,
                args.end_margin_s,
            )
        )
        reference_capacity_iops = campaign_meta[
            "reference_capacity_iops_by_workload"
        ][workload_name]
        metrics = common.compute_metrics(
            run_dir,
            reference_capacity_iops,
            [3, 1, 1, 1],
            measurement_start,
            measurement_end,
        )
        metrics["tail_latency"] = parse_tail_latency(run_dir, args.tenants)
        metrics["fault_timing"] = {
            "perform_seen_unix_s": perform_seen,
            "inject_started_unix_s": inject_started,
            "inject_finished_unix_s": inject_finished,
            "fault_complete_elapsed_s": onset_elapsed,
        }
        metrics["workload"] = workload
        common.write_json(run_dir / "metrics.json", metrics)
        manifest.update({
            "status": "valid",
            "date_finished_utc": common.now_utc(),
            "returncode": rc,
            "metrics_path": str(run_dir / "metrics.json"),
            "fault_timing": metrics["fault_timing"],
            "scheduler_state_gate": scheduler_state_gate,
        })
        return {
            "run_dir": str(run_dir),
            "workload": workload_name,
            "arm": arm_name,
            "rep": rep,
            "worst_tenant_fair_share": metrics["worst_tenant_fair_share"],
            "worst_tenant_p99_us": metrics["tail_latency"]["worst_tenant_p99_us"],
            "aggregate_median_iops": sum(metrics["tenant_median_iops"].values()),
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
            "date_finished_utc": common.now_utc(),
            "error": f"{type(exc).__name__}: {exc}",
        })
        raise
    finally:
        try:
            restored = set_path_b_latency(args.tenants, 27, 0)
            manifest["target_path_b_after_restore"] = restored
            manifest["target_restored"] = all(
                state["avg_read_latency_us"] == 27
                and state["avg_write_latency_us"] == 0
                for state in restored
            )
        except BaseException as restore_exc:
            manifest["target_restored"] = False
            manifest["restore_error"] = f"{type(restore_exc).__name__}: {restore_exc}"
        try:
            restored_caps = common.set_aggregate_path_capacities(
                [aggregate_before[path] for path in common.TARGET_PATH_ORDER]
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
    workloads = [item.strip() for item in args.workloads.split(",") if item.strip()]
    arms = [item.strip() for item in args.arms.split(",") if item.strip()]
    unknown_workloads = sorted(set(workloads) - set(WORKLOADS))
    unknown_arms = sorted(set(arms) - set(ARMS))
    if unknown_workloads or unknown_arms:
        raise SystemExit(
            f"unknown workloads={unknown_workloads} arms={unknown_arms}"
        )
    if args.path_capacities_iops is None:
        raise SystemExit(
            "--path-capacities-iops is required; do not infer K_p from "
            "--capacity-iops"
        )
    args.path_capacities_iops = [
        int(item.strip())
        for item in args.path_capacities_iops.split(",")
        if item.strip()
    ]
    if len(args.path_capacities_iops) != 3 or any(
        value <= 0 for value in args.path_capacities_iops
    ):
        raise SystemExit("--path-capacities-iops must contain three positive values")
    if args.sample_rate < 1:
        raise SystemExit("--sample-rate must be positive")
    if args.healthy_reference:
        if args.duration_s <= args.settle_s + args.end_margin_s + 5:
            raise SystemExit("duration leaves fewer than five seconds of steady state")
    elif args.duration_s <= args.fault_at_s + args.settle_s + args.end_margin_s + 5:
        raise SystemExit("duration leaves fewer than five seconds of steady state")

    references = {workload: args.capacity_iops for workload in workloads}
    for item in args.reference_capacity:
        try:
            workload, raw_iops = item.split("=", 1)
            value = int(raw_iops)
        except ValueError as exc:
            raise SystemExit(
                f"invalid --reference-capacity {item!r}; expected WORKLOAD=IOPS"
            ) from exc
        if workload not in WORKLOADS or value <= 0:
            raise SystemExit(f"invalid --reference-capacity {item!r}")
        references[workload] = value

    args.out_root.mkdir(parents=True, exist_ok=False)
    campaign_meta = {
        "campaign": "testbed_campaign_20260718/06_generality",
        "git": common.git_metadata(),
        "provenance": summit_v2.provenance_snapshot(),
        "campaign_command": sys.argv,
        "target_regime": common.target_regime_gate(args.tenants),
        "frozen_saps_link_cap_iops": args.capacity_iops,
        "reference_capacity_iops_by_workload": references,
        "reference_weights": [3, 1, 1, 1],
        "frozen_saps_config": True,
        "healthy_reference_only": args.healthy_reference,
        "fault_path": "B",
        "fault_fields": [] if args.healthy_reference else ["avg_read", "avg_write"],
        "fault_latency_us": None if args.healthy_reference else args.fault_latency_us,
        "collector_counter_semantics": "read_ops + write_ops",
        "path_deliverable_capacity_iops": args.path_capacities_iops,
        "completion_sample_rate": args.sample_rate,
    }
    common.write_json(args.out_root / "campaign_manifest.json", {
        **campaign_meta,
        "date_started_utc": common.now_utc(),
        "workloads": workloads,
        "arms": arms,
        "reps": args.reps,
        "duration_s": args.duration_s,
    })
    results = []
    try:
        for workload in workloads:
            for arm in arms:
                for rep in range(args.reps):
                    print(
                        f"[generality] start workload={workload} arm={arm} rep={rep}",
                        flush=True,
                    )
                    result = run_cell(args, workload, arm, rep, campaign_meta)
                    results.append(result)
                    common.write_json(args.out_root / "campaign_results.json", results)
                    print(
                        f"[generality] valid workload={workload} arm={arm} rep={rep} "
                        f"worst={result['worst_tenant_fair_share']:.3f} "
                        f"p99={result['worst_tenant_p99_us']:.1f}us",
                        flush=True,
                    )
    finally:
        set_path_b_latency(args.tenants, 27, 0)
    print(f"[generality] complete raw={args.out_root}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
