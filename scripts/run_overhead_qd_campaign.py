#!/usr/bin/env python3
"""Measure current-code healthy throughput overhead across queue depths."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import statistics
import subprocess
import sys
from pathlib import Path

import run_summit_campaign as common


REPO = Path("/home/aiden/DPA/nvme-of-controller")
HARNESS = REPO / "scripts/run_saps_q_e2_idle.py"
SPDK = Path("/home/aiden/spdk")
PROVENANCE_PATHS = {
    "dpa_device_source": (
        REPO
        / "dpa-smart-initiator/flexio_build/samples/dpa_plugin/dev/"
        "dpa_plugin_dev.c"
    ),
    "dpa_host_source": (
        REPO
        / "dpa-smart-initiator/flexio_build/samples/dpa_plugin/host/"
        "dpa_plugin.c"
    ),
    "dpa_shared_header": (
        REPO
        / "dpa-smart-initiator/flexio_build/samples/dpa_plugin/"
        "dpa_plugin_com.h"
    ),
    "host_path_selector": SPDK / "module/bdev/nvme/bdev_nvme.c",
    "experiment_harness": HARNESS,
    "campaign_driver": Path(__file__).resolve(),
    "bdevperf_binary": SPDK / "build/examples/bdevperf",
}
ARMS = {
    "baseline": {"mode": "stock_qd", "env": {}},
    "saps": {
        "mode": "saps_q",
        "env": {
            "SAPS_M2_ENABLED": "1",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "0",
        },
    },
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def provenance_snapshot() -> dict:
    missing = [str(path) for path in PROVENANCE_PATHS.values() if not path.is_file()]
    if missing:
        raise RuntimeError(f"missing provenance inputs: {missing}")
    return {
        name: {
            "path": str(path),
            "sha256": sha256_file(path),
            "mtime_ns": path.stat().st_mtime_ns,
            "size_bytes": path.stat().st_size,
        }
        for name, path in PROVENANCE_PATHS.items()
    }


def parse_ints(raw: str) -> list[int]:
    try:
        values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    except ValueError as exc:
        raise SystemExit(f"invalid queue-depth list: {raw!r}") from exc
    if not values or any(value <= 0 for value in values):
        raise SystemExit("queue depths must be positive")
    return values


def healthy_target_gate(tenants: int, baseline_delay_us: int) -> dict:
    states = {
        path: common.get_path_states(path, tenants) for path in common.TARGET_SOCKS
    }
    wrong = [
        state
        for path_states in states.values()
        for state in path_states
        if state["avg_read_latency_us"] != baseline_delay_us
        or state["p99_read_latency_us"] != baseline_delay_us
    ]
    if wrong:
        raise RuntimeError(
            "healthy target gate found a read-delay mismatch "
            f"against {baseline_delay_us} us: {wrong}"
        )
    return states


def run_cell(args: argparse.Namespace, qd: int, arm_name: str, rep: int,
             campaign_meta: dict) -> dict:
    arm = ARMS[arm_name]
    run_dir = args.out_root / f"QD{qd}" / arm_name / f"rep{rep}"
    run_dir.mkdir(parents=True, exist_ok=False)
    stdout_path = run_dir / "driver.stdout.log"
    stderr_path = run_dir / "driver.stderr.log"
    orch_log = run_dir / "orchestrator.log"

    gate = common.collision_gate()
    healthy = healthy_target_gate(args.tenants, args.baseline_delay_us)
    command = [
        sys.executable, str(HARNESS),
        "--num-tenants", str(args.tenants),
        "--weights", "3,1,1,1",
        "--mode", arm["mode"],
        "--idle-tenant", "7",
        "--idle-start", str(args.duration_s - 1),
        "--idle-end", str(args.duration_s),
        "--duration", str(args.duration_s),
        "--output-dir", str(run_dir),
        "--link-cap", str(args.capacity_iops),
        "--path-caps", ",".join(
            str(args.path_cap_iops) for _ in common.TARGET_SOCKS
        ),
        "--qd", str(qd),
        "--no-setup-arm1",
        "--collect-latency",
        "--target-ip", "10.0.0.1",
        "--delay-restore-us", str(args.baseline_delay_us),
    ]
    child_env = os.environ.copy()
    child_env.update({
        "DPA_PLUGIN_DEV": "mlx5_0",
        "SAPS_SAMPLE_RATE": str(args.sample_rate),
        **arm["env"],
    })
    manifest = {
        **campaign_meta,
        "experiment": "fig:overhead",
        "date_started_utc": common.now_utc(),
        "raw_path": str(run_dir),
        "queue_depth": qd,
        "arm": arm_name,
        "arm_config": arm,
        "rep": rep,
        "command": command,
        "environment": {
            key: child_env[key]
            for key in sorted({"DPA_PLUGIN_DEV", "SAPS_SAMPLE_RATE", *arm["env"]})
        },
        "collision_gate": gate,
        "healthy_target_gate": healthy,
        "status": "running",
    }
    common.write_json(run_dir / "manifest.json", manifest)

    proc = None
    try:
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            proc = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=child_env)
            common.wait_for_log(orch_log, "perform_tests (", 210)
            common.stop_bdevperf_after_measurement(
                orch_log, timeout_s=args.duration_s + 90
            )
            rc = proc.wait(timeout=args.duration_s + 420)
        if rc != 0:
            raise RuntimeError(f"harness exited rc={rc}; see {stderr_path}")
        summary = json.loads((run_dir / "e2_run_summary.json").read_text())
        if not summary.get("valid"):
            raise RuntimeError(
                f"harness marked run invalid: {summary.get('invalid_reason')}"
            )
        metrics = common.compute_metrics(
            run_dir,
            args.capacity_iops,
            [3, 1, 1, 1],
            args.settle_s,
            args.duration_s - args.end_margin_s,
        )
        aggregate = sum(metrics["tenant_median_iops"].values())
        metrics.update({"queue_depth": qd, "aggregate_median_iops": aggregate})
        common.write_json(run_dir / "metrics.json", metrics)
        manifest.update({
            "status": "valid",
            "date_finished_utc": common.now_utc(),
            "returncode": rc,
            "metrics_path": str(run_dir / "metrics.json"),
        })
        return {
            "queue_depth": qd,
            "arm": arm_name,
            "rep": rep,
            "aggregate_median_iops": aggregate,
            "worst_tenant_fair_share": metrics["worst_tenant_fair_share"],
            "path_total_median_iops": metrics["path_total_median_iops"],
            "raw_dir": str(run_dir),
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
        common.write_json(run_dir / "manifest.json", manifest)


def overhead_summary(rows: list[dict], qds: list[int]) -> list[dict]:
    result = []
    for qd in qds:
        grouped = {
            arm: [
                row["aggregate_median_iops"]
                for row in rows
                if row["queue_depth"] == qd and row["arm"] == arm
            ]
            for arm in ARMS
        }
        missing = [arm for arm, values in grouped.items() if not values]
        if missing:
            raise RuntimeError(
                f"QD={qd} is missing overhead arms: {', '.join(missing)}"
            )
        medians = {
            arm: statistics.median(values)
            for arm, values in grouped.items()
        }
        result.append({
            "queue_depth": qd,
            "baseline_median_iops": medians["baseline"],
            "saps_median_iops": medians["saps"],
            "throughput_overhead_pct": (
                100.0 * (medians["baseline"] - medians["saps"])
                / medians["baseline"]
            ),
        })
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument("--qds", default="1,8,32,128")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration-s", type=int, default=35)
    parser.add_argument("--settle-s", type=float, default=5.0)
    parser.add_argument("--end-margin-s", type=float, default=3.0)
    parser.add_argument("--capacity-iops", type=int, default=3_716_015)
    parser.add_argument("--path-cap-iops", type=int, default=1_500_000)
    parser.add_argument("--baseline-delay-us", type=int, default=27)
    parser.add_argument("--tenants", type=int, default=4)
    parser.add_argument("--sample-rate", type=int, default=32)
    args = parser.parse_args()
    qds = parse_ints(args.qds)
    if args.tenants != 4:
        parser.error("this campaign fixes T=4 and weights 3,1,1,1")
    if args.reps < 1:
        parser.error("--reps must be >= 1")
    if args.path_cap_iops <= 0:
        parser.error("--path-cap-iops must be positive")
    if args.baseline_delay_us < 0:
        parser.error("--baseline-delay-us must be non-negative")
    if args.sample_rate < 1:
        parser.error("--sample-rate must be >= 1")
    if args.duration_s <= args.settle_s + args.end_margin_s + 5:
        parser.error("duration leaves fewer than five steady-state seconds")

    args.out_root.mkdir(parents=True, exist_ok=False)
    campaign_meta = {
        "campaign": "testbed_campaign_20260718/11_overhead_qd",
        "git": common.git_metadata(),
        "provenance": provenance_snapshot(),
        "campaign_command": sys.argv,
        "target_regime": common.target_regime_gate(args.tenants),
        "reference_capacity_iops": args.capacity_iops,
        "path_deliverable_capacity_iops": [
            args.path_cap_iops for _ in common.TARGET_SOCKS
        ],
        "fixed_conditions": {
            "tenants": args.tenants,
            "weights": [3, 1, 1, 1],
            "workload": "4KB random read",
            "fault": "none",
            "completion_sample_rate": args.sample_rate,
            "target_delay_us": args.baseline_delay_us,
            "path_deliverable_capacity_iops": [
                args.path_cap_iops for _ in common.TARGET_SOCKS
            ],
        },
    }
    common.write_json(args.out_root / "campaign_manifest.json", {
        **campaign_meta,
        "date_started_utc": common.now_utc(),
        "queue_depths": qds,
        "arms": ARMS,
        "reps": args.reps,
        "duration_s": args.duration_s,
    })

    rows = []
    for qd in qds:
        for rep in range(args.reps):
            for arm in ARMS:
                print(f"[overhead] start qd={qd} arm={arm} rep={rep}", flush=True)
                row = run_cell(args, qd, arm, rep, campaign_meta)
                rows.append(row)
                common.write_json(args.out_root / "runs.json", rows)
                print(
                    f"[overhead] valid qd={qd} arm={arm} rep={rep} "
                    f"aggregate={row['aggregate_median_iops']:.0f}",
                    flush=True,
                )
    summary = overhead_summary(rows, qds)
    common.write_json(args.out_root / "overhead_summary.json", summary)
    print(f"[overhead] complete raw={args.out_root}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
