#!/usr/bin/env python3
"""Run summit v2's five-arm severity sweep with per-IO p99 latency."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np

import run_health_campaign as health_observation
import run_summit_campaign as common


REPO = Path("/home/aiden/DPA/nvme-of-controller")
HARNESS = REPO / "scripts/run_saps_q_summit_v2.py"
BDEVPERF_TC = Path("/home/aiden/spdk/build/examples/bdevperf")
RECORD = struct.Struct("<QQIIHH")
ANCHOR = struct.Struct("<QQ")
TSC_HZ = 1_000_000_000
DPA_PLUGIN_RING_SIZE = 1 << 16
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
    "host_path_selector": (
        Path("/home/aiden/spdk") / "module/bdev/nvme/bdev_nvme.c"
    ),
    "experiment_harness": HARNESS,
    "campaign_driver": Path(__file__).resolve(),
    "bdevperf_binary": BDEVPERF_TC,
}

ARMS = {
    "baseline": {
        "label": "baseline",
        "mode": "stock_qd",
        "bypass_d": False,
        "env": {},
        "description": "stock active-active round-robin without admission",
    },
    "blind_rr": {
        "label": "blind-RR",
        "mode": "saps_q",
        "bypass_d": True,
        "env": {
            "SAPS_M2_ENABLED": "0",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "1",
            "SAPSQ_FORCE_RR": "1",
        },
        "description": "weighted admission with health-blind round-robin",
    },
    "blind_qd": {
        "label": "blind-QD",
        "mode": "saps_q",
        "bypass_d": True,
        "env": {
            "SAPS_M2_ENABLED": "0",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "1",
            "SAPSQ_FORCE_RR": "1",
            "SAPSQ_FORCE_QD": "1",
        },
        "description": "weighted admission with active-active queue-depth selection",
    },
    "decoupled": {
        "label": "decoupled",
        "mode": "saps_q",
        "bypass_d": False,
        "env": {
            "SAPS_M2_ENABLED": "1",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "0",
            "SAPSQ_BYPASS_HEALTH_COUPLING": "1",
            "SAPSQ_BYPASS_BUDGET_SELECTION": "1",
        },
        "description": (
            "completion-informed steering with health-blind weighted allocation"
        ),
    },
    "saps": {
        "label": "saps",
        "mode": "saps_q",
        "bypass_d": False,
        "env": {
            "SAPS_M2_ENABLED": "1",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "0",
        },
        "description": "health-coupled weighted allocation and plugin steering",
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


def parse_csv_ints(raw: str, label: str) -> list[int]:
    try:
        values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    except ValueError as exc:
        raise SystemExit(f"invalid {label}: {raw!r}") from exc
    if not values or any(value < 0 for value in values):
        raise SystemExit(f"invalid {label}: {raw!r}")
    return values


def selector_budget_consistency(
    budget_by_path: dict[str, float],
    delivered_by_path: dict[str, float],
    probe_only_by_path: dict[str, bool] | None = None,
) -> tuple[bool, dict[str, bool]]:
    """Check that committed bulk budgets and probe liveness reach the data path."""
    paths = ("A", "B", "C")
    if probe_only_by_path is None:
        probe_only_by_path = {path: False for path in paths}
    if any(path not in budget_by_path or path not in delivered_by_path
           or path not in probe_only_by_path for path in paths):
        return False, {path: False for path in paths}
    if any(budget_by_path[path] < 0 or delivered_by_path[path] < 0
           for path in paths):
        return False, {path: False for path in paths}
    budget_sum = sum(budget_by_path[path] for path in paths)
    delivered_sum = sum(delivered_by_path[path] for path in paths)
    if budget_sum <= 0 or delivered_sum <= 0:
        return False, {path: False for path in paths}

    path_budget_used = {
        path: (
            budget_by_path[path] == 0
            or (
                probe_only_by_path[path]
                and delivered_by_path[path] > 0
            )
            or delivered_by_path[path] / delivered_sum
            >= min(0.25 * budget_by_path[path] / budget_sum, 0.02)
        )
        for path in paths
    }
    return all(path_budget_used.values()), path_budget_used


def select_pre_fault_path_rows(
    path_rows: list[dict],
    collector_start_unix_s: float,
    perform_seen_unix_s: float,
    inject_started_unix_s: float | None,
    fallback_window_s: float,
) -> list[dict]:
    """Select samples after I/O starts and before fault injection begins."""
    window_start = max(
        0.0,
        perform_seen_unix_s - collector_start_unix_s,
    )
    window_end = (
        inject_started_unix_s - collector_start_unix_s
        if inject_started_unix_s is not None
        else window_start + fallback_window_s
    )
    return [
        row for row in path_rows
        if window_start <= row["elapsed_s"] < window_end
    ]


def majority_path_flags(rows: list[list[bool]]) -> list[bool]:
    """Return the strict-majority state for each of the three paths."""
    if not rows or any(
        len(row) != 3 or any(not isinstance(value, bool) for value in row)
        for row in rows
    ):
        raise ValueError("path-flag rows must be nonempty three-boolean rows")
    return [
        sum(row[path] for row in rows) > len(rows) / 2
        for path in range(3)
    ]


def recovered_healthy_state(
    samples: list[dict],
    health_key: str,
    required_consecutive: int = 3,
) -> bool:
    """Require the final recovery samples to report all three paths healthy."""
    if len(samples) < required_consecutive:
        return False
    tail = samples[-required_consecutive:]
    return all(
        sample.get(health_key) == [65536, 65536, 65536]
        for sample in tail
    )


def scheduler_execution_state_ok(observation: dict) -> bool:
    """Validate initialization without selecting on the measured response."""
    return bool(observation["pre_fault_healthy_all_samples"])


def build_scheduler_state_gate(
    arm_name: str,
    sample_rate_gate: dict,
    scheduler_observation: dict,
    periodic_dump_errors: list[str],
) -> dict:
    """Validate runtime integrity without imposing SAPS policy on ablations."""
    execution_checks = {
        "sample_rate": sample_rate_gate["passed"],
        "ring_safe": scheduler_observation["ring_safe_all_samples"],
        "producer_monotonic": scheduler_observation["producer_monotonic"],
        "consumer_monotonic": scheduler_observation["consumer_monotonic"],
        "consumer_matches_dpa": scheduler_observation[
            "consumer_matches_dpa_consumed_all_samples"
        ],
        "snapshot_source_stable": scheduler_observation[
            "snapshot_source_stable"
        ],
        "initialized_healthy": scheduler_execution_state_ok(
            scheduler_observation
        ),
        "periodic_dump_clean": not periodic_dump_errors,
    }
    require_hcaa_policy = arm_name == "saps"
    policy_checks = {
        "allocation_formula": scheduler_observation[
            "allocation_formula_consistent_all_samples"
        ],
        "committed_budget": scheduler_observation[
            "committed_budget_consistent_all_samples"
        ],
    }
    return {
        "passed": (
            all(execution_checks.values())
            and (all(policy_checks.values()) if require_hcaa_policy else True)
        ),
        "requires_hcaa_policy": require_hcaa_policy,
        "execution_checks": execution_checks,
        "policy_checks": policy_checks,
        "sample_rate_gate": sample_rate_gate,
        "scheduler_observation": scheduler_observation,
        "periodic_dump_errors": periodic_dump_errors,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument(
        "--severities-us", default="0,500,2000,5000,20000,100000"
    )
    parser.add_argument("--arms", default=",".join(ARMS))
    parser.add_argument(
        "--cells",
        default=None,
        help="ordered arm:severity list for a gate, e.g. blind_rr:0,saps:2000",
    )
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration-s", type=int, default=35)
    parser.add_argument("--fault-at-s", type=float, default=5.0)
    parser.add_argument(
        "--fault-clear-at-s",
        type=float,
        default=None,
        help="restore the degraded path this many seconds after I/O starts",
    )
    parser.add_argument("--settle-s", type=float, default=3.0)
    parser.add_argument("--end-margin-s", type=float, default=3.0)
    parser.add_argument("--baseline-latency-us", type=int, default=27)
    parser.add_argument("--capacity-iops", type=int, default=900000)
    parser.add_argument(
        "--path-capacities-iops",
        default=None,
        help=(
            "required comma-separated deliverable capacities K_p for paths A/B/C"
        ),
    )
    parser.add_argument(
        "--sample-rate",
        type=int,
        default=16,
        help="publish one completion observation per N commands",
    )
    parser.add_argument(
        "--dump-interval-ms",
        type=int,
        default=1000,
        help="scheduler-state snapshot interval for SAPS cells",
    )
    parser.add_argument("--tenants", type=int, default=4)
    parser.add_argument(
        "--weights",
        default=None,
        help="comma-separated tenant weights; defaults to equal weights",
    )
    return parser.parse_args()


def parse_cells(args: argparse.Namespace) -> list[tuple[str, int, int]]:
    if args.cells:
        requested = []
        for item in args.cells.split(","):
            try:
                arm, severity_raw = item.strip().split(":", maxsplit=1)
                severity = int(severity_raw)
            except ValueError as exc:
                raise SystemExit(f"invalid --cells entry: {item!r}") from exc
            if arm not in ARMS or severity < 0:
                raise SystemExit(f"invalid --cells entry: {item!r}")
            requested.extend((arm, severity, rep) for rep in range(args.reps))
        return requested
    arms = [item.strip() for item in args.arms.split(",") if item.strip()]
    unknown = sorted(set(arms) - set(ARMS))
    if unknown:
        raise SystemExit(f"unknown arms: {unknown}")
    severities = parse_csv_ints(args.severities_us, "severities")
    return [
        (arm, severity, rep)
        for severity in severities
        for arm in arms
        for rep in range(args.reps)
    ]


def summit_collision_gate() -> dict:
    gate = common.collision_gate()
    custom = subprocess.run(
        ["pgrep", "-af", f"^{BDEVPERF_TC}"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    ).stdout.strip()
    if custom:
        raise RuntimeError(f"single-driver gate failed; summit bdevperf_tc:\n{custom}")
    gate["local_bdevperf_tc"] = []
    return gate


def stop_bdevperf_tc_after_measurement(
    orch_log: Path,
    timeout_s: int,
    dumper: health_observation.PeriodicDumper | None = None,
) -> None:
    try:
        common.wait_for_log(orch_log, "perform_tests phase wall-time=", timeout_s)
        common.wait_for_log(
            orch_log, "timeseries (iops+path+latency) written", 30
        )
    except TimeoutError:
        return
    if dumper is not None:
        dumper.stop()
    result = subprocess.run(
        ["pgrep", "-f", f"^{BDEVPERF_TC}"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    pids = [token for token in result.stdout.split() if token.isdigit()]
    if pids:
        subprocess.run(
            ["sudo", "kill", "-9", *pids],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )


def _load_anchor(path: Path) -> tuple[int, int]:
    data = path.read_bytes()
    if len(data) != ANCHOR.size:
        raise RuntimeError(f"invalid anchor size {len(data)} at {path}")
    return ANCHOR.unpack(data)


def validate_sample_rate_logs(
    run_dir: Path, tenants: int, expected_rate: int
) -> dict:
    observed = {}
    for tenant in range(tenants):
        log_path = run_dir / f"tenant_{tenant:02d}" / "bdevperf.log"
        text = log_path.read_text(errors="replace")
        if tenant == 0:
            matches = re.findall(r"init start .*?sample_rate=(\d+)", text)
        else:
            matches = re.findall(
                r"tenant init ok .*?sample_rate=(\d+)", text
            )
        observed[f"t{tenant}"] = int(matches[-1]) if matches else None
    passed = all(rate == expected_rate for rate in observed.values())
    return {
        "passed": passed,
        "expected_sample_rate": expected_rate,
        "observed_sample_rate": observed,
        "criterion": (
            "the coordinator and every tenant report the configured completion "
            "sampling rate from the running plugin binary"
        ),
    }


def summarize_scheduler_snapshots(
    periodic_dir: Path,
    perform_seen: float,
    inject_started: float | None,
    inject_finished: float | None,
    clear_started: float | None,
    clear_finished: float | None,
    settle_s: float,
    expected_capacities: list[int],
) -> dict:
    samples = []
    for path in sorted(periodic_dir.glob("snap_*.json")):
        payload = json.loads(path.read_text())
        ring = payload.get("tenant_00")
        if not isinstance(ring, dict):
            continue
        captured = payload.get("_meta", {}).get("captured_unix_s")
        producer = ring.get("producer_idx")
        consumer = ring.get("consumer_idx")
        dpa_consumed = ring.get("dpa_consumed")
        overrun_count = ring.get("ring_overrun_count")
        max_lag = ring.get("ring_max_lag")
        meta = payload.get("_meta", {})
        source_pid = meta.get("source_pid")
        source_fd = meta.get("source_fd")
        live_health = ring.get("path_health_factor_q16")
        health = ring.get("committed_path_health_factor_q16")
        capacities = ring.get("path_capacity_iops")
        effective = ring.get("committed_path_effective_capacity_iops")
        envelope = ring.get("link_cap_iops")
        admitted = ring.get("admitted_capacity_iops")
        demand = ring.get("demand_iops")
        budgets = ring.get("tenant_path_rate_budget_iops")
        probe_iops = ring.get("probe_rate_budget_iops")
        if not isinstance(captured, (int, float)):
            continue
        ring_lag = (
            producer - consumer
            if isinstance(producer, int)
            and isinstance(consumer, int)
            and producer >= consumer
            else None
        )
        expected_effective = (
            [
                (int(capacity) * int(factor)) >> 16
                for capacity, factor in zip(capacities, health)
            ]
            if isinstance(capacities, list)
            and isinstance(health, list)
            and len(capacities) == len(health) == 3
            else None
        )
        expected_admitted = (
            min(int(envelope), sum(expected_effective))
            if isinstance(envelope, int)
            and isinstance(expected_effective, list)
            else None
        )
        formula_ok = (
            capacities == expected_capacities
            and effective == expected_effective
            and ring.get("available_path_capacity_iops")
            == (
                sum(expected_effective)
                if isinstance(expected_effective, list)
                else None
            )
            and admitted == expected_admitted
        )
        budget_shape_ok = (
            isinstance(budgets, list)
            and len(budgets) > 0
            and all(
                isinstance(row, list)
                and len(row) == 3
                and all(isinstance(value, int) and value >= 0 for value in row)
                for row in budgets
            )
        )
        # Each cell crosses allocator and Q32-to-IOPS truncation boundaries.
        budget_tolerance_iops = (
            2 * len(budgets) * 3 if budget_shape_ok else 0
        )
        path_budget_sums = (
            [
                sum(row[path_idx] for row in budgets)
                for path_idx in range(3)
            ]
            if budget_shape_ok
            else None
        )
        path_probe_only = (
            [
                bool(positive_budgets)
                and all(value <= probe_iops for value in positive_budgets)
                for path_idx in range(3)
                for positive_budgets in [[
                    row[path_idx]
                    for row in budgets
                    if row[path_idx] > 0
                ]]
            ]
            if budget_shape_ok
            and isinstance(probe_iops, int)
            and probe_iops >= 0
            else None
        )
        budget_capacity_ok = (
            isinstance(path_budget_sums, list)
            and isinstance(expected_effective, list)
            and isinstance(probe_iops, int)
            and all(
                used
                <= capacity
                + (len(budgets) * probe_iops if capacity == 0 else 0)
                + budget_tolerance_iops
                for used, capacity in zip(
                    path_budget_sums, expected_effective
                )
            )
        )
        expected_total_budget = (
            min(admitted, sum(demand))
            if isinstance(admitted, int)
            and isinstance(demand, list)
            and all(isinstance(value, int) and value >= 0 for value in demand)
            else None
        )
        actual_total_budget = (
            sum(path_budget_sums)
            if isinstance(path_budget_sums, list)
            else None
        )
        budget_conservation_ok = (
            isinstance(expected_total_budget, int)
            and isinstance(actual_total_budget, int)
            and abs(actual_total_budget - expected_total_budget)
            <= budget_tolerance_iops
        )
        samples.append({
            "snapshot": str(path),
            "captured_unix_s": captured,
            "health": health,
            "live_health": live_health,
            "producer_idx": producer,
            "consumer_idx": consumer,
            "dpa_consumed": dpa_consumed,
            "source_pid": source_pid,
            "source_fd": source_fd,
            "ring_lag_events": ring_lag,
            "ring_overrun_count": overrun_count,
            "ring_max_lag": max_lag,
            "formula_consistent": formula_ok,
            "budget_within_path_capacity": budget_capacity_ok,
            "budget_conservation": budget_conservation_ok,
            "actual_total_budget_iops": actual_total_budget,
            "expected_total_budget_iops": expected_total_budget,
            "probe_rate_budget_iops": probe_iops,
            "path_budget_sums_iops": path_budget_sums,
            "path_probe_only": path_probe_only,
        })
    if not samples:
        raise RuntimeError("no valid scheduler snapshots were captured")
    post_start = (
        inject_finished + settle_s
        if inject_finished is not None
        else perform_seen + settle_s
    )
    post = [
        sample
        for sample in samples
        if sample["captured_unix_s"] >= post_start
        and (
            clear_started is None
            or sample["captured_unix_s"] < clear_started
        )
    ]
    if len(post) < 3:
        raise RuntimeError(
            f"insufficient post-settle scheduler snapshots: {len(post)}"
        )
    warmup_end = perform_seen + settle_s
    pre_end = inject_started if inject_started is not None else post_start
    pre = [
        sample
        for sample in samples
        if warmup_end <= sample["captured_unix_s"] < pre_end
    ]
    snapshot_lag_safe = all(
        isinstance(sample["ring_lag_events"], int)
        and 0 <= sample["ring_lag_events"] < DPA_PLUGIN_RING_SIZE
        for sample in samples
    )
    telemetry_safe = all(
        sample["ring_overrun_count"] == 0
        and isinstance(sample["ring_max_lag"], int)
        and 0 <= sample["ring_max_lag"] < DPA_PLUGIN_RING_SIZE
        for sample in samples
    )
    formula_consistent = all(
        sample["formula_consistent"] for sample in samples
    )
    budget_consistent = all(
        sample["budget_within_path_capacity"]
        and sample["budget_conservation"]
        for sample in samples
    )
    producer_monotonic = all(
        isinstance(previous["producer_idx"], int)
        and isinstance(current["producer_idx"], int)
        and current["producer_idx"] >= previous["producer_idx"]
        for previous, current in zip(samples, samples[1:])
    )
    consumer_monotonic = all(
        isinstance(previous["consumer_idx"], int)
        and isinstance(current["consumer_idx"], int)
        and current["consumer_idx"] >= previous["consumer_idx"]
        for previous, current in zip(samples, samples[1:])
    )
    consumer_consistent = all(
        isinstance(sample["consumer_idx"], int)
        and sample["consumer_idx"] == sample["dpa_consumed"]
        for sample in samples
    )
    source_stable = (
        len({
            (sample["source_pid"], sample["source_fd"])
            for sample in samples
            if isinstance(sample["source_pid"], int)
            and isinstance(sample["source_fd"], str)
        }) == 1
        and all(
            isinstance(sample["source_pid"], int)
            and isinstance(sample["source_fd"], str)
            for sample in samples
        )
    )
    post_health_rows = [
        sample["health"]
        for sample in post
        if isinstance(sample["health"], list)
        and len(sample["health"]) == 3
    ]
    if len(post_health_rows) != len(post):
        raise RuntimeError("post-settle snapshots contain malformed health state")
    median_health = [
        float(np.median([row[path] for row in post_health_rows]))
        for path in range(3)
    ]
    post_live_health_rows = [
        sample["live_health"]
        for sample in post
        if isinstance(sample["live_health"], list)
        and len(sample["live_health"]) == 3
    ]
    if len(post_live_health_rows) != len(post):
        raise RuntimeError(
            "post-settle snapshots contain malformed live health state"
        )
    post_budget_rows = [
        sample["path_budget_sums_iops"]
        for sample in post
        if isinstance(sample["path_budget_sums_iops"], list)
        and len(sample["path_budget_sums_iops"]) == 3
    ]
    if len(post_budget_rows) != len(post):
        raise RuntimeError(
            "post-settle snapshots contain malformed committed budgets"
        )
    median_live_health = [
        float(np.median([row[path] for row in post_live_health_rows]))
        for path in range(3)
    ]
    median_path_budget = [
        float(np.median([row[path] for row in post_budget_rows]))
        for path in range(3)
    ]
    post_probe_only_rows = [
        sample["path_probe_only"]
        for sample in post
        if isinstance(sample["path_probe_only"], list)
        and len(sample["path_probe_only"]) == 3
        and all(isinstance(value, bool) for value in sample["path_probe_only"])
    ]
    if len(post_probe_only_rows) != len(post):
        raise RuntimeError(
            "post-settle snapshots contain malformed probe-budget state"
        )
    post_path_probe_only = majority_path_flags(post_probe_only_rows)
    healthy_state = all(
        sample["health"] == [65536, 65536, 65536]
        for sample in post
    )
    live_healthy_state = all(
        sample["live_health"] == [65536, 65536, 65536]
        for sample in post
    )
    pre_fault_healthy = bool(pre) and all(
        sample["health"] == [65536, 65536, 65536] for sample in pre
    )
    consecutive_degraded = any(
        first["health"][1] < min(first["health"][0], first["health"][2], 65536)
        and second["health"][1]
        < min(second["health"][0], second["health"][2], 65536)
        for first, second in zip(post, post[1:])
    )
    degraded_state = (
        pre_fault_healthy
        and consecutive_degraded
        and median_health[1] < median_health[0]
        and median_health[1] < median_health[2]
        and median_health[1] < 65536
        and median_health[0] >= 58982
        and median_health[2] >= 58982
    )
    pre_live_healthy = bool(pre) and all(
        sample["live_health"] == [65536, 65536, 65536]
        for sample in pre
    )
    consecutive_live_degraded = any(
        first["live_health"][1]
        < min(first["live_health"][0], first["live_health"][2], 65536)
        and second["live_health"][1]
        < min(second["live_health"][0], second["live_health"][2], 65536)
        for first, second in zip(post, post[1:])
    )
    live_degraded_state = (
        pre_live_healthy
        and consecutive_live_degraded
        and median_live_health[1] < median_live_health[0]
        and median_live_health[1] < median_live_health[2]
        and median_live_health[1] < 65536
    )
    recovery = (
        [
            sample
            for sample in samples
            if sample["captured_unix_s"] >= clear_finished + settle_s
        ]
        if clear_finished is not None
        else []
    )
    recovered_state = (
        recovered_healthy_state(recovery, "health")
        if clear_finished is not None
        else None
    )
    live_recovered_state = (
        recovered_healthy_state(recovery, "live_health")
        if clear_finished is not None
        else None
    )
    return {
        "sample_count": len(samples),
        "post_settle_sample_count": len(post),
        "warmup_end_unix_s": warmup_end,
        "max_ring_lag_events": max(
            sample["ring_lag_events"]
            for sample in samples
            if isinstance(sample["ring_lag_events"], int)
        ),
        "instrumented_max_ring_lag_events": max(
            sample["ring_max_lag"]
            for sample in samples
            if isinstance(sample["ring_max_lag"], int)
        ),
        "final_ring_overrun_count": samples[-1]["ring_overrun_count"],
        "ring_capacity_events": DPA_PLUGIN_RING_SIZE,
        "ring_safe_all_samples": snapshot_lag_safe and telemetry_safe,
        "producer_monotonic": producer_monotonic,
        "consumer_monotonic": consumer_monotonic,
        "consumer_matches_dpa_consumed_all_samples": consumer_consistent,
        "snapshot_source_stable": source_stable,
        "allocation_formula_consistent_all_samples": formula_consistent,
        "committed_budget_consistent_all_samples": budget_consistent,
        "post_health_factor_q16_median": median_health,
        "post_live_health_factor_q16_median": median_live_health,
        "post_path_budget_iops_median": median_path_budget,
        "post_path_probe_only": post_path_probe_only,
        "pre_fault_healthy_all_samples": pre_fault_healthy,
        "healthy_state_all_post_samples": healthy_state,
        "degraded_path_b_state_observed": degraded_state,
        "live_healthy_state_all_samples": live_healthy_state,
        "live_degraded_path_b_state_observed": live_degraded_state,
        "recovery_sample_count": len(recovery),
        "recovered_healthy_state_observed": recovered_state,
        "live_recovered_healthy_state_observed": live_recovered_state,
        "samples": samples,
    }


def compute_per_io_p99(
    run_dir: Path,
    tenants: int,
    severity_us: int,
    inject_finished_unix_s: float | None,
    duration_s: int,
    settle_s: float,
    end_margin_s: float,
) -> dict:
    per_tenant = {}
    for tenant in range(tenants):
        directory = run_dir / f"tenant_{tenant:02d}" / "per_io"
        binary = directory / "per_io_hwts.bin"
        anchor_path = directory / "per_io_hwts.bin.anchor"
        file_size = binary.stat().st_size
        if file_size == 0:
            raise RuntimeError(f"empty per-IO log: {binary}")
        remainder = file_size % RECORD.size
        usable_bytes = file_size - remainder
        record_count = usable_bytes // RECORD.size
        dtype = np.dtype([
            ("submit_tsc", "<u8"),
            ("tsc_diff", "<u8"),
            ("sct", "<u4"),
            ("sc", "<u4"),
            ("inflight", "<u2"),
            ("success", "<u2"),
        ])
        records = np.memmap(
            binary, dtype=dtype, mode="r", shape=(record_count,)
        )
        first_submit = int(records["submit_tsc"].min())
        if severity_us > 0:
            if inject_finished_unix_s is None:
                raise RuntimeError("missing injection completion time")
            anchor_epoch_ns, anchor_tsc = _load_anchor(anchor_path)
            window_start = anchor_tsc + int(
                inject_finished_unix_s * TSC_HZ - anchor_epoch_ns
            ) + int(settle_s * TSC_HZ)
        else:
            window_start = first_submit + int(settle_s * TSC_HZ)
        window_end = first_submit + int((duration_s - end_margin_s) * TSC_HZ)
        selected_mask = (
            (records["submit_tsc"] >= window_start)
            & (records["submit_tsc"] <= window_end)
        )
        selected_count = int(np.count_nonzero(selected_mask))
        successful_mask = selected_mask & (records["success"] == 1)
        success_count = int(np.count_nonzero(successful_mask))
        if success_count < 1000:
            raise RuntimeError(
                f"too few successful per-IO records for tenant {tenant}: "
                f"selected={selected_count} success={success_count}"
            )
        latency_ticks = np.asarray(records["tsc_diff"][successful_mask])
        p99_index = max(0, math.ceil(0.99 * success_count) - 1)
        p99_ticks = int(np.partition(latency_ticks, p99_index)[p99_index])
        per_tenant[f"t{tenant}"] = {
            "binary_path": str(binary),
            "record_count": record_count,
            "trailing_bytes_dropped": remainder,
            "selected_count": selected_count,
            "success_count": success_count,
            "failure_count": selected_count - success_count,
            "window_start_tsc": window_start,
            "window_end_tsc": window_end,
            "p99_ticks": p99_ticks,
            "p99_read_latency_us": p99_ticks * 1_000_000.0 / TSC_HZ,
        }
        del latency_ticks, successful_mask, selected_mask, records
    worst = max(item["p99_read_latency_us"] for item in per_tenant.values())
    return {
        "record_format": "<QQIIHH",
        "record_size_bytes": RECORD.size,
        "tsc_hz_used": TSC_HZ,
        "percentile": 0.99,
        "per_tenant": per_tenant,
        "worst_tenant_p99_read_latency_us": worst,
    }


def run_cell(
    args: argparse.Namespace,
    arm_name: str,
    severity_us: int,
    rep: int,
    campaign_meta: dict,
) -> dict:
    arm = ARMS[arm_name]
    run_dir = (
        args.out_root / f"severity_{severity_us:06d}us" / arm_name / f"rep{rep}"
    )
    run_dir.mkdir(parents=True, exist_ok=False)
    orch_log = run_dir / "orchestrator.log"
    stdout_path = run_dir / "driver.stdout.log"
    stderr_path = run_dir / "driver.stderr.log"

    gate = summit_collision_gate()
    aggregate_before = common.get_aggregate_path_capacities()
    aggregate_configured = common.set_aggregate_path_capacities(
        args.path_capacities_iops
    )
    common.set_path_avg_read("B", args.tenants, args.baseline_latency_us)
    before = common.get_path_states("B", args.tenants)
    command = [
        sys.executable,
        str(HARNESS),
        "--num-tenants", str(args.tenants),
        "--weights", args.weights,
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
        "--delay-restore-us", str(args.baseline_latency_us),
    ]
    command.append("--bypass-d" if arm["bypass_d"] else "--no-bypass-d")
    child_env = os.environ.copy()
    child_env.update({
        "DPA_PLUGIN_DEV": "mlx5_0",
        "SAPS_SAMPLE_RATE": str(args.sample_rate),
        **arm["env"],
    })
    tracked_env = {
        "DPA_PLUGIN_DEV", "SAPS_SAMPLE_RATE", *arm["env"].keys()
    }
    manifest = {
        **campaign_meta,
        "experiment": "fig:summit-v2",
        "date_started_utc": common.now_utc(),
        "raw_path": str(run_dir),
        "severity_us": severity_us,
        "arm": arm_name,
        "arm_config": arm,
        "rep": rep,
        "command": command,
        "environment": {key: child_env[key] for key in sorted(tracked_env)},
        "collision_gate": gate,
        "aggregate_path_capacity_before_iops": aggregate_before,
        "aggregate_path_capacity_configured_iops": aggregate_configured,
        "target_path_b_before": before,
        "status": "running",
    }
    common.write_json(run_dir / "manifest.json", manifest)

    proc: subprocess.Popen | None = None
    dumper: health_observation.PeriodicDumper | None = None
    inject_started = None
    inject_finished = None
    clear_started = None
    clear_finished = None
    clear_errors: list[str] = []
    clear_thread: threading.Thread | None = None
    scheduler_observation = None
    try:
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            proc = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=child_env)
            perform_seen = common.wait_for_log(orch_log, "perform_tests (", 210)
            if arm["mode"] == "saps_q":
                dumper = health_observation.PeriodicDumper(
                    run_dir / "sapsq_periodic", args.dump_interval_ms
                )
                dumper.start()
            if severity_us > 0:
                time.sleep(max(0.0, args.fault_at_s - (time.time() - perform_seen)))
                inject_started = time.time()
                common.set_path_avg_read("B", args.tenants, severity_us)
                inject_finished = time.time()
                if args.fault_clear_at_s is not None:
                    def clear_fault() -> None:
                        nonlocal clear_started, clear_finished
                        try:
                            time.sleep(max(
                                0.0,
                                perform_seen
                                + args.fault_clear_at_s
                                - time.time(),
                            ))
                            clear_started = time.time()
                            common.set_path_avg_read(
                                "B",
                                args.tenants,
                                args.baseline_latency_us,
                            )
                            clear_finished = time.time()
                        except Exception as exc:
                            clear_errors.append(
                                f"{type(exc).__name__}: {exc}"
                            )

                    clear_thread = threading.Thread(
                        target=clear_fault,
                        name="summit-fault-clear",
                        daemon=True,
                    )
                    clear_thread.start()
            stop_bdevperf_tc_after_measurement(
                orch_log,
                timeout_s=args.duration_s + 90,
                dumper=dumper,
            )
            if clear_thread is not None:
                clear_thread.join(timeout=30)
                if clear_thread.is_alive():
                    clear_errors.append("fault-clear thread did not finish")
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
        manifest["fault_timing"] = {
            "perform_seen_unix_s": perform_seen,
            "inject_started_unix_s": inject_started,
            "inject_finished_unix_s": inject_finished,
            "clear_started_unix_s": clear_started,
            "clear_finished_unix_s": clear_finished,
            "clear_errors": clear_errors,
        }
        if clear_errors:
            raise RuntimeError(f"fault-clear failed: {clear_errors}")
        sample_rate_gate = None
        scheduler_state_gate = None
        if arm["mode"] == "saps_q":
            sample_rate_gate = validate_sample_rate_logs(
                run_dir, args.tenants, args.sample_rate
            )
            scheduler_observation = summarize_scheduler_snapshots(
                run_dir / "sapsq_periodic",
                perform_seen,
                inject_started,
                inject_finished,
                clear_started,
                clear_finished,
                args.settle_s,
                args.path_capacities_iops,
            )
            scheduler_state_gate = build_scheduler_state_gate(
                arm_name,
                sample_rate_gate,
                scheduler_observation,
                dumper.errors,
            )
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
        if clear_started is not None:
            measurement_end = min(
                measurement_end,
                clear_started - collector_start - 0.5,
            )
        metrics = common.compute_metrics(
            run_dir,
            args.capacity_iops,
            parse_csv_ints(args.weights, "weights"),
            measurement_start,
            measurement_end,
        )
        if arm_name == "saps":
            path_rows = common.read_timeseries(
                run_dir / "per_path_iops_timeseries.csv"
            )
            pre_fault_rows = select_pre_fault_path_rows(
                path_rows,
                collector_start,
                perform_seen,
                inject_started,
                args.fault_at_s,
            )
            if len(pre_fault_rows) < 2:
                raise RuntimeError(
                    "pre-fault path-balance window has fewer than two samples"
                )
            pre_fault_path_iops = {
                path: sum(
                    common.median_columns(
                        pre_fault_rows,
                        [
                            f"{path}_t{tenant}"
                            for tenant in range(args.tenants)
                        ],
                    ).values()
                )
                for path in "ABC"
            }
            pre_fault_min = min(pre_fault_path_iops.values())
            pre_fault_max = max(pre_fault_path_iops.values())
            pre_fault_balanced = (
                pre_fault_min > 0
                and pre_fault_max / pre_fault_min <= 1.25
            )

            median_budget = scheduler_observation[
                "post_path_budget_iops_median"
            ]
            budget_by_path = {
                path: median_budget[index]
                for index, path in enumerate("ABC")
            }
            probe_only_by_path = {
                path: scheduler_observation["post_path_probe_only"][index]
                for index, path in enumerate("ABC")
            }
            delivered_by_path = metrics["path_total_median_iops"]
            selector_budget_ok, path_budget_used = (
                selector_budget_consistency(
                    budget_by_path,
                    delivered_by_path,
                    probe_only_by_path,
                )
            )
            scheduler_state_gate.update({
                "pre_fault_path_iops": pre_fault_path_iops,
                "pre_fault_paths_balanced": pre_fault_balanced,
                "budget_by_path_iops": budget_by_path,
                "probe_only_by_path": probe_only_by_path,
                "delivered_by_path_iops": delivered_by_path,
                "nonzero_path_budgets_used": path_budget_used,
                "selector_budget_consistent": selector_budget_ok,
            })
            scheduler_state_gate["passed"] = (
                scheduler_state_gate["passed"]
                and pre_fault_balanced
                and selector_budget_ok
            )
            manifest["scheduler_state_gate"] = scheduler_state_gate
            if not scheduler_state_gate["passed"]:
                raise RuntimeError(
                    "end-to-end scheduler gate failed: "
                    f"{scheduler_state_gate}"
                )
        per_io = compute_per_io_p99(
            run_dir,
            args.tenants,
            severity_us,
            inject_finished,
            args.duration_s,
            args.settle_s,
            args.end_margin_s,
        )
        metrics.update({
            "arm": arm_name,
            "severity_us": severity_us,
            "rep": rep,
            "per_io": per_io,
            "fault_timing": {
                "perform_seen_unix_s": perform_seen,
                "inject_started_unix_s": inject_started,
                "inject_finished_unix_s": inject_finished,
                "clear_started_unix_s": clear_started,
                "clear_finished_unix_s": clear_finished,
                "fault_complete_elapsed_s": onset_elapsed,
            },
        })
        common.write_json(run_dir / "metrics.json", metrics)
        manifest.update({
            "status": "valid",
            "date_finished_utc": common.now_utc(),
            "returncode": returncode,
            "metrics_path": str(run_dir / "metrics.json"),
            "fault_timing": metrics["fault_timing"],
            "scheduler_state_gate": scheduler_state_gate,
        })
        return {
            "run_dir": str(run_dir),
            "severity_us": severity_us,
            "arm": arm_name,
            "rep": rep,
            "worst_tenant_fair_share": metrics["worst_tenant_fair_share"],
            "worst_tenant_p99_read_latency_us": per_io[
                "worst_tenant_p99_read_latency_us"
            ],
            "path_total_median_iops": metrics["path_total_median_iops"],
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
    cells = parse_cells(args)
    if args.weights is None:
        args.weights = ",".join("1" for _ in range(args.tenants))
    weights = parse_csv_ints(args.weights, "weights")
    if args.path_capacities_iops is None:
        raise SystemExit(
            "--path-capacities-iops is required; do not infer K_p from "
            "--capacity-iops"
        )
    args.path_capacities_iops = parse_csv_ints(
        args.path_capacities_iops, "path capacities"
    )
    if len(args.path_capacities_iops) != 3 or any(
        value <= 0 for value in args.path_capacities_iops
    ):
        raise SystemExit("--path-capacities-iops must contain three positive values")
    if len(weights) != args.tenants:
        raise SystemExit(
            f"--weights has {len(weights)} entries, expected --tenants={args.tenants}"
        )
    if args.reps < 1:
        raise SystemExit("--reps must be positive")
    if args.sample_rate < 1:
        raise SystemExit("--sample-rate must be positive")
    if args.dump_interval_ms < 100:
        raise SystemExit("--dump-interval-ms must be at least 100")
    if args.duration_s <= args.fault_at_s + args.settle_s + args.end_margin_s + 5:
        raise SystemExit("duration leaves fewer than five seconds of steady state")
    if args.fault_clear_at_s is not None:
        if args.fault_clear_at_s <= args.fault_at_s + args.settle_s + 5:
            raise SystemExit(
                "--fault-clear-at-s leaves fewer than five post-settle "
                "faulted seconds"
            )
        if args.fault_clear_at_s + args.settle_s + 5 >= args.duration_s:
            raise SystemExit(
                "--fault-clear-at-s leaves fewer than five post-settle "
                "recovery seconds"
            )
    if not BDEVPERF_TC.is_file():
        raise SystemExit(f"missing prepared binary: {BDEVPERF_TC}")

    args.out_root.mkdir(parents=True, exist_ok=False)
    campaign_meta = {
        "campaign": "testbed_campaign_20260718/05_summit_v2",
        "git": common.git_metadata(),
        "provenance": provenance_snapshot(),
        "campaign_command": sys.argv,
        "target_regime": common.target_regime_gate(args.tenants),
        "arms": {name: ARMS[name] for name in sorted({cell[0] for cell in cells})},
        "fixed_conditions": {
            "tenants": args.tenants,
            "weights": weights,
            "paths": ["A", "B", "C"],
            "qd": 32,
            "workload": "4KB random read",
            "fault_path": "B",
            "fault_clear_at_s": args.fault_clear_at_s,
            "baseline_path_latency_us": args.baseline_latency_us,
            "reference_capacity_iops": args.capacity_iops,
            "namespace_service_envelope_iops": args.capacity_iops,
            "path_deliverable_capacity_iops": args.path_capacities_iops,
            "completion_sample_rate": args.sample_rate,
            "scheduler_dump_interval_ms": args.dump_interval_ms,
            "per_io_record_format": "<QQIIHH",
            "per_io_tsc_hz": TSC_HZ,
        },
    }
    common.write_json(args.out_root / "campaign_manifest.json", {
        **campaign_meta,
        "date_started_utc": common.now_utc(),
        "cells": [
            {"arm": arm, "severity_us": severity, "rep": rep}
            for arm, severity, rep in cells
        ],
    })

    results = []
    try:
        for arm, severity, rep in cells:
            print(
                f"[summit-v2] start severity={severity}us arm={arm} rep={rep}",
                flush=True,
            )
            result = run_cell(args, arm, severity, rep, campaign_meta)
            results.append(result)
            common.write_json(args.out_root / "campaign_results.json", results)
            print(
                f"[summit-v2] valid severity={severity}us arm={arm} rep={rep} "
                f"worst={result['worst_tenant_fair_share']:.3f} "
                f"p99_us={result['worst_tenant_p99_read_latency_us']:.1f}",
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
    print(f"[summit-v2] complete raw={args.out_root}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
