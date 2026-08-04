#!/usr/bin/env python3
"""Run the path-health signal-response campaign.

The four arms use the same workload, namespace envelope, provisioned path
capacities, and HCAA integration.  They differ in how the controller constructs
published path health: completion semantics, binary reachability, queue depth,
or request completion time.  The campaign crosses those constructions with a
protocol-status fault, a path-local latency fault, and a common-mode latency
fault.  The paper uses the published health response from this campaign;
end-to-end control-loop benefit is evaluated separately.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
HARNESS = REPO_ROOT / "scripts/run_competitor_fault_experiment.py"
ARMS = (
    ("saps_completion", "completion"),
    ("saps_reachability", "reachability"),
    ("saps_qd", "queue_depth"),
    ("saps_rtt", "request_rtt"),
)
SOURCE_CODE = {
    "completion": 0,
    "reachability": 0,
    "queue_depth": 1,
    "request_rtt": 2,
}
FAULT_CASES = (
    {
        "name": "status_error",
        "fault_type": "media_error",
        "affected_paths": ("B",),
        "label": "Status error",
        "extra_args": (),
    },
    {
        "name": "path_delay",
        "fault_type": "failslow",
        "affected_paths": ("B",),
        "label": "Path-local delay",
        "extra_args": ("--slow-lat-us", "5000"),
    },
    {
        "name": "common_delay",
        "fault_type": "common_mode",
        "affected_paths": ("A", "B", "C"),
        "label": "Common-mode delay",
        "extra_args": (
            "--slow-lat-us", "5000",
            "--common-mode-paths", "A,B,C",
        ),
    },
)
PATH_INDEX = {"A": 0, "B": 1, "C": 2}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--reps", type=int, default=2)
    parser.add_argument("--initiator-iface", default="enP4p1s0f0np0")
    parser.add_argument("--initiator-ip", default="10.0.0.2/24")
    parser.add_argument("--target-ip", default="10.0.0.1")
    return parser.parse_args()


def ensure_initiator_path(args: argparse.Namespace) -> None:
    route = subprocess.run(
        ["ip", "route", "get", args.target_ip],
        capture_output=True, text=True,
    )
    if route.returncode == 0 and f"src {args.initiator_ip.split('/')[0]}" in route.stdout:
        return
    subprocess.run(
        ["sudo", "ip", "addr", "replace", args.initiator_ip,
         "dev", args.initiator_iface],
        check=True,
    )
    subprocess.run(
        ["ping", "-c", "1", "-W", "2", args.target_ip],
        check=True, stdout=subprocess.DEVNULL,
    )


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def build_run_command(
    arm: str,
    run_dir: Path,
    fault_case: dict,
    target_ip: str,
    *,
    setup_target: bool,
) -> list[str]:
    cmd = [
        sys.executable, str(HARNESS),
        "--arm", arm,
        "--fault-type", fault_case["fault_type"],
        "--output-dir", str(run_dir),
        "--num-tenants", "1",
        "--weights", "1",
        "--qd", "32",
        "--sample-rate", "16",
        "--duration", "35",
        "--fault-at-s", "8",
        "--hold-s", "18",
        "--settle-s", "3",
        "--link-cap", "900000",
        "--reference-capacity-iops", "900000",
        "--path-capacities-iops", "500000,500000,500000",
        "--enforce-path-capacities",
        "--topology", "e2",
        "--target-ip", target_ip,
        *fault_case["extra_args"],
    ]
    if not setup_target:
        cmd.append("--no-setup-arm1")
    return cmd


def require_provenance(manifest: dict, run_dir: Path) -> dict[str, str]:
    provenance = manifest.get("provenance")
    required = (
        "bdevperf_sha256",
        "dpa_firmware_sha256",
        "harness_sha256",
    )
    if not isinstance(provenance, dict):
        raise RuntimeError(f"{run_dir}: missing provenance")
    missing = [key for key in required if not provenance.get(key)]
    if missing:
        raise RuntimeError(
            f"{run_dir}: missing provenance fields {', '.join(missing)}"
        )
    return {key: provenance[key] for key in required}


def validate_run(run_dir: Path, expected_source: str, fault_case: dict) -> dict:
    manifest = load_json(run_dir / "manifest.json")
    metrics = load_json(run_dir / "metrics.json")
    contract = manifest.get("experiment_contract") or {}
    if not manifest.get("completed"):
        raise RuntimeError(f"{run_dir}: run did not complete")
    if contract.get("health_source") != expected_source:
        raise RuntimeError(f"{run_dir}: wrong health source {contract}")
    if contract.get("allocator") != "HCAA":
        raise RuntimeError(f"{run_dir}: allocator changed")
    if contract.get("selector") != "committed_budget":
        raise RuntimeError(f"{run_dir}: selector changed")
    reachability_only = expected_source == "reachability"
    if contract.get("classifier_bypassed") is not reachability_only:
        raise RuntimeError(f"{run_dir}: classifier state changed")
    if manifest.get("reference_capacity_iops") != 900000.0:
        raise RuntimeError(f"{run_dir}: namespace envelope changed")
    if manifest.get("path_capacities_iops") != [500000, 500000, 500000]:
        raise RuntimeError(f"{run_dir}: path capacities changed")
    if manifest.get("fault_type") != fault_case["fault_type"]:
        raise RuntimeError(f"{run_dir}: fault type changed")
    if fault_case["fault_type"] == "common_mode" and (
        manifest.get("common_mode_paths") != list(fault_case["affected_paths"])
    ):
        raise RuntimeError(f"{run_dir}: common-mode scope changed")
    fair = metrics.get("fair_share") or {}
    if not fair.get("valid"):
        raise RuntimeError(f"{run_dir}: invalid fair-share measurement")
    runtime_log = (run_dir / "tenant_00" / "bdevperf.log").read_text()
    source_code = SOURCE_CODE[expected_source]
    if f"health_source={source_code}" not in runtime_log:
        raise RuntimeError(f"{run_dir}: runtime health source does not match")
    expected_bypass = 1 if reachability_only else 0
    if f"bypass_d={expected_bypass}" not in runtime_log:
        raise RuntimeError(f"{run_dir}: runtime classifier state does not match")

    sampler_path = run_dir / "controller_sampler.csv"
    rows = list(csv.DictReader(sampler_path.open()))
    if len(rows) < 10:
        raise RuntimeError(f"{run_dir}: insufficient controller samples")
    timeline = manifest.get("timeline") or {}
    window_start = float(timeline["measure_start_s"])
    window_end = float(timeline["measure_end_s"])
    window_rows = [
        row for row in rows
        if window_start <= float(row["t_s"]) <= window_end
    ]
    if len(window_rows) < 5:
        raise RuntimeError(f"{run_dir}: insufficient samples in fault window")
    affected_health_by_path = {
        path: [
            float(row[f"chf{PATH_INDEX[path]}"])
            for row in window_rows
        ]
        for path in fault_case["affected_paths"]
    }
    affected_health = [
        value
        for path_values in affected_health_by_path.values()
        for value in path_values
    ]
    path_medians = {
        path: statistics.median(path_values)
        for path, path_values in affected_health_by_path.items()
    }
    if expected_source == "completion" and fault_case["name"] == "status_error":
        if min(affected_health) > 0.10:
            raise RuntimeError(f"{run_dir}: completion health never demoted path B")
    elif (
        expected_source == "reachability"
        and fault_case["name"] == "status_error"
        and min(affected_health) < 0.99
    ):
        raise RuntimeError(
            f"{run_dir}: a reachable path was demoted by the reachability arm"
        )

    provenance = require_provenance(manifest, run_dir)
    return {
        "run_dir": str(run_dir),
        "fault": fault_case["name"],
        "fault_type": fault_case["fault_type"],
        "affected_paths": list(fault_case["affected_paths"]),
        "source": expected_source,
        "runtime_health_source_code": source_code,
        "classifier_bypassed": reachability_only,
        "attainment": fair.get("worst_tenant_fair_share"),
        "first_detection_latency_ms":
            (metrics.get("detection") or {}).get("first_detection_latency_ms"),
        "median_committed_affected_path_health": statistics.median(
            affected_health
        ),
        "min_committed_affected_path_health": min(affected_health),
        "per_path_median_committed_health": path_medians,
        "min_path_median_committed_health": min(path_medians.values()),
        "bdevperf_sha256": provenance.get("bdevperf_sha256"),
        "dpa_firmware_sha256": provenance.get("dpa_firmware_sha256"),
        "harness_sha256": provenance.get("harness_sha256"),
    }


def main() -> int:
    args = parse_args()
    if args.reps < 2:
        raise SystemExit("--reps must be at least 2")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    records = []
    first = True

    for fault_case in FAULT_CASES:
        for rep in range(args.reps):
            for arm, source in ARMS:
                ensure_initiator_path(args)
                run_dir = (
                    args.output_dir
                    / f"{fault_case['name']}_{arm}_rep{rep}"
                )
                cmd = build_run_command(
                    arm,
                    run_dir,
                    fault_case,
                    args.target_ip,
                    setup_target=first,
                )
                print(
                    f"[signal-capability] fault={fault_case['name']} "
                    f"rep={rep} source={source}",
                    flush=True,
                )
                subprocess.run(
                    cmd,
                    cwd=REPO_ROOT,
                    check=True,
                )
                records.append(validate_run(run_dir, source, fault_case))
                first = False

    binary_hashes = {
        (row["bdevperf_sha256"], row["dpa_firmware_sha256"],
         row["harness_sha256"])
        for row in records
    }
    if len(binary_hashes) != 1:
        raise RuntimeError("campaign mixed different binaries or harnesses")
    bdevperf_sha256, dpa_firmware_sha256, harness_sha256 = next(
        iter(binary_hashes)
    )

    summary = {
        "schema_version": 2,
        "experiment": "path_health_signal_response_matrix",
        "repetitions": args.reps,
        "controlled_factors": {
            "allocator": "HCAA",
            "namespace_envelope_iops": 900000,
            "path_capacities_iops": [500000, 500000, 500000],
            "workload": "4 KiB random read, queue depth 32",
            "reachability_condition": "transport paths remain connected",
        },
        "fault_regimes": [
            {
                "name": case["name"],
                "label": case["label"],
                "fault_type": case["fault_type"],
                "affected_paths": list(case["affected_paths"]),
            }
            for case in FAULT_CASES
        ],
        "varied_factors": ["health_source", "fault_regime"],
        "provenance": {
            "bdevperf_sha256": bdevperf_sha256,
            "dpa_firmware_sha256": dpa_firmware_sha256,
            "harness_sha256": harness_sha256,
        },
        "runs": records,
    }
    (args.output_dir / "campaign_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    print(f"[signal-capability] valid runs={len(records)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
