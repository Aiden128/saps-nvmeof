#!/usr/bin/env python3
"""Compare HCAA with nominal weighted max-min as path and tenant scale grows."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import sys
from pathlib import Path


REPO = Path("/home/aiden/DPA/nvme-of-controller")
SPDK = Path("/home/aiden/spdk")
HARNESS = REPO / "scripts/run_saps_q_e1_coordinator.py"
TARGET_SETUP = (
    REPO
    / "experiments/3path_targets/setup_arm1_scale_shared_file.sh"
)
TARGET_HOST = "arm-1"
TARGET_IP = "10.0.0.1"
TARGET_NQN = "nqn.2024-01.io.spdk:saps-shared-ns0"
TARGET_PORT_BASE = 4800
TARGET_SOCKET_TEMPLATE = "/var/tmp/spdk_saps_scale_p{path}.sock"
TARGET_TOPOLOGY = "/tmp/saps_scale_shared_file_topology.json"
ARMS = {
    "hcaa": "continuous",
    "nominal": "fixed",
}


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run(command: list[str], *, timeout: int = 60,
        check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
    )


def git_metadata(allow_dirty: bool) -> dict:
    commit = run(["git", "rev-parse", "HEAD"], timeout=10).stdout.strip()
    status = run(["git", "status", "--porcelain"], timeout=20).stdout
    dirty = bool(status.strip())
    if dirty and not allow_dirty:
        raise RuntimeError(
            "formal campaign requires a clean worktree; commit harness changes first"
        )
    return {"commit": commit, "dirty": dirty, "status": status.splitlines()}


def source_snapshot(allow_dirty: bool) -> dict:
    paths = {
        "campaign_driver": Path(__file__).resolve(),
        "coordinator_harness": HARNESS,
        "target_setup": TARGET_SETUP,
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
        "initiator_binary": SPDK / "build/examples/bdevperf",
        "target_binary": SPDK / "build/bin/nvmf_tgt",
        "snapshot_tool": REPO / "scripts/sapsq_dump",
    }
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise RuntimeError(f"missing provenance inputs: {missing}")
    return {
        "git": git_metadata(allow_dirty),
        "sha256": {
            name: {"path": str(path), "sha256": sha256_file(path)}
            for name, path in paths.items()
        },
    }


def csv_ints(raw: str, label: str, upper: int) -> list[int]:
    try:
        values = [int(value) for value in raw.split(",") if value]
    except ValueError as error:
        raise SystemExit(f"invalid {label}: {raw!r}") from error
    if not values or any(value < 1 or value > upper for value in values):
        raise SystemExit(f"{label} must contain values in 1..{upper}")
    return values


def path_capacity_iops(paths: int, envelope_iops: int,
                       post_fault_ratio: float) -> int:
    if paths < 2:
        raise ValueError("a degraded-path scale point requires at least two paths")
    return round(envelope_iops * post_fault_ratio / (paths - 1))


def plan_cells(path_counts: list[int], tenant_counts: list[int],
               fixed_tenants: int, fixed_paths: int) -> list[dict]:
    points = {
        *[(paths, fixed_tenants, "path") for paths in path_counts],
        *[(fixed_paths, tenants, "tenant") for tenants in tenant_counts],
        (max(path_counts), max(tenant_counts), "corner"),
    }
    # The P=fixed_paths,T=fixed_tenants point belongs to both axes.  Store it
    # once and let the plotting code reuse the same measurement in each panel.
    unique = {}
    for paths, tenants, axis in points:
        key = (paths, tenants)
        unique.setdefault(key, {"paths": paths, "tenants": tenants, "axes": []})
        unique[key]["axes"].append(axis)
    return [unique[key] for key in sorted(unique)]


def setup_target(paths: int, path_cap: int, root: Path) -> dict:
    command = [
        "ssh",
        TARGET_HOST,
        (
            f"NUM_PATHS={paths} PATH_CAP_IOPS={path_cap} "
            f"bash {TARGET_SETUP}"
        ),
    ]
    completed = run(command, timeout=180)
    topology_raw = run(
        ["ssh", TARGET_HOST, "cat", TARGET_TOPOLOGY],
        timeout=30,
    ).stdout
    topology = json.loads(topology_raw)
    if int(topology.get("num_paths", 0)) != paths:
        raise RuntimeError("target topology records the wrong path count")
    if int(topology.get("path_capacity_iops", 0)) != path_cap:
        raise RuntimeError("target topology records the wrong path capacity")
    write_json(root / f"target_topology_p{paths}.json", topology)
    (root / f"target_setup_p{paths}.stdout.log").write_text(completed.stdout)
    (root / f"target_setup_p{paths}.stderr.log").write_text(completed.stderr)
    return topology


def harness_command(args: argparse.Namespace, cell: dict, arm: str,
                    path_cap: int, run_dir: Path) -> list[str]:
    paths = int(cell["paths"])
    tenants = int(cell["tenants"])
    return [
        sys.executable,
        str(HARNESS),
        "--num-tenants", str(tenants),
        "--num-paths", str(paths),
        "--weights", ",".join("1" for _ in range(tenants)),
        "--link-cap", str(args.envelope_iops),
        "--path-caps", ",".join(str(path_cap) for _ in range(paths)),
        "--sample-rate", str(args.sample_rate),
        "--dump-interval-ms", str(args.dump_interval_ms),
        "--iostat-interval-ms", str(args.iostat_interval_ms),
        "--duration", str(args.duration_s),
        "--qd", str(args.qd),
        "--shared-nqn", TARGET_NQN,
        "--port-base", str(TARGET_PORT_BASE),
        "--target-ip", TARGET_IP,
        "--target-rpc-socket-template", TARGET_SOCKET_TEMPLATE,
        "--fault-path", str(paths - 1),
        "--fault-delay-us", str(args.fault_delay_us),
        "--fault-onset-s", str(args.fault_onset_s),
        "--fault-clear-s", str(args.fault_clear_s),
        "--health-coupling-mode", ARMS[arm],
        "--no-bypass-d",
        "--no-bypass-fsm",
        "--no-setup-arm1",
        "--output-dir", str(run_dir),
    ]


def validate_summary(summary: dict, cell: dict, arm: str,
                     path_cap: int) -> dict:
    live = summary.get("live_evidence") or {}
    attribution = live.get("attribution") or []
    demand_seen = live.get("demand_seen") or []
    expected_tenants = int(cell["tenants"])
    checks = {
        "path_count": int(summary.get("num_paths", 0)) == int(cell["paths"]),
        "tenant_count": (
            int(summary.get("num_tenants", 0)) == int(cell["tenants"])
        ),
        "coupling_mode": summary.get("health_coupling_mode") == ARMS[arm],
        "path_capacity": summary.get("path_capacity_iops") == (
            [path_cap] * int(cell["paths"])
        ),
        "fault_window_iostat": (
            (summary.get("fault_window_iostat") or {}).get("pass") is True
        ),
        "live_observation": (
            not live.get("errors")
            and len(attribution) == expected_tenants
            and len(demand_seen) == expected_tenants
            and all(
                item.get("pass") is True
                for item in attribution
            )
            and all(
                item.get("pass") is True
                for item in demand_seen
            )
        ),
        "fault_injection": (
            not (summary.get("target_listener_fault") or {}).get("errors")
            and (
                (summary.get("target_listener_fault") or {}).get(
                    "activation_counters", {}
                ).get("pass") is True
            )
            and (
                (summary.get("target_listener_fault") or {}).get(
                    "path_response", {}
                ).get("detected") is True
            )
        ),
        "io_completion": (
            (summary.get("metrics") or {}).get("pass_io_failed") is True
        ),
        "epoch_commit": (
            (summary.get("metrics") or {}).get("pass_epoch_commit") is True
        ),
        "ring_integrity": (
            (summary.get("metrics") or {}).get("pass_ring_integrity") is True
        ),
        "capacity_formula": (
            (summary.get("metrics") or {}).get("pass_capacity_formula") is True
        ),
        "mode_observed": (
            (summary.get("metrics") or {}).get("pass_health_coupling_mode")
            is True
        ),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"measurement failed integrity checks: {failed}")
    return checks


def result_from_summary(summary: dict, cell: dict, arm: str, rep: int,
                        run_dir: Path, path_cap: int,
                        returncode: int) -> dict:
    checks = validate_summary(summary, cell, arm, path_cap)
    observation = summary["fault_window_iostat"]
    return {
        **cell,
        "arm": arm,
        "health_coupling_mode": ARMS[arm],
        "rep": rep,
        "run_dir": str(run_dir),
        "path_capacity_iops": path_cap,
        "nominal_surviving_capacity_ratio": (
            path_cap * (int(cell["paths"]) - 1)
            / float(summary["link_cap"])
        ),
        "measured_effective_capacity_ratio": (
            sum(
                summary["sapsq_dump"]["path_effective_capacity_iops"].values()
            )
            / float(summary["link_cap"])
        ),
        "minimum_entitlement_delivered": float(
            observation["minimum_entitlement_delivered"]
        ),
        "fault_window_total_iops": float(observation["total_iops"]),
        "fault_window_per_tenant": observation["per_tenant"],
        "integrity_checks": checks,
        "harness_returncode": returncode,
    }


def run_one(args: argparse.Namespace, campaign: dict, cell: dict,
            arm: str, rep: int, path_cap: int) -> dict:
    run_dir = (
        args.out_root
        / f"paths_{cell['paths']}_tenants_{cell['tenants']}"
        / f"rep_{rep:02d}"
        / arm
    )
    run_dir.mkdir(parents=True, exist_ok=False)
    command = harness_command(args, cell, arm, path_cap, run_dir)
    manifest = {
        "campaign": campaign["campaign"],
        "source": campaign["source"],
        "cell": cell,
        "arm": arm,
        "rep": rep,
        "command": command,
        "date_started_utc": now_utc(),
        "status": "running",
    }
    write_json(run_dir / "manifest.json", manifest)
    try:
        with (run_dir / "driver.stdout.log").open("w") as stdout, (
            run_dir / "driver.stderr.log"
        ).open("w") as stderr:
            completed = subprocess.run(
                command,
                stdout=stdout,
                stderr=stderr,
                timeout=args.duration_s + 600,
                check=False,
            )
        summary = json.loads(
            (run_dir / "e1_coordinator_summary.json").read_text()
        )
        result = result_from_summary(
            summary, cell, arm, rep, run_dir, path_cap, completed.returncode
        )
        write_json(run_dir / "manifest.json", {
            **manifest,
            "date_finished_utc": now_utc(),
            "status": "valid",
            "returncode": completed.returncode,
            "result": result,
        })
        return result
    except BaseException as error:
        write_json(run_dir / "manifest.json", {
            **manifest,
            "date_finished_utc": now_utc(),
            "status": "invalid",
            "error": f"{type(error).__name__}: {error}",
        })
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument("--path-counts", default="2,4,8")
    parser.add_argument("--tenant-counts", default="1,2,4,8,16")
    parser.add_argument("--fixed-tenants", type=int, default=4)
    parser.add_argument("--fixed-paths", type=int, default=4)
    parser.add_argument("--reps", type=int, default=2)
    parser.add_argument("--envelope-iops", type=int, default=600000)
    parser.add_argument("--post-fault-ratio", type=float, default=0.9)
    parser.add_argument("--duration-s", type=int, default=45)
    parser.add_argument("--fault-onset-s", type=float, default=5.0)
    parser.add_argument("--fault-clear-s", type=float, default=40.0)
    parser.add_argument("--fault-delay-us", type=int, default=5000)
    parser.add_argument("--qd", type=int, default=32)
    parser.add_argument("--sample-rate", type=int, default=32)
    parser.add_argument("--dump-interval-ms", type=int, default=500)
    parser.add_argument("--iostat-interval-ms", type=int, default=1000)
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--allow-dirty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path_counts = csv_ints(args.path_counts, "path counts", 8)
    tenant_counts = csv_ints(args.tenant_counts, "tenant counts", 16)
    if min(path_counts) < 2:
        raise SystemExit("path scale starts at two because degradation needs an alternative")
    if args.fixed_paths not in path_counts:
        raise SystemExit("fixed path count must appear on the path axis")
    if args.fixed_tenants not in tenant_counts:
        raise SystemExit("fixed tenant count must appear on the tenant axis")
    if not 0 < args.post_fault_ratio < 1:
        raise SystemExit("post-fault ratio must be in (0, 1)")
    if args.reps not in (2, 3):
        raise SystemExit("this campaign expects two or three repetitions")
    if args.fault_clear_s >= args.duration_s:
        raise SystemExit("fault clear time must leave a recovery interval")
    if args.fault_clear_s - args.fault_onset_s < 10:
        raise SystemExit("fault interval is too short")

    cells = plan_cells(
        path_counts,
        tenant_counts,
        args.fixed_tenants,
        args.fixed_paths,
    )
    args.out_root.mkdir(parents=True, exist_ok=False)
    campaign = {
        "campaign": "hcaa_scale_at_fixed_capacity_pressure",
        "date_started_utc": now_utc(),
        "source": source_snapshot(args.allow_dirty),
        "path_counts": path_counts,
        "tenant_counts": tenant_counts,
        "fixed_paths": args.fixed_paths,
        "fixed_tenants": args.fixed_tenants,
        "max_corner": {
            "paths": max(path_counts),
            "tenants": max(tenant_counts),
        },
        "arms": ARMS,
        "reps": args.reps,
        "envelope_iops": args.envelope_iops,
        "post_fault_capacity_ratio": args.post_fault_ratio,
        "duration_s": args.duration_s,
        "fault": {
            "delay_us": args.fault_delay_us,
            "onset_s": args.fault_onset_s,
            "clear_s": args.fault_clear_s,
        },
        "status": "running",
    }
    write_json(args.out_root / "campaign_manifest.json", campaign)

    results = []
    failures = []
    for paths in sorted({int(cell["paths"]) for cell in cells}):
        path_cap = path_capacity_iops(
            paths, args.envelope_iops, args.post_fault_ratio
        )
        topology = setup_target(paths, path_cap, args.out_root)
        for cell in [item for item in cells if int(item["paths"]) == paths]:
            for rep in range(args.reps):
                arm_order = list(ARMS)
                if rep % 2:
                    arm_order.reverse()
                for arm in arm_order:
                    label = (
                        f"P={cell['paths']} T={cell['tenants']} "
                        f"rep={rep} arm={arm}"
                    )
                    print(f"[hcaa-scale] start {label}", flush=True)
                    try:
                        result = run_one(
                            args, campaign, cell, arm, rep, path_cap
                        )
                        results.append(result)
                        print(
                            f"[hcaa-scale] valid {label} "
                            f"minimum={result['minimum_entitlement_delivered']:.3f}",
                            flush=True,
                        )
                    except BaseException as error:
                        failures.append({
                            "cell": cell,
                            "arm": arm,
                            "rep": rep,
                            "error": f"{type(error).__name__}: {error}",
                        })
                        print(f"[hcaa-scale] invalid {label}: {error}", flush=True)
                        if not args.continue_on_failure:
                            break
                    finally:
                        write_json(args.out_root / "campaign_results.json", {
                            "valid": results,
                            "invalid": failures,
                        })
                if failures and not args.continue_on_failure:
                    break
            if failures and not args.continue_on_failure:
                break
        if failures and not args.continue_on_failure:
            break

    expected = len(cells) * args.reps * len(ARMS)
    complete = not failures and len(results) == expected
    write_json(args.out_root / "campaign_results.json", {
        "valid": results,
        "invalid": failures,
    })
    write_json(args.out_root / "campaign_manifest.json", {
        **campaign,
        "date_finished_utc": now_utc(),
        "status": "complete" if complete else "incomplete",
        "valid_runs": len(results),
        "invalid_runs": len(failures),
        "expected_valid_runs": expected,
    })
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
