#!/usr/bin/env python3
"""Shared host-side control loop for the external zeSAPS baselines.

This module only talks to already-running SPDK applications over their UNIX
JSON-RPC sockets.  It never starts or stops a workload or an NVMe-oF target.
Path removal decisions are enacted as a non-disruptive preferred-path change;
controllers remain connected and outstanding I/O is not aborted.

The exact per-path latency source is bdev_nvme_get_path_iostat.  Its counters
are cumulative, so this module converts interval deltas to latency in usec and
IOPS.  ANA strings come from bdev_get_bdevs; bdev_nvme_get_io_paths supplies
the controller ID and live path flags.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


DEFAULT_SPDK_ROOT = Path("/home/aiden/spdk")
DEFAULT_SOCKET_TEMPLATE = "/var/tmp/bdevperf_sapsq_proc{i}.sock"


class BaselineError(RuntimeError):
    """A configuration, RPC, or safety error that must stop the controller."""


class RpcBackend(Protocol):
    def call(self, socket_path: str, method: str,
             params: Optional[Mapping[str, Any]] = None) -> Any:
        ...


class SpdkJsonRpcBackend:
    """Thin wrapper around the JSONRPCClient shipped by the selected SPDK tree."""

    def __init__(self, spdk_root: Path, timeout_s: float):
        python_dir = spdk_root / "python"
        if not python_dir.is_dir():
            raise BaselineError(f"SPDK python package not found: {python_dir}")
        sys.path.insert(0, str(python_dir))
        try:
            from spdk.rpc.client import JSONRPCClient, JSONRPCException
        except ImportError as exc:
            raise BaselineError(f"cannot import SPDK JSON-RPC client from {python_dir}: {exc}") from exc
        self._client_cls = JSONRPCClient
        self._exception_cls = JSONRPCException
        self.timeout_s = timeout_s

    def call(self, socket_path: str, method: str,
             params: Optional[Mapping[str, Any]] = None) -> Any:
        client = None
        try:
            client = self._client_cls(socket_path, timeout=self.timeout_s)
            return client.call(method, dict(params or {}))
        except self._exception_cls as exc:
            message = getattr(exc, "message", str(exc))
            raise BaselineError(f"{socket_path}: {method}: {message}") from exc
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass


@dataclass(frozen=True)
class TenantTarget:
    tenant: str
    socket_path: str
    bdev_name: str
    controller_name: str


@dataclass
class CounterState:
    ops: int
    latency_ticks: int
    timestamp: float


@dataclass
class ControlState:
    disabled_by_us: bool = False
    disabled_since: Optional[float] = None
    last_enabled_at: Optional[float] = None
    disable_count: int = 0
    enable_count: int = 0
    last_rpc_result: str = ""
    reroute_destination_path: Optional[str] = None
    reroute_destination_cntlid: Optional[int] = None


@dataclass
class PathSample:
    timestamp_unix: float
    elapsed_s: float
    tenant: str
    socket_path: str
    bdev_name: str
    controller_name: str
    path_id: str
    path_key: str
    trtype: str
    traddr: str
    trsvcid: str
    subnqn: str
    cntlid: Optional[int]
    current: Optional[bool]
    connected: Optional[bool]
    accessible: Optional[bool]
    ana_state: str
    ana_observed: bool
    interval_duration_s: Optional[float]
    interval_ops: Optional[int]
    interval_latency_us: Optional[float]
    interval_iops: Optional[float]
    tick_rate: int


@dataclass
class Decision:
    action: str = "none"
    reason: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)


class PathPolicy(Protocol):
    name: str
    requires_path_iostat: bool

    def observe(self, sample: PathSample, control: ControlState,
                now: float) -> Decision:
        ...

    def describe(self) -> Mapping[str, Any]:
        ...


@dataclass
class RawTenantSnapshot:
    target: TenantTarget
    timestamp: float
    io_paths: Mapping[str, Any]
    bdevs: Sequence[Mapping[str, Any]]
    path_iostat: Optional[Mapping[str, Any]]


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def transport_key(trid: Mapping[str, Any]) -> str:
    """Stable path identity across RPCs whose transport schemas differ slightly."""
    trtype = _norm_text(trid.get("trtype")).lower()
    traddr = _norm_text(trid.get("traddr"))
    trsvcid = _norm_text(trid.get("trsvcid"))
    return "|".join((trtype, traddr, trsvcid))


def _transport_sort_key(key: str) -> Tuple[str, str, Tuple[int, Any]]:
    trtype, traddr, trsvcid = (key.split("|", 2) + ["", "", ""])[:3]
    try:
        port_key: Tuple[int, Any] = (0, int(trsvcid))
    except ValueError:
        port_key = (1, trsvcid)
    return trtype, traddr, port_key


def _as_bdev_list(value: Any) -> List[Mapping[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    if isinstance(value, Mapping):
        nested = value.get("bdevs")
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, Mapping)]
        return [value]
    return []


def parse_io_paths(result: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Dedupe identical paths emitted once per SPDK poll group/thread."""
    merged: Dict[str, Dict[str, Any]] = {}
    for poll_group in result.get("poll_groups", []) if isinstance(result, Mapping) else []:
        if not isinstance(poll_group, Mapping):
            continue
        for item in poll_group.get("io_paths", []):
            if not isinstance(item, Mapping):
                continue
            transport = item.get("transport", {})
            if not isinstance(transport, Mapping):
                continue
            key = transport_key(transport)
            if not key.strip("|"):
                continue
            dst = merged.setdefault(key, {
                "transport": dict(transport),
                "cntlid": item.get("cntlid"),
                "current": False,
                "connected": False,
                "accessible": False,
            })
            if dst.get("cntlid") is None and item.get("cntlid") is not None:
                dst["cntlid"] = item.get("cntlid")
            for flag in ("current", "connected", "accessible"):
                if item.get(flag) is True:
                    dst[flag] = True
    return merged


def parse_ana_paths(bdevs: Sequence[Mapping[str, Any]],
                    bdev_name: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    paths: Dict[str, Dict[str, Any]] = {}
    bdev_meta: Dict[str, Any] = {}
    for bdev in bdevs:
        if _norm_text(bdev.get("name")) != bdev_name:
            continue
        driver = bdev.get("driver_specific", {})
        if not isinstance(driver, Mapping):
            driver = {}
        bdev_meta = {
            "mp_policy": driver.get("mp_policy"),
            "selector": driver.get("selector"),
            "rr_min_io": driver.get("rr_min_io"),
        }
        nvme_entries = driver.get("nvme", bdev.get("nvme", []))
        if not isinstance(nvme_entries, list):
            continue
        for item in nvme_entries:
            if not isinstance(item, Mapping):
                continue
            trid = item.get("trid", {})
            if not isinstance(trid, Mapping):
                continue
            key = transport_key(trid)
            if not key.strip("|"):
                continue
            ctrlr = item.get("ctrlr_data", {})
            ns_data = item.get("ns_data", {})
            paths[key] = {
                "trid": dict(trid),
                "cntlid": ctrlr.get("cntlid") if isinstance(ctrlr, Mapping) else None,
                "ana_state": _norm_text(ns_data.get("ana_state")).lower()
                if isinstance(ns_data, Mapping) else "",
            }
    return paths, bdev_meta


def parse_path_iostat(result: Optional[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    paths: Dict[str, Dict[str, Any]] = {}
    if not isinstance(result, Mapping):
        return paths
    for item in result.get("stats", []):
        if not isinstance(item, Mapping):
            continue
        trid = item.get("trid", {})
        stat = item.get("stat", {})
        if not isinstance(trid, Mapping) or not isinstance(stat, Mapping):
            continue
        key = transport_key(trid)
        if key.strip("|"):
            paths[key] = {"trid": dict(trid), "stat": dict(stat)}
    return paths


def parse_controller_states(result: Any, controller_name: str) -> Dict[str, Dict[str, Any]]:
    """Index bdev_nvme_get_controllers output by stable transport identity."""
    states: Dict[str, Dict[str, Any]] = {}
    groups = result if isinstance(result, list) else []
    for group in groups:
        if not isinstance(group, Mapping) or group.get("name") != controller_name:
            continue
        for ctrlr in group.get("ctrlrs", []):
            if not isinstance(ctrlr, Mapping):
                continue
            trid = ctrlr.get("trid", {})
            if not isinstance(trid, Mapping):
                continue
            key = transport_key(trid)
            if key.strip("|"):
                states[key] = {
                    "state": _norm_text(ctrlr.get("state")).lower(),
                    "cntlid": ctrlr.get("cntlid"),
                    "trid": dict(trid),
                    "alternate_trids": ctrlr.get("alternate_trids", []),
                }
    return states


def stat_counters(stat: Mapping[str, Any], io_kind: str) -> Tuple[int, int]:
    kinds = ("read", "write", "unmap", "copy") if io_kind == "all" else (io_kind,)
    ops = sum(int(stat.get(f"num_{kind}_ops", 0) or 0) for kind in kinds)
    latency = sum(int(stat.get(f"{kind}_latency_ticks", 0) or 0) for kind in kinds)
    return ops, latency


class SnapshotCollector:
    def __init__(self, backend: RpcBackend, targets: Sequence[TenantTarget],
                 io_kind: str, require_path_iostat: bool):
        self.backend = backend
        self.targets = list(targets)
        self.io_kind = io_kind
        self.require_path_iostat = require_path_iostat
        self.tick_rates: Dict[str, int] = {}
        self.counters: Dict[Tuple[str, str], CounterState] = {}
        self.known: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.path_ids: Dict[Tuple[str, str], str] = {}
        self.next_path_id: Dict[str, int] = {}

    def load_tick_rates(self) -> None:
        for target in self.targets:
            result = self.backend.call(target.socket_path, "bdev_get_iostat",
                                       {"name": target.bdev_name})
            if not isinstance(result, Mapping) or int(result.get("tick_rate", 0) or 0) <= 0:
                raise BaselineError(f"{target.tenant}: bdev_get_iostat returned no tick_rate")
            self.tick_rates[target.tenant] = int(result["tick_rate"])

    def collect_one(self, target: TenantTarget) -> RawTenantSnapshot:
        io_paths = self.backend.call(target.socket_path, "bdev_nvme_get_io_paths",
                                     {"name": target.bdev_name})
        bdevs_raw = self.backend.call(target.socket_path, "bdev_get_bdevs",
                                      {"name": target.bdev_name})
        path_iostat: Optional[Mapping[str, Any]]
        try:
            value = self.backend.call(target.socket_path, "bdev_nvme_get_path_iostat",
                                      {"name": target.bdev_name})
            path_iostat = value if isinstance(value, Mapping) else None
        except BaselineError:
            if self.require_path_iostat:
                raise
            path_iostat = None
        timestamp = time.time()
        return RawTenantSnapshot(
            target=target,
            timestamp=timestamp,
            io_paths=io_paths if isinstance(io_paths, Mapping) else {},
            bdevs=_as_bdev_list(bdevs_raw),
            path_iostat=path_iostat,
        )

    def collect_all(self, max_workers: int) -> Tuple[List[RawTenantSnapshot], List[str]]:
        snapshots: List[RawTenantSnapshot] = []
        errors: List[str] = []
        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(self.targets)))) as pool:
            futures = {pool.submit(self.collect_one, target): target for target in self.targets}
            for future in as_completed(futures):
                target = futures[future]
                try:
                    snapshots.append(future.result())
                except Exception as exc:
                    errors.append(f"{target.tenant}: {exc}")
        snapshots.sort(key=lambda item: item.target.tenant)
        return snapshots, errors

    def _path_id(self, tenant: str, key: str) -> str:
        pair = (tenant, key)
        if pair not in self.path_ids:
            index = self.next_path_id.get(tenant, 0)
            self.path_ids[pair] = f"p{index}"
            self.next_path_id[tenant] = index + 1
        return self.path_ids[pair]

    def samples(self, snapshot: RawTenantSnapshot, start_unix: float) -> Tuple[List[PathSample], Dict[str, Any]]:
        target = snapshot.target
        io_paths = parse_io_paths(snapshot.io_paths)
        ana_paths, bdev_meta = parse_ana_paths(snapshot.bdevs, target.bdev_name)
        stats = parse_path_iostat(snapshot.path_iostat)
        keys = set(io_paths) | set(ana_paths) | set(stats)
        keys |= {key for tenant, key in self.known if tenant == target.tenant}

        # Assign human-readable p0/p1/... labels deterministically on first sight.
        for key in sorted(keys, key=_transport_sort_key):
            self._path_id(target.tenant, key)

        result: List[PathSample] = []
        tick_rate = self.tick_rates[target.tenant]
        for key in sorted(keys, key=lambda item: self.path_ids[(target.tenant, item)]):
            pair = (target.tenant, key)
            previous = self.known.get(pair, {})
            io_info = io_paths.get(key, {})
            ana_info = ana_paths.get(key, {})
            stat_info = stats.get(key, {})
            trid = {}
            for candidate in (io_info.get("transport"), ana_info.get("trid"),
                              stat_info.get("trid"), previous.get("trid")):
                if isinstance(candidate, Mapping) and candidate:
                    trid = dict(candidate)
                    break

            cntlid_value = io_info.get("cntlid", ana_info.get("cntlid", previous.get("cntlid")))
            try:
                cntlid = int(cntlid_value) if cntlid_value is not None else None
            except (TypeError, ValueError):
                cntlid = None

            ana_observed = bool(ana_info.get("ana_state"))
            ana_state = _norm_text(ana_info.get("ana_state")) or _norm_text(previous.get("ana_state"))
            if not ana_state:
                ana_state = "unknown"

            interval_ops: Optional[int] = None
            interval_duration_s: Optional[float] = None
            latency_us: Optional[float] = None
            iops: Optional[float] = None
            stat = stat_info.get("stat")
            if isinstance(stat, Mapping):
                ops, latency_ticks = stat_counters(stat, self.io_kind)
                old = self.counters.get(pair)
                if old is not None:
                    dt = snapshot.timestamp - old.timestamp
                    if ops >= old.ops and latency_ticks >= old.latency_ticks and dt > 0:
                        interval_duration_s = dt
                        interval_ops = ops - old.ops
                        delta_latency = latency_ticks - old.latency_ticks
                        iops = interval_ops / dt
                        if interval_ops > 0:
                            latency_us = (delta_latency / interval_ops) * 1_000_000.0 / tick_rate
                self.counters[pair] = CounterState(ops, latency_ticks, snapshot.timestamp)

            merged_known = {
                "trid": trid,
                "cntlid": cntlid,
                "ana_state": ana_state,
            }
            self.known[pair] = merged_known
            result.append(PathSample(
                timestamp_unix=snapshot.timestamp,
                elapsed_s=snapshot.timestamp - start_unix,
                tenant=target.tenant,
                socket_path=target.socket_path,
                bdev_name=target.bdev_name,
                controller_name=target.controller_name,
                path_id=self.path_ids[pair],
                path_key=key,
                trtype=_norm_text(trid.get("trtype")),
                traddr=_norm_text(trid.get("traddr")),
                trsvcid=_norm_text(trid.get("trsvcid")),
                subnqn=_norm_text(trid.get("subnqn")),
                cntlid=cntlid,
                current=io_info.get("current") if io_info else None,
                connected=io_info.get("connected") if io_info else None,
                accessible=io_info.get("accessible") if io_info else None,
                ana_state=ana_state,
                ana_observed=ana_observed,
                interval_duration_s=interval_duration_s,
                interval_ops=interval_ops,
                interval_latency_us=latency_us,
                interval_iops=iops,
                tick_rate=tick_rate,
            ))
        return result, bdev_meta


PATH_COLUMNS = [
    "timestamp_unix", "elapsed_s", "policy", "tenant", "socket_path",
    "bdev_name", "controller_name", "path_id", "path_key", "trtype",
    "traddr", "trsvcid", "subnqn", "cntlid", "current", "connected",
    "accessible", "ana_state", "ana_observed", "interval_duration_s", "interval_ops",
    "interval_latency_us", "interval_iops", "rerouted_by_controller",
    "decision", "decision_reason", "threshold_us", "ewma_us",
    "ewma_dev_us", "cusum_us", "newma_fast_log2_us",
    "newma_slow_log2_us", "newma_distance_log2", "fault_target",
    "in_measurement_window", "rpc_result",
]

TENANT_COLUMNS = [
    "timestamp_unix", "elapsed_s", "policy", "tenant", "path_count",
    "active_path_count", "interval_ops", "interval_iops",
    "weighted_latency_us", "rerouted_path_count", "weight",
    "entitlement_iops", "fair_share_ratio", "in_measurement_window",
]


class OutputWriter:
    def __init__(self, output_dir: Path, policy_name: str,
                 policy_config: Mapping[str, Any], args: argparse.Namespace,
                 targets: Sequence[TenantTarget]):
        output_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir = output_dir
        self.policy_name = policy_name
        self.started_at_unix = time.time()
        self.targets = list(targets)
        raw_weights = getattr(args, "weights", (1.0,))
        if isinstance(raw_weights, str):
            raw_weights = tuple(float(item) for item in raw_weights.split(",") if item.strip())
        weights = tuple(float(item) for item in raw_weights)
        if len(weights) == 1:
            weights = weights * len(self.targets)
        if len(weights) != len(self.targets):
            raise BaselineError(
                f"--weights has {len(weights)} entries for {len(self.targets)} tenants"
            )
        self.weights = {
            target.tenant: weights[index] for index, target in enumerate(self.targets)
        }
        self.reference_capacity_iops = float(
            getattr(args, "reference_capacity_iops", 0.0) or 0.0
        )
        self.measure_start_s = float(getattr(args, "measure_start_s", 0.0) or 0.0)
        self.measure_end_s = float(getattr(args, "measure_end_s", 0.0) or 0.0)
        self.fault_at_s = getattr(args, "fault_at_s", None)
        self.fault_at_unix = getattr(args, "fault_at_unix", None)
        self.fault_path_ids = set(getattr(args, "fault_path_id", None) or [])
        requested_fault_tenants = set(getattr(args, "fault_tenant", None) or [])
        known_tenants = {target.tenant for target in self.targets}
        unknown_fault_tenants = requested_fault_tenants - known_tenants
        if unknown_fault_tenants:
            raise BaselineError(
                "--fault-tenant names are not configured: "
                + ", ".join(sorted(unknown_fault_tenants))
            )
        self.fault_tenants = requested_fault_tenants or known_tenants
        self.measurement_tenants: Dict[str, Dict[str, Any]] = {}
        self.measurement_paths: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.detection_events: List[Dict[str, Any]] = []
        self.pre_fault_disables: List[Dict[str, Any]] = []
        self.last_tenant_timestamp: Dict[str, float] = {}
        self.last_path_timestamp: Dict[Tuple[str, str], float] = {}
        self.path_fp = (output_dir / "per_path_samples.csv").open("w", newline="")
        self.tenant_fp = (output_dir / "per_tenant_samples.csv").open("w", newline="")
        self.event_fp = (output_dir / "events.jsonl").open("w")
        self.path_writer = csv.DictWriter(self.path_fp, fieldnames=PATH_COLUMNS)
        self.tenant_writer = csv.DictWriter(self.tenant_fp, fieldnames=TENANT_COLUMNS)
        self.path_writer.writeheader()
        self.tenant_writer.writeheader()
        self.summary: Dict[str, Dict[str, Dict[str, Any]]] = {}
        def json_safe(value: Any) -> Any:
            if isinstance(value, Path):
                return str(value)
            if isinstance(value, set):
                return sorted(json_safe(item) for item in value)
            if isinstance(value, tuple):
                return [json_safe(item) for item in value]
            if isinstance(value, list):
                return [json_safe(item) for item in value]
            if isinstance(value, dict):
                return {str(key): json_safe(item) for key, item in value.items()}
            return value

        manifest = {
            "schema_version": 2,
            "policy": policy_name,
            "policy_config": dict(policy_config),
            "started_at_unix": self.started_at_unix,
            "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.started_at_unix)),
            "git_commit": getattr(args, "git_commit", None),
            "cmdline": shlex.join([sys.executable, *sys.argv]),
            "raw_path": str(output_dir.resolve()),
            "targets": [asdict(target) for target in targets],
            "args": {key: json_safe(value) for key, value in vars(args).items()},
            "safety": {
                "starts_workload_or_target": False,
                "requires_nonzero_unique_cntlid": True,
                "restore_on_exit": args.restore_on_exit,
                "observe_only": args.observe_only,
                "reroute_primitive": "bdev_nvme_set_preferred_path",
                "disconnects_controller": False,
            },
        }
        (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    def _in_measurement_window(self, elapsed_s: float) -> bool:
        if elapsed_s < self.measure_start_s:
            return False
        return self.measure_end_s <= 0 or elapsed_s <= self.measure_end_s

    def _is_fault_target(self, sample: PathSample) -> bool:
        return (
            sample.tenant in self.fault_tenants
            and (not self.fault_path_ids or sample.path_id in self.fault_path_ids)
        )

    def _fault_relative_s(self, sample: PathSample) -> Optional[float]:
        if self.fault_at_unix is not None:
            return sample.timestamp_unix - float(self.fault_at_unix)
        if self.fault_at_s is not None:
            return sample.elapsed_s - float(self.fault_at_s)
        return None

    def _record_detection(self, sample: PathSample, decision: Decision) -> None:
        if decision.action != "disable":
            return
        relative_s = self._fault_relative_s(sample)
        record = {
            "tenant": sample.tenant,
            "path_id": sample.path_id,
            "path_key": sample.path_key,
            "timestamp_unix": sample.timestamp_unix,
            "elapsed_s": sample.elapsed_s,
            "reason": decision.reason,
        }
        if relative_s is None:
            return
        if relative_s < 0:
            record["lead_time_ms"] = -relative_s * 1000.0
            self.pre_fault_disables.append(record)
        elif self._is_fault_target(sample):
            record["detection_latency_ms"] = relative_s * 1000.0
            self.detection_events.append(record)

    def event(self, event: str, **fields: Any) -> None:
        record = {"timestamp_unix": time.time(), "event": event, **fields}
        self.event_fp.write(json.dumps(record, sort_keys=True) + "\n")
        self.event_fp.flush()

    def path_sample(self, sample: PathSample, control: ControlState,
                    decision: Decision, rpc_result: str) -> None:
        metrics = decision.metrics
        in_measurement_window = self._in_measurement_window(sample.elapsed_s)
        row = {
            **asdict(sample),
            "policy": self.policy_name,
            "rerouted_by_controller": control.disabled_by_us,
            "decision": decision.action,
            "decision_reason": decision.reason,
            "threshold_us": metrics.get("threshold_us"),
            "ewma_us": metrics.get("ewma_us"),
            "ewma_dev_us": metrics.get("ewma_dev_us"),
            "cusum_us": metrics.get("cusum_us"),
            "newma_fast_log2_us": metrics.get("newma_fast_log2_us"),
            "newma_slow_log2_us": metrics.get("newma_slow_log2_us"),
            "newma_distance_log2": metrics.get("newma_distance_log2"),
            "fault_target": self._is_fault_target(sample),
            "in_measurement_window": in_measurement_window,
            "rpc_result": rpc_result,
        }
        self.path_writer.writerow({name: row.get(name) for name in PATH_COLUMNS})
        self.path_fp.flush()

        tenant_summary = self.summary.setdefault(sample.tenant, {})
        path_summary = tenant_summary.setdefault(sample.path_id, {
            "path_key": sample.path_key,
            "trtype": sample.trtype,
            "traddr": sample.traddr,
            "trsvcid": sample.trsvcid,
            "samples": 0,
            "intervals_with_io": 0,
            "total_interval_ops": 0,
            "total_observed_s": 0.0,
            "latency_weighted_sum_us": 0.0,
            "reroute_count": 0,
            "probe_count": 0,
            "last_ana_state": sample.ana_state,
        })
        path_summary["samples"] += 1
        path_summary["last_ana_state"] = sample.ana_state
        if sample.interval_ops is not None:
            path_summary["total_interval_ops"] += sample.interval_ops
        if sample.interval_duration_s is not None:
            path_summary["total_observed_s"] += sample.interval_duration_s
        if sample.interval_ops and sample.interval_latency_us is not None:
            path_summary["intervals_with_io"] += 1
            path_summary["latency_weighted_sum_us"] += sample.interval_ops * sample.interval_latency_us
        path_summary["reroute_count"] = control.disable_count
        path_summary["probe_count"] = control.enable_count

        pair = (sample.tenant, sample.path_id)
        previous_timestamp = self.last_path_timestamp.get(pair)
        self.last_path_timestamp[pair] = sample.timestamp_unix
        effective_dt = sample.interval_duration_s
        if effective_dt is None and control.disabled_by_us and previous_timestamp is not None:
            effective_dt = max(0.0, sample.timestamp_unix - previous_timestamp)
        if in_measurement_window and effective_dt is not None and effective_dt > 0:
            accumulator = self.measurement_paths.setdefault(pair, {
                "tenant": sample.tenant,
                "path_id": sample.path_id,
                "path_key": sample.path_key,
                "traddr": sample.traddr,
                "trsvcid": sample.trsvcid,
                "samples": 0,
                "total_ops": 0,
                "observed_s": 0.0,
                "latency_ops": 0,
                "latency_weighted_sum_us": 0.0,
            })
            accumulator["samples"] += 1
            accumulator["total_ops"] += sample.interval_ops or 0
            accumulator["observed_s"] += effective_dt
            if sample.interval_ops and sample.interval_latency_us is not None:
                accumulator["latency_ops"] += sample.interval_ops
                accumulator["latency_weighted_sum_us"] += (
                    sample.interval_ops * sample.interval_latency_us
                )
        self._record_detection(sample, decision)

    def tenant_sample(self, samples: Sequence[PathSample],
                      controls: Mapping[Tuple[str, str], ControlState]) -> None:
        if not samples:
            return
        tenant = samples[0].tenant
        timestamp = max(item.timestamp_unix for item in samples)
        elapsed_s = max(item.elapsed_s for item in samples)
        previous_timestamp = self.last_tenant_timestamp.get(tenant)
        self.last_tenant_timestamp[tenant] = timestamp
        observed_s = (
            max(0.0, timestamp - previous_timestamp)
            if previous_timestamp is not None else None
        )
        interval_ops = sum(item.interval_ops or 0 for item in samples)
        interval_iops = sum(item.interval_iops or 0.0 for item in samples)
        latency_numer = sum(
            (item.interval_ops or 0) * (item.interval_latency_us or 0.0)
            for item in samples if item.interval_latency_us is not None
        )
        latency_ops = sum(
            item.interval_ops or 0 for item in samples
            if item.interval_latency_us is not None
        )
        weighted_latency = latency_numer / latency_ops if latency_ops else None
        weight = self.weights[tenant]
        weight_sum = sum(self.weights.values())
        entitlement = (
            self.reference_capacity_iops * weight / weight_sum
            if self.reference_capacity_iops > 0 else None
        )
        fair_share_ratio = interval_iops / entitlement if entitlement else None
        in_measurement_window = self._in_measurement_window(elapsed_s)
        row = {
            "timestamp_unix": timestamp,
            "elapsed_s": elapsed_s,
            "policy": self.policy_name,
            "tenant": tenant,
            "path_count": len(samples),
            "active_path_count": sum(item.current is True for item in samples),
            "interval_ops": interval_ops,
            "interval_iops": interval_iops,
            "weighted_latency_us": weighted_latency,
            "rerouted_path_count": sum(
                controls[(item.tenant, item.path_key)].disabled_by_us for item in samples
            ),
            "weight": weight,
            "entitlement_iops": entitlement,
            "fair_share_ratio": fair_share_ratio,
            "in_measurement_window": in_measurement_window,
        }
        self.tenant_writer.writerow(row)
        self.tenant_fp.flush()

        if in_measurement_window and observed_s is not None and observed_s > 0:
            accumulator = self.measurement_tenants.setdefault(tenant, {
                "samples": 0,
                "total_ops": 0,
                "observed_s": 0.0,
                "latency_ops": 0,
                "latency_weighted_sum_us": 0.0,
            })
            accumulator["samples"] += 1
            accumulator["total_ops"] += interval_ops
            accumulator["observed_s"] += observed_s
            if latency_ops:
                accumulator["latency_ops"] += latency_ops
                accumulator["latency_weighted_sum_us"] += latency_numer

    def _measurement_metrics(self, finished_at_unix: float) -> Dict[str, Any]:
        weight_sum = sum(self.weights.values())
        per_tenant: Dict[str, Dict[str, Any]] = {}
        missing_tenants: List[str] = []
        for target in self.targets:
            tenant = target.tenant
            accumulator = self.measurement_tenants.get(tenant)
            mean_iops = None
            mean_latency_us = None
            if accumulator and accumulator["observed_s"] > 0:
                mean_iops = accumulator["total_ops"] / accumulator["observed_s"]
                if accumulator["latency_ops"] > 0:
                    mean_latency_us = (
                        accumulator["latency_weighted_sum_us"]
                        / accumulator["latency_ops"]
                    )
            else:
                missing_tenants.append(tenant)
            entitlement = (
                self.reference_capacity_iops * self.weights[tenant] / weight_sum
                if self.reference_capacity_iops > 0 else None
            )
            fair_share_ratio = (
                mean_iops / entitlement
                if mean_iops is not None and entitlement else None
            )
            per_tenant[tenant] = {
                "weight": self.weights[tenant],
                "samples": accumulator["samples"] if accumulator else 0,
                "total_ops": accumulator["total_ops"] if accumulator else 0,
                "observed_s": accumulator["observed_s"] if accumulator else 0.0,
                "mean_iops": mean_iops,
                "mean_latency_us": mean_latency_us,
                "entitlement_iops": entitlement,
                "fair_share_ratio": fair_share_ratio,
            }

        fair_values = [
            (tenant, values["fair_share_ratio"])
            for tenant, values in per_tenant.items()
            if values["fair_share_ratio"] is not None
        ]
        worst = min(fair_values, key=lambda item: item[1]) if fair_values else None
        valid = self.reference_capacity_iops > 0 and not missing_tenants

        per_path: Dict[str, Dict[str, Any]] = {}
        for (tenant, path_id), accumulator in sorted(self.measurement_paths.items()):
            tenant_paths = per_path.setdefault(tenant, {})
            observed_s = accumulator["observed_s"]
            latency_ops = accumulator["latency_ops"]
            tenant_paths[path_id] = {
                "path_key": accumulator["path_key"],
                "traddr": accumulator["traddr"],
                "trsvcid": accumulator["trsvcid"],
                "samples": accumulator["samples"],
                "total_ops": accumulator["total_ops"],
                "observed_s": observed_s,
                "mean_iops": accumulator["total_ops"] / observed_s if observed_s else None,
                "mean_latency_us": (
                    accumulator["latency_weighted_sum_us"] / latency_ops
                    if latency_ops else None
                ),
            }

        effective_end_s = self.measure_end_s or (finished_at_unix - self.started_at_unix)
        reason = None
        if self.reference_capacity_iops <= 0:
            reason = "--reference-capacity-iops was not supplied"
        elif missing_tenants:
            reason = "measurement window has no usable samples for: " + ", ".join(missing_tenants)
        return {
            "valid": valid,
            "invalid_reason": reason,
            "window": {
                "start_s": self.measure_start_s,
                "end_s": effective_end_s,
                "reference_capacity_iops": self.reference_capacity_iops,
                "reference_definition": "fault-free all-tenant aggregate throughput shared by all arms in one panel",
            },
            "per_tenant": per_tenant,
            "per_path": per_path,
            "worst_tenant": worst[0] if valid and worst else None,
            "worst_tenant_fair_share": worst[1] if valid and worst else None,
            "missing_tenants": missing_tenants,
        }

    def _detection_metrics(self) -> Dict[str, Any]:
        configured = self.fault_at_s is not None or self.fault_at_unix is not None
        first_by_tenant: Dict[str, Dict[str, Any]] = {}
        for event in sorted(self.detection_events, key=lambda item: item["detection_latency_ms"]):
            first_by_tenant.setdefault(event["tenant"], event)
        per_tenant: Dict[str, Dict[str, Any]] = {}
        for tenant in sorted(self.fault_tenants):
            event = first_by_tenant.get(tenant)
            per_tenant[tenant] = {
                "detected": event is not None,
                "detection_latency_ms": event["detection_latency_ms"] if event else None,
                "path_id": event["path_id"] if event else None,
                "path_key": event["path_key"] if event else None,
                "reason": event["reason"] if event else None,
            }
        never = [tenant for tenant, values in per_tenant.items() if not values["detected"]]
        latencies = [
            values["detection_latency_ms"] for values in per_tenant.values()
            if values["detection_latency_ms"] is not None
        ]
        return {
            "configured": configured,
            "fault_at_s": self.fault_at_s,
            "fault_at_unix": self.fault_at_unix,
            "fault_tenants": sorted(self.fault_tenants),
            "fault_path_ids": sorted(self.fault_path_ids),
            "per_tenant": per_tenant,
            "all_expected_tenants_detected": configured and not never,
            "never_detected_tenants": never if configured else [],
            "first_detection_latency_ms": min(latencies) if latencies else None,
            "worst_detection_latency_ms": max(latencies) if configured and not never and latencies else None,
            "false_positive_reroute_count_before_fault": len(self.pre_fault_disables),
            "false_positive_reroutes_before_fault": self.pre_fault_disables,
            # Backward-compatible aliases for existing aggregation scripts.
            "false_positive_disable_count_before_fault": len(self.pre_fault_disables),
            "false_positive_disables_before_fault": self.pre_fault_disables,
        }

    def close(self, completed: bool, stop_reason: str) -> None:
        for tenant_paths in self.summary.values():
            for path in tenant_paths.values():
                ops = path.pop("total_interval_ops")
                observed_s = path.pop("total_observed_s")
                weighted_sum = path.pop("latency_weighted_sum_us")
                path["total_interval_ops"] = ops
                path["total_observed_s"] = observed_s
                path["mean_iops"] = ops / observed_s if observed_s else None
                path["mean_latency_us"] = weighted_sum / ops if ops else None
        finished_at_unix = time.time()
        metrics = {
            "measurement": self._measurement_metrics(finished_at_unix),
            "detection": self._detection_metrics(),
        }
        summary = {
            "schema_version": 2,
            "policy": self.policy_name,
            "completed": completed,
            "stop_reason": stop_reason,
            "started_at_unix": self.started_at_unix,
            "finished_at_unix": finished_at_unix,
            "tenants": self.summary,
            "metrics": metrics,
        }
        (self.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        (self.output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
        self.path_fp.close()
        self.tenant_fp.close()
        self.event_fp.close()


def parse_targets(args: argparse.Namespace) -> List[TenantTarget]:
    specs: List[Tuple[str, str]] = []
    if args.tenant:
        for raw in args.tenant:
            if "=" not in raw:
                raise BaselineError(f"--tenant must be ID=SOCKET, got {raw!r}")
            tenant, socket_path = raw.split("=", 1)
            if not tenant or not socket_path:
                raise BaselineError(f"--tenant must be ID=SOCKET, got {raw!r}")
            specs.append((tenant, socket_path))
    else:
        specs = [(f"t{i}", args.socket_template.format(i=i))
                 for i in range(args.num_tenants)]
    seen = set()
    targets = []
    for tenant, socket_path in specs:
        if tenant in seen:
            raise BaselineError(f"duplicate tenant ID: {tenant}")
        seen.add(tenant)
        targets.append(TenantTarget(tenant, socket_path, args.bdev_name,
                                    args.controller_name))
    if not targets:
        raise BaselineError("no tenant sockets configured")
    return targets


def wait_for_sockets(targets: Sequence[TenantTarget], timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    missing = [target.socket_path for target in targets if not os.path.exists(target.socket_path)]
    while missing and time.monotonic() < deadline:
        time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
        missing = [target.socket_path for target in targets if not os.path.exists(target.socket_path)]
    if missing:
        raise BaselineError(f"RPC sockets did not appear within {timeout_s:g}s: {', '.join(missing)}")


def validate_path_targeting(samples: Sequence[PathSample], min_paths: int,
                            observe_only: bool,
                            expected_tenants: Optional[Iterable[str]] = None) -> None:
    by_tenant: Dict[str, List[PathSample]] = {}
    for sample in samples:
        by_tenant.setdefault(sample.tenant, []).append(sample)
    expected = set(expected_tenants or by_tenant)
    missing = sorted(expected - set(by_tenant))
    if missing:
        raise BaselineError(
            "no matching bdev/path data for configured tenants: " + ", ".join(missing)
        )
    for tenant, paths in by_tenant.items():
        if len(paths) < min_paths:
            raise BaselineError(f"{tenant}: discovered {len(paths)} paths; need at least {min_paths}")
        if observe_only:
            continue
        cntlids = [path.cntlid for path in paths]
        if any(value is None or value <= 0 for value in cntlids):
            raise BaselineError(
                f"{tenant}: refusing path control because cntlid values are not all nonzero: {cntlids}; "
                "SPDK treats cntlid=0 as an all-controller operation"
            )
        if len(set(cntlids)) != len(cntlids):
            raise BaselineError(
                f"{tenant}: refusing path control because cntlid values are not unique per path: {cntlids}; "
                "the controller may be in failover rather than true multipath mode"
            )


def _policy_params(args: argparse.Namespace) -> Optional[Dict[str, Any]]:
    if args.multipath_policy == "keep":
        return None
    params: Dict[str, Any] = {
        "name": args.bdev_name,
        "policy": args.multipath_policy,
    }
    if args.multipath_policy == "active_active":
        params["selector"] = args.selector
        if args.selector == "round_robin" and args.rr_min_io is not None:
            params["rr_min_io"] = args.rr_min_io
    return params


def _restore_policy_params(bdev_name: str, meta: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    policy = meta.get("mp_policy")
    if policy not in ("active_passive", "active_active", "plugin"):
        return None
    params: Dict[str, Any] = {"name": bdev_name, "policy": policy}
    if policy == "active_active":
        selector = meta.get("selector") or "round_robin"
        params["selector"] = selector
        if selector == "round_robin" and meta.get("rr_min_io") is not None:
            params["rr_min_io"] = int(meta["rr_min_io"])
    return params


class BaselineRunner:
    def __init__(self, args: argparse.Namespace, policy: PathPolicy,
                 backend: Optional[RpcBackend] = None):
        self.args = args
        self.policy = policy
        self.targets = parse_targets(args)
        self.backend = backend or SpdkJsonRpcBackend(Path(args.spdk_root), args.rpc_timeout_s)
        self.collector = SnapshotCollector(self.backend, self.targets, args.io_kind,
                                           policy.requires_path_iostat)
        self.controls: Dict[Tuple[str, str], ControlState] = {}
        self.latest_samples: Dict[Tuple[str, str], PathSample] = {}
        self.original_policies: Dict[str, Mapping[str, Any]] = {}
        self.original_preferred: Dict[str, PathSample] = {}
        self.active_passive_tenants: set[str] = set()
        self.stop_requested = False
        self.writer: Optional[OutputWriter] = None

    def _signal(self, _signum: int, _frame: Any) -> None:
        self.stop_requested = True

    def _record_control_success(self, control: ControlState, action: str,
                                now: float, result: str) -> None:
        if action == "disable":
            control.disabled_by_us = True
            control.disabled_since = now
            control.disable_count += 1
        else:
            control.disabled_by_us = False
            control.disabled_since = None
            control.last_enabled_at = now
            control.enable_count += 1
        control.last_rpc_result = result

    def _alternate_path(self, sample: PathSample) -> Optional[PathSample]:
        candidates = []
        for pair, candidate in self.latest_samples.items():
            if candidate.tenant != sample.tenant or candidate.path_key == sample.path_key:
                continue
            state = self.controls.get(pair)
            if state is not None and state.disabled_by_us:
                continue
            if candidate.cntlid is None or candidate.cntlid <= 0:
                continue
            if candidate.connected is False or candidate.accessible is False:
                continue
            if candidate.ana_state in {"inaccessible", "persistent_loss", "change"}:
                continue
            candidates.append(candidate)
        if not candidates:
            return None
        # Destination choice is deterministic and is not part of fault
        # detection: prefer a path with recent low latency, then path label.
        return min(
            candidates,
            key=lambda item: (
                item.interval_latency_us is None,
                item.interval_latency_us if item.interval_latency_us is not None else 0.0,
                item.path_id,
            ),
        )

    def _ensure_active_passive(self, sample: PathSample) -> None:
        if sample.tenant in self.active_passive_tenants:
            return
        params = {"name": sample.bdev_name, "policy": "active_passive"}
        self.backend.call(
            sample.socket_path, "bdev_nvme_set_multipath_policy", params
        )
        self.active_passive_tenants.add(sample.tenant)
        if self.writer:
            self.writer.event(
                "multipath_policy_set_for_reroute",
                tenant=sample.tenant,
                socket_path=sample.socket_path,
                params=params,
            )

    def _reconcile_action(self, sample: PathSample, action: str,
                          original_error: Exception) -> str:
        """Resolve an ambiguous mutating RPC result through controller state."""
        desired = "disabled" if action == "disable" else "enabled"
        deadline = time.monotonic() + self.args.reconcile_timeout_s
        last_state = "unknown"
        last_error = str(original_error)
        while True:
            try:
                result = self.backend.call(
                    sample.socket_path,
                    "bdev_nvme_get_controllers",
                    {"name": sample.controller_name},
                )
                states = parse_controller_states(result, sample.controller_name)
                info = states.get(sample.path_key)
                if info is None:
                    info = next(
                        (item for item in states.values()
                         if item.get("cntlid") == sample.cntlid),
                        None,
                    )
                if info is not None:
                    last_state = _norm_text(info.get("state")).lower() or "unknown"
                    if last_state == desired:
                        message = (
                            f"reconciled-after-error:{type(original_error).__name__}:"
                            f"controller-state={last_state}"
                        )
                        self._record_control_success(
                            self.controls[(sample.tenant, sample.path_key)],
                            action, time.time(), message,
                        )
                        return message
            except Exception as exc:
                last_error = str(exc)
            if time.monotonic() >= deadline:
                break
            time.sleep(0.2)
        raise BaselineError(
            f"{sample.tenant}/{sample.path_id}: {action} RPC outcome is unresolved; "
            f"last controller state={last_state}; original={original_error}; "
            f"reconcile={last_error}"
        )

    def _apply(self, sample: PathSample, action: str) -> str:
        control = self.controls[(sample.tenant, sample.path_key)]
        if self.args.observe_only:
            return f"observe-only:{action}"
        if sample.cntlid is None or sample.cntlid <= 0:
            raise BaselineError(f"{sample.tenant}/{sample.path_id}: unsafe cntlid {sample.cntlid}")
        if action == "disable":
            destination = self._alternate_path(sample)
            if destination is None:
                result_text = json.dumps({
                    "primitive": "bdev_nvme_set_preferred_path",
                    "result": "no_eligible_alternate_kept_last_path",
                }, sort_keys=True)
                self._record_control_success(control, action, time.time(), result_text)
                return result_text
            result = self.backend.call(
                sample.socket_path,
                "bdev_nvme_set_preferred_path",
                {"name": sample.bdev_name, "cntlid": destination.cntlid},
            )
            self._ensure_active_passive(sample)
            control.reroute_destination_path = destination.path_id
            control.reroute_destination_cntlid = destination.cntlid
            result_text = json.dumps({
                "primitive": "bdev_nvme_set_preferred_path",
                "source_path": sample.path_id,
                "preferred_path": destination.path_id,
                "preferred_cntlid": destination.cntlid,
                "rpc_result": result,
            }, sort_keys=True)
        else:
            result = self.backend.call(
                sample.socket_path,
                "bdev_nvme_set_preferred_path",
                {"name": sample.bdev_name, "cntlid": sample.cntlid},
            )
            self._ensure_active_passive(sample)
            result_text = json.dumps({
                "primitive": "bdev_nvme_set_preferred_path",
                "probe_path": sample.path_id,
                "preferred_cntlid": sample.cntlid,
                "rpc_result": result,
            }, sort_keys=True)
            control.reroute_destination_path = None
            control.reroute_destination_cntlid = None
        self._record_control_success(control, action, time.time(), result_text)
        return result_text

    def _configure_policy(self, first_meta: Mapping[str, Mapping[str, Any]],
                          first_samples: Sequence[PathSample]) -> None:
        for target in self.targets:
            candidates = sorted(
                (item for item in first_samples if item.tenant == target.tenant),
                key=lambda item: item.path_id,
            )
            current = [item for item in candidates if item.current is True]
            if candidates:
                self.original_preferred[target.tenant] = (current or candidates)[0]
        params = _policy_params(self.args)
        if self.args.observe_only:
            return
        for target in self.targets:
            original = dict(first_meta.get(target.tenant, {}))
            if _restore_policy_params(target.bdev_name, original) is None:
                raise BaselineError(
                    f"{target.tenant}: cannot identify original multipath policy; "
                    "refusing a policy change that could not be restored"
                )
            self.original_policies[target.tenant] = original
            if original.get("mp_policy") == "active_passive":
                self.active_passive_tenants.add(target.tenant)
        if params is None:
            return
        for target in self.targets:
            self.backend.call(target.socket_path, "bdev_nvme_set_multipath_policy", params)
            if params.get("policy") == "active_passive":
                self.active_passive_tenants.add(target.tenant)
            assert self.writer is not None
            self.writer.event("multipath_policy_set", tenant=target.tenant,
                              socket_path=target.socket_path, params=params)
        # Do not judge an interval that straddles a policy transition.
        self.collector.counters.clear()

    def _restore(self) -> bool:
        if not self.args.restore_on_exit or self.args.observe_only:
            return True
        ok = True
        if self.writer:
            self.writer.event("restore_begin")
        for target in self.targets:
            preferred = self.original_preferred.get(target.tenant)
            if preferred is None or preferred.cntlid is None:
                continue
            try:
                result = self.backend.call(
                    target.socket_path,
                    "bdev_nvme_set_preferred_path",
                    {"name": target.bdev_name, "cntlid": preferred.cntlid},
                )
                if self.writer:
                    self.writer.event(
                        "preferred_path_restored",
                        tenant=target.tenant,
                        path_id=preferred.path_id,
                        cntlid=preferred.cntlid,
                        rpc_result=result,
                    )
            except Exception as exc:
                ok = False
                if self.writer:
                    self.writer.event(
                        "preferred_path_restore_error",
                        tenant=target.tenant,
                        error=str(exc),
                    )
        for target in self.targets:
            params = _restore_policy_params(target.bdev_name,
                                            self.original_policies.get(target.tenant, {}))
            if params is None:
                continue
            try:
                self.backend.call(target.socket_path, "bdev_nvme_set_multipath_policy", params)
                if self.writer:
                    self.writer.event("multipath_policy_restored", tenant=target.tenant,
                                      params=params)
            except Exception as exc:
                ok = False
                if self.writer:
                    self.writer.event("policy_restore_error", tenant=target.tenant,
                                      error=str(exc))
        for control in self.controls.values():
            control.disabled_by_us = False
            control.disabled_since = None
            control.reroute_destination_path = None
            control.reroute_destination_cntlid = None
        return ok

    def run(self) -> int:
        try:
            wait_for_sockets(self.targets, self.args.wait_for_sockets_s)
            output_dir = Path(self.args.output_dir)
            self.writer = OutputWriter(output_dir, self.policy.name,
                                       self.policy.describe(), self.args, self.targets)
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr, flush=True)
            return 2
        start_unix = time.time()
        stop_reason = "completed"
        completed = False
        old_handlers = {
            sig: signal.signal(sig, self._signal) for sig in (signal.SIGINT, signal.SIGTERM)
        }
        consecutive_errors = 0
        exit_code = 0
        try:
            self.collector.load_tick_rates()
            first, errors = self.collector.collect_all(self.args.max_workers)
            if errors:
                raise BaselineError("initial RPC collection failed: " + "; ".join(errors))
            first_samples: List[PathSample] = []
            first_meta: Dict[str, Mapping[str, Any]] = {}
            for snapshot in first:
                samples, meta = self.collector.samples(snapshot, start_unix)
                first_samples.extend(samples)
                first_meta[snapshot.target.tenant] = meta
            validate_path_targeting(
                first_samples,
                self.args.min_paths,
                self.args.observe_only,
                expected_tenants=(target.tenant for target in self.targets),
            )
            for sample in first_samples:
                pair = (sample.tenant, sample.path_key)
                self.controls.setdefault(pair, ControlState())
                self.latest_samples[pair] = sample
            preflight = {
                "tick_rates": self.collector.tick_rates,
                "bdev_metadata": first_meta,
                "paths": [
                    {
                        "tenant": sample.tenant,
                        "path_id": sample.path_id,
                        "path_key": sample.path_key,
                        "cntlid": sample.cntlid,
                        "ana_state": sample.ana_state,
                    }
                    for sample in first_samples
                ],
            }
            (self.writer.output_dir / "preflight.json").write_text(
                json.dumps(preflight, indent=2) + "\n"
            )
            self._configure_policy(first_meta, first_samples)
            self.writer.event("preflight_ok", tenants=len(self.targets),
                              paths=len(first_samples), tick_rates=self.collector.tick_rates)

            next_poll = time.monotonic()
            while not self.stop_requested:
                if self.args.duration_s > 0 and time.time() - start_unix >= self.args.duration_s:
                    stop_reason = "duration"
                    break
                next_poll += self.args.interval_s
                snapshots, errors = self.collector.collect_all(self.args.max_workers)
                if errors:
                    consecutive_errors += 1
                    self.writer.event("rpc_collection_error", errors=errors,
                                      consecutive=consecutive_errors)
                    if consecutive_errors >= self.args.max_consecutive_rpc_errors:
                        raise BaselineError(
                            f"RPC collection failed {consecutive_errors} consecutive times: " +
                            "; ".join(errors)
                        )
                else:
                    consecutive_errors = 0

                for snapshot in snapshots:
                    samples, _meta = self.collector.samples(snapshot, start_unix)
                    for sample in samples:
                        pair = (sample.tenant, sample.path_key)
                        control = self.controls.setdefault(pair, ControlState())
                        self.latest_samples[pair] = sample
                        decision = self.policy.observe(sample, control, sample.timestamp_unix)
                        rpc_result = ""
                        if decision.action in ("disable", "enable"):
                            try:
                                rpc_result = self._apply(sample, decision.action)
                                event_action = (
                                    "reroute" if decision.action == "disable" else "probe"
                                )
                                self.writer.event(
                                    "path_action", policy=self.policy.name,
                                    tenant=sample.tenant, path_id=sample.path_id,
                                    path_key=sample.path_key, cntlid=sample.cntlid,
                                    action=event_action,
                                    policy_decision_action=decision.action,
                                    primitive="bdev_nvme_set_preferred_path",
                                    reason=decision.reason,
                                    metrics=decision.metrics, rpc_result=rpc_result,
                                )
                            except Exception as exc:
                                rpc_result = f"ERROR: {exc}"
                                self.writer.event(
                                    "path_action_error", tenant=sample.tenant,
                                    path_id=sample.path_id, action=decision.action,
                                    error=str(exc),
                                )
                                raise BaselineError(str(exc)) from exc
                        self.writer.path_sample(sample, control, decision, rpc_result)
                    self.writer.tenant_sample(samples, self.controls)

                sleep_s = next_poll - time.monotonic()
                if sleep_s > 0:
                    time.sleep(sleep_s)
                else:
                    next_poll = time.monotonic()
            completed = True
            if self.stop_requested:
                stop_reason = "signal"
        except Exception as exc:
            stop_reason = f"error: {exc}"
            exit_code = 2
            if self.writer:
                self.writer.event("fatal_error", error=str(exc))
            print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        finally:
            try:
                if not self._restore():
                    completed = False
                    exit_code = 2
                    stop_reason += "; restore failed"
            finally:
                if self.writer:
                    self.writer.close(completed, stop_reason)
                for sig, handler in old_handlers.items():
                    signal.signal(sig, handler)
        return exit_code


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    target = parser.add_argument_group("existing SPDK initiator sockets")
    target.add_argument(
        "--tenant", action="append", metavar="ID=SOCKET",
        help="explicit tenant/socket mapping; repeat for each tenant (overrides template)",
    )
    target.add_argument("--num-tenants", type=int, default=8,
                        help="tenant count when using --socket-template (default: 8)")
    target.add_argument("--socket-template", default=DEFAULT_SOCKET_TEMPLATE,
                        help="existing RPC socket template containing {i}")
    target.add_argument("--bdev-name", default="mpn1",
                        help="multipath namespace bdev name (default: mpn1)")
    target.add_argument("--controller-name", default="mp",
                        help="NVMe bdev-controller prefix used for path metadata (default: mp)")
    target.add_argument("--spdk-root", type=Path, default=DEFAULT_SPDK_ROOT)
    target.add_argument("--wait-for-sockets-s", type=float, default=120.0)
    target.add_argument("--rpc-timeout-s", type=float, default=15.0)
    target.add_argument("--reconcile-timeout-s", type=float, default=5.0,
                        help="read-back window after an ambiguous mutating RPC")

    sampling = parser.add_argument_group("sampling and output")
    sampling.add_argument("--output-dir", type=Path, required=True)
    sampling.add_argument("--interval-s", type=float, default=1.0)
    sampling.add_argument("--duration-s", "--duration", dest="duration_s",
                          type=float, default=0.0,
                          help="0 means run until SIGINT/SIGTERM")
    sampling.add_argument("--io-kind", choices=("read", "write", "all"), default="read")
    sampling.add_argument("--max-workers", type=int, default=8)
    sampling.add_argument("--max-consecutive-rpc-errors", type=int, default=5)

    workload = parser.add_argument_group("workload metadata and paper metrics")
    workload.add_argument(
        "--weights", type=positive_float_csv, default=(1.0,),
        help=("tenant weights in target order; one value means equal weights "
              "(example: 3,1,1,1)"),
    )
    workload.add_argument(
        "--reference-capacity-iops", type=nonnegative_float, default=0.0,
        help=("fault-free all-tenant aggregate throughput C shared by every arm in "
              "the panel; required for a valid fair-share summary"),
    )
    workload.add_argument("--queue-depth", "--qd", dest="queue_depth", type=int,
                          help="workload queue depth recorded in the manifest")
    workload.add_argument("--workload-label", default="unspecified",
                          help="opaque workload label recorded in the manifest")
    workload.add_argument("--git-commit",
                          help="repository commit recorded in the run manifest")
    workload.add_argument("--measure-start-s", type=nonnegative_float, default=0.0,
                          help="discard earlier samples from fair-share aggregation")
    workload.add_argument("--measure-end-s", type=nonnegative_float, default=0.0,
                          help="measurement-window end; 0 means controller stop")
    onset = workload.add_mutually_exclusive_group()
    onset.add_argument("--fault-at-s", type=nonnegative_float,
                       help="fault onset relative to this sidecar's start")
    onset.add_argument("--fault-at-unix", type=positive_float,
                       help="absolute UNIX fault-onset timestamp")
    workload.add_argument(
        "--fault-path-id", action="append",
        help="expected failed path label (for example p1); repeat if needed",
    )
    workload.add_argument(
        "--fault-tenant", action="append",
        help="expected affected tenant ID; repeat as needed (default: all tenants)",
    )

    control = parser.add_argument_group("safe path control")
    control.add_argument("--observe-only", action="store_true",
                         help="collect/decide but never issue mutating RPCs")
    control.add_argument("--no-restore-on-exit", dest="restore_on_exit",
                         action="store_false",
                         help="do not restore preferred path or policy on exit")
    control.set_defaults(restore_on_exit=True)
    control.add_argument("--min-paths", type=int, default=2)
    control.add_argument(
        "--multipath-policy", choices=("keep", "active_passive", "active_active"),
        default="active_passive",
        help=("host-only policy (fixed/kernel default: active_passive; adaptive default: "
              "active_active; use keep deliberately)"),
    )
    control.add_argument("--selector", choices=("round_robin", "queue_depth"),
                         default="round_robin")
    control.add_argument("--rr-min-io", type=int)


def positive_float(value: str) -> float:
    number = float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return number


def nonnegative_float(value: str) -> float:
    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return number


def positive_float_csv(value: str) -> Tuple[float, ...]:
    try:
        numbers = tuple(float(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a comma-separated number list") from exc
    if not numbers or any(number <= 0 for number in numbers):
        raise argparse.ArgumentTypeError("all weights must be > 0")
    return numbers


def validate_common_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.num_tenants <= 0:
        parser.error("--num-tenants must be > 0")
    if args.interval_s <= 0:
        parser.error("--interval-s must be > 0")
    if args.duration_s < 0:
        parser.error("--duration-s must be >= 0")
    if args.rpc_timeout_s <= 0:
        parser.error("--rpc-timeout-s must be > 0")
    if args.reconcile_timeout_s < 0:
        parser.error("--reconcile-timeout-s must be >= 0")
    if args.min_paths <= 0:
        parser.error("--min-paths must be > 0")
    if args.max_workers <= 0:
        parser.error("--max-workers must be > 0")
    if args.max_consecutive_rpc_errors <= 0:
        parser.error("--max-consecutive-rpc-errors must be > 0")
    target_count = len(args.tenant) if args.tenant else args.num_tenants
    if len(args.weights) not in (1, target_count):
        parser.error(
            f"--weights needs one value or {target_count} values for the configured tenants"
        )
    if args.queue_depth is not None and args.queue_depth <= 0:
        parser.error("--queue-depth/--qd must be > 0")
    if args.measure_end_s > 0 and args.measure_end_s <= args.measure_start_s:
        parser.error("--measure-end-s must be greater than --measure-start-s")
    for path_id in args.fault_path_id or []:
        if not re.fullmatch(r"p\d+", path_id):
            parser.error("--fault-path-id must use the discovered pN label (for example p1)")
    if not args.tenant and "{i}" not in args.socket_template:
        parser.error("--socket-template must contain {i}")
    if args.multipath_policy != "active_active" and args.rr_min_io is not None:
        parser.error("--rr-min-io requires --multipath-policy active_active")
    if args.rr_min_io is not None and args.rr_min_io <= 0:
        parser.error("--rr-min-io must be > 0")
    if re.search(r"n\d+$", args.controller_name):
        parser.error("--controller-name is the controller prefix (for example mp), not bdev mpn1")


def run_policy(parser: argparse.ArgumentParser, args: argparse.Namespace,
               policy: PathPolicy) -> int:
    validate_common_args(parser, args)
    try:
        return BaselineRunner(args, policy).run()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 2
