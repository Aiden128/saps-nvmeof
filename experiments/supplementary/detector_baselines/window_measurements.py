#!/usr/bin/env python3
"""Windowed per-IO and SPDK thread measurements for baseline_v2 runs."""

from __future__ import annotations

import heapq
import json
import math
import struct
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Mapping

from runtime_common import rpc

RECORD = struct.Struct("<QQIIHH")
ANCHOR = struct.Struct("<QQ")
DEFAULT_BDEV_NAME = "mpn1"
MIN_SUCCESSFUL_READS = 1000
READ_CHUNK_RECORDS = 65536


def _tenant_key(index: int) -> str:
    return f"t{index}"


def _tenant_per_io_paths(run_dir: Path, tenant: int) -> tuple[Path, Path]:
    directory = run_dir / f"tenant_{tenant:02d}" / "per_io"
    binary = directory / "per_io_hwts.bin"
    return binary, binary.with_suffix(binary.suffix + ".anchor")


def _load_anchor(path: Path) -> tuple[int, int]:
    data = path.read_bytes()
    if len(data) != ANCHOR.size:
        raise RuntimeError(f"invalid anchor size {len(data)} at {path}")
    epoch_ns, counter = ANCHOR.unpack(data)
    if epoch_ns <= 0 or counter <= 0:
        raise RuntimeError(f"invalid anchor values at {path}: epoch_ns={epoch_ns}, counter={counter}")
    return epoch_ns, counter


def _window_counter_bounds(
    epoch: float,
    window: tuple[float, float],
    anchor_epoch_ns: int,
    anchor_counter: int,
    counter_hz: float,
) -> tuple[int, int, int, int]:
    if counter_hz <= 0:
        raise ValueError("counter_hz must be positive")
    start_s, end_s = window
    if end_s <= start_s:
        raise ValueError(f"invalid window: {window!r}")
    start_ns = int(round((epoch + start_s) * 1_000_000_000))
    end_ns = int(round((epoch + end_s) * 1_000_000_000))
    start_counter = anchor_counter + math.ceil((start_ns - anchor_epoch_ns) * counter_hz / 1_000_000_000)
    end_counter = anchor_counter + math.floor((end_ns - anchor_epoch_ns) * counter_hz / 1_000_000_000)
    return start_counter, end_counter, start_ns, end_ns


def _iter_records(binary: Path):
    size = binary.stat().st_size
    if size == 0:
        raise RuntimeError(f"empty per-IO log: {binary}")
    if size % RECORD.size != 0:
        raise RuntimeError(
            f"per-IO log has trailing partial record: {binary} size={size} record_size={RECORD.size}"
        )
    with binary.open("rb") as source:
        while True:
            chunk = source.read(RECORD.size * READ_CHUNK_RECORDS)
            if not chunk:
                break
            if len(chunk) % RECORD.size != 0:
                raise RuntimeError(f"short read from {binary}")
            yield from RECORD.iter_unpack(chunk)


def _count_successful_reads(binary: Path, start_counter: int, end_counter: int) -> tuple[int, int, int, int]:
    total = 0
    selected = 0
    successes = 0
    failures = 0
    for submit_counter, _latency, _sct, _sc, _inflight, success in _iter_records(binary):
        total += 1
        if start_counter <= submit_counter <= end_counter:
            selected += 1
            if success == 1:
                successes += 1
            else:
                failures += 1
    return total, selected, successes, failures


def _nearest_rank_p99_ticks(
    binary: Path,
    start_counter: int,
    end_counter: int,
    expected_total: int,
    success_count: int,
) -> tuple[int, int, int]:
    if success_count < MIN_SUCCESSFUL_READS:
        raise RuntimeError(
            f"too few successful per-IO records for {binary}: "
            f"success={success_count}, required={MIN_SUCCESSFUL_READS}"
        )
    rank_from_low = math.ceil(0.99 * success_count)
    keep_highest = success_count - rank_from_low + 1
    heap: list[int] = []
    total = 0
    successes = 0
    for submit_counter, latency_ticks, _sct, _sc, _inflight, success in _iter_records(binary):
        total += 1
        if success != 1 or not (start_counter <= submit_counter <= end_counter):
            continue
        successes += 1
        if len(heap) < keep_highest:
            heapq.heappush(heap, latency_ticks)
        elif latency_ticks > heap[0]:
            heapq.heapreplace(heap, latency_ticks)
    if total != expected_total:
        raise RuntimeError(
            f"per-IO record count changed between passes for {binary}: "
            f"first={expected_total}, second={total}"
        )
    if successes != success_count:
        raise RuntimeError(
            f"successful per-IO count changed between passes for {binary}: "
            f"first={success_count}, second={successes}"
        )
    if len(heap) != keep_highest:
        raise RuntimeError(
            f"per-IO p99 pass count mismatch for {binary}: expected_heap={keep_highest}, got={len(heap)}"
        )
    return heap[0], total, successes


def cleanup_per_io(run_dir: Path, tenants: int = 4) -> dict[str, Any]:
    """Remove per-IO binary logs and anchors after measurement processes stop."""

    removed: list[str] = []
    missing: list[str] = []
    errors: dict[str, str] = {}
    for tenant in range(tenants):
        binary, anchor = _tenant_per_io_paths(Path(run_dir), tenant)
        frequency = binary.with_suffix(binary.suffix + ".tsc_hz")
        for path in (binary, anchor, frequency):
            try:
                path.unlink()
                removed.append(str(path))
            except FileNotFoundError:
                missing.append(str(path))
            except OSError as exc:
                errors[str(path)] = str(exc)
    if errors:
        raise RuntimeError(f"failed to clean per-IO logs: {errors}")
    return {"removed": removed, "missing": missing}


def summarize_per_io(
    run_dir: Path,
    epoch: float,
    window: tuple[float, float],
    counter_hz: float = 1_000_000_000,
    tenants: int = 4,
) -> dict[str, Any]:
    """Summarize exact nearest-rank p99 latency for successful read records.

    The anchor maps submit counter ticks to canonical UNIX time as:
    anchor_epoch_ns + (submit_counter - anchor_counter) / counter_hz.
    The returned latency unit is microseconds.
    """

    run_path = Path(run_dir)
    per_tenant: dict[str, dict[str, Any]] = {}
    for tenant in range(tenants):
        binary, anchor = _tenant_per_io_paths(run_path, tenant)
        anchor_epoch_ns, anchor_counter = _load_anchor(anchor)
        frequency = binary.with_suffix(binary.suffix + ".tsc_hz")
        logged_hz = None
        if frequency.exists():
            raw_hz = frequency.read_bytes()
            if len(raw_hz) != 8:
                raise RuntimeError(f"invalid frequency sidecar: {frequency}")
            logged_hz = struct.unpack("<Q", raw_hz)[0]
            if logged_hz == 0:
                # Deployed bdevperf writes this sidecar before spdk_app_start,
                # while DPDK's frequency is still uninitialized. The counter
                # itself is the same CNTVCT before/after initialization.
                ready = json.loads((run_path / "collector/preflight.json").read_text())
                hz = ready.get("tick_rates", {}).get(_tenant_key(tenant))
                if hz != counter_hz:
                    raise RuntimeError(f"zero startup frequency lacks matching live iostat proof: {hz}")
            elif logged_hz != counter_hz:
                raise RuntimeError(f"per-IO frequency {logged_hz} disagrees with configured {counter_hz}")
        start_counter, end_counter, start_ns, end_ns = _window_counter_bounds(
            epoch, window, anchor_epoch_ns, anchor_counter, counter_hz
        )
        total_records, selected_count, success_count, failure_count = _count_successful_reads(
            binary, start_counter, end_counter
        )
        p99_ticks, second_pass_records, second_pass_successes = _nearest_rank_p99_ticks(
            binary, start_counter, end_counter, total_records, success_count
        )
        per_tenant[_tenant_key(tenant)] = {
            "binary_path": str(binary),
            "anchor_path": str(anchor),
            "anchor_epoch_ns": anchor_epoch_ns,
            "anchor_counter": anchor_counter,
            "logged_counter_hz": logged_hz,
            "frequency_source": "live iostat; pre-init sidecar zero" if logged_hz == 0 else "sidecar/configured",
            "window_start_unix_ns": start_ns,
            "window_end_unix_ns": end_ns,
            "window_start_counter": start_counter,
            "window_end_counter": end_counter,
            "record_count": total_records,
            "second_pass_record_count": second_pass_records,
            "selected_count": selected_count,
            "success_count": success_count,
            "second_pass_success_count": second_pass_successes,
            "failure_count": failure_count,
            "p99_ticks": p99_ticks,
            "p99_read_latency_us": p99_ticks * 1_000_000.0 / counter_hz,
        }

    cleanup = cleanup_per_io(run_path, tenants=tenants)
    worst = max(item["p99_read_latency_us"] for item in per_tenant.values())
    return {
        "record_format": "<QQIIHH",
        "record_size_bytes": RECORD.size,
        "anchor_format": "<QQ",
        "anchor_fields": ["anchor_epoch_ns", "anchor_counter"],
        "counter_hz_used": counter_hz,
        "window_unix_s": [epoch + window[0], epoch + window[1]],
        "percentile": 0.99,
        "percentile_method": "nearest-rank over successful read records, inclusive window bounds",
        "per_tenant": per_tenant,
        "worst_tenant_p99_read_latency_us": worst,
        "cleanup": cleanup,
    }


def _normalize_sockets(sockets: Mapping[str, str | Path] | list[str | Path] | tuple[str | Path, ...]) -> list[tuple[str, str]]:
    if isinstance(sockets, Mapping):
        items = [(str(key), str(value)) for key, value in sockets.items()]
    else:
        items = [(_tenant_key(index), str(value)) for index, value in enumerate(sockets)]
    if not items:
        raise ValueError("at least one tenant socket is required")
    return items


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{label} must be a JSON object, got {type(value).__name__}")
    return value


def _require_int(mapping: Mapping[str, Any], keys: tuple[str, ...], label: str) -> tuple[int, str]:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            try:
                value = int(mapping[key])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"{label}.{key} is not an integer: {mapping[key]!r}") from exc
            if value < 0:
                raise RuntimeError(f"{label}.{key} is negative: {value}")
            return value, key
    raise RuntimeError(f"{label} missing required keys {keys}")


def _require_positive_int(mapping: Mapping[str, Any], keys: tuple[str, ...], label: str) -> tuple[int, str]:
    value, key = _require_int(mapping, keys, label)
    if value <= 0:
        raise RuntimeError(f"{label}.{key} is not positive: {value}")
    return value, key


def _extract_thread_tick_rate(payload: Mapping[str, Any]) -> tuple[int, str]:
    return _require_positive_int(payload, ("tick_rate", "ticks_per_second", "tsc_rate"), "thread_get_stats")


def _extract_threads(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_threads = payload.get("threads")
    if not isinstance(raw_threads, list) or not raw_threads:
        raise RuntimeError("thread_get_stats missing non-empty threads list")
    threads: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_threads):
        thread = _require_mapping(raw, f"thread_get_stats.threads[{index}]")
        busy, busy_key = _require_int(thread, ("busy_ticks", "busy", "busy_tsc"), f"thread[{index}]")
        idle, idle_key = _require_int(thread, ("idle_ticks", "idle", "idle_tsc"), f"thread[{index}]")
        thread_id = thread.get("id", thread.get("thread_id", thread.get("name")))
        if thread_id is None:
            raise RuntimeError(f"thread[{index}] missing id/thread_id/name")
        threads.append(
            {
                "thread_id": str(thread_id),
                "name": str(thread.get("name", thread_id)),
                "busy_ticks": busy,
                "idle_ticks": idle,
                "busy_source_key": busy_key,
                "idle_source_key": idle_key,
            }
        )
    return threads


def _extract_bdev_read_ops(payload: Mapping[str, Any]) -> tuple[int, dict[str, int], int]:
    tick_rate, _tick_key = _require_positive_int(payload, ("tick_rate",), "bdev_get_iostat")
    raw_bdevs = payload.get("bdevs")
    if not isinstance(raw_bdevs, list) or not raw_bdevs:
        raise RuntimeError("bdev_get_iostat missing non-empty bdevs list")
    per_bdev: dict[str, int] = {}
    for index, raw in enumerate(raw_bdevs):
        bdev = _require_mapping(raw, f"bdev_get_iostat.bdevs[{index}]")
        name = bdev.get("name")
        if not isinstance(name, str) or not name:
            raise RuntimeError(f"bdev_get_iostat.bdevs[{index}] missing name")
        read_ops, _ops_key = _require_int(bdev, ("num_read_ops",), f"bdev[{name}]")
        per_bdev[name] = read_ops
    return sum(per_bdev.values()), per_bdev, tick_rate


def _capture_one_thread_stats(tenant: str, socket_path: str) -> dict[str, Any]:
    thread_started = time.time()
    thread_payload = _require_mapping(
        rpc(socket_path, "thread_get_stats", timeout=2),
        f"{tenant} thread_get_stats",
    )
    thread_completed = time.time()
    thread_tick_rate, thread_tick_key = _extract_thread_tick_rate(thread_payload)
    threads = _extract_threads(thread_payload)

    iostat_started = time.time()
    iostat_payload = _require_mapping(
        rpc(socket_path, "bdev_get_iostat", {"name": DEFAULT_BDEV_NAME}, timeout=2),
        f"{tenant} bdev_get_iostat",
    )
    iostat_completed = time.time()
    total_read_ops, per_bdev_read_ops, bdev_tick_rate = _extract_bdev_read_ops(iostat_payload)

    return {
        "tenant": tenant,
        "socket_path": socket_path,
        "captured_unix_s": (thread_started + iostat_completed) / 2.0,
        "capture_started_unix_s": thread_started,
        "capture_completed_unix_s": iostat_completed,
        "thread_stats_started_unix_s": thread_started,
        "thread_stats_completed_unix_s": thread_completed,
        "iostat_started_unix_s": iostat_started,
        "iostat_completed_unix_s": iostat_completed,
        "capture_skew_s": iostat_completed - thread_started,
        "thread_tick_rate": thread_tick_rate,
        "thread_tick_rate_source_key": thread_tick_key,
        "bdev_iostat_tick_rate": bdev_tick_rate,
        "busy_tick_definition": "SPDK thread_get_stats busy tick cycles; not CPU cycles",
        "threads": threads,
        "per_bdev_completed_read_ops": per_bdev_read_ops,
        "completed_read_ops": total_read_ops,
    }


def capture_thread_stats(
    sockets: Mapping[str, str | Path] | list[str | Path] | tuple[str | Path, ...],
) -> dict[str, Any]:
    """Capture per-tenant SPDK thread busy/idle ticks and bdev read counters."""

    items = _normalize_sockets(sockets)
    per_tenant: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(items))) as pool:
        futures = {
            pool.submit(_capture_one_thread_stats, tenant, socket_path): tenant
            for tenant, socket_path in items
        }
        for future in as_completed(futures):
            tenant = futures[future]
            per_tenant[tenant] = future.result()
    started = min(item["capture_started_unix_s"] for item in per_tenant.values())
    completed = max(item["capture_completed_unix_s"] for item in per_tenant.values())
    window_skew_s = completed - started
    return {
        "captured_unix_s": (started + completed) / 2.0,
        "capture_started_unix_s": started,
        "capture_completed_unix_s": completed,
        "capture_skew_s": window_skew_s,
        "window_skew_s": window_skew_s,
        "busy_tick_definition": "SPDK thread_get_stats busy tick cycles; not CPU cycles",
        "per_tenant": per_tenant,
    }


def _threads_by_id(snapshot: Mapping[str, Any], tenant: str) -> dict[str, Mapping[str, Any]]:
    raw_threads = snapshot.get("threads")
    if not isinstance(raw_threads, list) or not raw_threads:
        raise RuntimeError(f"{tenant} snapshot missing threads")
    by_id: dict[str, Mapping[str, Any]] = {}
    for raw in raw_threads:
        thread = _require_mapping(raw, f"{tenant}.threads[]")
        thread_id = str(thread.get("thread_id", ""))
        if not thread_id:
            raise RuntimeError(f"{tenant} thread missing thread_id")
        if thread_id in by_id:
            raise RuntimeError(f"{tenant} duplicate thread_id {thread_id}")
        by_id[thread_id] = thread
    return by_id


def _nonnegative_delta(end_value: int, start_value: int, label: str) -> int:
    delta = end_value - start_value
    if delta < 0:
        raise RuntimeError(f"counter reset or wrap detected for {label}: start={start_value}, end={end_value}")
    return delta


def summarize_thread_stats(start: Mapping[str, Any], end: Mapping[str, Any]) -> dict[str, Any]:
    """Compute busy tick cycles per completed read I/O between two snapshots."""

    start_tenants = _require_mapping(start.get("per_tenant"), "start.per_tenant")
    end_tenants = _require_mapping(end.get("per_tenant"), "end.per_tenant")
    capture_interval_s = float(end["captured_unix_s"]) - float(start["captured_unix_s"])
    if capture_interval_s <= 0:
        raise RuntimeError(f"nonpositive capture interval: {capture_interval_s}")
    if set(start_tenants) != set(end_tenants):
        raise RuntimeError(
            f"tenant set changed: start={sorted(start_tenants)}, end={sorted(end_tenants)}"
        )

    per_tenant: dict[str, dict[str, Any]] = {}
    aggregate_busy = 0
    aggregate_idle = 0
    aggregate_reads = 0
    for tenant in sorted(start_tenants):
        start_item = _require_mapping(start_tenants[tenant], f"start.per_tenant.{tenant}")
        end_item = _require_mapping(end_tenants[tenant], f"end.per_tenant.{tenant}")
        if start_item.get("socket_path") != end_item.get("socket_path"):
            raise RuntimeError(f"{tenant} socket changed between snapshots")
        start_thread_tick_rate = int(start_item.get("thread_tick_rate", 0))
        end_thread_tick_rate = int(end_item.get("thread_tick_rate", -1))
        if start_thread_tick_rate <= 0 or end_thread_tick_rate <= 0:
            raise RuntimeError(f"{tenant} nonpositive thread tick rate")
        if start_thread_tick_rate != end_thread_tick_rate:
            raise RuntimeError(f"{tenant} thread tick rate changed between snapshots")
        start_bdev_tick_rate = int(start_item.get("bdev_iostat_tick_rate", 0))
        end_bdev_tick_rate = int(end_item.get("bdev_iostat_tick_rate", -1))
        if start_bdev_tick_rate <= 0 or end_bdev_tick_rate <= 0:
            raise RuntimeError(f"{tenant} nonpositive bdev iostat tick rate")
        if start_bdev_tick_rate != end_bdev_tick_rate:
            raise RuntimeError(f"{tenant} bdev iostat tick rate changed between snapshots")

        start_threads = _threads_by_id(start_item, tenant)
        end_threads = _threads_by_id(end_item, tenant)
        if set(start_threads) != set(end_threads):
            raise RuntimeError(
                f"{tenant} thread set changed: start={sorted(start_threads)}, end={sorted(end_threads)}"
            )

        thread_deltas: dict[str, dict[str, Any]] = {}
        busy_delta = 0
        idle_delta = 0
        for thread_id in sorted(start_threads):
            s_thread = start_threads[thread_id]
            e_thread = end_threads[thread_id]
            thread_busy = _nonnegative_delta(
                int(e_thread["busy_ticks"]), int(s_thread["busy_ticks"]), f"{tenant}.{thread_id}.busy_ticks"
            )
            thread_idle = _nonnegative_delta(
                int(e_thread["idle_ticks"]), int(s_thread["idle_ticks"]), f"{tenant}.{thread_id}.idle_ticks"
            )
            busy_delta += thread_busy
            idle_delta += thread_idle
            thread_deltas[thread_id] = {
                "name": e_thread.get("name", thread_id),
                "busy_tick_delta": thread_busy,
                "idle_tick_delta": thread_idle,
            }

        start_bdevs = _require_mapping(start_item.get("per_bdev_completed_read_ops"), f"{tenant}.start.bdevs")
        end_bdevs = _require_mapping(end_item.get("per_bdev_completed_read_ops"), f"{tenant}.end.bdevs")
        if set(start_bdevs) != set(end_bdevs):
            raise RuntimeError(
                f"{tenant} bdev set changed: start={sorted(start_bdevs)}, end={sorted(end_bdevs)}"
            )
        per_bdev_delta = {
            name: _nonnegative_delta(int(end_bdevs[name]), int(start_bdevs[name]), f"{tenant}.{name}.read_ops")
            for name in sorted(start_bdevs)
        }
        completed_read_delta = sum(per_bdev_delta.values())
        if completed_read_delta <= 0:
            raise RuntimeError(f"{tenant} has no completed read I/O delta")
        tenant_capture_interval_s = float(end_item["captured_unix_s"]) - float(start_item["captured_unix_s"])
        if tenant_capture_interval_s <= 0:
            raise RuntimeError(f"{tenant} nonpositive capture interval: {tenant_capture_interval_s}")

        aggregate_busy += busy_delta
        aggregate_idle += idle_delta
        aggregate_reads += completed_read_delta
        per_tenant[tenant] = {
            "socket_path": end_item["socket_path"],
            "thread_tick_rate": end_thread_tick_rate,
            "bdev_iostat_tick_rate": end_bdev_tick_rate,
            "busy_tick_definition": "SPDK thread_get_stats busy tick cycles; not CPU cycles",
            "busy_tick_delta": busy_delta,
            "idle_tick_delta": idle_delta,
            "completed_read_ops_delta": completed_read_delta,
            "per_bdev_completed_read_ops_delta": per_bdev_delta,
            "thread_deltas": thread_deltas,
            "busy_cycles_per_io": busy_delta / completed_read_delta,
            "capture_interval_s": tenant_capture_interval_s,
            "start_capture_skew_s": float(start_item["capture_skew_s"]),
            "end_capture_skew_s": float(end_item["capture_skew_s"]),
        }

    return {
        "busy_tick_definition": "SPDK thread_get_stats busy tick cycles; not CPU cycles",
        "per_tenant": per_tenant,
        "aggregate": {
            "busy_tick_delta": aggregate_busy,
            "idle_tick_delta": aggregate_idle,
            "completed_read_ops_delta": aggregate_reads,
            "busy_cycles_per_io": aggregate_busy / aggregate_reads,
        },
        "busy_cycles_per_io": aggregate_busy / aggregate_reads,
        "capture_interval_s": capture_interval_s,
        "start_capture_skew_s": float(start["capture_skew_s"]),
        "end_capture_skew_s": float(end["capture_skew_s"]),
        "start_window_skew_s": float(start.get("window_skew_s", start["capture_skew_s"])),
        "end_window_skew_s": float(end.get("window_skew_s", end["capture_skew_s"])),
    }


__all__ = [
    "cleanup_per_io",
    "capture_thread_stats",
    "summarize_per_io",
    "summarize_thread_stats",
]
