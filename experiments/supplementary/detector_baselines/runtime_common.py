#!/usr/bin/env python3
"""Small local runtime helpers for the baseline campaign.

The helpers deliberately avoid process-name matching.  Every process is stopped
only if its current /proc start tick still matches the value recorded at spawn.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

SPDK_ROOT = Path(os.environ.get("SPDK_ROOT", "/home/aiden/spdk"))
RPC_PY = SPDK_ROOT / "scripts" / "rpc.py"
SPDK_PYTHON = SPDK_ROOT / "python"

if str(SPDK_PYTHON) not in sys.path:
    sys.path.insert(0, str(SPDK_PYTHON))


def _read_state_start_ticks(pid: int) -> tuple[str, int]:
    stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    rparen = stat.rfind(")")
    if rparen == -1:
        raise RuntimeError(f"cannot parse /proc/{pid}/stat")
    fields_after_state = stat[rparen + 2 :].split()
    if len(fields_after_state) < 20:
        raise RuntimeError(f"short /proc/{pid}/stat")
    return fields_after_state[0], int(fields_after_state[19])


def _read_start_ticks(pid: int) -> int:
    return _read_state_start_ticks(pid)[1]


def _load_registry(registry_path: str | os.PathLike[str]) -> dict[str, Any]:
    path = Path(registry_path)
    if not path.exists():
        return {"version": 1, "processes": []}
    with path.open("r", encoding="utf-8") as source:
        data = json.load(source)
    data.setdefault("version", 1)
    data.setdefault("processes", [])
    return data


def _save_registry(registry_path: str | os.PathLike[str], data: dict[str, Any]) -> None:
    path = Path(registry_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as sink:
        json.dump(data, sink, indent=2, sort_keys=True)
        sink.write("\n")
    os.replace(tmp, path)


def rpc(socket_path: str, method: str, params: dict[str, Any] | None = None, timeout: int = 15) -> Any:
    """Run one local SPDK JSON-RPC and return the parsed result.

    This uses SPDK's Python JSONRPCClient from /home/aiden/spdk, so callers pass
    the raw method name and JSON params instead of shell-quoted rpc.py commands.
    The RPC script path is still recorded in errors/provenance for arm-2 parity.
    """

    try:
        from spdk.rpc.client import JSONRPCClient  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on arm-2 SPDK checkout.
        raise RuntimeError(
            f"cannot import SPDK JSONRPCClient from {SPDK_PYTHON}; rpc.py={RPC_PY}"
        ) from exc

    client = JSONRPCClient(socket_path, timeout=timeout)
    try:
        return client.call(method, params or {})
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def spawn_recorded(
    cmd: list[str],
    log_path: str | os.PathLike[str],
    registry_path: str | os.PathLike[str],
    env: dict[str, str] | None = None,
) -> subprocess.Popen:
    """Spawn cmd directly, teeing stdout/stderr to log_path, and record PID proof."""

    if not cmd:
        raise ValueError("cmd must not be empty")
    log = Path(log_path)
    log.parent.mkdir(parents=True, exist_ok=True)
    log_fh = log.open("ab", buffering=0)
    try:
        proc = subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, env=env)
    finally:
        log_fh.close()
    try:
        start_ticks = _read_start_ticks(proc.pid)
        data = _load_registry(registry_path)
        data["processes"].append(
            {
                "pid": proc.pid,
                "start_ticks": start_ticks,
                "cmd": cmd,
                "log": str(log),
                "spawned_unix": time.time(),
            }
        )
        _save_registry(registry_path, data)
    except Exception:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        raise
    return proc


def _pid_matches(record: dict[str, Any]) -> bool:
    pid = int(record["pid"])
    try:
        state, start_ticks = _read_state_start_ticks(pid)
        return state != "Z" and start_ticks == int(record["start_ticks"])
    except (FileNotFoundError, ProcessLookupError):
        # The process exited between the existence check and the read.
        return False


def _signal_live(records: list[dict[str, Any]], sig: signal.Signals) -> list[int]:
    failures: list[int] = []
    for record in records:
        pid = int(record["pid"])
        if not _pid_matches(record):
            continue
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            continue
        except PermissionError:
            failures.append(pid)
    return failures


def _wait_gone(records: list[dict[str, Any]], deadline: float) -> None:
    while time.monotonic() < deadline:
        if not any(_pid_matches(record) for record in records):
            return
        time.sleep(0.1)


def stop_recorded(
    registry_path: str | os.PathLike[str],
    only_pids: list[int] | tuple[int, ...] | set[int] | None = None,
) -> None:
    """Terminate recorded processes only, proving every live PID by start tick."""

    data = _load_registry(registry_path)
    wanted = {int(pid) for pid in only_pids} if only_pids is not None else None
    selected = [
        record
        for record in data.get("processes", [])
        if wanted is None or int(record.get("pid", -1)) in wanted
    ]
    failures = _signal_live(selected, signal.SIGTERM)
    _wait_gone(selected, time.monotonic() + 8.0)
    stubborn = [record for record in selected if _pid_matches(record)]
    failures.extend(_signal_live(stubborn, signal.SIGKILL))
    _wait_gone(stubborn, time.monotonic() + 4.0)
    survivors = [int(record["pid"]) for record in selected if _pid_matches(record)]
    # SPDK processes without a shared-memory id use "spdk_pid<PID>" hugepage files;
    # remove those left by our stopped processes so the pages return to the pool.
    for record in selected:
        pid = int(record["pid"])
        if pid not in survivors:
            for leftover in Path("/dev/hugepages").glob(f"spdk_pid{pid}map_*"):
                try:
                    leftover.unlink()
                except OSError:
                    pass
    if failures or survivors:
        raise RuntimeError(
            f"failed to stop recorded processes; signal_failures={failures}, survivors={survivors}"
        )
    data["processes"] = [
        record
        for record in data.get("processes", [])
        if wanted is not None and int(record.get("pid", -1)) not in wanted
    ]
    _save_registry(registry_path, data)
