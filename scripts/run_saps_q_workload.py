#!/usr/bin/env python3
"""Thin workload adapter for run_saps_q_e2_idle.py.

The shared harness keeps its coordinator/tenant and SAPS environment logic.
This adapter changes only bdevperf's workload arguments and makes its target
counter collector account for both reads and writes.
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys

import run_saps_q_e2_idle as e2


def parse_adapter_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--eval-io-size", type=int, required=True)
    parser.add_argument(
        "--eval-workload",
        choices=["read", "write", "randread", "randwrite", "rw", "randrw"],
        required=True,
    )
    parser.add_argument("--eval-rwmixread", type=int, default=None)
    return parser.parse_known_args()


def install_workload_adapter(io_size: int, workload: str,
                             rwmixread: int | None) -> None:
    original_cmd_list = e2.TenantProcE2.cmd_list

    def cmd_list(self):
        cmd = original_cmd_list(self)
        cmd[cmd.index("-o") + 1] = str(io_size)
        cmd[cmd.index("-w") + 1] = workload
        if workload in {"rw", "randrw"}:
            mix = 70 if rwmixread is None else rwmixread
            cmd[cmd.index("-w"):cmd.index("-w")] = ["-M", str(mix)]
        return cmd

    e2.TenantProcE2.cmd_list = cmd_list

    def get_arm1_full(self):
        """Return combined read+write operations and latency for every path."""
        n = self.n_tenants
        script = (
            "import json,subprocess\n"
            "socks={'A':'/var/tmp/spdk_a.sock','B':'/var/tmp/spdk_b.sock','C':'/var/tmp/spdk_c.sock'}\n"
            "out={'tsc_hz':0,'reads':{},'lat_ticks':{},'max_lat_ticks':{}}\n"
            f"N={n}\n"
            "for P,sock in socks.items():\n"
            "    out['reads'][P]={}; out['lat_ticks'][P]={}; out['max_lat_ticks'][P]={}\n"
            "    try:\n"
            "        cmd=['sudo','/home/aiden/spdk/scripts/rpc.py','-s',sock,'bdev_get_iostat']\n"
            "        d=json.loads(subprocess.check_output(cmd,stderr=subprocess.DEVNULL,timeout=8))\n"
            "        if out['tsc_hz']==0 and isinstance(d,dict): out['tsc_hz']=d.get('tick_rate',0)\n"
            "        bdevs=d.get('bdevs',d) if isinstance(d,dict) else d\n"
            "        idx={b.get('name','').lower():b for b in bdevs}\n"
            "        for i in range(N):\n"
            "            b=idx.get(f'delay_t{i}_{P}'.lower(),{})\n"
            "            out['reads'][P][str(i)]=b.get('num_read_ops',0)+b.get('num_write_ops',0)\n"
            "            out['lat_ticks'][P][str(i)]=b.get('read_latency_ticks',0)+b.get('write_latency_ticks',0)\n"
            "            out['max_lat_ticks'][P][str(i)]=max(b.get('max_read_latency_ticks',0),b.get('max_write_latency_ticks',0))\n"
            "    except Exception:\n"
            "        for i in range(N):\n"
            "            out['reads'][P][str(i)]=-1; out['lat_ticks'][P][str(i)]=0; out['max_lat_ticks'][P][str(i)]=0\n"
            "print(json.dumps(out))\n"
        )
        encoded = base64.b64encode(script.encode()).decode()
        result = subprocess.run(
            ["sudo", "-u", "aiden", "ssh", "arm-1",
             f"echo {encoded} | base64 -d | python3"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        try:
            return json.loads(result.stdout.strip())
        except Exception:
            return None

    e2.PerTenantTimeseries._get_arm1_full = get_arm1_full


def main() -> None:
    adapter, remaining = parse_adapter_args()
    if adapter.eval_io_size <= 0:
        raise SystemExit("--eval-io-size must be > 0")
    if adapter.eval_rwmixread is not None and not 0 <= adapter.eval_rwmixread <= 100:
        raise SystemExit("--eval-rwmixread must be in [0,100]")
    install_workload_adapter(
        adapter.eval_io_size,
        adapter.eval_workload,
        adapter.eval_rwmixread,
    )
    sys.argv = [sys.argv[0], *remaining]
    e2.main()


if __name__ == "__main__":
    main()
