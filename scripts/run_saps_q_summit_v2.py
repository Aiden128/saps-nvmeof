#!/usr/bin/env python3
"""Per-IO logging and queue-depth selector adapter for summit v2.

The shared E2 harness remains unchanged.  This adapter uses the current SPDK
build, gives each tenant a private per-IO log directory, and adds the blind-QD
selector override used by the steelman comparison arm.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import run_saps_q_e2_idle as e2


BDEVPERF_TC = Path("/home/aiden/spdk/build/examples/bdevperf")


class SummitV2TenantProc(e2.TenantProcE2):
    def cmd_list(self) -> list[str]:
        per_io_dir = self.out_dir / "per_io"
        per_io_dir.mkdir(parents=True, exist_ok=True)
        return super().cmd_list() + ["--per-io-log", str(per_io_dir)]

    def setup_controllers(self) -> bool:
        if not super().setup_controllers():
            return False
        if os.environ.get("SAPSQ_FORCE_QD") != "1":
            return True
        command = [
            "sudo", e2.RPCPY, "-s", self.sock,
            "bdev_nvme_set_multipath_policy", "-b", "mpn1",
            "-p", "active_active", "-s", "queue_depth",
        ]
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=15
            )
        except subprocess.TimeoutExpired:
            self._log("summit-v2 queue_depth policy timed out")
            return False
        self._log(f"summit-v2 multipath policy=active_active+queue_depth: "
                  f"rc={result.returncode}")
        return result.returncode == 0


def main() -> None:
    if not BDEVPERF_TC.is_file():
        raise SystemExit(f"missing summit-v2 binary: {BDEVPERF_TC}")
    e2.BDEVPERF = str(BDEVPERF_TC)
    e2.TenantProcE2 = SummitV2TenantProc
    e2.main()


if __name__ == "__main__":
    main()
