#!/usr/bin/env python3
"""Two-path adapter for the existing E2 SAPS-Q harness.

The shared E2 harness intentionally remains unchanged.  This adapter narrows
controller attachment and the advertised SAPS-Q path count to paths A and B so
the health-signal experiment has one healthy reference and one injected path.
"""

from __future__ import annotations

import run_saps_q_e2_idle as e2


N_PATHS = 2


class TwoPathTenantProc(e2.TenantProcE2):
    """E2 tenant process with only target paths A and B attached."""

    def env_list(self) -> list[str]:
        bypass_d = "1" if getattr(self.args, "bypass_d", False) else "0"
        return e2.build_env(
            self.args.mode,
            self.tid,
            self.args.num_tenants,
            N_PATHS,
            self.args.link_cap,
            [int(weight) for weight in self.args.weights.split(",")],
            self.args.path_caps[:N_PATHS],
            bypass_d=bypass_d,
        )

    def attach_cmds(self) -> list[list[str]]:
        commands = []
        for path_idx in range(N_PATHS):
            port = self._base_port + e2.PATH_PORT_OFFSETS[path_idx]
            commands.append(e2.raw_rpc_command(
                self.sock,
                "bdev_nvme_attach_controller",
                {
                    "name": "mp",
                    "trtype": "rdma",
                    "traddr": e2.TARGET_IP,
                    "trsvcid": str(port),
                    "adrfam": "ipv4",
                    "subnqn": self._nqn,
                    "multipath": "multipath",
                },
                timeout_s=90,
            ))
        return commands


def main() -> None:
    e2.PATH_PORT_OFFSETS = e2.PATH_PORT_OFFSETS[:N_PATHS]
    e2.TenantProcE2 = TwoPathTenantProc
    e2.main()


if __name__ == "__main__":
    main()
