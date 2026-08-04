#!/usr/bin/env python3

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_competitor_fault_experiment import saps_q_env
from run_signal_isolation_campaign import (
    ARMS,
    FAULT_CASES,
    build_run_command,
    require_provenance,
)


def env_map(entries):
    return dict(entry.split("=", maxsplit=1) for entry in entries)


class SignalIsolationProfileTest(unittest.TestCase):
    def test_campaign_crosses_four_sources_with_three_fault_regimes(self):
        self.assertEqual(
            [source for _arm, source in ARMS],
            ["completion", "reachability", "queue_depth", "request_rtt"],
        )
        self.assertEqual(
            [case["name"] for case in FAULT_CASES],
            ["status_error", "path_delay", "common_delay"],
        )

    def test_run_command_changes_fault_not_allocator(self):
        for case in FAULT_CASES:
            cmd = build_run_command(
                "saps_completion",
                Path("/tmp/run"),
                case,
                "10.0.0.1",
                setup_target=False,
            )
            self.assertIn("--no-setup-arm1", cmd)
            self.assertEqual(cmd[cmd.index("--fault-type") + 1], case["fault_type"])
            self.assertEqual(cmd[cmd.index("--link-cap") + 1], "900000")
            self.assertEqual(
                cmd[cmd.index("--path-capacities-iops") + 1],
                "500000,500000,500000",
            )

    def test_provenance_requires_all_campaign_hashes(self):
        with self.assertRaisesRegex(RuntimeError, "missing provenance fields"):
            require_provenance(
                {
                    "provenance": {
                        "bdevperf_sha256": "bdevperf",
                        "dpa_firmware_sha256": "firmware",
                    }
                },
                Path("/tmp/run"),
            )

    def test_provenance_returns_complete_campaign_hashes(self):
        provenance = {
            "bdevperf_sha256": "bdevperf",
            "dpa_firmware_sha256": "firmware",
            "harness_sha256": "harness",
        }
        self.assertEqual(
            require_provenance({"provenance": provenance}, Path("/tmp/run")),
            provenance,
        )

    def test_completion_profile_keeps_classifier_enabled(self):
        profile = env_map(
            saps_q_env(
                tenant_id=0,
                n_tenants=1,
                link_cap=900_000,
                weights=[1],
                path_capacities_iops=[500_000, 500_000, 500_000],
                sample_rate=16,
                health_source="completion",
            )
        )
        self.assertEqual(profile["SAPSQ_BYPASS_D_CLASSIFIER"], "0")
        self.assertEqual(profile["SAPSQ_BYPASS_SAPS_FSM"], "0")
        self.assertEqual(profile["SAPSQ_HEALTH_SOURCE"], "completion")

    def test_reachability_profile_keeps_reachable_paths_healthy(self):
        profile = env_map(
            saps_q_env(
                tenant_id=0,
                n_tenants=1,
                link_cap=900_000,
                weights=[1],
                path_capacities_iops=[500_000, 500_000, 500_000],
                sample_rate=16,
                health_source="reachability",
            )
        )
        self.assertEqual(profile["SAPSQ_BYPASS_D_CLASSIFIER"], "1")
        self.assertEqual(profile["SAPSQ_BYPASS_SAPS_FSM"], "1")
        self.assertEqual(profile["SAPSQ_HEALTH_SOURCE"], "completion")


if __name__ == "__main__":
    unittest.main()
