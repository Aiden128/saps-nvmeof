#!/usr/bin/env python3

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_overhead_qd_campaign import overhead_summary


class OverheadSummaryTest(unittest.TestCase):
    def test_summary_requires_both_arms(self):
        with self.assertRaisesRegex(RuntimeError, "missing overhead arms: saps"):
            overhead_summary(
                [
                    {
                        "queue_depth": 32,
                        "arm": "baseline",
                        "aggregate_median_iops": 1_000_000,
                    }
                ],
                [32],
            )

    def test_summary_uses_per_arm_medians(self):
        summary = overhead_summary(
            [
                {
                    "queue_depth": 32,
                    "arm": "baseline",
                    "aggregate_median_iops": 1_000_000,
                },
                {
                    "queue_depth": 32,
                    "arm": "baseline",
                    "aggregate_median_iops": 900_000,
                },
                {
                    "queue_depth": 32,
                    "arm": "saps",
                    "aggregate_median_iops": 950_000,
                },
                {
                    "queue_depth": 32,
                    "arm": "saps",
                    "aggregate_median_iops": 850_000,
                },
            ],
            [32],
        )
        self.assertEqual(summary[0]["baseline_median_iops"], 950_000)
        self.assertEqual(summary[0]["saps_median_iops"], 900_000)
        self.assertAlmostEqual(
            summary[0]["throughput_overhead_pct"],
            100.0 * 50_000 / 950_000,
        )


if __name__ == "__main__":
    unittest.main()
