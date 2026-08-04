#!/usr/bin/env python3

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_summit_v2_campaign import (
    majority_path_flags,
    recovered_healthy_state,
    scheduler_execution_state_ok,
    select_pre_fault_path_rows,
    selector_budget_consistency,
)


class SelectorBudgetConsistencyTest(unittest.TestCase):
    def test_proportional_delivery_passes(self):
        passed, used = selector_budget_consistency(
            {"A": 300_000, "B": 8_000, "C": 300_000},
            {"A": 296_000, "B": 8_200, "C": 297_000},
        )
        self.assertTrue(passed)
        self.assertEqual(used, {"A": True, "B": True, "C": True})

    def test_nonzero_budget_without_delivery_fails(self):
        passed, used = selector_budget_consistency(
            {"A": 300_000, "B": 8_000, "C": 300_000},
            {"A": 300_000, "B": 0, "C": 300_000},
        )
        self.assertFalse(passed)
        self.assertFalse(used["B"])

    def test_probe_budget_with_probe_delivery_passes(self):
        passed, used = selector_budget_consistency(
            {"A": 450_000, "B": 3_992, "C": 450_000},
            {"A": 446_000, "B": 661, "C": 446_000},
            {"A": False, "B": True, "C": False},
        )
        self.assertTrue(passed)
        self.assertTrue(used["B"])

    def test_probe_budget_without_probe_delivery_fails(self):
        passed, used = selector_budget_consistency(
            {"A": 450_000, "B": 3_992, "C": 450_000},
            {"A": 446_000, "B": 0, "C": 446_000},
            {"A": False, "B": True, "C": False},
        )
        self.assertFalse(passed)
        self.assertFalse(used["B"])

    def test_zero_budget_path_is_not_required(self):
        passed, used = selector_budget_consistency(
            {"A": 450_000, "B": 0, "C": 450_000},
            {"A": 445_000, "B": 0, "C": 445_000},
        )
        self.assertTrue(passed)
        self.assertTrue(used["B"])

    def test_pre_fault_window_uses_observed_event_times(self):
        rows = [
            {"elapsed_s": 0.7},
            {"elapsed_s": 3.7},
            {"elapsed_s": 5.3},
            {"elapsed_s": 6.8},
        ]
        selected = select_pre_fault_path_rows(
            rows,
            collector_start_unix_s=100.0,
            perform_seen_unix_s=100.17,
            inject_started_unix_s=105.17,
            fallback_window_s=5.0,
        )
        self.assertEqual([row["elapsed_s"] for row in selected], [0.7, 3.7])

    def test_probe_state_uses_post_settle_majority(self):
        rows = (
            [[False, True, False]] * 13
            + [[False, False, False]] * 2
        )
        self.assertEqual(
            majority_path_flags(rows),
            [False, True, False],
        )

    def test_recovery_requires_three_final_healthy_samples(self):
        samples = [
            {"health": [65536, 0, 65536]},
            {"health": [65536, 65536, 65536]},
            {"health": [65536, 65536, 65536]},
            {"health": [65536, 65536, 65536]},
        ]
        self.assertTrue(recovered_healthy_state(samples, "health"))
        samples[-1]["health"] = [65536, 57344, 65536]
        self.assertFalse(recovered_healthy_state(samples, "health"))

    def test_execution_gate_does_not_require_a_degraded_outcome(self):
        observation = {
            "pre_fault_healthy_all_samples": True,
            "degraded_path_b_state_observed": False,
            "live_degraded_path_b_state_observed": False,
        }
        self.assertTrue(scheduler_execution_state_ok(observation))
        observation["pre_fault_healthy_all_samples"] = False
        self.assertFalse(scheduler_execution_state_ok(observation))


if __name__ == "__main__":
    unittest.main()
