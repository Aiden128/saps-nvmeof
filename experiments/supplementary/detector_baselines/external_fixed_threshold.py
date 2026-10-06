#!/usr/bin/env python3
"""External baseline: one fixed latency threshold per observed path.

Every path is judged independently against the same absolute threshold.  No
cross-path consensus is used, intentionally preserving the baseline's known
failure mode: a healthy but busy path can be classified as failed.
"""

from __future__ import annotations

import argparse
from typing import Any, Mapping

from external_baseline_common import (
    ControlState,
    Decision,
    PathSample,
    add_common_arguments,
    nonnegative_float,
    positive_float,
    run_policy,
)


class FixedThresholdPolicy:
    name = "fixed-threshold"
    requires_path_iostat = True

    def __init__(self, threshold_us: float, min_interval_ops: int,
                 hold_down_s: float, probe_grace_s: float):
        self.threshold_us = threshold_us
        self.min_interval_ops = min_interval_ops
        self.hold_down_s = hold_down_s
        self.probe_grace_s = probe_grace_s

    def describe(self) -> Mapping[str, Any]:
        return {
            "threshold_us": self.threshold_us,
            "min_interval_ops": self.min_interval_ops,
            "hold_down_s": self.hold_down_s,
            "probe_grace_s": self.probe_grace_s,
            "cross_path_consensus": False,
        }

    def observe(self, sample: PathSample, control: ControlState,
                now: float) -> Decision:
        metrics = {"threshold_us": self.threshold_us}
        if control.disabled_by_us:
            disabled_for = now - (control.disabled_since or now)
            if disabled_for >= self.hold_down_s:
                return Decision("enable", "hold-down expired; prefer path for an independent probe", metrics)
            return Decision(metrics=metrics)

        if control.last_enabled_at is not None and now - control.last_enabled_at < self.probe_grace_s:
            return Decision(reason="probe grace period", metrics=metrics)
        if sample.interval_ops is None or sample.interval_ops < self.min_interval_ops:
            return Decision(reason="insufficient interval I/O", metrics=metrics)
        if sample.interval_latency_us is None:
            return Decision(reason="no interval latency", metrics=metrics)
        if sample.interval_latency_us > self.threshold_us:
            return Decision(
                "disable",
                f"path latency {sample.interval_latency_us:.3f}us exceeded fixed threshold",
                metrics,
            )
        return Decision(metrics=metrics)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Host-side fixed-latency-threshold multipath baseline",
    )
    add_common_arguments(parser)
    policy = parser.add_argument_group("fixed-threshold policy")
    policy.add_argument("--threshold-us", type=positive_float, default=500.0,
                        help="absolute per-path mean latency threshold (default: 500us)")
    policy.add_argument("--min-interval-ops", type=int, default=32,
                        help="minimum completed I/O count before judging an interval")
    policy.add_argument("--hold-down-s", type=nonnegative_float, default=3.0,
                        help="time traffic stays on the alternate preferred path before probe")
    policy.add_argument("--probe-grace-s", type=nonnegative_float, default=1.0,
                        help="ignore latency immediately after re-enabling a path")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.min_interval_ops <= 0:
        parser.error("--min-interval-ops must be > 0")
    policy = FixedThresholdPolicy(
        args.threshold_us, args.min_interval_ops,
        args.hold_down_s, args.probe_grace_s,
    )
    return run_policy(parser, args, policy)


if __name__ == "__main__":
    raise SystemExit(main())
