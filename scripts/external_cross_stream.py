#!/usr/bin/env python3
"""Host-side -cross-stream ablation: path-local NEWMA, no peer consensus.

This sidecar keeps one dual-EWMA change detector for every tenant/path pair.
It deliberately never compares a path with its peers.  A common-mode latency
shift can therefore make every path independently request reroute, matching
the ablation's intended blind spot.  The implementation observes an already
running workload and never starts a workload or target.

The DPA implementation updates NEWMA per completion.  This host-side arm uses
interval mean latency from bdev_nvme_get_path_iostat, so it is a behavioral
ablation for the evaluation matrix, not a bit-identical firmware execution.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Tuple

from external_baseline_common import (
    ControlState,
    Decision,
    PathSample,
    add_common_arguments,
    nonnegative_float,
    positive_float,
    run_policy,
)


@dataclass
class PathLocalNewmaState:
    samples: int = 0
    fast_log2_us: float = 0.0
    slow_log2_us: float = 0.0


class MinusCrossStreamPolicy:
    name = "minus-cross-stream"
    requires_path_iostat = True

    def __init__(self, fast_alpha: float, slow_alpha: float,
                 distance_threshold_log2: float, warmup_samples: int,
                 min_interval_ops: int, min_latency_us: float,
                 hold_down_s: float, probe_grace_s: float):
        self.fast_alpha = fast_alpha
        self.slow_alpha = slow_alpha
        self.distance_threshold_log2 = distance_threshold_log2
        self.warmup_samples = warmup_samples
        self.min_interval_ops = min_interval_ops
        self.min_latency_us = min_latency_us
        self.hold_down_s = hold_down_s
        self.probe_grace_s = probe_grace_s
        self.states: Dict[Tuple[str, str], PathLocalNewmaState] = {}

    def describe(self) -> Mapping[str, Any]:
        return {
            "figure_label": "-cross-stream",
            "detector": "path-local dual-EWMA NEWMA on log2 interval latency",
            "fast_alpha": self.fast_alpha,
            "slow_alpha": self.slow_alpha,
            "distance_threshold_log2": self.distance_threshold_log2,
            "warmup_samples": self.warmup_samples,
            "min_interval_ops": self.min_interval_ops,
            "min_latency_us": self.min_latency_us,
            "hold_down_s": self.hold_down_s,
            "probe_grace_s": self.probe_grace_s,
            "cross_path_consensus": False,
            "cross_tenant_pooling": False,
            "common_mode_suppression": False,
            "firmware_bit_identical": False,
        }

    @staticmethod
    def _metrics(state: PathLocalNewmaState) -> Dict[str, Any]:
        return {
            "newma_fast_log2_us": state.fast_log2_us,
            "newma_slow_log2_us": state.slow_log2_us,
            "newma_distance_log2": abs(state.fast_log2_us - state.slow_log2_us),
            "adaptive_samples": state.samples,
        }

    def observe(self, sample: PathSample, control: ControlState,
                now: float) -> Decision:
        key = (sample.tenant, sample.path_key)
        state = self.states.setdefault(key, PathLocalNewmaState())

        if control.disabled_by_us:
            disabled_for = now - (control.disabled_since or now)
            if disabled_for >= self.hold_down_s:
                # Match the SAPS NEWMA reset shape: anchor the fast estimate to
                # the slow estimate before reopening a path for an independent probe.
                state.fast_log2_us = state.slow_log2_us
                return Decision(
                    "enable",
                    "hold-down expired; prefer path for a path-local probe",
                    self._metrics(state),
                )
            return Decision(metrics=self._metrics(state))

        if control.last_enabled_at is not None and now - control.last_enabled_at < self.probe_grace_s:
            return Decision(reason="probe grace period", metrics=self._metrics(state))
        if sample.interval_ops is None or sample.interval_ops < self.min_interval_ops:
            return Decision(reason="insufficient interval I/O", metrics=self._metrics(state))
        if sample.interval_latency_us is None:
            return Decision(reason="no interval latency", metrics=self._metrics(state))

        value = math.log2(max(sample.interval_latency_us, self.min_latency_us))
        if state.samples == 0:
            state.fast_log2_us = value
            state.slow_log2_us = value
            state.samples = 1
            return Decision(reason="path-local NEWMA warmup", metrics=self._metrics(state))

        state.fast_log2_us += self.fast_alpha * (value - state.fast_log2_us)
        state.slow_log2_us += self.slow_alpha * (value - state.slow_log2_us)
        state.samples += 1
        metrics = self._metrics(state)
        if state.samples < self.warmup_samples:
            return Decision(reason="path-local NEWMA warmup", metrics=metrics)
        if metrics["newma_distance_log2"] > self.distance_threshold_log2:
            return Decision(
                "disable",
                "path-local NEWMA crossed threshold without consulting peer paths",
                metrics,
            )
        return Decision(metrics=metrics)


def _unit_interval(value: str) -> float:
    number = float(value)
    if not 0.0 < number <= 1.0:
        raise argparse.ArgumentTypeError("must be in (0, 1]")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Host-side -cross-stream ablation with independent path-local NEWMA",
    )
    add_common_arguments(parser)
    parser.set_defaults(multipath_policy="active_active", selector="round_robin")
    policy = parser.add_argument_group("path-local NEWMA policy")
    policy.add_argument("--fast-alpha", type=_unit_interval, default=0.10)
    policy.add_argument("--slow-alpha", type=_unit_interval, default=0.01)
    policy.add_argument("--distance-threshold-log2", type=positive_float, default=2.0)
    policy.add_argument("--warmup-samples", type=int, default=8)
    policy.add_argument("--min-interval-ops", type=int, default=32)
    policy.add_argument("--min-latency-us", type=positive_float, default=1.0)
    policy.add_argument("--hold-down-s", type=nonnegative_float, default=3.0)
    policy.add_argument("--probe-grace-s", type=nonnegative_float, default=1.0)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.fast_alpha <= args.slow_alpha:
        parser.error("--fast-alpha must be greater than --slow-alpha")
    if args.warmup_samples < 2:
        parser.error("--warmup-samples must be >= 2")
    if args.min_interval_ops <= 0:
        parser.error("--min-interval-ops must be > 0")
    policy = MinusCrossStreamPolicy(
        fast_alpha=args.fast_alpha,
        slow_alpha=args.slow_alpha,
        distance_threshold_log2=args.distance_threshold_log2,
        warmup_samples=args.warmup_samples,
        min_interval_ops=args.min_interval_ops,
        min_latency_us=args.min_latency_us,
        hold_down_s=args.hold_down_s,
        probe_grace_s=args.probe_grace_s,
    )
    return run_policy(parser, args, policy)


if __name__ == "__main__":
    raise SystemExit(main())
