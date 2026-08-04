#!/usr/bin/env python3
"""External baseline: independent per-path EWMA/deviation/CUSUM detectors.

Each path owns all detector state.  The implementation deliberately performs
no cross-path vote or common-mode suppression, preserving the baseline's known
failure mode: a fabric-wide latency shift can make every path look faulty.
"""

from __future__ import annotations

import argparse
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
class AdaptiveState:
    samples: int = 0
    ewma_us: float = 0.0
    ewma_dev_us: float = 0.0
    cusum_us: float = 0.0


class PerPathAdaptivePolicy:
    name = "per-path-adaptive"
    requires_path_iostat = True

    def __init__(self, alpha: float, deviation_alpha: float, sigma_k: float,
                 cusum_h: float, cusum_drift: float, min_deviation_us: float,
                 min_threshold_us: float, warmup_samples: int,
                 min_interval_ops: int, hold_down_s: float,
                 probe_grace_s: float):
        self.alpha = alpha
        self.deviation_alpha = deviation_alpha
        self.sigma_k = sigma_k
        self.cusum_h = cusum_h
        self.cusum_drift = cusum_drift
        self.min_deviation_us = min_deviation_us
        self.min_threshold_us = min_threshold_us
        self.warmup_samples = warmup_samples
        self.min_interval_ops = min_interval_ops
        self.hold_down_s = hold_down_s
        self.probe_grace_s = probe_grace_s
        self.states: Dict[Tuple[str, str], AdaptiveState] = {}

    def describe(self) -> Mapping[str, Any]:
        return {
            "alpha": self.alpha,
            "deviation_alpha": self.deviation_alpha,
            "sigma_k": self.sigma_k,
            "cusum_h": self.cusum_h,
            "cusum_drift": self.cusum_drift,
            "min_deviation_us": self.min_deviation_us,
            "min_threshold_us": self.min_threshold_us,
            "warmup_samples": self.warmup_samples,
            "min_interval_ops": self.min_interval_ops,
            "hold_down_s": self.hold_down_s,
            "probe_grace_s": self.probe_grace_s,
            "cross_path_consensus": False,
            "common_mode_suppression": False,
        }

    def _metrics(self, state: AdaptiveState) -> Dict[str, Any]:
        scale = max(state.ewma_dev_us, self.min_deviation_us)
        threshold = max(self.min_threshold_us, state.ewma_us + self.sigma_k * scale)
        return {
            "threshold_us": threshold,
            "ewma_us": state.ewma_us,
            "ewma_dev_us": state.ewma_dev_us,
            "cusum_us": state.cusum_us,
            "adaptive_samples": state.samples,
        }

    def observe(self, sample: PathSample, control: ControlState,
                now: float) -> Decision:
        key = (sample.tenant, sample.path_key)
        state = self.states.setdefault(key, AdaptiveState())
        metrics = self._metrics(state)
        if control.disabled_by_us:
            disabled_for = now - (control.disabled_since or now)
            if disabled_for >= self.hold_down_s:
                return Decision("enable", "hold-down expired; prefer this path for an independent probe", metrics)
            return Decision(metrics=metrics)

        if control.last_enabled_at is not None and now - control.last_enabled_at < self.probe_grace_s:
            return Decision(reason="probe grace period", metrics=metrics)
        if sample.interval_ops is None or sample.interval_ops < self.min_interval_ops:
            return Decision(reason="insufficient interval I/O", metrics=metrics)
        if sample.interval_latency_us is None:
            return Decision(reason="no interval latency", metrics=metrics)

        latency = sample.interval_latency_us
        if state.samples == 0:
            state.ewma_us = latency
            state.ewma_dev_us = 0.0
            state.samples = 1
            return Decision(reason="adaptive warmup", metrics=self._metrics(state))

        scale = max(state.ewma_dev_us, self.min_deviation_us)
        threshold = max(self.min_threshold_us, state.ewma_us + self.sigma_k * scale)
        residual = latency - state.ewma_us - self.cusum_drift * scale
        candidate_cusum = max(0.0, state.cusum_us + residual)

        if state.samples >= self.warmup_samples:
            triggered = latency > threshold and candidate_cusum > self.cusum_h * scale
            if triggered:
                state.cusum_us = 0.0
                metrics = self._metrics(state)
                metrics["observed_latency_us"] = latency
                metrics["cusum_us"] = candidate_cusum
                return Decision(
                    "disable",
                    "independent EWMA/CUSUM detector crossed its per-path threshold",
                    metrics,
                )

        # A triggered outlier is not learned into the baseline.  Normal/warmup
        # samples update only this path's state.
        old_mean = state.ewma_us
        state.ewma_us = (1.0 - self.alpha) * state.ewma_us + self.alpha * latency
        abs_deviation = abs(latency - old_mean)
        state.ewma_dev_us = (
            (1.0 - self.deviation_alpha) * state.ewma_dev_us
            + self.deviation_alpha * abs_deviation
        )
        state.cusum_us = candidate_cusum
        state.samples += 1
        reason = "adaptive warmup" if state.samples < self.warmup_samples else ""
        return Decision(reason=reason, metrics=self._metrics(state))


def _unit_interval(value: str) -> float:
    number = float(value)
    if not 0.0 < number <= 1.0:
        raise argparse.ArgumentTypeError("must be in (0, 1]")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Host-side independent per-path EWMA/CUSUM multipath baseline",
    )
    add_common_arguments(parser)
    # All paths need healthy pre-fault samples to own meaningful detector state.
    parser.set_defaults(multipath_policy="active_active", selector="round_robin")
    policy = parser.add_argument_group("per-path adaptive policy")
    policy.add_argument("--alpha", type=_unit_interval, default=0.20)
    policy.add_argument("--deviation-alpha", type=_unit_interval, default=0.20)
    policy.add_argument("--sigma-k", type=positive_float, default=3.0)
    policy.add_argument("--cusum-h", type=positive_float, default=5.0)
    policy.add_argument("--cusum-drift", type=nonnegative_float, default=0.5)
    policy.add_argument("--min-deviation-us", type=positive_float, default=10.0)
    policy.add_argument("--min-threshold-us", type=positive_float, default=100.0)
    policy.add_argument("--warmup-samples", type=int, default=8)
    policy.add_argument("--min-interval-ops", type=int, default=32)
    policy.add_argument("--hold-down-s", type=nonnegative_float, default=3.0)
    policy.add_argument("--probe-grace-s", type=nonnegative_float, default=1.0)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.warmup_samples < 2:
        parser.error("--warmup-samples must be >= 2")
    if args.min_interval_ops <= 0:
        parser.error("--min-interval-ops must be > 0")
    policy = PerPathAdaptivePolicy(
        alpha=args.alpha,
        deviation_alpha=args.deviation_alpha,
        sigma_k=args.sigma_k,
        cusum_h=args.cusum_h,
        cusum_drift=args.cusum_drift,
        min_deviation_us=args.min_deviation_us,
        min_threshold_us=args.min_threshold_us,
        warmup_samples=args.warmup_samples,
        min_interval_ops=args.min_interval_ops,
        hold_down_s=args.hold_down_s,
        probe_grace_s=args.probe_grace_s,
    )
    return run_policy(parser, args, policy)


if __name__ == "__main__":
    raise SystemExit(main())
