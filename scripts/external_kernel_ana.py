#!/usr/bin/env python3
"""External baseline: ANA-only path failover, analogous to kernel NVMe ANA.

Latency is recorded but never enters the decision.  Traffic is rerouted only
when the target-reported ANA state enters the configured hard-state set.  This
intentionally misses fail-slow paths whose ANA state stays optimized.
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, Mapping, Set, Tuple

from external_baseline_common import (
    ControlState,
    Decision,
    PathSample,
    add_common_arguments,
    nonnegative_float,
    run_policy,
)


KNOWN_ANA_STATES = {
    "optimized", "non_optimized", "inaccessible", "persistent_loss", "change",
}
DEFAULT_BAD_STATES = "inaccessible,persistent_loss,change"


class KernelAnaPolicy:
    name = "kernel-ANA"
    requires_path_iostat = False

    def __init__(self, bad_states: Set[str], hold_down_s: float):
        self.bad_states = set(bad_states)
        self.hold_down_s = hold_down_s
        self.last_ana: Dict[Tuple[str, str], str] = {}

    def describe(self) -> Mapping[str, Any]:
        return {
            "bad_ana_states": sorted(self.bad_states),
            "hold_down_s": self.hold_down_s,
            "uses_latency_for_decision": False,
            "cross_path_consensus": False,
        }

    def observe(self, sample: PathSample, control: ControlState,
                now: float) -> Decision:
        key = (sample.tenant, sample.path_key)
        previous = self.last_ana.get(key, "unknown")
        if sample.ana_observed:
            self.last_ana[key] = sample.ana_state
        metrics = {
            "previous_ana_state": previous,
            "observed_ana_state": sample.ana_state,
        }

        if control.disabled_by_us:
            if sample.ana_observed and sample.ana_state not in self.bad_states:
                return Decision("enable", "ANA returned to an allowed state", metrics)
            disabled_for = now - (control.disabled_since or now)
            if disabled_for >= self.hold_down_s:
                return Decision("enable", "ANA path health probe after hold-down", metrics)
            return Decision(metrics=metrics)

        if not sample.ana_observed:
            return Decision(reason="ANA not reported; latency intentionally ignored", metrics=metrics)
        transitioned = previous != "unknown" and previous != sample.ana_state
        is_reopen_probe = control.last_enabled_at is not None
        if sample.ana_state in self.bad_states and (transitioned or is_reopen_probe):
            return Decision(
                "disable",
                f"ANA hard-state transition {previous}->{sample.ana_state}",
                metrics,
            )
        if sample.ana_state not in self.bad_states:
            control.last_enabled_at = None
        return Decision(metrics=metrics)


def _ana_states(value: str) -> Set[str]:
    states = {item.strip().lower() for item in value.split(",") if item.strip()}
    unknown = states - KNOWN_ANA_STATES
    if not states:
        raise argparse.ArgumentTypeError("must contain at least one ANA state")
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown ANA states: {','.join(sorted(unknown))}")
    return states


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Host-side ANA-only multipath baseline (latency-blind)",
    )
    add_common_arguments(parser)
    policy = parser.add_argument_group("kernel ANA policy")
    policy.add_argument(
        "--bad-ana-states", type=_ana_states, default=_ana_states(DEFAULT_BAD_STATES),
        help=f"comma-separated states that reroute away from a path (default: {DEFAULT_BAD_STATES})",
    )
    policy.add_argument("--hold-down-s", type=nonnegative_float, default=3.0,
                        help="time before reconnecting a removed path to re-read ANA")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    policy = KernelAnaPolicy(args.bad_ana_states, args.hold_down_s)
    return run_policy(parser, args, policy)


if __name__ == "__main__":
    raise SystemExit(main())
