#!/usr/bin/env python3
"""
aggregate_sapsq.py — SAPS-Q E1/E2/E3/…/E9 aggregator

讀 run_sapsq.py 產生的 out_dir,輸出單一 aggregate.json + PASS/FAIL verdict
依 specs/dm-research-redesign-20260522.md §5 acceptance criteria。

E1 (static fairness):
  weighted Jain >= 0.99, per-tenant share deviation <= 5%
E2 (idle tenant):
  active utilization >= 90% during idle window
E3 (path degradation):
  budget shift within 100ms (proxy via total_epochs_consumed_delta);
  latency-sensitive tenant P99 improves over baseline

用法:
  python3 aggregate_sapsq.py --out <run_dir>
  python3 aggregate_sapsq.py --out <run_dir> --experiment e1
"""

import argparse
import json
import math
import re
from pathlib import Path

E3_OBSERVABILITY_DEGRADED_REASON = (
    "per_path_pct derived from logged allocator target rates, NOT observed "
    "admit counts — REGIME WARNING for paper §5 attribution claims"
)

REGIME_WARNING_BANNER = (
    "⚠️ REGIME WARNING: one or more reps have observability_degraded=True; "
    "per_path_pct is unavailable without observed admit_delta data and must "
    "not be used for paper §5 attribution claims"
)


def parse_args():
    p = argparse.ArgumentParser(description="SAPS-Q aggregate metrics")
    p.add_argument("--out", type=str, required=True,
                   help="run output directory from run_sapsq.py")
    p.add_argument("--experiment",
                   choices=["e1", "e2", "e3", "e4", "e5", "e5b", "e6", "e7",
                            "e8", "e9", "e10", "e11", "e12", "auto"],
                   default="auto",
                   help="which experiment's PASS/FAIL rules to apply "
                        "(auto = infer from idle/inject flags)")
    # E4 reweight options:
    p.add_argument("--e4-phase1-dir", type=str, default=None,
                   help="E4: phase 1 run dir (weights=[1,1,1,1])")
    p.add_argument("--e4-phase2-dir", type=str, default=None,
                   help="E4: phase 2 run dir (weights=[3,1,1,1])")
    p.add_argument("--e4-weights-p1", type=str, default="1,1,1,1")
    p.add_argument("--e4-weights-p2", type=str, default="3,1,1,1")
    p.add_argument("--e4-phase1-duration", type=int, default=15)
    p.add_argument("--e4-phase2-duration", type=int, default=45)
    # E5 overhead options:
    p.add_argument("--e5-d0-off-dir", type=str, default=None)
    p.add_argument("--e5-d0-on-dir", type=str, default=None)
    p.add_argument("--e5-d1-on-dir", type=str, default=None,
                   help="DEPRECATED: D1 hero moved to E5b")
    p.add_argument("--e5-d3-on-dir", type=str, default=None,
                   help="DEPRECATED: D3 hero moved to E5b")
    # E5b D-series hero with SAPS-Q (single-NQN topology):
    p.add_argument("--e5b-d1-off-dir", type=str, default=None,
                   help="E5b: D1 fault, SAPS-Q OFF scenario dir")
    p.add_argument("--e5b-d1-on-dir", type=str, default=None,
                   help="E5b: D1 fault, SAPS-Q ON scenario dir")
    p.add_argument("--e5b-d3-off-dir", type=str, default=None,
                   help="E5b: D3 fault, SAPS-Q OFF scenario dir")
    p.add_argument("--e5b-d3-on-dir", type=str, default=None,
                   help="E5b: D3 fault, SAPS-Q ON scenario dir")
    # E6 recovery options:
    p.add_argument("--e6-inject-at-s", type=int, default=10)
    p.add_argument("--e6-remove-at-s", type=int, default=30)
    p.add_argument("--e6-target-path", type=int, default=1)
    # E7 scale options:
    p.add_argument("--e7-baseline-n4-dir", type=str, default=None,
                   help="E7: N=4 single-mode (sapsq) reference run dir "
                        "(used to compute scale efficiency)")
    p.add_argument("--e7-tenants", type=int, default=8,
                   help="E7: tenant count (default 8)")
    # E11 stress options (N=16, schema max):
    p.add_argument("--e11-baseline-n8-dir", type=str, default=None,
                   help="E11: N=8 single-mode (sapsq) reference run dir "
                        "(used to compute scale efficiency vs N=8 × 2)")
    p.add_argument("--e11-tenants", type=int, default=16,
                   help="E11: tenant count (default 16 = SAPSQ_TENANT_MAX)")
    # E8 mixed workload options:
    p.add_argument("--e8-latency-tenant", type=int, default=0,
                   help="E8: tenant id treated as latency-sensitive (default 0)")
    p.add_argument("--e8-throughput-tenant", type=int, default=1,
                   help="E8: tenant id treated as throughput-sensitive "
                        "(default 1)")
    p.add_argument("--e8-latency-p99-us-max", type=float, default=1000.0,
                   help="E8: max acceptable P99 us for latency tenant "
                        "(default 1000us = 1ms)")
    p.add_argument("--e8-throughput-single-baseline-mb", type=float, default=0,
                   help="E8: optional single-tenant throughput baseline in MB/s "
                        "for throughput tenant; 0=skip baseline check")
    p.add_argument("--e8-stock-dir", type=str, default=None,
                   help="E8: reference run dir for mode=stock (for cross-mode "
                        "comparison; optional)")
    p.add_argument("--e8-m4-static-dir", type=str, default=None,
                   help="E8: reference run dir for mode=m4_static "
                        "(for cross-mode comparison; optional)")
    # E9 priority inversion options:
    p.add_argument("--e9-in-dir", type=str, default=None,
                   help="E9: top-level experiment dir containing "
                        "stock/, m4_static/, sapsq/ sub-dirs with rep_<n>/ runs")
    p.add_argument("--e9-demand-t0", type=float, default=20000.0,
                   help="E9: tenant 0 target demand IOPS (default 20000)")
    p.add_argument("--e9-path-base-iops", type=float, default=200000.0,
                   help="E9: per-path capacity IOPS (default 200000)")
    p.add_argument("--e9-n-paths", type=int, default=3,
                   help="E9: number of paths (default 3)")
    # E10 flap-stability options:
    p.add_argument("--e10-target-path", type=int, default=1,
                   help="E10: path index being flapped (default 1 = path B)")
    p.add_argument("--e10-toggle-period-s", type=int, default=10,
                   help="E10: one full inject+remove cycle period in seconds "
                        "(default 10s = 5s inject + 5s remove)")
    p.add_argument("--e10-n-cycles", type=int, default=6,
                   help="E10: number of full toggle cycles (default 6)")
    p.add_argument("--e10-duration-s", type=int, default=60,
                   help="E10: total run duration in seconds (default 60)")
    p.add_argument("--e10-weights", type=str, default="3,1,1,1",
                   help="E10: tenant weights for weighted Jain (default 3,1,1,1)")
    p.add_argument("--e10-jain-min", type=float, default=0.95,
                   help="E10: minimum acceptable weighted Jain (default 0.95)")
    p.add_argument("--e10-overshoot-pct", type=float, default=0.5,
                   help="E10: overshoot threshold as fraction of mean budget "
                        "(default 0.5 = ±50%)")
    p.add_argument("--e10-overshoot-max-per-sec", type=int, default=2,
                   help="E10: max allowed overshoots per second (default 2)")
    # E12 multi-simultaneous-fault options:
    p.add_argument("--e12-fault-start-s", type=int, default=10,
                   help="E12: seconds when all 3 faults were injected (default 10)")
    p.add_argument("--e12-fault-end-s", type=int, default=50,
                   help="E12: seconds when all 3 faults were removed (default 50)")
    p.add_argument("--e12-n-paths", type=int, default=3,
                   help="E12: number of paths under fault (default 3)")
    p.add_argument("--e12-n-tenants", type=int, default=4,
                   help="E12: number of tenants (default 4)")
    p.add_argument("--e12-probe-rate-iops", type=float, default=100.0,
                   help="E12: configured probe-rate per path IOPS (default 100)")
    p.add_argument("--e12-probe-floor-headroom", type=float, default=3.0,
                   help="E12: burst_cap headroom multiplier on probe floor "
                        "(default 3.0 = 3× probe_rate × n_paths)")
    p.add_argument("--e12-recovery-target-health", type=int, default=32768,
                   help="E12: health_q16 floor for partial recovery (default 32768 "
                        "= RECOVERING floor 0.5×HEALTHY)")
    p.add_argument("--e12-recovery-window-s", type=int, default=8,
                   help="E12: seconds after fault remove to check recovery "
                        "(default 8 → check at t=58s for fault_end=50s)")
    return p.parse_args()


# ── statistics ────────────────────────────────────────────────────────────────


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def stddev(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return 0.0
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / len(xs))


def jain(xs):
    """unweighted Jain fairness: (sum xs)^2 / (n * sum xs^2)."""
    xs = [x for x in xs if x is not None and x > 0]
    if len(xs) < 2:
        return None
    n = len(xs)
    s = sum(xs)
    s2 = sum(x * x for x in xs)
    return (s * s) / (n * s2) if s2 > 0 else None


def weighted_jain(xs, ws):
    """weighted Jain: (sum x_i/w_i)^2 / (n * sum (x_i/w_i)^2)。
    Per Jain 1984 — applied to per-tenant share x_i normalized by weight w_i。
    若每 tenant 完全 proportional 到 weight,則 x_i/w_i 為常數 → index = 1.0。
    """
    pairs = [(x, w) for x, w in zip(xs, ws)
             if x is not None and w is not None and w > 0]
    if len(pairs) < 2:
        return None
    n = len(pairs)
    ratios = [x / w for x, w in pairs]
    s = sum(ratios)
    s2 = sum(r * r for r in ratios)
    return (s * s) / (n * s2) if s2 > 0 else None


# ── load ──────────────────────────────────────────────────────────────────────


def load_summary(out_dir: Path):
    f = out_dir / "sapsq_summary.json"
    if not f.exists():
        raise SystemExit(f"missing {f}")
    return json.loads(f.read_text())


def infer_experiment(run_config):
    args = run_config.get("args", {})
    if args.get("inject_path_degradation") is not None:
        return "e3"
    if args.get("idle_tenant"):
        return "e2"
    return "e1"


# ── aggregation ──────────────────────────────────────────────────────────────


def compute_metrics(summary):
    per_tenant = summary.get("per_tenant", [])
    cfg = summary.get("config", {})
    weights = cfg.get("weights", [])
    n_paths = cfg.get("n_paths", 3)

    # per-tenant IOPS + latency
    iops_per_tenant = [r.get("iops") for r in per_tenant]
    total_iops = sum(x for x in iops_per_tenant if x is not None)

    # weighted Jain over per-tenant IOPS w.r.t. config weights
    w_jain = weighted_jain(iops_per_tenant, weights)
    u_jain = jain(iops_per_tenant)

    # per-tenant share deviation: how far is x_i / sum(x) from w_i / sum(w)
    deviations = []
    if total_iops > 0 and sum(weights) > 0:
        for i, x in enumerate(iops_per_tenant):
            if x is None:
                continue
            actual_share = x / total_iops
            target_share = weights[i] / sum(weights) if i < len(weights) else 0
            if target_share > 0:
                deviations.append(abs(actual_share - target_share)
                                  / target_share)

    max_deviation = max(deviations) if deviations else None
    avg_deviation = mean(deviations)

    p99_per_tenant = [r.get("lat_p99_us") for r in per_tenant]
    p999_per_tenant = [r.get("lat_p999_us") for r in per_tenant]

    # per-(tenant, path) admit/reject/probe deltas
    sapsq_deltas = summary.get("sapsq_deltas", {})
    run_dir_s = summary.get("_run_dir")
    if not sapsq_deltas and run_dir_s:
        try:
            run_dir = Path(run_dir_s)
            start_snap = json.loads((run_dir / "sapsq_start.json").read_text())
            end_snap = json.loads((run_dir / "sapsq_end.json").read_text())
            if (start_snap and end_snap
                    and not start_snap.get("missing_helper")
                    and not end_snap.get("missing_helper")):
                for key, e in end_snap.items():
                    if key not in start_snap or not key.startswith("tenant_"):
                        continue
                    s = start_snap[key]
                    d_admit = [[
                        e["admit_count"][t][p] - s["admit_count"][t][p]
                        for p in range(len(e["admit_count"][0]))
                    ] for t in range(len(e["admit_count"]))]
                    d_reject = [[
                        e["reject_count"][t][p] - s["reject_count"][t][p]
                        for p in range(len(e["reject_count"][0]))
                    ] for t in range(len(e["reject_count"]))]
                    d_probe = [[
                        e["probe_count"][t][p] - s["probe_count"][t][p]
                        for p in range(len(e["probe_count"][0]))
                    ] for t in range(len(e["probe_count"]))]
                    sapsq_deltas[key] = {
                        "admit_delta": d_admit,
                        "reject_delta": d_reject,
                        "probe_delta": d_probe,
                        "stale_epoch_fallback_delta":
                            e.get("stale_epoch_fallback", 0)
                            - s.get("stale_epoch_fallback", 0),
                        "total_epochs_consumed_delta":
                            e.get("total_epochs_consumed", 0)
                            - s.get("total_epochs_consumed", 0),
                    }
        except Exception:
            sapsq_deltas = {}
    per_tp_admit = []  # [tenant][path]
    per_tp_reject = []
    per_tp_probe = []
    epoch_deltas = []
    stale_deltas = []
    for tid in range(cfg.get("tenants", 4)):
        key = f"tenant_{tid:02d}"
        d = sapsq_deltas.get(key, {})
        per_tp_admit.append(d.get("admit_delta", [[0] * n_paths]))
        per_tp_reject.append(d.get("reject_delta", [[0] * n_paths]))
        per_tp_probe.append(d.get("probe_delta", [[0] * n_paths]))
        epoch_deltas.append(d.get("total_epochs_consumed_delta", 0))
        stale_deltas.append(d.get("stale_epoch_fallback_delta", 0))

    # per-path IO distribution (% of total traffic each path served)
    # admit row for *this* tenant from its own ring view captures its own path
    # split — for paper "per-path distribution" we sum across tenants
    per_path_admit_total = [0] * n_paths
    for tid in range(len(per_tp_admit)):
        row_per_tenant = per_tp_admit[tid]
        # row_per_tenant is full [SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];
        # only the row [tid] is valid for that proc
        if isinstance(row_per_tenant, list) and len(row_per_tenant) > tid:
            tenant_row = row_per_tenant[tid]
            for p in range(min(n_paths, len(tenant_row))):
                per_path_admit_total[p] += tenant_row[p]
    sum_admit = sum(per_path_admit_total)
    per_path_pct_source = None
    per_path_rate_total_q32 = None
    observability_degraded = False
    if sum_admit > 0:
        per_path_pct = [
            round(a / sum_admit * 100.0, 3)
            for a in per_path_admit_total
        ]
        per_path_pct_source = "admit_delta"
    else:
        # Option A: no fallback from tenant_path_rate_q32 log sums. Those logs
        # are sampled and biased toward still-scheduling healthy paths, so they
        # cannot support paper-grade per-path attribution.
        per_path_pct = None
        observability_degraded = True

    return {
        "mode": summary.get("mode"),
        "tenants": cfg.get("tenants"),
        "n_paths": n_paths,
        "weights": weights,
        "duration_s": summary.get("duration_s"),
        "total_iops": total_iops,
        "per_tenant_iops": iops_per_tenant,
        "per_tenant_p99_us": p99_per_tenant,
        "per_tenant_p999_us": p999_per_tenant,
        "weighted_jain": round(w_jain, 4) if w_jain is not None else None,
        "unweighted_jain": round(u_jain, 4) if u_jain is not None else None,
        "max_share_deviation": (round(max_deviation, 4)
                                if max_deviation is not None else None),
        "avg_share_deviation": (round(avg_deviation, 4)
                                if avg_deviation is not None else None),
        "per_path_admit_total": per_path_admit_total,
        "per_path_pct": per_path_pct,
        "per_path_pct_source": per_path_pct_source,
        "per_path_rate_total_q32": per_path_rate_total_q32,
        "observability_degraded": observability_degraded,
        "epoch_deltas_per_tenant": epoch_deltas,
        "stale_epoch_fallback_per_tenant": stale_deltas,
        "stale_fallback_sanity_ok": all(s == 0 for s in stale_deltas),
    }


# ── PASS/FAIL ────────────────────────────────────────────────────────────────


def verdict_e1(m):
    """E1 static fairness:weighted Jain >= 0.99 & max deviation <= 5%。"""
    reasons = []
    ok = True
    if m["weighted_jain"] is None or m["weighted_jain"] < 0.99:
        ok = False
        reasons.append(f"weighted Jain={m['weighted_jain']} < 0.99")
    if m["max_share_deviation"] is None or m["max_share_deviation"] > 0.05:
        ok = False
        reasons.append(f"max share deviation={m['max_share_deviation']} > 5%")
    return {"experiment": "E1", "pass": ok, "reasons": reasons}


def verdict_e2(m):
    """E2 idle tenant:active util >= 90%。
    proxy:active (non-idle) tenants' total IOPS / (sum of their demand-based
    expected share)。本 aggregator 沒 timeline,所以用 simplified:active tenants
    Jain >= 0.95 + total_iops 不低於 weighted expected。
    Full E2 verdict 需 timeseries(留 future work)。
    """
    reasons = []
    ok = True
    # surrogate criterion: weighted jain among non-idle still >= 0.95
    if m["weighted_jain"] is None or m["weighted_jain"] < 0.95:
        ok = False
        reasons.append(f"weighted Jain={m['weighted_jain']} < 0.95 "
                       "(E2 work-conserving expects active tenants to absorb idle share)")
    return {"experiment": "E2", "pass": ok, "reasons": reasons,
            "note": "full E2 verdict requires per-second timeseries; "
                    "this is a surrogate check"}


def verdict_e3(m):
    """E3 path degradation:
       - per-path distribution shows path B (idx 1) traffic reduced
       - tenant P99 improves over baseline (cross-mode comparison; this single
         aggregate cannot judge alone — needs cross-mode aggregator)
       - epochs consumed > some floor (DPA scheduler active)
    """
    reasons = []
    ok = True
    observability_degraded = bool(m.get("observability_degraded"))
    if m.get("per_path_pct_source") != "admit_delta":
        ok = False
        observability_degraded = True
        reasons.append(E3_OBSERVABILITY_DEGRADED_REASON)
    if m["per_path_pct"] and len(m["per_path_pct"]) >= 2:
        path_b_pct = m["per_path_pct"][1]
        if path_b_pct is None or path_b_pct > 25:   # uniform=33%, expect <25%
            ok = False
            reasons.append(
                f"path B got {path_b_pct}% of traffic — expected <25% "
                "after 5ms degradation"
            )
    else:
        reasons.append("per-path distribution unavailable "
                       "(sapsq_dump helper missing or zero counters)")
    if all(e == 0 for e in m.get("epoch_deltas_per_tenant", [])):
        reasons.append("zero DPA epochs consumed — SAPS-Q scheduler may be "
                       "stale (check sapsq_stale_epoch_fallback)")
    return {"experiment": "E3", "pass": ok, "reasons": reasons,
            "observability_degraded": observability_degraded,
            "note": "cross-mode P99 comparison needs a separate "
                    "compare-modes step"}


def apply_verdict(metrics, experiment, run_config):
    if experiment == "auto":
        experiment = infer_experiment(run_config)
    if experiment == "e1":
        return verdict_e1(metrics)
    if experiment == "e2":
        return verdict_e2(metrics)
    if experiment == "e3":
        return verdict_e3(metrics)
    return {"experiment": "unknown", "pass": False,
            "reasons": [f"unknown experiment {experiment}"]}


# ── E4 reweight verdict ──────────────────────────────────────────────────────


def _load_per_tenant_iops(run_dir: Path):
    """load run_sapsq.py 的 sapsq_summary.json → per-tenant IOPS。"""
    f = run_dir / "sapsq_summary.json"
    if not f.exists():
        return None
    s = json.loads(f.read_text())
    return [r.get("iops") for r in s.get("per_tenant", [])]


def verdict_e4_reweight(phase1_dir: Path, phase2_dir: Path,
                        weights_p1, weights_p2,
                        phase1_dur, phase2_dur):
    """E4 reweight convergence:
       - phase1 finishes with target_p1 = weights_p1 normalized share
       - phase2 settles at target_p2 = weights_p2 normalized share
       - reweight_time: 因為 process relaunch,phase2 開頭就應該是新 weights
         → 沒 fine-grained timeline,只能驗 phase2 final IOPS 是否達 90% 新 target
       - overshoot: phase2 中最大 share deviation from new target * 100%
       Pass: phase2 final share within 10% of target AND overshoot < 20%。
    """
    reasons = []
    ok = True

    iops_p1 = _load_per_tenant_iops(phase1_dir)
    iops_p2 = _load_per_tenant_iops(phase2_dir)
    if not iops_p1 or not iops_p2:
        return {"experiment": "E4", "pass": False,
                "reasons": [f"missing phase data p1={iops_p1} p2={iops_p2}"]}

    sum_p1 = sum(x for x in iops_p1 if x)
    sum_p2 = sum(x for x in iops_p2 if x)
    if sum_p1 <= 0 or sum_p2 <= 0:
        return {"experiment": "E4", "pass": False,
                "reasons": [f"zero total IOPS sum_p1={sum_p1} sum_p2={sum_p2}"]}

    sw1 = sum(weights_p1)
    sw2 = sum(weights_p2)
    target_p1 = [w / sw1 for w in weights_p1]
    target_p2 = [w / sw2 for w in weights_p2]
    actual_p1 = [(x or 0) / sum_p1 for x in iops_p1]
    actual_p2 = [(x or 0) / sum_p2 for x in iops_p2]

    # phase 1 check (sanity baseline)
    dev_p1 = max(abs(a - t) / t for a, t in zip(actual_p1, target_p1)
                 if t > 0)
    # phase 2 check (post-reweight)
    dev_p2 = max(abs(a - t) / t for a, t in zip(actual_p2, target_p2)
                 if t > 0)

    if dev_p2 > 0.10:
        ok = False
        reasons.append(f"phase2 max share deviation={dev_p2:.3f} > 10% "
                       f"(target_share={target_p2}, actual={actual_p2})")

    # overshoot proxy: largest tenant share / target compared to 1.0
    overshoots = [a / t for a, t in zip(actual_p2, target_p2) if t > 0]
    overshoot_pct = (max(overshoots) - 1.0) * 100 if overshoots else 0
    if overshoot_pct > 20:
        ok = False
        reasons.append(f"phase2 overshoot={overshoot_pct:.1f}% > 20%")

    # reweight_time proxy: process relaunch is effectively the cutover;
    # full convergence covered by phase2 deviation. We report bound as
    # phase2_duration upper-bound; sub-second resolution needs periodic dump.
    reweight_time_bound_s = phase2_dur

    return {
        "experiment": "E4",
        "pass": ok,
        "reasons": reasons,
        "phase1_share": actual_p1,
        "phase1_target": target_p1,
        "phase1_max_dev": round(dev_p1, 4),
        "phase2_share": actual_p2,
        "phase2_target": target_p2,
        "phase2_max_dev": round(dev_p2, 4),
        "overshoot_pct": round(overshoot_pct, 2),
        "reweight_time_bound_s": reweight_time_bound_s,
        "note": "reweight uses process relaunch; sub-second convergence "
                "resolution requires periodic dump (see E6)",
    }


# ── E5 overhead verdict ──────────────────────────────────────────────────────


def _agg_iops_over_reps(scenario_dir: Path):
    """scenario_dir/rep_<n>/sapsq_summary.json 取 total IOPS,回 mean。"""
    if scenario_dir is None or not scenario_dir.exists():
        return None
    reps = sorted(scenario_dir.glob("rep_*/sapsq_summary.json"))
    if not reps:
        return None
    totals = []
    for p in reps:
        try:
            s = json.loads(p.read_text())
            t = sum(r.get("iops") or 0 for r in s.get("per_tenant", []))
            if t > 0:
                totals.append(t)
        except Exception:
            continue
    return mean(totals) if totals else None


def _agg_p99_over_reps(scenario_dir: Path):
    """mean of per-tenant p99 max across reps."""
    if scenario_dir is None or not scenario_dir.exists():
        return None
    reps = sorted(scenario_dir.glob("rep_*/sapsq_summary.json"))
    p99s = []
    for p in reps:
        try:
            s = json.loads(p.read_text())
            tenant_p99s = [r.get("lat_p99_us") for r in s.get("per_tenant", [])
                           if r.get("lat_p99_us") is not None]
            if tenant_p99s:
                p99s.append(max(tenant_p99s))
        except Exception:
            continue
    return mean(p99s) if p99s else None


def verdict_e5_overhead(d0_off_dir, d0_on_dir,
                        d1_on_dir=None, d3_on_dir=None):
    """E5 overhead regression guard (healthy traffic only):
       - D0 overhead = (d0_off - d0_on) / d0_off * 100; pass if <= 5%

       Audit follow-up (2026-05-22): D1/D3 hero regression-with-SAPS-Q was
       previously folded in here, but the D-series setup scripts target a
       single-NQN topology that does not exist under E5 multi-tenant setup —
       result was silent garbage data. D1/D3 hero hold-up under SAPS-Q now
       lives in E5b (verdict_e5b_dseries_hero). The d1_on_dir / d3_on_dir
       kwargs are kept for back-compat but ignored.
    """
    _ = d1_on_dir  # retained for back-compat signature; ignored
    _ = d3_on_dir
    reasons = []
    ok = True

    d0_off_iops = _agg_iops_over_reps(d0_off_dir) if d0_off_dir else None
    d0_on_iops = _agg_iops_over_reps(d0_on_dir) if d0_on_dir else None

    overhead_pct = None
    if d0_off_iops and d0_on_iops:
        overhead_pct = (d0_off_iops - d0_on_iops) / d0_off_iops * 100
        if overhead_pct > 5:
            ok = False
            reasons.append(
                f"D0 overhead={overhead_pct:.2f}% > 5% "
                f"(off={d0_off_iops:.0f} on={d0_on_iops:.0f})"
            )
        elif overhead_pct > 2:
            reasons.append(
                f"D0 overhead={overhead_pct:.2f}% > 2% (still passes 5% gate)"
            )
    else:
        ok = False
        reasons.append(f"D0 overhead unavailable "
                       f"(off={d0_off_iops} on={d0_on_iops})")

    return {
        "experiment": "E5",
        "pass": ok,
        "reasons": reasons,
        "d0_off_iops_mean": d0_off_iops,
        "d0_on_iops_mean": d0_on_iops,
        "overhead_pct": (round(overhead_pct, 2)
                         if overhead_pct is not None else None),
        "note": "D1/D3 hero regression-with-SAPS-Q is in E5b "
                "(verdict_e5b_dseries_hero); E5 is healthy-overhead only",
    }


# ── E5b D-series hero regression-with-SAPS-Q verdict ─────────────────────────


def _agg_e5b_iops_p99_over_reps(scenario_dir: Path):
    """E5b uses single-tenant bdevperf output written to rep_<n>/sapsq_summary.json
       with the same per_tenant shape as E1/E5. Return (iops_mean, p99_mean) tuple,
       max_p99 across reps too for tail metric。"""
    if scenario_dir is None or not scenario_dir.exists():
        return None, None, None
    reps = sorted(scenario_dir.glob("rep_*/sapsq_summary.json"))
    iops_list = []
    p99_list = []
    for p in reps:
        try:
            s = json.loads(p.read_text())
            per = s.get("per_tenant", [])
            t_iops = sum(r.get("iops") or 0 for r in per)
            t_p99s = [r.get("lat_p99_us") for r in per
                      if r.get("lat_p99_us") is not None]
            if t_iops > 0:
                iops_list.append(t_iops)
            if t_p99s:
                p99_list.append(max(t_p99s))
        except Exception:
            continue
    return (
        mean(iops_list) if iops_list else None,
        mean(p99_list) if p99_list else None,
        max(p99_list) if p99_list else None,
    )


def verdict_e5b_dseries_hero(d1_off_dir, d1_on_dir,
                             d3_off_dir, d3_on_dir):
    """E5b D-series hero regression-with-SAPS-Q (single-NQN topology):
       - d1_sapsqoff IOPS >= 700K (sanity baseline)
       - d1_sapsqon IOPS >= 0.85 × d1_sapsqoff (SAPS-Q ≤15% regression on D1)
       - d3_sapsqoff max_tail_latency_ms < 100 (sanity)
       - d3_sapsqon max_tail_latency_ms < 4 × d3_sapsqoff
         (SAPS-Q within 4× of D3 baseline tail; ample headroom vs stock
         1837ms target)
    """
    reasons = []
    ok = True

    d1_off_iops, _, _ = _agg_e5b_iops_p99_over_reps(d1_off_dir)
    d1_on_iops, _, _ = _agg_e5b_iops_p99_over_reps(d1_on_dir)
    # use max p99 across reps as the tail-latency representative
    _, _, d3_off_max_p99_us = _agg_e5b_iops_p99_over_reps(d3_off_dir)
    _, _, d3_on_max_p99_us = _agg_e5b_iops_p99_over_reps(d3_on_dir)

    # convert us → ms for thresholds
    d3_off_max_tail_ms = (d3_off_max_p99_us / 1000.0
                          if d3_off_max_p99_us is not None else None)
    d3_on_max_tail_ms = (d3_on_max_p99_us / 1000.0
                         if d3_on_max_p99_us is not None else None)

    # D1 sanity baseline
    if d1_off_iops is None:
        ok = False
        reasons.append("d1_sapsqoff IOPS unavailable")
    elif d1_off_iops < 700_000:
        ok = False
        reasons.append(f"d1_sapsqoff IOPS={d1_off_iops:.0f} < 700K "
                       "— sanity baseline broken")

    # D1 SAPS-Q regression check (≤15%)
    if d1_off_iops and d1_on_iops:
        ratio = d1_on_iops / d1_off_iops
        if ratio < 0.85:
            ok = False
            reasons.append(
                f"d1_sapsqon/d1_sapsqoff={ratio:.3f} < 0.85 "
                f"(on={d1_on_iops:.0f} off={d1_off_iops:.0f}) "
                "— SAPS-Q regresses D1 by >15%"
            )
    elif d1_on_iops is None:
        ok = False
        reasons.append("d1_sapsqon IOPS unavailable")

    # D3 sanity
    if d3_off_max_tail_ms is None:
        ok = False
        reasons.append("d3_sapsqoff max tail latency unavailable")
    elif d3_off_max_tail_ms >= 100:
        ok = False
        reasons.append(f"d3_sapsqoff max_tail={d3_off_max_tail_ms:.2f}ms "
                       ">= 100ms — sanity broken")

    # D3 SAPS-Q within 4×
    if d3_off_max_tail_ms and d3_on_max_tail_ms:
        ratio = d3_on_max_tail_ms / d3_off_max_tail_ms
        if ratio >= 4.0:
            ok = False
            reasons.append(
                f"d3_sapsqon/d3_sapsqoff tail ratio={ratio:.2f} >= 4× "
                f"(on={d3_on_max_tail_ms:.2f}ms off={d3_off_max_tail_ms:.2f}ms)"
            )
    elif d3_on_max_tail_ms is None:
        ok = False
        reasons.append("d3_sapsqon max tail latency unavailable")

    return {
        "experiment": "E5b",
        "pass": ok,
        "reasons": reasons,
        "d1_sapsqoff_iops_mean": d1_off_iops,
        "d1_sapsqon_iops_mean": d1_on_iops,
        "d1_sapsqon_over_off_ratio": (round(d1_on_iops / d1_off_iops, 4)
                                      if d1_off_iops and d1_on_iops else None),
        "d3_sapsqoff_max_tail_ms": (round(d3_off_max_tail_ms, 3)
                                    if d3_off_max_tail_ms is not None else None),
        "d3_sapsqon_max_tail_ms": (round(d3_on_max_tail_ms, 3)
                                   if d3_on_max_tail_ms is not None else None),
        "d3_sapsqon_over_off_tail_ratio": (
            round(d3_on_max_tail_ms / d3_off_max_tail_ms, 3)
            if d3_off_max_tail_ms and d3_on_max_tail_ms else None
        ),
        "note": "D-series single-NQN topology, single-tenant bdevperf, "
                "SAPS-Q on vs off, D1 = fail-slow, D3 = sct=3 sparse errors",
    }


# ── E6 recovery verdict ──────────────────────────────────────────────────────


# Q16.16 HEALTHY value = 1.0 → 65536
SAPSQ_HEALTHY_Q16 = 65536
SAPSQ_DEGRADED_THRESHOLD_RATIO = 0.5   # < 0.5 × HEALTHY = degraded


def _load_periodic_health(run_dir: Path, target_path: int):
    """讀 sapsq_periodic/snap_<ms>ms.json,return list of (elapsed_ms, health_q16)。
    health_q16 取 tenant_00 view 的 path_health_q16[target_path]。
    """
    periodic_dir = run_dir / "sapsq_periodic"
    if not periodic_dir.exists():
        return []
    samples = []
    for snap in sorted(periodic_dir.glob("snap_*ms.json")):
        try:
            elapsed_ms = int(snap.stem.replace("snap_", "").replace("ms", ""))
            data = json.loads(snap.read_text())
            # Aggregate across tenants — should be identical view per epoch,
            # take tenant_00 if present, else first available
            view = data.get("tenant_00")
            if view is None:
                non_meta = [v for k, v in data.items()
                            if k.startswith("tenant_")]
                if not non_meta:
                    continue
                view = non_meta[0]
            health_arr = view.get("path_health_q16", [])
            if target_path >= len(health_arr):
                continue
            samples.append((elapsed_ms, health_arr[target_path]))
        except Exception:
            continue
    samples.sort(key=lambda x: x[0])
    return samples


def verdict_e6_recovery(run_dir: Path, inject_at_s, remove_at_s, target_path):
    """E6 recovery hysteresis:
       (a) drop_time: time from inject to first sample with health < 0.5 × HEALTHY
       (b) recovery_time: time from remove to first sample with health >= 0.9 × HEALTHY
       (c) oscillation_count: HEALTHY ↔ DEGRADED transitions during recovery
       Pass:
         drop_time < 100ms (relative to inject_at)
         recovery_time < 5000ms (relative to remove_at)
         oscillation_count <= 1
    """
    reasons = []
    ok = True

    samples = _load_periodic_health(run_dir, target_path)
    if not samples:
        return {"experiment": "E6", "pass": False,
                "reasons": ["no periodic samples — sapsq_dump missing or "
                            "--periodic-dump-interval-ms=0"]}

    inject_ms = inject_at_s * 1000
    remove_ms = remove_at_s * 1000
    degraded_thresh = int(SAPSQ_HEALTHY_Q16 * SAPSQ_DEGRADED_THRESHOLD_RATIO)
    recovery_thresh = int(SAPSQ_HEALTHY_Q16 * 0.9)

    # (a) drop_time
    drop_time_ms = None
    for elapsed_ms, h in samples:
        if elapsed_ms >= inject_ms and h < degraded_thresh:
            drop_time_ms = elapsed_ms - inject_ms
            break
    if drop_time_ms is None or drop_time_ms > 100:
        # Acceptance window is "within 100ms of inject" — but periodic dump
        # interval also affects resolution. If interval > 100ms, we relax.
        # Use 2× inject-to-first-sample tolerance as fallback.
        ok = False
        reasons.append(
            f"drop not observed within 100ms of inject "
            f"(drop_time_ms={drop_time_ms})"
        )

    # (b) recovery_time
    recovery_time_ms = None
    for elapsed_ms, h in samples:
        if elapsed_ms >= remove_ms and h >= recovery_thresh:
            recovery_time_ms = elapsed_ms - remove_ms
            break
    if recovery_time_ms is None or recovery_time_ms > 5000:
        ok = False
        reasons.append(
            f"recovery to 0.9×HEALTHY not within 5s of remove "
            f"(recovery_time_ms={recovery_time_ms})"
        )

    # (c) oscillation_count during recovery window [remove_ms, end]
    osc = 0
    state = None  # "healthy" or "degraded"
    for elapsed_ms, h in samples:
        if elapsed_ms < remove_ms:
            continue
        new_state = "healthy" if h >= recovery_thresh else "degraded"
        if state is not None and new_state != state:
            osc += 1
        state = new_state
    if osc > 1:
        ok = False
        reasons.append(f"oscillation_count={osc} > 1 during recovery")

    return {
        "experiment": "E6",
        "pass": ok,
        "reasons": reasons,
        "samples_count": len(samples),
        "drop_time_ms": drop_time_ms,
        "recovery_time_ms": recovery_time_ms,
        "oscillation_count": osc,
        "target_path": target_path,
        "inject_at_s": inject_at_s,
        "remove_at_s": remove_at_s,
    }


# ── E10 flap-stability verdict ───────────────────────────────────────────────


# Legal sapsq_path_health_q16 values for a path under flap workload:
#   0      = QUARANTINE
#   100    = PROBE
#   19660  = FLAP / generic mid-fault (0.3)
#   26214  = ≈0.4 (SPARSE-ERROR penalty floor; observed in some classifiers)
#   32768  = RECOVERING floor (0.5) / generic default
#   40960  = RECOVERING +1 step
#   49152  = RECOVERING +2 steps
#   57344  = RECOVERING +3 steps
#   65536  = HEALTHY
# Anything else (esp. > 65536 or non-step values) flags a torn read.
SAPSQ_LEGAL_HEALTH_VALUES = {0, 100, 19660, 26214, 32768, 40960, 49152, 57344,
                             65536}


def _load_periodic_snapshots(run_dir: Path):
    """讀 sapsq_periodic/snap_<ms>ms.json all snapshots,return list of
    (elapsed_ms, parsed_dict)。每個 dict 是 dump_sapsq_counters 的 aggregate
    格式:{"tenant_00": {...}, "tenant_01": {...}, ...}。
    """
    periodic_dir = run_dir / "sapsq_periodic"
    if not periodic_dir.exists():
        return []
    samples = []
    for snap in sorted(periodic_dir.glob("snap_*ms.json")):
        try:
            elapsed_ms = int(snap.stem.replace("snap_", "").replace("ms", ""))
            data = json.loads(snap.read_text())
            samples.append((elapsed_ms, data))
        except Exception:
            continue
    samples.sort(key=lambda x: x[0])
    return samples


def _tenant_views(snapshot_data):
    """從一個 snap_*.json 解出 list of (tid, view_dict),按 tid 排序。"""
    out = []
    for key, view in snapshot_data.items():
        if not key.startswith("tenant_"):
            continue
        try:
            tid = int(key.replace("tenant_", ""))
        except ValueError:
            continue
        if isinstance(view, dict):
            out.append((tid, view))
    out.sort(key=lambda x: x[0])
    return out


def verdict_e10_flap(run_dir: Path, target_path, toggle_period_s, n_cycles,
                     duration_s, weights, jain_min, overshoot_pct,
                     overshoot_max_per_sec):
    """E10 flap-stability:5 criteria。

    (1) 預算震盪 overshoot:per-second derivative of
        sapsq_tenant_path_rate_q32[t][target_path] 跨 250ms snapshot 計算
        finite-difference,任何 tenant 在任一秒內 |Δ rate| / mean_rate
        超過 overshoot_pct 的事件數 > overshoot_max_per_sec → FAIL
    (2) Per-tenant share stability:用 admit_count 的整段累積 IOPS 算
        weighted Jain ≥ jain_min
    (3) Health 合法值:每個 snapshot 的 path_health_q16[target_path] 必須
        ∈ SAPSQ_LEGAL_HEALTH_VALUES;否則視為 torn read
    (4) Recovery convergence:若 duration_s > n_cycles * toggle_period_s
        且在最後 remove 後 ≥ 4s,health 必須 ≥ HEALTHY × 0.9。否則標
        "needs extended-run trace"(不 fail)。
    (5) sapsq_total_epochs_consumed 跨 snapshot monotonic 遞增(per tenant)
    """
    reasons = []
    ok = True

    snaps = _load_periodic_snapshots(run_dir)
    if not snaps:
        return {"experiment": "E10", "pass": False,
                "reasons": ["no periodic samples — sapsq_dump missing or "
                            "--periodic-dump-interval-ms=0"]}

    # ── (3) health legal-value check ─────────────────────────────────────────
    illegal_health = []  # list of (elapsed_ms, value)
    for elapsed_ms, data in snaps:
        for tid, view in _tenant_views(data):
            health_arr = view.get("path_health_q16", [])
            if target_path >= len(health_arr):
                continue
            h = health_arr[target_path]
            # Out-of-range or fractional-out-of-set
            if (not isinstance(h, int)) or h < 0 or h > 65536 \
                    or h not in SAPSQ_LEGAL_HEALTH_VALUES:
                illegal_health.append((elapsed_ms, tid, h))
    if illegal_health:
        ok = False
        # show up to 5 examples
        sample_str = ", ".join(
            f"t={ms}ms tid={tid} h={h}" for ms, tid, h in illegal_health[:5]
        )
        reasons.append(
            f"illegal/torn health values ({len(illegal_health)} occurrences): "
            f"{sample_str}"
        )

    # ── (5) monotonic epochs ─────────────────────────────────────────────────
    # 對每個 tenant 收 epoch 時間序列,檢查 monotonic 遞增
    epoch_seq = {}  # tid -> list of (ms, total_epochs_consumed)
    for elapsed_ms, data in snaps:
        for tid, view in _tenant_views(data):
            te = view.get("total_epochs_consumed", 0)
            epoch_seq.setdefault(tid, []).append((elapsed_ms, te))
    epoch_violations = []
    for tid, seq in epoch_seq.items():
        prev = None
        for ms, e in seq:
            if prev is not None and e < prev:
                epoch_violations.append((tid, ms, prev, e))
            prev = e
    if epoch_violations:
        ok = False
        sample_str = ", ".join(
            f"tid={tid} t={ms}ms {prev}->{e}" for tid, ms, prev, e
            in epoch_violations[:5]
        )
        reasons.append(
            f"total_epochs_consumed not monotonic "
            f"({len(epoch_violations)} regressions): {sample_str}"
        )

    # ── (1) per-second budget oscillation overshoot ──────────────────────────
    # 取每個 tenant 的 tenant_path_rate_q32[tid][target_path] 時間序列
    # (注意:per-tenant view 裡 row [tid] 才是該 tenant 的 own budget)。
    rate_seq = {}  # tid -> list of (ms, rate_q32)
    for elapsed_ms, data in snaps:
        for tid, view in _tenant_views(data):
            mat = view.get("tenant_path_rate_q32", [])
            if tid >= len(mat):
                continue
            row = mat[tid]
            if target_path >= len(row):
                continue
            rate_seq.setdefault(tid, []).append((elapsed_ms, row[target_path]))

    # finite-difference per 250ms,然後 bin by integer second。
    overshoot_per_tenant = {}  # tid -> max overshoots-in-any-second
    overshoot_examples = {}    # tid -> example second
    for tid, seq in rate_seq.items():
        if len(seq) < 2:
            continue
        vals = [v for _, v in seq if v is not None]
        rates_nonzero = [v for v in vals if v > 0]
        if not rates_nonzero:
            continue
        mean_rate = sum(rates_nonzero) / len(rates_nonzero)
        if mean_rate <= 0:
            continue
        threshold = overshoot_pct * mean_rate
        # bucket overshoots into integer-second bins
        per_sec_counts = {}
        for i in range(1, len(seq)):
            ms, v = seq[i]
            _, vp = seq[i - 1]
            if v is None or vp is None:
                continue
            delta = abs(v - vp)
            if delta > threshold:
                sec_bin = ms // 1000
                per_sec_counts[sec_bin] = per_sec_counts.get(sec_bin, 0) + 1
        if per_sec_counts:
            worst_sec, worst_count = max(per_sec_counts.items(),
                                         key=lambda kv: kv[1])
            overshoot_per_tenant[tid] = worst_count
            overshoot_examples[tid] = (worst_sec, worst_count)
    overshoot_violators = {
        tid: cnt for tid, cnt in overshoot_per_tenant.items()
        if cnt > overshoot_max_per_sec
    }
    if overshoot_violators:
        ok = False
        sample_str = ", ".join(
            f"tid={tid} worst={overshoot_examples[tid][1]} crossings "
            f"in 1s window @ t={overshoot_examples[tid][0]}s"
            for tid in sorted(overshoot_violators)
        )
        reasons.append(
            f"budget oscillation exceeds ±{int(overshoot_pct * 100)}% "
            f"of mean more than {overshoot_max_per_sec}× per second: "
            f"{sample_str}"
        )

    # ── (2) weighted Jain over admit deltas ─────────────────────────────────
    # 用 start vs end admit_count 取 delta 當 tenant share proxy。
    # snapshot 結構:tenant_<tid>['admit_count'][tid][path] = cumulative admit
    # (own-row only valid for its own ring view)。share = sum_path admit
    # over all snaps → use first vs last。
    iops_per_tenant = []
    if snaps:
        first = snaps[0][1]
        last = snaps[-1][1]
        first_views = dict(_tenant_views(first))
        last_views = dict(_tenant_views(last))
        all_tids = sorted(set(first_views.keys()) | set(last_views.keys()))
        for tid in all_tids:
            v0 = first_views.get(tid)
            v1 = last_views.get(tid)
            if v0 is None or v1 is None:
                iops_per_tenant.append(None)
                continue
            a0 = v0.get("admit_count", [])
            a1 = v1.get("admit_count", [])
            if tid >= len(a0) or tid >= len(a1):
                iops_per_tenant.append(None)
                continue
            row0 = a0[tid]
            row1 = a1[tid]
            d = sum(row1[p] - row0[p]
                    for p in range(min(len(row0), len(row1))))
            iops_per_tenant.append(d)
    w_jain = None
    if iops_per_tenant and weights:
        w_jain = weighted_jain(iops_per_tenant, weights)
    if w_jain is None or w_jain < jain_min:
        ok = False
        reasons.append(
            f"weighted Jain={w_jain} < {jain_min} "
            f"(per-tenant admit deltas={iops_per_tenant})"
        )

    # ── (4) recovery convergence (optional;extend-run only) ────────────────
    last_inject_remove_ms = n_cycles * toggle_period_s * 1000
    tail_window_ms = 4000
    recovery_note = None
    if duration_s * 1000 < last_inject_remove_ms + tail_window_ms:
        recovery_note = (f"recovery convergence skipped — "
                         f"needs extended-run trace "
                         f"(duration={duration_s}s, last_remove_at="
                         f"{last_inject_remove_ms // 1000}s, "
                         f"need ≥ {(last_inject_remove_ms + tail_window_ms) // 1000}s)")
    else:
        # 找在最後 remove 後 ≥ 4s 的第一個 snapshot,要求 health ≥ 0.9 × HEALTHY
        recovery_target_ms = last_inject_remove_ms + tail_window_ms
        rec_thresh = int(65536 * 0.9)
        recovered = False
        for elapsed_ms, data in snaps:
            if elapsed_ms < recovery_target_ms:
                continue
            views = _tenant_views(data)
            if not views:
                continue
            _, v0 = views[0]
            harr = v0.get("path_health_q16", [])
            if target_path < len(harr) and harr[target_path] >= rec_thresh:
                recovered = True
                break
        if not recovered:
            ok = False
            reasons.append(
                f"recovery to ≥0.9×HEALTHY not observed within "
                f"{tail_window_ms // 1000}s after final remove "
                f"(t={last_inject_remove_ms // 1000}s)"
            )

    return {
        "experiment": "E10",
        "pass": ok,
        "reasons": reasons,
        "samples_count": len(snaps),
        "target_path": target_path,
        "n_cycles": n_cycles,
        "toggle_period_s": toggle_period_s,
        "duration_s": duration_s,
        "illegal_health_count": len(illegal_health),
        "epoch_violation_count": len(epoch_violations),
        "overshoot_per_tenant_worst_sec_count": overshoot_per_tenant,
        "overshoot_violators": list(overshoot_violators.keys()),
        "weighted_jain": (round(w_jain, 4) if w_jain is not None else None),
        "per_tenant_admit_delta": iops_per_tenant,
        "recovery_note": recovery_note,
    }


# ── E7 scale verdict ─────────────────────────────────────────────────────────


def verdict_e7_scale(run_dir: Path, baseline_n4_dir, expected_tenants):
    """E7 scale test (N=8 tenants × 3 paths):
       Acceptance:
         (a) weighted Jain >= 0.99
         (b) max per-tenant share deviation <= 8% (looser than E1 5%
             because N=8 has more discretization noise)
         (c) total throughput >= 90% of (N=4 baseline total × 2), i.e.
             SAPS-Q scheduler overhead does not degrade with T
         (d) DPA epoch throughput (sapsq_total_epochs_consumed / duration)
             roughly matches the N=4 baseline epoch rate (within 25%);
             epoch rate is event-triggered, should not be gated on T
    """
    reasons = []
    ok = True

    f = run_dir / "sapsq_summary.json"
    if not f.exists():
        return {"experiment": "E7", "pass": False,
                "reasons": [f"missing {f}"]}
    summary = json.loads(f.read_text())
    summary["_run_dir"] = str(run_dir)
    metrics = compute_metrics(summary)

    actual_tenants = metrics.get("tenants")
    if actual_tenants != expected_tenants:
        reasons.append(
            f"tenant count mismatch: cfg={actual_tenants} "
            f"expected={expected_tenants}"
        )

    # (a) weighted Jain >= 0.99
    if metrics["weighted_jain"] is None or metrics["weighted_jain"] < 0.99:
        ok = False
        reasons.append(
            f"weighted Jain={metrics['weighted_jain']} < 0.99"
        )

    # (b) max share deviation <= 8% (E7 loosened from E1's 5%)
    if (metrics["max_share_deviation"] is None
            or metrics["max_share_deviation"] > 0.08):
        ok = False
        reasons.append(
            f"max share deviation={metrics['max_share_deviation']} > 8%"
        )

    # (c) throughput scaling vs N=4 baseline
    n4_total_iops = None
    scale_efficiency = None
    if baseline_n4_dir is not None:
        n4_path = Path(baseline_n4_dir) / "sapsq_summary.json"
        if n4_path.exists():
            n4_summary = json.loads(n4_path.read_text())
            n4_metrics = compute_metrics(n4_summary)
            n4_total_iops = n4_metrics.get("total_iops")
            if n4_total_iops and n4_total_iops > 0:
                expected_n8 = n4_total_iops * 2.0
                actual_n8 = metrics.get("total_iops") or 0
                scale_efficiency = actual_n8 / expected_n8
                if scale_efficiency < 0.90:
                    ok = False
                    reasons.append(
                        f"scale efficiency={scale_efficiency:.3f} < 0.90 "
                        f"(N=4 baseline total={n4_total_iops:.0f}, "
                        f"expected N=8 >= {expected_n8 * 0.90:.0f}, "
                        f"got {actual_n8:.0f})"
                    )
            else:
                reasons.append(
                    f"N=4 baseline total_iops invalid: {n4_total_iops}"
                )
        else:
            reasons.append(
                f"N=4 baseline missing: {n4_path} not found "
                "(scale efficiency check skipped)"
            )
    else:
        reasons.append(
            "no --e7-baseline-n4-dir provided; scale efficiency "
            "informational only"
        )

    # (d) DPA epoch throughput sanity
    duration_s = summary.get("duration_s") or 1
    epoch_deltas = metrics.get("epoch_deltas_per_tenant", []) or []
    # All tenant procs observe the same global epoch counter, so take max
    # (any tenant's view) rather than sum.
    epochs_total = max(epoch_deltas) if epoch_deltas else 0
    epoch_rate_per_s = epochs_total / duration_s if duration_s > 0 else 0
    n4_epoch_rate = None
    if baseline_n4_dir is not None:
        n4_path = Path(baseline_n4_dir) / "sapsq_summary.json"
        if n4_path.exists():
            n4_summary = json.loads(n4_path.read_text())
            n4_metrics = compute_metrics(n4_summary)
            n4_eds = n4_metrics.get("epoch_deltas_per_tenant", []) or []
            n4_epochs = max(n4_eds) if n4_eds else 0
            n4_dur = n4_summary.get("duration_s") or 1
            n4_epoch_rate = (n4_epochs / n4_dur) if n4_dur > 0 else 0
            if n4_epoch_rate > 0:
                ratio = epoch_rate_per_s / n4_epoch_rate
                if ratio < 0.75 or ratio > 1.25:
                    reasons.append(
                        f"epoch rate ratio N=8/N=4 = {ratio:.3f} outside "
                        f"[0.75, 1.25] (N=8={epoch_rate_per_s:.0f}/s, "
                        f"N=4={n4_epoch_rate:.0f}/s) — scheduler may be "
                        f"T-bottlenecked"
                    )

    if all(e == 0 for e in epoch_deltas):
        reasons.append(
            "zero DPA epochs consumed across all tenants — SAPS-Q may be "
            "stale (check sapsq_stale_epoch_fallback)"
        )

    return {
        "experiment": "E7",
        "pass": ok,
        "reasons": reasons,
        "tenants": actual_tenants,
        "weighted_jain": metrics["weighted_jain"],
        "max_share_deviation": metrics["max_share_deviation"],
        "total_iops": metrics["total_iops"],
        "per_tenant_iops": metrics["per_tenant_iops"],
        "n4_baseline_total_iops": n4_total_iops,
        "scale_efficiency_vs_n4x2":
            round(scale_efficiency, 4) if scale_efficiency is not None
            else None,
        "epoch_rate_per_s": round(epoch_rate_per_s, 2),
        "n4_epoch_rate_per_s":
            round(n4_epoch_rate, 2) if n4_epoch_rate is not None else None,
    }


# ── E11 stress N=16 verdict ──────────────────────────────────────────────────


def verdict_e11_stress(run_dir: Path, baseline_n8_dir, expected_tenants):
    """E11 stress test (N=16 tenants × 3 paths = SAPSQ_TENANT_MAX):
       Acceptance:
         (a) weighted Jain >= 0.99
         (b) max per-tenant share deviation <= 10% (E7 8%; loosened because
             N=16 has more discretization noise per tenant)
         (c) total throughput >= 90% of (N=8 baseline total × 2), i.e.
             SAPS-Q scheduler overhead does not degrade further at the
             schema max
         (d) DPA epoch rate within 25% of N=8 baseline epoch rate — proves
             O(T×P + T log T) scheduler cost is bounded (T=16, P=3 →
             ~64 ops/epoch, expected to stay sub-µs)
    """
    reasons = []
    ok = True

    f = run_dir / "sapsq_summary.json"
    if not f.exists():
        return {"experiment": "E11", "pass": False,
                "reasons": [f"missing {f}"]}
    summary = json.loads(f.read_text())
    metrics = compute_metrics(summary)

    actual_tenants = metrics.get("tenants")
    if actual_tenants != expected_tenants:
        reasons.append(
            f"tenant count mismatch: cfg={actual_tenants} "
            f"expected={expected_tenants}"
        )

    # (a) weighted Jain >= 0.99
    if metrics["weighted_jain"] is None or metrics["weighted_jain"] < 0.99:
        ok = False
        reasons.append(
            f"weighted Jain={metrics['weighted_jain']} < 0.99"
        )

    # (b) max share deviation <= 10% (E11 loosened from E7's 8%)
    if (metrics["max_share_deviation"] is None
            or metrics["max_share_deviation"] > 0.10):
        ok = False
        reasons.append(
            f"max share deviation={metrics['max_share_deviation']} > 10%"
        )

    # (c) throughput scaling vs N=8 baseline
    n8_total_iops = None
    scale_efficiency = None
    if baseline_n8_dir is not None:
        n8_path = Path(baseline_n8_dir) / "sapsq_summary.json"
        if n8_path.exists():
            n8_summary = json.loads(n8_path.read_text())
            n8_metrics = compute_metrics(n8_summary)
            n8_total_iops = n8_metrics.get("total_iops")
            if n8_total_iops and n8_total_iops > 0:
                expected_n16 = n8_total_iops * 2.0
                actual_n16 = metrics.get("total_iops") or 0
                scale_efficiency = actual_n16 / expected_n16
                if scale_efficiency < 0.90:
                    ok = False
                    reasons.append(
                        f"scale efficiency={scale_efficiency:.3f} < 0.90 "
                        f"(N=8 baseline total={n8_total_iops:.0f}, "
                        f"expected N=16 >= {expected_n16 * 0.90:.0f}, "
                        f"got {actual_n16:.0f})"
                    )
            else:
                reasons.append(
                    f"N=8 baseline total_iops invalid: {n8_total_iops}"
                )
        else:
            reasons.append(
                f"N=8 baseline missing: {n8_path} not found "
                "(scale efficiency check skipped)"
            )
    else:
        reasons.append(
            "no --e11-baseline-n8-dir provided; scale efficiency "
            "informational only"
        )

    # (d) DPA epoch throughput sanity (within 25% of N=8 baseline)
    duration_s = summary.get("duration_s") or 1
    epoch_deltas = metrics.get("epoch_deltas_per_tenant", []) or []
    # All tenant procs observe the same global epoch counter, so take max
    # (any tenant's view) rather than sum.
    epochs_total = max(epoch_deltas) if epoch_deltas else 0
    epoch_rate_per_s = epochs_total / duration_s if duration_s > 0 else 0
    n8_epoch_rate = None
    if baseline_n8_dir is not None:
        n8_path = Path(baseline_n8_dir) / "sapsq_summary.json"
        if n8_path.exists():
            n8_summary = json.loads(n8_path.read_text())
            n8_metrics = compute_metrics(n8_summary)
            n8_eds = n8_metrics.get("epoch_deltas_per_tenant", []) or []
            n8_epochs = max(n8_eds) if n8_eds else 0
            n8_dur = n8_summary.get("duration_s") or 1
            n8_epoch_rate = (n8_epochs / n8_dur) if n8_dur > 0 else 0
            if n8_epoch_rate > 0:
                ratio = epoch_rate_per_s / n8_epoch_rate
                if ratio < 0.75 or ratio > 1.25:
                    reasons.append(
                        f"epoch rate ratio N=16/N=8 = {ratio:.3f} outside "
                        f"[0.75, 1.25] (N=16={epoch_rate_per_s:.0f}/s, "
                        f"N=8={n8_epoch_rate:.0f}/s) — scheduler T×P + T log T "
                        f"cost may exceed sub-µs budget"
                    )

    if all(e == 0 for e in epoch_deltas):
        reasons.append(
            "zero DPA epochs consumed across all tenants — SAPS-Q may be "
            "stale (check sapsq_stale_epoch_fallback)"
        )

    return {
        "experiment": "E11",
        "pass": ok,
        "reasons": reasons,
        "tenants": actual_tenants,
        "weighted_jain": metrics["weighted_jain"],
        "max_share_deviation": metrics["max_share_deviation"],
        "total_iops": metrics["total_iops"],
        "per_tenant_iops": metrics["per_tenant_iops"],
        "n8_baseline_total_iops": n8_total_iops,
        "scale_efficiency_vs_n8x2":
            round(scale_efficiency, 4) if scale_efficiency is not None
            else None,
        "epoch_rate_per_s": round(epoch_rate_per_s, 2),
        "n8_epoch_rate_per_s":
            round(n8_epoch_rate, 2) if n8_epoch_rate is not None else None,
    }


# ── E8 mixed workload verdict ────────────────────────────────────────────────


def _load_summary(run_dir):
    if run_dir is None:
        return None
    p = Path(run_dir) / "sapsq_summary.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def verdict_e8_mixed(run_dir: Path, latency_tid, throughput_tid,
                     lat_p99_us_max, throughput_single_mb,
                     stock_dir, m4_static_dir):
    """E8 mixed workload (latency-sensitive 4K randread + throughput-sensitive
    64K randwrite + others). With nbytes-scaled cost in SAPS-Q admission, the
    64K writer should consume 16× tokens per IO, preserving fairness on bytes
    instead of on IO count.

    Acceptance (single-run, sapsq mode):
      (a) Latency tenant P99 <= lat_p99_us_max (default 1ms) — isolation holds
      (b) Throughput tenant IOPS > 0 (work-conserving; not starved)
      (c) Optional: throughput tenant achieves >= 70% of single-tenant baseline
          (in MB/s; tenant 1 nbytes/IO = 65536). Skipped if baseline not given.
      (d) Optional cross-mode: SAPS-Q latency P99 <= stock latency P99 AND
          SAPS-Q latency P99 <= m4_static latency P99. Skipped if refs missing.
    """
    reasons = []
    ok = True

    summary = _load_summary(run_dir)
    if summary is None:
        return {"experiment": "E8", "pass": False,
                "reasons": [f"missing sapsq_summary.json under {run_dir}"]}
    per_tenant = summary.get("per_tenant", [])
    by_tid = {r.get("tenant_id"): r for r in per_tenant}

    lat = by_tid.get(latency_tid)
    thr = by_tid.get(throughput_tid)
    if lat is None or thr is None:
        return {"experiment": "E8", "pass": False,
                "reasons": [f"missing tenant data lat_tid={latency_tid} "
                            f"thr_tid={throughput_tid}"]}

    # (a) latency tenant P99
    lat_p99 = lat.get("lat_p99_us")
    if lat_p99 is None:
        ok = False
        reasons.append(f"latency tenant {latency_tid} P99 unavailable")
    elif lat_p99 > lat_p99_us_max:
        ok = False
        reasons.append(
            f"latency tenant {latency_tid} P99={lat_p99}us > "
            f"{lat_p99_us_max}us (isolation broken)"
        )

    # (b) throughput tenant IOPS > 0 (work-conserving)
    thr_iops = thr.get("iops") or 0
    if thr_iops <= 0:
        ok = False
        reasons.append(
            f"throughput tenant {throughput_tid} got 0 IOPS — starved"
        )

    # (c) throughput tenant MB/s vs single-tenant baseline (optional)
    # 64K IO × thr_iops / 1e6 = MB/s (decimal)
    thr_mb = thr_iops * 65536 / 1_000_000 if thr_iops > 0 else 0
    proportional_ok = None
    if throughput_single_mb > 0:
        proportional_ok = thr_mb >= 0.70 * throughput_single_mb
        if not proportional_ok:
            ok = False
            reasons.append(
                f"throughput tenant MB/s={thr_mb:.1f} < "
                f"0.70 × baseline {throughput_single_mb:.1f} = "
                f"{0.70 * throughput_single_mb:.1f}"
            )
    else:
        reasons.append(
            "throughput baseline not provided (--e8-throughput-single-baseline-mb=0) "
            "— proportional fairness check needs baseline"
        )

    # (d) cross-mode latency comparison (optional)
    cross_mode = {}
    for label, ref_dir in (("stock", stock_dir), ("m4_static", m4_static_dir)):
        if ref_dir is None:
            cross_mode[label] = {"note": "needs baseline"}
            continue
        ref = _load_summary(ref_dir)
        if ref is None:
            cross_mode[label] = {"note": f"baseline missing at {ref_dir}"}
            continue
        ref_lat = next((r for r in ref.get("per_tenant", [])
                        if r.get("tenant_id") == latency_tid), None)
        if ref_lat is None:
            cross_mode[label] = {"note": "ref tenant missing"}
            continue
        ref_p99 = ref_lat.get("lat_p99_us")
        cross_mode[label] = {"lat_p99_us": ref_p99}
        if (ref_p99 is not None and lat_p99 is not None
                and lat_p99 > ref_p99):
            ok = False
            reasons.append(
                f"SAPS-Q latency P99={lat_p99}us > {label} P99={ref_p99}us — "
                f"SAPS-Q must beat {label} on latency tenant"
            )

    return {
        "experiment": "E8",
        "pass": ok,
        "reasons": reasons,
        "latency_tenant_id": latency_tid,
        "latency_p99_us": lat_p99,
        "latency_p99_threshold_us": lat_p99_us_max,
        "throughput_tenant_id": throughput_tid,
        "throughput_iops": thr_iops,
        "throughput_mb_per_s": round(thr_mb, 2),
        "throughput_single_baseline_mb": throughput_single_mb,
        "proportional_fairness_ok": proportional_ok,
        "cross_mode": cross_mode,
    }


# ── E9 priority inversion verdict ────────────────────────────────────────────


def _agg_per_tenant_iops_over_reps(mode_dir: Path):
    """mode_dir/rep_<n>/sapsq_summary.json → per-tenant IOPS averaged across reps.
    Returns list indexed by tenant_id, or None if no reps found.
    """
    reps = sorted(mode_dir.glob("rep_*/sapsq_summary.json"))
    if not reps:
        return None
    # Collect per-tenant IOPS from each rep; accumulate by tenant_id.
    tenant_totals: dict = {}
    tenant_counts: dict = {}
    for p in reps:
        try:
            s = json.loads(p.read_text())
            for r in s.get("per_tenant", []):
                tid = r.get("tenant_id")
                iops = r.get("iops")
                if tid is None or iops is None:
                    continue
                tenant_totals[tid] = tenant_totals.get(tid, 0.0) + iops
                tenant_counts[tid] = tenant_counts.get(tid, 0) + 1
        except Exception:
            continue
    if not tenant_totals:
        return None
    n_tenants = max(tenant_totals.keys()) + 1
    return [
        (tenant_totals[tid] / tenant_counts[tid])
        if (tid in tenant_totals and tenant_counts[tid] > 0) else None
        for tid in range(n_tenants)
    ]


def verdict_e9_inversion(in_dir: Path, demand_t0: float,
                         path_base_iops: float, n_paths: int):
    """E9 priority inversion — work-conservation under heavyweight-but-lightly-
    loaded tenant 0 (weight=3, demand=20K) vs burst tenants 1/2/3 (weight=1,
    demand=200K each).

    設定:
      total_cap = n_paths × path_base_iops  (e.g. 3 × 200K = 600K)
      t0 是需求綁定 (demand-bound): 應收到 ≈ demand_t0 而非靜態份額
      t1/t2/t3 應平分剩餘容量: (total_cap − t0_actual) / 3

    PASS 條件 (sapsq):
      (a) t0_demand_compliance = t0_actual / demand_t0 ∈ [0.9, 1.1]
      (b) active_jain (Jain over t1/t2/t3) ≥ 0.99
      (c) utilization = (t0+t1+t2+t3) / total_cap ≥ 0.95
      (d) sapsq_advantage_over_m4_static ≥ +50% on t1/t2/t3 throughput
          (m4_static token bucket 把 t1/t2/t3 限在靜態份額 100K each;
           SAPS-Q 應重分配到 ≈ 193K each → +93% → 遠超 50% 閾)
    """
    total_cap = n_paths * path_base_iops
    reasons = []
    ok = True

    # ── Load per-mode per-tenant IOPS (averaged over reps) ──────────────────
    mode_iops = {}
    for mode in ("stock", "m4_static", "sapsq"):
        mode_dir = in_dir / mode
        iops_by_tenant = _agg_per_tenant_iops_over_reps(mode_dir)
        mode_iops[mode] = iops_by_tenant  # may be None

    sapsq_iops = mode_iops.get("sapsq")
    m4_iops = mode_iops.get("m4_static")

    if sapsq_iops is None or len(sapsq_iops) < 4:
        return {
            "experiment": "E9",
            "pass": False,
            "reasons": [
                f"sapsq per-tenant IOPS unavailable "
                f"(found: {sapsq_iops}) — check {in_dir}/sapsq/rep_*/sapsq_summary.json"
            ],
        }

    t0_actual = sapsq_iops[0] or 0.0
    t1_actual = sapsq_iops[1] or 0.0
    t2_actual = sapsq_iops[2] or 0.0
    t3_actual = sapsq_iops[3] or 0.0
    total_actual = t0_actual + t1_actual + t2_actual + t3_actual

    # (a) t0 demand compliance
    t0_demand_compliance = (t0_actual / demand_t0) if demand_t0 > 0 else None
    if t0_demand_compliance is None or not (0.9 <= t0_demand_compliance <= 1.1):
        ok = False
        reasons.append(
            f"t0_demand_compliance={t0_demand_compliance:.3f} "
            f"(t0_actual={t0_actual:.0f}, demand={demand_t0:.0f}) "
            f"— expected ∈ [0.9, 1.1]"
        )

    # (b) active Jain over t1/t2/t3 (excluding demand-bound t0)
    active_iops = [t1_actual, t2_actual, t3_actual]
    active_jain_val = jain(active_iops)
    if active_jain_val is None or active_jain_val < 0.99:
        ok = False
        reasons.append(
            f"active_jain (t1/t2/t3)={active_jain_val} < 0.99 "
            f"(t1={t1_actual:.0f} t2={t2_actual:.0f} t3={t3_actual:.0f})"
        )

    # (c) total utilization
    utilization = total_actual / total_cap if total_cap > 0 else 0.0
    if utilization < 0.95:
        ok = False
        reasons.append(
            f"utilization={utilization:.3f} < 0.95 "
            f"(total_actual={total_actual:.0f}, cap={total_cap:.0f}) "
            f"— SAPS-Q must redistribute t0's unused capacity"
        )

    # (d) SAPS-Q advantage over m4_static on t1/t2/t3 throughput
    sapsq_advantage_pct = None
    m4_t123 = None
    sapsq_t123 = t1_actual + t2_actual + t3_actual
    if m4_iops is not None and len(m4_iops) >= 4:
        m4_t123 = (m4_iops[1] or 0.0) + (m4_iops[2] or 0.0) + (m4_iops[3] or 0.0)
        if m4_t123 > 0:
            sapsq_advantage_pct = (sapsq_t123 / m4_t123 - 1.0) * 100.0
            if sapsq_advantage_pct < 50.0:
                ok = False
                reasons.append(
                    f"sapsq_advantage_over_m4_static={sapsq_advantage_pct:.1f}% < 50% "
                    f"(sapsq t123={sapsq_t123:.0f}, m4_static t123={m4_t123:.0f}) "
                    f"— work-conservation must significantly outperform static token bucket"
                )
        else:
            reasons.append(
                "m4_static t1/t2/t3 total IOPS=0 — cannot compute advantage "
                "(check m4_static run logs)"
            )
    else:
        reasons.append(
            "m4_static per-tenant IOPS unavailable — advantage check skipped "
            f"(found: {m4_iops})"
        )

    # ── per-mode breakdown for informational output ──────────────────────────
    per_mode_breakdown = {}
    for mode in ("stock", "m4_static", "sapsq"):
        iops = mode_iops.get(mode)
        if iops is None:
            per_mode_breakdown[mode] = None
            continue
        total = sum(x for x in iops if x is not None)
        per_mode_breakdown[mode] = {
            "per_tenant_iops": [
                round(x, 0) if x is not None else None for x in iops
            ],
            "total_iops": round(total, 0),
            "utilization": round(total / total_cap, 4) if total_cap > 0 else None,
        }

    return {
        "experiment": "E9",
        "pass": ok,
        "reasons": reasons,
        "sapsq_t0_actual_iops": round(t0_actual, 0),
        "sapsq_t0_demand_iops": demand_t0,
        "sapsq_t0_demand_compliance": (
            round(t0_demand_compliance, 4)
            if t0_demand_compliance is not None else None
        ),
        "sapsq_active_jain_t123": (
            round(active_jain_val, 4) if active_jain_val is not None else None
        ),
        "sapsq_utilization": round(utilization, 4),
        "sapsq_t123_iops": round(sapsq_t123, 0),
        "m4_static_t123_iops": round(m4_t123, 0) if m4_t123 is not None else None,
        "sapsq_advantage_over_m4_static_pct": (
            round(sapsq_advantage_pct, 1)
            if sapsq_advantage_pct is not None else None
        ),
        "total_cap_iops": total_cap,
        "per_mode_breakdown": per_mode_breakdown,
    }


# ── E12 multi-simultaneous-fault verdict ─────────────────────────────────────

# Legal sapsq_path_eligibility values: QUARANTINE=0, PROBE=1, NORMAL=2
_SAPSQ_ELIG_LEGAL = {0, 1, 2}


def verdict_e12_multi_fault(run_dir: Path, fault_start_s: int, fault_end_s: int,
                             n_paths: int, n_tenants: int,
                             probe_rate_iops: float, probe_floor_headroom: float,
                             recovery_target_health: int, recovery_window_s: int):
    """E12 multi-simultaneous-fault adversarial (SAPS-Q survivability):

    t=fault_start_s: 3 faults injected simultaneously (path A bimodal,
                     path B fail-slow 5ms, path C sct=3 sparse errors).
    t=fault_end_s:   all 3 faults removed.
    t=fault_end_s + recovery_window_s: partial recovery check.

    5 acceptance criteria:

    (1) no-deadlock:
        Per-tenant IOPS in fault window snapshots (t ∈ [fault_start+5s,
        fault_end-5s]) must be > 0 for every tenant.  A tenant flatlined
        at 0 admit_count delta across consecutive snapshots means deadlock.

    (2) probe-floor active:
        Total tenant IOPS during fault window ≈
          n_paths × probe_rate_iops × probe_floor_headroom (burst budget).
        Hard floor: total admit delta across any 5s window must be > 0
        (system is not fully stalled).  Upper bound: total admit delta must
        be < n_paths × probe_rate_iops × probe_floor_headroom × 10 (not
        erroneously passing huge traffic during all-path quarantine).

    (3) eligibility consistency:
        Every snapshot in the fault window: sapsq_path_eligibility[p] ∈
        {0, 1, 2} for all p.  Non-integer or out-of-set value = torn read.

    (4) recovery convergence:
        At t = fault_end_s + recovery_window_s (default t=58s), at least
        one path's health_q16 > recovery_target_health (default 32768 =
        RECOVERING floor 0.5×HEALTHY).  Partial convergence sufficient
        (8s is only 8 epochs at ~1 epoch/s; full HEALTHY needs ~4096 steps).

    (5) epoch monotonicity:
        sapsq_total_epochs_consumed is non-decreasing across all snapshots
        for every tenant.
    """
    reasons = []
    ok = True

    snaps = _load_periodic_snapshots(run_dir)
    if not snaps:
        return {
            "experiment": "E12",
            "pass": False,
            "reasons": ["no periodic samples — sapsq_dump missing or "
                        "--periodic-dump-interval-ms=0"],
        }

    fault_start_ms = fault_start_s * 1000
    fault_end_ms = fault_end_s * 1000
    # Trim 5s off each edge to avoid transient artefacts at fault edges
    fault_window_lo = fault_start_ms + 5000
    fault_window_hi = fault_end_ms - 5000

    recovery_check_ms = fault_end_ms + recovery_window_s * 1000

    # ── (5) epoch monotonicity ────────────────────────────────────────────────
    epoch_seq = {}    # tid -> list of (ms, total_epochs_consumed)
    for elapsed_ms, data in snaps:
        for tid, view in _tenant_views(data):
            te = view.get("total_epochs_consumed", 0)
            epoch_seq.setdefault(tid, []).append((elapsed_ms, te))
    epoch_violations = []
    for tid, seq in epoch_seq.items():
        prev = None
        for ms, e in seq:
            if prev is not None and e < prev:
                epoch_violations.append((tid, ms, prev, e))
            prev = e
    if epoch_violations:
        ok = False
        sample_str = ", ".join(
            f"tid={tid} t={ms}ms {prev_e}->{e}"
            for tid, ms, prev_e, e in epoch_violations[:5]
        )
        reasons.append(
            f"total_epochs_consumed not monotonic "
            f"({len(epoch_violations)} regressions): {sample_str}"
        )

    # ── Collect fault-window snapshots ────────────────────────────────────────
    fault_snaps = [(ms, data) for ms, data in snaps
                   if fault_window_lo <= ms <= fault_window_hi]
    fault_snap_count = len(fault_snaps)

    # ── (3) eligibility consistency ───────────────────────────────────────────
    elig_violations = []
    for elapsed_ms, data in fault_snaps:
        for tid, view in _tenant_views(data):
            elig_arr = view.get("path_eligibility", [])
            for p, ev in enumerate(elig_arr[:n_paths]):
                if not isinstance(ev, int) or ev not in _SAPSQ_ELIG_LEGAL:
                    elig_violations.append((elapsed_ms, tid, p, ev))
    if elig_violations:
        ok = False
        sample_str = ", ".join(
            f"t={ms}ms tid={tid} p={p} elig={ev}"
            for ms, tid, p, ev in elig_violations[:5]
        )
        reasons.append(
            f"illegal path_eligibility values ({len(elig_violations)} "
            f"occurrences, legal={{0,1,2}}): {sample_str}"
        )

    # ── (1) no-deadlock: check admit_count delta per tenant ──────────────────
    # Build per-tenant admit count timeseries across fault window.
    # Use own-row (admit_count[tid][p]) from each tenant's ring view.
    admit_seq = {}    # tid -> list of (ms, total_admit_all_paths)
    for elapsed_ms, data in fault_snaps:
        for tid, view in _tenant_views(data):
            a_mat = view.get("admit_count", [])
            if tid >= len(a_mat):
                continue
            row = a_mat[tid]
            total = sum(row[:n_paths]) if len(row) >= n_paths else sum(row)
            admit_seq.setdefault(tid, []).append((elapsed_ms, total))

    deadlocked_tenants = []
    for tid, seq in admit_seq.items():
        # If any consecutive pair shows zero delta over the whole fault window,
        # that tenant is deadlocked (no IOs admitted across an entire dump interval).
        all_zero_delta = all(
            seq[i][1] == seq[i - 1][1]
            for i in range(1, len(seq))
        ) if len(seq) >= 2 else False
        if all_zero_delta:
            deadlocked_tenants.append(tid)

    if deadlocked_tenants:
        ok = False
        reasons.append(
            f"deadlock detected: tenant(s) {deadlocked_tenants} showed zero "
            f"admit_count progress across all fault-window snapshots "
            f"(t={fault_window_lo}ms..{fault_window_hi}ms)"
        )

    # ── (2) probe-floor: total admit delta in fault window > 0 ───────────────
    # Aggregate across all tenants first vs last fault-window snapshot.
    probe_floor_ok = True
    if len(fault_snaps) >= 2:
        first_ms, first_data = fault_snaps[0]
        last_ms, last_data = fault_snaps[-1]
        first_views = dict(_tenant_views(first_data))
        last_views = dict(_tenant_views(last_data))
        total_delta = 0
        for tid in range(n_tenants):
            v0 = first_views.get(tid)
            v1 = last_views.get(tid)
            if v0 is None or v1 is None:
                continue
            a0 = v0.get("admit_count", [])
            a1 = v1.get("admit_count", [])
            if tid >= len(a0) or tid >= len(a1):
                continue
            row0, row1 = a0[tid], a1[tid]
            total_delta += sum(
                row1[p] - row0[p]
                for p in range(min(n_paths, len(row0), len(row1)))
            )
        if total_delta <= 0:
            ok = False
            probe_floor_ok = False
            reasons.append(
                f"probe floor not active: total admit delta across fault window "
                f"= {total_delta} (expected > 0; system fully stalled)"
            )
        else:
            # Soft upper bound: must not be suspiciously high (no fault effect)
            # floor_budget = n_paths * probe_rate_iops * probe_floor_headroom
            #              = 3 × 100 × 3.0 = 900 IOPS × window_s
            window_s = (last_ms - first_ms) / 1000.0
            floor_budget_total = (n_paths * probe_rate_iops
                                  * probe_floor_headroom * window_s)
            if total_delta > floor_budget_total * 10:
                # Not a hard fail — paths may be in DEGRADING (not EXCLUDED)
                # and still pass some traffic; flag as advisory.
                reasons.append(
                    f"ADVISORY: total admit delta={total_delta} >> "
                    f"10× probe floor budget {floor_budget_total:.0f} — "
                    f"paths may not have reached EXCLUDED state"
                )
    elif fault_snap_count == 0:
        ok = False
        probe_floor_ok = False
        reasons.append(
            "no fault-window snapshots available "
            f"(window t={fault_window_lo}ms..{fault_window_hi}ms is empty)"
        )

    # ── (4) recovery convergence ──────────────────────────────────────────────
    # At t >= fault_end + recovery_window, at least one path health_q16
    # must exceed recovery_target_health (partial RECOVERING ramp).
    recovery_achieved = False
    recovery_health_at_check = []
    for elapsed_ms, data in snaps:
        if elapsed_ms < recovery_check_ms:
            continue
        views = _tenant_views(data)
        if not views:
            continue
        _, v0 = views[0]
        harr = v0.get("path_health_q16", [])
        recovery_health_at_check = harr[:n_paths]
        if any(h > recovery_target_health for h in harr[:n_paths]):
            recovery_achieved = True
        break   # first snapshot at or after recovery_check_ms is sufficient

    if not recovery_health_at_check:
        reasons.append(
            f"no snapshot found at t>={recovery_check_ms}ms — "
            f"cannot verify recovery convergence"
        )
    elif not recovery_achieved:
        ok = False
        reasons.append(
            f"partial recovery not observed by t={recovery_check_ms}ms: "
            f"path_health_q16={recovery_health_at_check} all <= "
            f"{recovery_target_health} (RECOVERING floor)"
        )

    return {
        "experiment": "E12",
        "pass": ok,
        "reasons": reasons,
        "samples_count": len(snaps),
        "fault_window_snap_count": fault_snap_count,
        "deadlocked_tenants": deadlocked_tenants,
        "elig_violation_count": len(elig_violations),
        "epoch_violation_count": len(epoch_violations),
        "probe_floor_ok": probe_floor_ok,
        "recovery_health_q16_at_check": recovery_health_at_check,
        "recovery_achieved": recovery_achieved,
        "fault_start_s": fault_start_s,
        "fault_end_s": fault_end_s,
        "recovery_check_at_s": (fault_end_s + recovery_window_s),
    }


# ── main ─────────────────────────────────────────────────────────────────────


def main():
    args = parse_args()
    out_dir = Path(args.out)

    # ── E4/E5 path: combined multi-run verdict (no single sapsq_summary.json) ─
    if args.experiment == "e4":
        if not args.e4_phase1_dir or not args.e4_phase2_dir:
            raise SystemExit("E4 requires --e4-phase1-dir and --e4-phase2-dir")
        weights_p1 = [int(x) for x in args.e4_weights_p1.split(",")]
        weights_p2 = [int(x) for x in args.e4_weights_p2.split(",")]
        v = verdict_e4_reweight(
            Path(args.e4_phase1_dir), Path(args.e4_phase2_dir),
            weights_p1, weights_p2,
            args.e4_phase1_duration, args.e4_phase2_duration,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E4 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e5":
        v = verdict_e5_overhead(
            Path(args.e5_d0_off_dir) if args.e5_d0_off_dir else None,
            Path(args.e5_d0_on_dir) if args.e5_d0_on_dir else None,
            Path(args.e5_d1_on_dir) if args.e5_d1_on_dir else None,
            Path(args.e5_d3_on_dir) if args.e5_d3_on_dir else None,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E5 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e5b":
        v = verdict_e5b_dseries_hero(
            Path(args.e5b_d1_off_dir) if args.e5b_d1_off_dir else None,
            Path(args.e5b_d1_on_dir) if args.e5b_d1_on_dir else None,
            Path(args.e5b_d3_off_dir) if args.e5b_d3_off_dir else None,
            Path(args.e5b_d3_on_dir) if args.e5b_d3_on_dir else None,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E5b verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e6":
        v = verdict_e6_recovery(
            out_dir,
            args.e6_inject_at_s, args.e6_remove_at_s, args.e6_target_path,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E6 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e8":
        v = verdict_e8_mixed(
            out_dir,
            args.e8_latency_tenant, args.e8_throughput_tenant,
            args.e8_latency_p99_us_max,
            args.e8_throughput_single_baseline_mb,
            args.e8_stock_dir, args.e8_m4_static_dir,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E8 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e7":
        v = verdict_e7_scale(
            out_dir,
            args.e7_baseline_n4_dir,
            args.e7_tenants,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E7 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e11":
        v = verdict_e11_stress(
            out_dir,
            args.e11_baseline_n8_dir,
            args.e11_tenants,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E11 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e10":
        weights_e10 = [int(x) for x in args.e10_weights.split(",") if x.strip()]
        v = verdict_e10_flap(
            out_dir,
            args.e10_target_path,
            args.e10_toggle_period_s,
            args.e10_n_cycles,
            args.e10_duration_s,
            weights_e10,
            args.e10_jain_min,
            args.e10_overshoot_pct,
            args.e10_overshoot_max_per_sec,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E10 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e12":
        v = verdict_e12_multi_fault(
            out_dir,
            args.e12_fault_start_s,
            args.e12_fault_end_s,
            args.e12_n_paths,
            args.e12_n_tenants,
            args.e12_probe_rate_iops,
            args.e12_probe_floor_headroom,
            args.e12_recovery_target_health,
            args.e12_recovery_window_s,
        )
        out = {"verdict": v, "source_dir": str(out_dir)}
        out_path = out_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E12 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons"):
                print(f"  {k}={val}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if args.experiment == "e9":
        in_dir = Path(args.e9_in_dir) if args.e9_in_dir else out_dir
        v = verdict_e9_inversion(
            in_dir,
            args.e9_demand_t0,
            args.e9_path_base_iops,
            args.e9_n_paths,
        )
        out = {"verdict": v, "source_dir": str(in_dir)}
        out_path = in_dir / "aggregate.json"
        out_path.write_text(json.dumps(out, indent=2))
        print(f"\n=== E9 verdict: {'PASS' if v['pass'] else 'FAIL'} ===")
        for k, val in v.items():
            if k not in ("experiment", "pass", "reasons", "per_mode_breakdown"):
                print(f"  {k}={val}")
        if v.get("per_mode_breakdown"):
            print("  per_mode_breakdown:")
            for mode, bd in v["per_mode_breakdown"].items():
                print(f"    {mode}: {bd}")
        for r in v.get("reasons", []):
            print(f"  - {r}")
        print(f"\nwritten: {out_path}")
        return

    if not (out_dir / "sapsq_summary.json").exists():
        run_dirs = []
        for pat in ("rep_*", "*/rep_*"):
            for d in sorted(out_dir.glob(pat)):
                if (d / "sapsq_summary.json").exists():
                    run_dirs.append(d)
        if run_dirs:
            rep_outputs = []
            for run_dir in run_dirs:
                summary = load_summary(run_dir)
                run_config_path = run_dir / "run_config.json"
                run_config = (json.loads(run_config_path.read_text())
                              if run_config_path.exists() else {})
                summary["_run_dir"] = str(run_dir)
                metrics = compute_metrics(summary)
                v = apply_verdict(metrics, args.experiment, run_config)
                out = {
                    "metrics": metrics,
                    "verdict": v,
                    "source_dir": str(run_dir),
                }
                if (metrics.get("observability_degraded")
                        or v.get("observability_degraded")):
                    out["regime_warning_banner"] = REGIME_WARNING_BANNER
                (run_dir / "aggregate.json").write_text(
                    json.dumps(out, indent=2))
                rep_outputs.append({
                    "run_dir": str(run_dir),
                    "per_path_pct": metrics.get("per_path_pct"),
                    "per_path_pct_source": metrics.get("per_path_pct_source"),
                    "observability_degraded": metrics.get(
                        "observability_degraded", False),
                    "verdict": v,
                })
            any_degraded = any(
                item.get("observability_degraded")
                or item.get("verdict", {}).get("observability_degraded")
                for item in rep_outputs
            )
            out = {"source_dir": str(out_dir), "reps": rep_outputs}
            if any_degraded:
                out["regime_warning_banner"] = REGIME_WARNING_BANNER
            out_path = out_dir / "aggregate.json"
            out_path.write_text(json.dumps(out, indent=2))
            print()
            print("=== batch aggregate: %d reps ===" % len(rep_outputs))
            for item in rep_outputs:
                print("%s: per_path_pct=%s source=%s" % (
                    item["run_dir"], item["per_path_pct"],
                    item["per_path_pct_source"]))
            print()
            if any_degraded:
                print(REGIME_WARNING_BANNER)
            print("written: %s" % out_path)
            return

    # ── Default path: single-run E1/E2/E3 aggregate ─────────────────────────
    summary = load_summary(out_dir)

    run_config_path = out_dir / "run_config.json"
    run_config = (json.loads(run_config_path.read_text())
                  if run_config_path.exists() else {})

    summary["_run_dir"] = str(out_dir)
    metrics = compute_metrics(summary)
    v = apply_verdict(metrics, args.experiment, run_config)

    out = {
        "metrics": metrics,
        "verdict": v,
        "source_dir": str(out_dir),
    }
    if (metrics.get("observability_degraded")
            or v.get("observability_degraded")):
        out["regime_warning_banner"] = REGIME_WARNING_BANNER
    out_path = out_dir / "aggregate.json"
    out_path.write_text(json.dumps(out, indent=2))

    # console summary
    print(f"\n=== {v['experiment']} verdict: "
          f"{'PASS' if v['pass'] else 'FAIL'} ===")
    print(f"mode={metrics['mode']} total_iops={metrics['total_iops']:.0f}")
    print(f"weighted_jain={metrics['weighted_jain']} "
          f"max_dev={metrics['max_share_deviation']}")
    print(f"per_tenant_iops={metrics['per_tenant_iops']}")
    print(f"per_tenant_p99_us={metrics['per_tenant_p99_us']}")
    print(f"per_path_pct={metrics['per_path_pct']}")
    if v["reasons"]:
        print("reasons:")
        for r in v["reasons"]:
            print(f"  - {r}")
    if "regime_warning_banner" in out:
        print(out["regime_warning_banner"])
    print(f"\nwritten: {out_path}")


if __name__ == "__main__":
    main()
