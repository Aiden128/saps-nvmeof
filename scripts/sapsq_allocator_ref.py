#!/usr/bin/env python3
"""Reference implementation of SAPS weighted max-min progressive fill.

Used to validate the DPA C implementation (sapsq_allocate() in dpa_plugin_dev.c)
on synthetic scenarios before touching testbed hardware.

Algorithm: Bertsekas weighted max-min progressive fill, per
specs/dm-research-redesign-20260522.md §4.3.

Run: python3 scripts/sapsq_allocator_ref.py
     (runs 6 scenarios, prints expected per-tenant + per-path matrix,
      computes weighted Jain and verifies acceptance criteria).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class Scenario:
    name: str
    weights: List[float]            # per-tenant raw weights
    demand: List[float]             # per-tenant demand (same unit as capacity, e.g. IOPS)
    service_envelope: float         # maximum service admitted for the namespace
    base_capacity: List[float]      # per-path base capacity
    health_factor: List[float]      # per-path health [0..1]
    eligibility: List[str]          # per-path 'NORMAL' / 'PROBE' / 'QUARANTINE'
    probe_rate: List[float]         # per-path floor when PROBE-eligible
    expected_jain_min: float = 0.99 # default E1 acceptance
    notes: str = ""


def weighted_jain(
    rates: List[float],
    weights: List[float],
    demand: List[float] | None = None,
) -> float:
    """Weighted Jain index over normalized shares r_i / w_i.

    Returns 1.0 when all active tenants get the same r_i/w_i.
    """
    if not rates or not weights:
        return 0.0
    norm = [r / w if w > 0 else 0.0 for r, w in zip(rates, weights)]
    if demand is None:
        active = [n for n, w in zip(norm, weights) if w > 0]
    else:
        active = [
            n
            for n, w, d in zip(norm, weights, demand)
            if w > 0 and d > 0
        ]
    if not active:
        return 0.0
    s1 = sum(active)
    s2 = sum(n * n for n in active)
    return (s1 * s1) / (len(active) * s2) if s2 > 0 else 0.0


def allocate(sc: Scenario):
    """Pure-Python progressive fill. Returns (per_tenant_rate, per_(t,p)_rate matrix)."""
    T = len(sc.weights)
    P = len(sc.base_capacity)

    # 1. Per-path effective capacity from base × health
    eff_cap = [sc.base_capacity[p] * sc.health_factor[p] for p in range(P)]

    # 2. Eligibility-aware capacity available for normal traffic.
    #    Quarantined paths contribute only their probe_rate (not normal capacity).
    avail = []
    for p in range(P):
        if sc.eligibility[p] == 'QUARANTINE':
            avail.append(0.0)
        elif sc.eligibility[p] == 'PROBE':
            avail.append(max(sc.probe_rate[p], eff_cap[p]))
        else:  # NORMAL
            avail.append(eff_cap[p])

    # 3. Active tenants = those with w>0 AND demand>0.
    active = [i for i in range(T) if sc.weights[i] > 0 and sc.demand[i] > 0]

    if not active:
        return [0.0] * T, [[0.0] * P for _ in range(T)]

    available_path_capacity = sum(avail)
    total_cap = min(sc.service_envelope, available_path_capacity)

    # 4. Progressive fill across active tenants by ascending demand/weight ratio.
    #    Tenants whose demand <= fair_share are satisfied (released cap to others);
    #    saturated tenants split remaining_cap proportional to weight in second pass.
    sorted_by_ratio = sorted(active, key=lambda i: sc.demand[i] / sc.weights[i])

    remaining_cap = total_cap
    remaining_weight = sum(sc.weights[i] for i in active)
    rate = [0.0] * T
    saturated = []

    for i in sorted_by_ratio:
        if remaining_weight <= 0:
            break
        fair_rate = remaining_cap * sc.weights[i] / remaining_weight
        if sc.demand[i] <= fair_rate:
            rate[i] = sc.demand[i]
            remaining_cap -= rate[i]
            remaining_weight -= sc.weights[i]
        else:
            saturated.append(i)

    # 5. Saturated tenants split remaining_cap by weight
    if saturated and remaining_weight > 0:
        for i in saturated:
            rate[i] = remaining_cap * sc.weights[i] / remaining_weight

    # 6. Split each tenant's rate across paths proportional to (avail).
    #    Quarantined paths get 0; probe paths get at least probe_rate when tenant is active.
    matrix = [[0.0] * P for _ in range(T)]
    eligible_cap = sum(avail[p] for p in range(P) if sc.eligibility[p] != 'QUARANTINE')
    for i in range(T):
        if rate[i] <= 0 or eligible_cap <= 0:
            continue
        for p in range(P):
            if sc.eligibility[p] == 'QUARANTINE':
                matrix[i][p] = 0.0
            else:
                matrix[i][p] = rate[i] * avail[p] / eligible_cap
        # Probe floor: ensure x_{i,p} >= probe_rate[p] on PROBE paths (per redesign §4.3 step 5)
        for p in range(P):
            if sc.eligibility[p] == 'PROBE' and matrix[i][p] < sc.probe_rate[p]:
                deficit = sc.probe_rate[p] - matrix[i][p]
                matrix[i][p] = sc.probe_rate[p]
                # Pay for the deficit by shaving normal paths proportionally
                normal_paths = [q for q in range(P) if sc.eligibility[q] == 'NORMAL' and matrix[i][q] > 0]
                if normal_paths:
                    shave_total = sum(matrix[i][q] for q in normal_paths)
                    if shave_total > deficit:
                        for q in normal_paths:
                            matrix[i][q] *= (shave_total - deficit) / shave_total

    return rate, matrix


# ── 6 scenarios per Task #6 description ──────────────────────────────────────

def scenarios() -> List[Scenario]:
    return [
        Scenario(
            name="S1 all-healthy w=[3,1,1,1] saturated",
            weights=[3, 1, 1, 1],
            demand=[1_000_000] * 4,           # huge: every tenant saturated
            service_envelope=600_000,
            base_capacity=[200_000, 200_000, 200_000],
            health_factor=[1.0, 1.0, 1.0],
            eligibility=['NORMAL', 'NORMAL', 'NORMAL'],
            probe_rate=[0.0, 0.0, 0.0],
            notes="E1 hero: weighted Jain ≥ 0.99, tenant 0 gets 3× the rest",
        ),
        Scenario(
            name="S2 tenant 1 idle (work-conserving)",
            weights=[3, 1, 1, 1],
            demand=[1_000_000, 0, 1_000_000, 1_000_000],
            service_envelope=600_000,
            base_capacity=[200_000, 200_000, 200_000],
            health_factor=[1.0, 1.0, 1.0],
            eligibility=['NORMAL', 'NORMAL', 'NORMAL'],
            probe_rate=[0.0, 0.0, 0.0],
            notes="E2: tenant 1 idle → unused share goes to active by weight (3:1:1)",
        ),
        Scenario(
            name="S3 path B health=0.3",
            weights=[3, 1, 1, 1],
            demand=[1_000_000] * 4,
            service_envelope=600_000,
            base_capacity=[200_000, 200_000, 200_000],
            health_factor=[1.0, 0.3, 1.0],     # path B degraded
            eligibility=['NORMAL', 'NORMAL', 'NORMAL'],
            probe_rate=[0.0, 0.0, 0.0],
            notes="E3 hero: total cap = 200K+60K+200K = 460K; weights still 3:1:1:1",
        ),
        Scenario(
            name="S4 path B EXCLUDED with probe",
            weights=[3, 1, 1, 1],
            demand=[1_000_000] * 4,
            service_envelope=600_000,
            base_capacity=[200_000, 200_000, 200_000],
            health_factor=[1.0, 0.001, 1.0],   # essentially zero
            eligibility=['NORMAL', 'PROBE', 'NORMAL'],
            probe_rate=[0.0, 100.0, 0.0],      # 100 IOPS probe on path B
            notes="E3 escalation: path B quarantined, probe budget keeps recovery probe alive",
        ),
        Scenario(
            name="S5 all paths quarantined except probe",
            weights=[3, 1, 1, 1],
            demand=[1_000_000] * 4,
            service_envelope=600_000,
            base_capacity=[200_000, 200_000, 200_000],
            health_factor=[0.001, 0.001, 0.001],
            eligibility=['PROBE', 'PROBE', 'PROBE'],
            probe_rate=[100.0, 100.0, 100.0],
            expected_jain_min=0.0,  # degenerate; just verify no division-by-zero
            notes="Degenerate edge: all paths probe-only — verifies no crash + tiny total throughput",
        ),
        Scenario(
            name="S6 phase 1 weights=[1,1,1,1] all saturated",
            weights=[1, 1, 1, 1],
            demand=[1_000_000] * 4,
            service_envelope=600_000,
            base_capacity=[200_000, 200_000, 200_000],
            health_factor=[1.0, 1.0, 1.0],
            eligibility=['NORMAL', 'NORMAL', 'NORMAL'],
            probe_rate=[0.0, 0.0, 0.0],
            notes="E4 reweight phase 1: equal share = 150K each",
        ),
    ]


def main() -> int:
    failures = 0
    for sc in scenarios():
        print(f"\n=== {sc.name} ===")
        print(f"    {sc.notes}")
        rate, matrix = allocate(sc)
        T = len(sc.weights)
        P = len(sc.base_capacity)
        total_alloc = sum(rate)
        available_path_capacity = sum(
            sc.base_capacity[p] * sc.health_factor[p] for p in range(P)
        )
        admitted_capacity = min(sc.service_envelope, available_path_capacity)
        jain = weighted_jain(rate, sc.weights, sc.demand)
        print(f"    per-tenant rate: {[round(r, 1) for r in rate]}")
        print(f"    per-(t,p) matrix (rows=tenants, cols=paths):")
        for i in range(T):
            row = "      [" + ", ".join(f"{matrix[i][p]:9.1f}" for p in range(P)) + "]"
            print(row)
        print(
            f"    total alloc: {total_alloc:.1f}   "
            f"admitted cap: {admitted_capacity:.1f}   "
            f"available path cap: {available_path_capacity:.1f}   "
            f"util: {total_alloc/admitted_capacity*100 if admitted_capacity > 0 else 0:.1f}%"
        )
        print(f"    weighted Jain: {jain:.4f}   threshold: {sc.expected_jain_min:.2f}")
        if jain >= sc.expected_jain_min:
            print(f"    VERDICT: PASS")
        else:
            print(f"    VERDICT: FAIL (Jain below threshold)")
            failures += 1

    # Spot checks (per-tenant rate ratios)
    print("\n=== Spot checks ===")
    sc1 = scenarios()[0]  # S1
    rate1, _ = allocate(sc1)
    if rate1[0] > 0 and rate1[1] > 0:
        ratio = rate1[0] / rate1[1]
        ok = abs(ratio - 3.0) < 0.05
        print(f"S1 tenant0/tenant1 rate ratio: {ratio:.3f} (expected 3.0)  {'PASS' if ok else 'FAIL'}")
        if not ok:
            failures += 1

    sc2 = scenarios()[1]  # S2 idle
    rate2, _ = allocate(sc2)
    expected_active_total = min(
        sc2.service_envelope,
        sum(
            sc2.base_capacity[p] * sc2.health_factor[p]
            for p in range(len(sc2.base_capacity))
        ),
    )
    actual_active_total = rate2[0] + rate2[2] + rate2[3]
    util = actual_active_total / expected_active_total
    ok = util >= 0.99  # work-conserving: active tenants consume essentially all capacity
    print(f"S2 work-conserving util on active tenants: {util*100:.2f}% (expected ≥99%)  {'PASS' if ok else 'FAIL'}")
    if not ok:
        failures += 1

    sc3 = scenarios()[2]  # S3 path B degraded
    rate3, mat3 = allocate(sc3)
    pathB_share = sum(mat3[i][1] for i in range(len(sc3.weights))) / sum(sum(mat3[i]) for i in range(len(sc3.weights)))
    # Path B has 60K / 460K = 13% of total cap → traffic split should match
    ok = 0.10 < pathB_share < 0.16
    print(f"S3 path B share of total IO: {pathB_share*100:.2f}% (expected ~13%)  {'PASS' if ok else 'FAIL'}")
    if not ok:
        failures += 1

    print(f"\nTotal failures: {failures}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
