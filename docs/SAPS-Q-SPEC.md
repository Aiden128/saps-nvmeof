# D/M Research Redesign For Submission

Date: 2026-05-22
Status: stricter redesign after reviewer-style audit
Scope: `/home/user/DPA/nvme-of-controller`

This document supersedes the earlier assumption that D-series is simply
"closed." D has strong evidence, but it is not unconditionally submission-ready.
M needs a clearer problem statement, a real scheduler algorithm, and experiments
that connect D path-health signals to multi-tenant QoS.

## 1. External Anchors

The system contribution should be positioned against these facts:

- SPDK NVMe multipath active-active selection is round-robin or minimum queue
  depth. This makes stock SPDK local-load-aware, not path-health-aware.
  Source: https://spdk.io/doc/nvme_multipath.html
- SPDK retry behavior treats DNR and path errors through generic retry/failover
  logic. Any DPA retry override must be framed carefully because DNR has
  semantic implications.
  Source: https://spdk.io/doc/nvme_multipath.html
- SPDK bdev QoS exposes per-bdev rate-limit APIs. This is a useful baseline, but
  it is not a per-tenant/per-path scheduler and does not consume DPA path-health
  signals by itself.
  Source: https://spdk.io/doc/bdev_8h.html
- NVIDIA DPA is intended for datapath acceleration, runs event/RPC handlers, is
  not a standalone CPU, and is managed by host/DPU processes. This supports a
  hybrid design: DPA computes signals and budgets; host enforces them.
  Source: https://docs.nvidia.com/doca/sdk/dpa-development/index.html
- DRR is the right family for implementation constraints: fair queueing with
  simple per-flow state and O(1)-style scheduling logic.
  Source: Shreedhar and Varghese, SIGCOMM 1995 / IEEE/ACM ToN 1996,
  https://dblp.org/rec/conf/sigcomm/ShreedharV95

## 2. Re-audit Of D-series

### 2.1 What Is Actually Strong

D has a real contribution if it is framed narrowly:

> DPA is useful as a near-NIC signal processor for selected NVMe-oF multipath
> fault classes where stock SPDK's queue-depth policy lacks semantic and
> tail-latency visibility, and host-side classifiers consume too much reactor
> budget.

Strong D evidence:

- D0: healthy overhead is small, so the system can be left on.
- D1: DPA vs host_saps is the strongest offload result. If the same classifier
  collapses on host but works on DPA, that is a real systems argument.
- D3 sct=3 / mixed: path-level errors and retry/failover policy are plausible
  cases where DPA semantic observation can help.
- D2: active probe / recovery is a good idea, but the "0 ms" result must be
  rewritten as "below measurement resolution" unless microsecond traces exist.

### 2.2 What Is Not Yet Strong Enough

D is not ready for a top-tier submission if the paper overclaims these points:

| Area | Problem | Required action |
|---|---|---|
| D4 bimodal | Sensitivity sweep contradicts the hero story: DPA IOPS is often below stock and P99.9 saturates at the same value. | Re-run after fixing classifier floor, or downgrade claim to routing suppression, not 65x latency win. |
| D5 per-opcode | Read-side routing works; write-side routing does not show signal. | Claim read-side only, or fix write classifier and re-run. |
| D7 shared-fate | Sweep shows `fsm_shared_fate_triggers=0`; the benefit is WRS routing equalization, not shared-fate FSM. | Rewrite mechanism, remove FSM hero language. |
| D3 media/DNR | Overriding media-error DNR can look unsafe unless the injected fault is clearly path/controller-level and data integrity is preserved. | Either narrow to path errors or include a correctness argument and data-integrity check. |
| D2 timing | "0 ms" detection/recovery is not credible as a literal number. | Use trace resolution, distribution, and threshold-crossing time. |
| Host_saps baseline | A 17.4x collapse is powerful but reviewer will ask if host_saps is implemented fairly. | Add profiling: cycles/IO, reactor utilization, same algorithm, same build flags. |
| Sensitivity | Several D wins are single-configuration heroes. | Use sweeps to define applicability envelope instead of hiding weak regions. |

### 2.3 D Verdict

D is **conditionally strong**, not automatically sufficient.

It can support a serious paper if the paper claims:

> SAPS improves selected latency/error/tail-aware multipath decisions under a
> documented fault envelope, with small healthy overhead.

It cannot safely claim:

> DPA generally solves multipath degradation, contention, partial capacity loss,
> or all tail-latency cases.

For a top-tier target, D must be cleaned in two ways:

1. Separate **validated claims** from **failed/deferred claims**.
2. Turn D8 and the D4/D7 contradictions into explicit motivation for M, not
   hidden weaknesses.

## 3. The Correct M Problem

M should not be "multi-client experiments." M should be:

> Path-health-aware weighted scheduling for multi-tenant NVMe-oF initiators.

### 3.1 Problem Statement

Multiple SPDK initiators or tenant processes share the same NVMe-oF path set.
Each tenant has a weight and demand. Each path has time-varying health, capacity,
tail risk, and error state.

Existing mechanisms are incomplete:

- Stock SPDK sees local queue depth, not global tenant demand or DPA path health.
- SPDK bdev QoS can rate-limit bdevs but is target/bdev-oriented and static
  unless driven by an external control loop.
- A host-only scheduler needs shared cross-process state in the hot path.
- A DPA-only admission gate can deadlock if refresh depends on completion events.

M's research question should be:

> Can a DPA signal plane compute per-tenant/per-path scheduling budgets from
> path-health signals, while host fast-path enforcement preserves progress and
> low overhead?

## 4. SAPS-Q Algorithm

SAPS-Q is a two-plane scheduler.

### 4.1 State

Let:

- `T` be tenants.
- `P` be paths.
- `w_i` be tenant `i`'s weight.
- `d_i(t)` be tenant `i`'s estimated demand.
- `C_p(t)` be path `p`'s usable capacity.
- `R_p(t)` be path `p`'s tail/error risk.
- `E_p(t)` be path eligibility: 1 for usable, 0 for quarantined, epsilon for
  probe-only.
- `x_{i,p}(t)` be the allocated IO/s budget from tenant `i` to path `p`.

The scheduler solves:

```text
maximize weighted fairness and work conservation
subject to:
  sum_i x_{i,p} <= C_p(t) * E_p(t)          for each path p
  sum_p x_{i,p} <= d_i(t)                  for each tenant i
  x_{i,p} >= probe_rate                    for probe-eligible paths
  x_{i,p} = 0                              for quarantined non-probe paths
```

The fairness target is weighted max-min:

```text
served_i / w_i should be equal among saturated active tenants,
while unsaturated tenants receive only their demand and unused share is
redistributed.
```

### 4.2 Path Health To Capacity

DPA should not estimate capacity only from post-admission completions. D8 showed
why: that creates self-reinforcing feedback.

Instead:

```text
C_p(t) = C_base_p * health_factor_p(t)
```

Where `health_factor_p` is produced from D-series classifiers:

| D classifier state | Scheduler treatment |
|---|---|
| HEALTHY | `health_factor = 1.0` |
| ONSET / SUSPECT | reduce to 0.25-0.75 depending on tail risk |
| PERPETUAL_SLOW | near-zero normal budget, keep probe budget |
| BIMODAL_TAIL | cap by tail-risk penalty, not mean latency |
| SPARSE_PATH_ERROR | quarantine normal traffic, keep recovery probe |
| MEDIA_ERROR-like | do not blindly reroute unless correctness policy says path-level transient |
| RECOVERING | ramp up with hysteresis |

For partial degradation, add explicit probes:

- Reserve `probe_rate_p` independent of normal allocation.
- Use probe latency/completion rate to update `health_factor_p`.
- Do not let a path's observed served rate become its capacity estimate.

### 4.3 Weighted Max-Min Allocation

Every scheduler epoch:

1. Compute active tenants:

```text
A = { i | d_i > demand_threshold }
```

2. Compute usable path capacity:

```text
C_eff = sum_p C_p * E_p
```

3. Progressive fill tenant rates:

```text
remaining_capacity = C_eff
remaining_weight = sum_{i in A} w_i
for tenants sorted by demand_i / w_i:
    fair_rate = remaining_capacity * w_i / remaining_weight
    if demand_i <= fair_rate:
        r_i = demand_i
        remaining_capacity -= r_i
        remaining_weight -= w_i
    else:
        defer; saturated tenants later receive w_i-proportional share
```

After unsaturated tenants are assigned, saturated tenants split remaining
capacity by weight. This gives work conservation: idle tenants do not waste
their share.

4. Split each tenant's rate across eligible paths:

```text
x_{i,p} = r_i * (C_p * E_p) / sum_{q in eligible(i)} C_q * E_q
```

Optional tail-aware version:

```text
effective_C_p = C_p / (1 + lambda * tail_risk_p)
```

5. Publish:

```text
sapsq_epoch++
sapsq_tenant_path_rate_q32[i][p] = encode(x_{i,p})
sapsq_path_health[p] = health state
sapsq_epoch_commit = sapsq_epoch
```

Host reads only stable epochs.

### 4.4 Host Enforcement

Host enforces budgets in `dpa_plugin_admission_check()` or the nearest submit
hook:

```text
on submit(tenant i, path p, nbytes):
    read stable scheduler epoch
    if epoch stale:
        use static M3-v2 token bucket fallback
    refresh token[i][p] using rate_q32[i][p] and host TSC
    cost = max(1, nbytes / 4096)
    if token[i][p] >= cost:
        token[i][p] -= cost
        admit
    else if another eligible path has tokens:
        steer to that path
    else:
        return -EAGAIN through SPDK queued request path
```

Important: refresh must be host-side or time-driven, not completion-driven.
Otherwise the system can enter no-completion/no-refresh/no-admit deadlock.

### 4.5 Why Not Use Current M5 As Hero

The current M5 DRR idea is directionally useful, but the testbed summaries do
not prove weighted scheduling:

- `m3_v3_drr_scenC`: `[47201, 43609, 43776, 43551]`
- `m3_v3_drr_oversub`: `[44222, 42444, 43096, 43117]`

Those are close to equal share, not `[100000, 33333, 33333, 33333]`.

Likely issue: the DPA grant model does not have a clean demand queue or
per-path scheduling surface; it issues grants against a gap/burst heuristic and
does not actually choose tenant/path IOs. For the next step, use M3-v2's proven
host token bucket as the enforcement substrate, and make rates dynamic via DPA
published budgets. Revisit M5 only after its demand accounting and weighted
testbed behavior are fixed.

## 5. Evaluation That Would Be Publishable

### E1. D Cleanup

Before M hero experiments, repair or rewrite D:

- D4: fix classifier floor or downgrade to routing suppression.
- D5: fix write-side or claim read-side only.
- D7: remove shared-fate FSM claim unless triggers appear.
- D3: separate path error from media data corruption.
- D2: report timing with real trace resolution.

### E2. Static Scheduler Sanity

Show SAPS-Q reduces to M3-v2 when D health is uniform:

- 4 tenants, weights `[3,1,1,1]`.
- All active.
- Expected: weighted Jain >= 0.99.

### E3. Work-Conserving Idle Tenant

This is mandatory.

- 4 tenants, `[3,1,1,1]`.
- Tenant 1 idle in middle phase.
- Static token bucket should waste share.
- SAPS-Q should redistribute unused budget to active tenants.

### E4. Path Degradation With QoS

This is the M hero.

- 4 tenants, 3 paths.
- Inject D1 or D4 on one path.
- SAPS-Q should reduce budget on degraded path and preserve tenant weights.
- Compare:
  1. stock SPDK
  2. SPDK bdev QoS
  3. static M3-v2 token bucket
  4. SAPS-Q

Metrics:

- per-tenant IOPS
- weighted Jain
- per-tenant P99/P999
- per-path distribution
- path health transition time
- budget convergence time

### E5. Reweight And Recovery

- Change weights `[1,1,1,1] -> [3,1,1,1]`.
- Inject and remove path degradation.
- Report time to 90% of target and overshoot.

### E6. Overhead

- D0 with M off vs SAPS-Q on.
- D1/D3 with SAPS-Q on to ensure M does not damage D wins.

## 6. Paper Claim After Redesign

Do not write:

> DPA is generally better than stock for multipath and QoS.

Write:

> SAPS demonstrates that a DPA can be used as a near-NIC signal plane for
> NVMe-oF. For selected path fault classes, DPA-side classifiers provide
> path-health signals that stock queue-depth policies miss. For multi-tenant
> sharing, SAPS-Q converts those signals into per-tenant/per-path budgets and
> enforces them in the host fast path, preserving weighted fairness and
> work-conserving behavior under path degradation.

This is stronger because it is falsifiable:

- If D classifiers do not fire, SAPS-Q should not move budgets.
- If a tenant is idle, SAPS-Q should redistribute.
- If a path degrades, SAPS-Q should reduce its budget while keeping tenant
  fairness.
- If DPA stalls, host fallback should preserve progress.

## 7. Immediate Implementation Direction

Do not start by polishing paper text.

Start with one implementation slice:

1. Add shared-memory fields:
   - `sapsq_epoch`
   - `sapsq_tenant_path_rate_q32[T][P]`
   - `sapsq_path_health[P]`
   - `sapsq_admit_count[T][P]`
   - `sapsq_reject_count[T][P]`
2. DPA computes `health_factor_p` from existing D classifier state.
3. DPA computes weighted max-min tenant rates once per epoch.
4. Host M4 token bucket consumes DPA-published rates instead of env-only rates.
5. Orchestrator dumps per-tenant/per-path metrics.
6. Run E2, E3, E4 in that order.

If E4 passes, M becomes a real scheduler contribution. If E4 fails, the paper
should remain D-focused and M should be presented as future work / negative
design exploration.

