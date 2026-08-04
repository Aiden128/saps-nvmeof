# SAPS controller specification

This specification states the behavior implemented by the current prototype.
For the design rationale, see `DESIGN.md`. For source-level mapping, see
`IMPLEMENTATION.md`.

## Inputs

For each path:

- a provisioned deliverable capacity `K_p`
- a completion-derived health value `h_p`
- a path state used to distinguish normal service from recovery probing

For each tenant:

- a positive weight `w_t`
- an observed demand `d_t`
- a stable tenant identifier shared by the coordinator and submission process

For the namespace:

- a configured service envelope `C`
- an allocation epoch
- a bounded recovery-probe allowance

## Outputs

Every committed epoch contains:

- the health used by allocation for each path
- the effective capacity `K_p h_p` for each path
- a total admitted rate for each tenant
- a rate for each tenant and path
- a sequence and matching commit sequence

## Allocation

The controller computes:

```text
e_p = K_p h_p
B   = min(C, sum_p e_p)
```

It applies demand-aware weighted progressive filling to `B`. Tenants whose
demand is below their current weighted share are satisfied first. Remaining
capacity is divided among saturated tenants by weight.

Each tenant's rate is divided among service-capable paths in proportion to
effective capacity. Paths with zero effective capacity receive only their share
of the bounded probe allowance.

## Required behavior

1. Configuration fails when a path capacity is missing, zero, or has the wrong
   number of entries.
2. Allocation never exceeds the feasible namespace budget.
3. A tenant never receives more than its current demand estimate.
4. Weighted max-min fairness holds among saturated active tenants.
5. Probe traffic is included in the admitted tenant rate.
6. The selector and admission hook consume the same committed matrix.
7. The host never consumes an incompletely published epoch.
8. A single tenant and multiple tenants use the same mechanism.

## Comparison modes

The following modes exist to isolate design choices:

- `continuous` uses graded health for both admission and placement.
- `fixed` uses the nominal envelope for admission and observed health for
  placement.
- `binary` excludes every path whose health is below one.

The signal source can also be changed while allocation remains fixed. The
published comparison sources are completion semantics, reachability, queue
depth, and request completion time.

## Bounds

The current static layout supports sixteen tenants and eight paths. These are
implementation bounds rather than properties of the allocation method.
