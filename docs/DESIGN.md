# SAPS control-loop design

SAPS coordinates path selection and tenant scheduling through one representation
of deliverable path capacity. The controller receives completion observations,
estimates path health, computes a feasible namespace budget, and publishes a
per-tenant, per-path rate matrix. The submission path consumes that matrix
without recomputing policy.

## System model

A namespace is reachable through a set of paths `P` and shared by a set of
tenants `T`.

- `K_p` is the provisioned deliverable capacity of path `p`.
- `h_p` is the current health estimate for path `p`, bounded by zero and one.
- `C` is the service envelope configured for the namespace.
- `d_t` and `w_t` are tenant demand and weight.
- `r_t` is the admitted tenant rate.
- `x_t,p` is the rate assigned to tenant `t` on path `p`.

The controller does not estimate `K_p` from traffic after steering. Moving
work away from a path would otherwise make that path appear to have less
capacity. `K_p` therefore comes from provisioning or an independent capacity
measurement.

## Completion-driven health

The host records sampled submissions and completions in a shared ring. A DPA
polling loop consumes those records and maintains state for each path.
Completion status distinguishes protocol outcomes that reachability, queue
occupancy, and timing alone cannot express. Relative latency and tail behavior
capture reachable but degraded paths.

The estimator publishes a dimensionless health value. A healthy path has health
one. A degraded path receives a graded value below one. A path that should not
carry bulk traffic reaches zero effective capacity and receives only a bounded
recovery probe.

## Health-coupled adaptive allocation

The effective capacity of path `p` is:

```text
e_p = K_p h_p
```

The feasible namespace budget is:

```text
B = min(C, sum over p of e_p)
```

A demand-aware weighted progressive fill allocates `B`. It satisfies tenants
whose demand is below their weighted fair share, removes them from the active
set, and divides the remaining budget among saturated tenants by weight. The
result obeys:

```text
0 <= r_t <= d_t
sum over t of r_t <= B
```

For paths with nonzero effective capacity, SAPS splits each tenant rate in
proportion to `e_p`:

```text
x_t,p = r_t e_p / (sum over q of e_q)
```

The probe allowance is taken from the admitted rate, so recovery traffic cannot
increase the namespace budget.

## Publication and enforcement

The DPA publishes the rate matrix and its associated health state with a
sequence and commit sequence. The host accepts a matrix only when both values
match. This prevents a submission thread from reading a partially written
epoch.

The SPDK selector samples paths in proportion to the committed row for the
current tenant. Per-path token buckets enforce the same row. Selection and
admission therefore consume one decision instead of maintaining independent
views of capacity.

## Safety properties

The current implementation checks the following properties at each validated
run:

1. Total tenant allocation does not exceed the smaller of the service envelope
   and effective path capacity.
2. Tenant allocation does not exceed observed demand.
3. Saturated tenants receive weighted max-min service.
4. Paths with zero bulk capacity receive at most the configured probe allowance.
5. A committed matrix is consumed only after complete publication.
6. Every configured path capacity is explicit. A missing `K_p` is rejected
   rather than replaced with the namespace envelope.

## Controlled comparison modes

The implementation exposes three allocation modes for experiments. They share
the estimator, demand tracking, weighted progressive fill, and selector.

- `continuous` uses graded health in both the feasible budget and path split.
- `fixed` keeps admission at the nominal namespace envelope while using
  observed health for path placement.
- `binary` maps any non-healthy path to zero bulk capacity.

The signal-isolation campaign also selects completion semantics, queue depth, or
request completion time as the health source while keeping the allocator fixed.
Reachability is represented by a controller configuration that leaves every
reachable path healthy.
