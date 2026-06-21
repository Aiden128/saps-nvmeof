# SAPS-Q Algorithm-Level Architecture

Date: 2026-05-26
Scope: current M-series / SAPS-Q algorithm path

This diagram expands the algorithm path behind SAPS-Q: how IO events become
path health, how path health becomes a tenant/path rate matrix, and how the host
enforces that matrix safely.

## ASCII Version

```text
                         SAPS-Q ALGORITHM-LEVEL ARCHITECTURE

  HOST IO FAST PATH                         SHARED MEMORY / MEMFD                 DPA EPOCH ALGORITHM
  -----------------                         --------------------                 -------------------

  submit(req)
    |
    |  fields:
    |  tenant_id, qp_id, path_id,
    |  opcode, nbytes, submit_tsc
    v
  +----------------------+        write       +----------------------+      read       +----------------------+
  | host event producer  | -----------------> | IO event ring        | -------------> | DPA process_event()  |
  | sample submit events |                    | sampled events       |                | update per-QP/path   |
  +----------------------+                    +----------------------+                | latency/status stats |
                                                                                     +----------+-----------+
                                                                                                |
                                                                                                v
  complete(req)                                                                      +----------------------+
    |                                                                                | per-QP/per-path      |
    |  fields:                                                                       | classifier state     |
    |  complete_tsc, status, latency                                                 |                      |
    v                                                                                | EWMA / NEWMA / P99   |
  +----------------------+        write       +----------------------+      read       | FSM state            |
  | host event producer  | -----------------> | IO event ring        | -------------> | fault_type           |
  | sample complete evts |                    | submit + complete    |                | retry verdict        |
  +----------------------+                    +----------------------+                +----------+-----------+
                                                                                                |
                                                                                                |
                                                                                                v
                                                                                     +----------------------+
                                                                                     | epoch trigger        |
                                                                                     |                      |
                                                                                     | if event_count >= N  |
                                                                                     | or TSC fallback hit  |
                                                                                     | then run scheduler   |
                                                                                     +----------+-----------+
                                                                                                |
                                                                                                v
                                                                                     +----------------------+
                                                                                     | sapsq_compute_health |
                                                                                     |                      |
                                                                                     | for each path p:     |
                                                                                     |  1. scan active QPs  |
                                                                                     |  2. take worst FSM   |
                                                                                     |  3. keep bad fault   |
                                                                                     |  4. map to health    |
                                                                                     |     and eligibility  |
                                                                                     +----------+-----------+
                                                                                                |
                                                      health[p], eligibility[p]                  |
                                                                                                v
                                                                                     +----------------------+
                                                                                     | health mapping       |
                                                                                     |                      |
                                                                                     | HEALTHY    -> 1.0 N  |
                                                                                     | DEGRADING -> fault   |
                                                                                     |              specific|
                                                                                     | EXCLUDED   -> Q/P    |
                                                                                     | RECOVERING -> ramp   |
                                                                                     +----------+-----------+
                                                                                                |
                                                                                                v
                                                                                     +----------------------+
                                                                                     | effective capacity   |
                                                                                     |                      |
                                                                                     | C[p] = base[p]       |
                                                                                     |      * health[p]     |
                                                                                     | quarantine -> 0      |
                                                                                     | probe -> probe floor |
                                                                                     +----------+-----------+
                                                                                                |
                                                                                                v
                                                                                     +----------------------+
                                                                                     | sapsq_allocate()     |
                                                                                     | weighted max-min     |
                                                                                     | progressive fill     |
                                                                                     +----------+-----------+
                                                                                                |
                                            +---------------------------------------------------+-------------------+
                                            |                                                                       |
                                            v                                                                       v
                              +----------------------------+                                       +----------------------------+
                              | tenant allocation          |                                       | path split                 |
                              |                            |                                       |                            |
                              | active if w[t] > 0        |                                       | x[t,p] = r[t] * C[p]      |
                              | and demand[t] > 0         |                                       |          / sum(C[eligible])|
                              |                            |                                       | quarantine path -> 0       |
                              | sort by demand[t] / w[t]  |                                       | probe path gets floor      |
                              | satisfy low demand first  |                                       +--------------+-------------+
                              | split rest by weight      |                                                      |
                              +-------------+--------------+                                                      |
                                            |                                                                     |
                                            +---------------------------+-----------------------------------------+
                                                                        |
                                                                        v
                                                          +----------------------------+
                                                          | tenant_path_rate_q32[t][p] |
                                                          | path_health_q16[p]         |
                                                          | path_eligibility[p]        |
                                                          +-------------+--------------+
                                                                        |
                                                                        | stable publication:
                                                                        | write matrix
                                                                        | writeback + release fence
                                                                        | commit epoch
                                                                        | writeback commit group
                                                                        v
  HOST ENFORCEMENT                         SHARED MEMORY / MEMFD
  ----------------                         --------------------

  +----------------------+       read       +----------------------------+
  | admission_check()    | <--------------- | committed epoch            |
  | on every submit      |                  | rates / health / elig      |
  +----------+-----------+                  +----------------------------+
             |
             v
  +----------------------+
  | stale epoch check    |
  |                      |
  | if stale:            |
  |   fallback to M4     |
  |   static rates       |
  | else: use SAPS-Q     |
  +----------+-----------+
             |
             v
  +----------------------+
  | token refresh        |
  |                      |
  | dt = host_tsc - last |
  | tokens[t,p] +=       |
  |   rate[t,p] * dt     |
  | cost = max(1,        |
  |   nbytes / 4096)     |
  +----------+-----------+
             |
             v
  +----------------------+          enough tokens          +----------------------+
  | token decision       | ------------------------------> | admit IO             |
  |                      |                                 | consume tokens       |
  | if tokens >= cost    |                                 | admit_count[t,p]++   |
  | else reject          |                                 +----------------------+
  +----------+-----------+
             |
             | not enough tokens
             v
  +----------------------+
  | return -EAGAIN       |
  | reject_count[t,p]++  |
  | SPDK queued_req      |
  | timer drain resubmit |
  +----------------------+


  OBSERVABILITY / EXPERIMENT GATE
  -------------------------------

  +----------------------+       dump       +----------------------+       aggregate       +----------------------+
  | sapsq_dump           | <--------------- | shared counters      | -------------------> | aggregate_sapsq.py  |
  | start/end snapshots  |                  | admit/reject/probe   |                      | E1/E2/E3 verdicts   |
  +----------------------+                  | epochs/stale fallback|                      | no fake per_path_pct|
                                            +----------------------+                      +----------+-----------+
                                                                                                    |
                                                                                                    v
                                                                                     +----------------------+
                                                                                     | paper-grade rule     |
                                                                                     |                      |
                                                                                     | per_path_pct must    |
                                                                                     | come from observed   |
                                                                                     | admit_delta only     |
                                                                                     +----------------------+


  CURRENT FAILURE POINT
  ---------------------

  The D classifier can label healthy saturated multi-tenant paths as SHARED_FATE.
  That bad fault_type contaminates health[p], then C[p], then x[t,p], so E3 cannot
  defend "SAPS-Q rerouted because of real path-health signal" yet.
```

## Compact Algorithm Summary

```text
for each sampled IO event:
    update per-QP/per-path latency, status, and FSM classifier state

when epoch fires:
    for each path:
        aggregate active QPs by worst FSM state and non-healthy fault_type
        map state/fault_type to health_q16 and eligibility

    for each path:
        capacity[p] = base_iops[p] * health_q16[p]
        capacity[p] = 0 if quarantined, probe floor if probe-only

    active_tenants = tenants where weight > 0 and demand > 0
    sort active tenants by demand / weight
    satisfy tenants whose demand is below weighted fair share
    split remaining capacity across saturated tenants by weight

    for each tenant and path:
        tenant_path_rate[t][p] = tenant_rate[t] * capacity[p] / sum(capacity)
        zero quarantined paths
        apply probe floor on probe paths

    publish rates with stable epoch protocol

on every host submit:
    if epoch stale:
        use M4 static fallback
    refresh token bucket using host TSC and tenant_path_rate[t][p]
    if tokens >= IO cost:
        consume tokens and admit
    else:
        return EAGAIN and let SPDK queued_req retry
```

