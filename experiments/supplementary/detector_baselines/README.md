# detector baseline campaign

Copy this directory to `/mnt/nvme0n1p1/aiden/DPA/baseline_single` on arm-2, then run:
```bash
cd /mnt/nvme0n1p1/aiden/DPA/baseline_single
sudo -n bash ./drive_campaign.sh --smoke --out "/mnt/nvme0n1p1/aiden/DPA/baseline-results/smoke-$(date -u +%Y%m%dT%H%M%S)"
sudo -n bash ./drive_campaign.sh --full --out "/mnt/nvme0n1p1/aiden/DPA/baseline-results/full-$(date -u +%Y%m%dT%H%M%S)"
```
Use a new/empty output directory. Scripts need Python 3.10+, Bash, taskset, flock, and tee.
Estimated time: smoke 2–4 minutes; full 20–35 minutes (reserve 45). Run smoke first.
Workloads alone take 40 seconds for smoke and 900 seconds for the full matrix.
Smoke runs SAPS_Q and fixed_threshold for 20s each, B fault at 5s, restore at 18s, measure 8–17s.
Full runs 3 healthy stock calibrations, then 3 repetitions of each of:
`saps_q`, `fixed_threshold`, `per_path_adaptive`, and **stock round robin**.
Every full workload is 60s; B fault starts at 20s, restores at 50s; measurement is 23–49s.
All runs use 4 tenants, weights 3:1:1:1, QD32, 4096-byte random reads, 3 RDMA paths.
Targets are 3 independent 1GiB malloc→delay→error stacks sharing NQN/NSID/UUID/NGUID.
Read-only data permits independent malloc copies; this topology assumes no coherent writes.
A/B/C listen on 10.0.0.2:4430/4431/4432 over mlx5_0; targets cores40–45, initiators4–11.
Each target uses `-g -s 1536`; each initiator uses `-g -s 384` MiB.
The driver reserves 6400MiB worth of hugepages (rounded up), retains larger existing reserves,
checks free pages, then stops recorded PID/start-tick matches and restores the prior count on EXIT.
Its cooperative lock protects these scripts; unrelated hugepage changes during a run are excluded.

Default fixed aggregate path capacities are 300000,300000,300000 IOPS; service limit is 900000.
Override together with `--path-capacities-iops K_A,K_B,K_C --service-limit-iops C`.
Capacities must be positive multiples of 1000 and C must equal their sum; no tenant target QoS.
Stock/detector arms enforce no tenant weights; 3:1:1:1 is their accounting entitlement only.
SAPS uses the original full profile: sample rate32, completion health, M2/M5 enabled, FSM live.
External policies retain their original algorithms; fixed threshold500us, adaptive EWMA/CUSUM defaults,
1Hz polling, and hold-down through the fault. Options: `--fixed-threshold-us`, `--poll-interval-s` (≤1).
Counter frequency comes from the plugin's cntfrq_el0 read; `--counter-hz HZ` records an explicit override.
The fixed reference denominator is the median of 3 healthy stock aggregate IOPS measurements;
each aggregate sums tenant interval-weighted means over 23–49s. Smoke reports no calibrated shares.
Timing uses a common future epoch; individual workload dispatch times/skew are recorded (limit0.5s).
Fault writes use local SPDK rpc.py; all four B delay fields must read back 5000us, A/C stay27us.
Every run requires all 4 tenants' usable samples and ≥60% measurement coverage. Fault runs require
B interval mean latency≥max(1000us,2×healthy) or a SAPS health drop; SAPS also requires ring/budget progress.
Failure exits nonzero and stops that campaign; failed runs remain marked invalid for inspection.

Outputs: `driver.log`, `driver_provenance.txt`, `runtime_artifacts/` target logs/setup/recorded PIDs.
`smoke/` or `full/`: campaign.json status/results, provenance.json HEAD/hashes/topology,
summary.csv per-run aggregates/shares; full/calibration.json holds the one fixed denominator.
Each run: manifest.json commands/environment/timing, initiator_preflight.json paths/controller IDs,
target_before/after.json caps/delays, fault_inject/restore.json incremental updates/readbacks,
tenant*.log, perform_t*.json/workload_results.json RPC results, metrics.json validated measurements,
controller_sampler.csv for SAPS demand/budgets/health, and failure.json when validation fails.
collector/: preflight/manifest/summary/metrics.json, per_path_samples.csv, per_tenant_samples.csv,
events.jsonl detector actions/path state. Passive observer decisions are counterfactual diagnostics;
use run-level metrics.json for actual external actions. SAPS health timing is 1Hz, not reroute latency.

Unverified on arm-2: CPUs/ports available; RDMA and four-initiator multipath/controller IDs;
SPDK RPC/readback/result schemas and patched plugin/path statistics/sampler support; coordinator ring
startup, firmware/libraries, memlock permissions, counter autodetection, and budget enforcement.
Also unverified: sufficient RAM/hugepage allocation at this budget, clean process exit/page release,
polling/startup deadlines and clock stability, actual 5ms response, threshold suitability, capacities,
calibration stability, available output space, and wall time. Smoke checks the observable assumptions.
HEAD is read from repository metadata; dirty state and binary-to-source/firmware identity stay unverified.
Validation here is static source review plus py_compile/bash -n only; no hardware workload was run.
