# baseline_v2: four campaigns

Copy this directory to /mnt/nvme0n1p1/aiden/DPA/baseline_v2 on arm-2.
Requires root, Python 3.10+, SPDK patched bdevperf/rpc.py, Bash, taskset, flock and tee; no new Python dependencies.
Use a new empty output directory for every invocation; run each smoke before its full campaign.

```bash
cd /mnt/nvme0n1p1/aiden/DPA/baseline_v2
sudo -n bash drive_campaign.sh --campaign c1 --smoke --reference-capacity-iops 899983.2185078033 --out /mnt/nvme0n1p1/aiden/DPA/baseline-results/c1-smoke-v2
sudo -n bash drive_campaign.sh --campaign c1 --full --reference-capacity-iops 899983.2185078033 --out /mnt/nvme0n1p1/aiden/DPA/baseline-results/c1-full-v2
sudo -n bash drive_campaign.sh --campaign c2 --smoke --out /mnt/nvme0n1p1/aiden/DPA/baseline-results/c2-smoke-v2
sudo -n bash drive_campaign.sh --campaign c2 --full --out /mnt/nvme0n1p1/aiden/DPA/baseline-results/c2-full-v2
sudo -n bash drive_campaign.sh --campaign c3 --smoke --out /mnt/nvme0n1p1/aiden/DPA/baseline-results/c3-smoke-v2
sudo -n bash drive_campaign.sh --campaign c3 --full --out /mnt/nvme0n1p1/aiden/DPA/baseline-results/c3-full-v2
sudo -n bash drive_campaign.sh --campaign c4 --smoke --out /mnt/nvme0n1p1/aiden/DPA/baseline-results/c4-smoke-v2
sudo -n bash drive_campaign.sh --campaign c4 --full --out /mnt/nvme0n1p1/aiden/DPA/baseline-results/c4-full-v2
```
Full workloads last 60s: inject B at 20s, restore at 50s, measure 23–49s. c3 has no injection.
Smoke workloads last 20s: inject at 5s, restore at 18s, measure 8–17s. c3 remains healthy.
Smoke covers each arm once at the first delay; c4 covers all three sample rates at 1000us.

| Campaign | Full arms / delays / sample rates | Path IOPS; C | Smoke/full runs | Workload time smoke/full | Estimated wall time smoke/full |
|---|---|---|---|---|---|
| c1 detector add-on | health_only, saps_binary; 5000us; 32; 3 reps | 300000 x3; 900000 | 2 / 6 | 40s / 6min | 2–5 / 10–20min |
| c2 p99 sweep | saps_q, saps_binary, health_only; 500,1000,2000,5000us; 32; 2 reps | 400000 x3; 900000 | 3 / 24 | 60s / 24min | 3–7 / 35–65min |
| c3 placement | stock_round_robin, host_saps, saps_q; healthy; 3 reps | 300000 x3; 900000 | 3 / 9 | 60s / 9min | 3–7 / 15–30min |
| c4 sensitivity | saps_q; 1000,5000us; 16,32,64; 2 reps | 400000 x3; 900000 | 3 / 12 | 60s / 12min | 3–7 / 20–40min |

No fresh calibration cells in c1–c4. c1 uses the supplied fixed 899983.2185078033 reference (override option above).
c2–c4 use entitlement at configured C; c2/c4 provisioning exceeds C. Never derive K_p from C/3 for DPA arms.
Cap/C overrides: --path-capacities-iops A,B,C --service-limit-iops C; c3 requires K_p=C/3 because of host support.
Original arms/profiles and legacy matrix remain available by omitting --campaign: smoke 2 cells; full 3 calibration + 12 fault cells.
All cells retain four tenants, weights 3:1:1:1, QD32, 4096-byte random reads and the working target/initiator setup.
health_only copies summit-v2 decoupled overrides and sets coupling=fixed; saps_binary only changes coupling=binary.
Safety: cooperative lock, hugepage reservation/restore, PID/start-tick stop, local fault readbacks/restore and recorded-PID map cleanup.
Controller attachment preflight uses bdev_nvme_get_controllers; existing target fixes and runtime_common.py remain unchanged.
Every extended cell records per-IO logs for all policies; p99 is exact nearest-rank over successful reads submitted within the common window.
Counter frequency defaults to 1GHz, checked against collector iostat rates and the optional binary frequency sidecar; override with --counter-hz HZ when hardware evidence requires it.
Per-IO records <QQIIHH and binary anchor <QQ epoch_ns,counter must match summit format; >=1000 successful window records per tenant required.
Files are read only after processes stop; per-IO binary/anchor/frequency files are deleted after summary or on failure to bound output size.
c3 snapshots thread_get_stats and bdev_get_iostat at both window boundaries; late captures (>1s) or resets fail the cell.
Busy cycles/I/O = sum SPDK thread busy-tick deltas / sum completed-read deltas, per process and aggregate; these are counter ticks, not CPU PMU cycles.
Snapshot timestamps/skew, thread IDs, busy/idle deltas and completed-read counts are retained; RPC activity contributes to measured busy time.
summary.csv columns: run_id,experiment,repeat,arm,label,fault_delay_us,sample_rate,aggregate_iops,reference_capacity_iops,reference_kind,
worst_tenant_fair_share,worst_tenant_p99_read_latency_us,busy_cycles_per_io,fault_manifested,status,
and t0/t1/t2/t3 each with _iops,_p99_us,_busy_cycles_per_io. sample_rate is blank for non-DPA policies; busy fields only c3.
campaign.json records completion/failure; metrics.json and per_io_summary.json retain per-cell/per-tenant measurements; failed cells are not valid CSV rows.
Host requests C, weights, tenant ID/count, paths, 1ms epoch, 1000IOPS probes and host M3/M4/M5; DPA init is disabled.
Host source evidence: /home/dpu/saps/saps-code/integration/spdk/0001-saps-spdk-integration.patch plus host_saps.h; standalone host_saps.c is absent locally.
Known host gaps: process-local controller has no shared tenant ring; this harness restricts c3 to K_p=C/3; no host sampling-rate knob is asserted.
Host selector/classifier/admission behavior is not proven identical to the evaluated DPA firmware; c3 measures whole implementations pending parity validation.
Host requires activation/skip-DPA log plus positive CP epochs and own-tenant budgets; CP evidence files remain. Optional --host-proof-pattern changes activation matching only.
Not statically verifiable: arm-2 binary includes host CP hooks, honors all host knobs/weights, emits proof, skips DPA, shares timing/record schema, and samples as expected.
Also verify CPUs4–11/40–45, RDMA endpoints/ports, RPC schemas/rates, plugin/FSM/ring progress, fault response, and process/page release on smoke.
RAM/hugepages/output disk, libraries/firmware/memlock, provisioning/calibration stability, clock alignment/dispatch skew and wall times require arm-2 validation.
Fingerprints/HEAD are metadata only; matching deployed binaries to source/firmware and dirty state remain unverified. No SSH/git/network commands are used here.
Final static checks: py_compile and bash -n passed. A delegated helper also ran local synthetic fixtures contrary to the static-only brief; its temporary artifacts were removed.
