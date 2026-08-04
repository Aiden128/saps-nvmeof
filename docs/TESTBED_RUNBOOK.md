# SAPS testbed runbook

The published drivers target the two-machine prototype used for the paper.

- The initiator runs SPDK `bdevperf`, the host plugin, and the BlueField-3 DPA
  program.
- The target is reachable through the SSH alias `arm-1` and runs one SPDK
  NVMe-oF target process per logical path.
- Campaigns use RDMA listeners and memory-backed SPDK block devices.
- Current scale experiments expose the same backing file through multiple
  listeners so that all logical paths reach the same data.

The scripts contain the exact paths, interfaces, ports, and socket names used by
the testbed. Review them before running on another machine.

## 1. Build the controller

Build the DPA application and host library through the parent DOCA sample tree.
Build the shared-memory reader:

```bash
cc -O2 -std=c11 -Isrc -o scripts/sapsq_dump scripts/sapsq_dump.c
```

Build SPDK with the SAPS integration and confirm that the selected benchmark
binary links the host plugin.

## 2. Check connectivity

On the initiator:

```bash
ssh arm-1 true
ip route get 10.0.0.1
```

Confirm that the target RPC script, `nvmf_tgt`, and hugepage configuration are
available. The setup scripts stop only the target instances identified by their
own socket names.

## 3. Prepare a target topology

The signal and severity campaigns use a three-path topology:

```bash
ssh arm-1 'sudo bash -s' < experiments/3path_targets/setup_arm1.sh
```

Multi-tenant campaigns use:

```bash
ssh arm-1 'sudo bash -s' < experiments/3path_targets/setup_arm1_sapsq_Nt3p.sh
```

The path-and-tenant scale campaign invokes
`setup_arm1_scale_shared_file.sh` itself with the requested path count.

## 4. Run hardware-independent checks

```bash
python3 evaluation/sapsq_allocator_ref.py
python3 -m unittest \
  scripts/test_run_signal_isolation_campaign.py \
  scripts/test_run_summit_v2_campaign.py \
  scripts/test_run_overhead_qd_campaign.py
```

These checks validate allocator arithmetic, comparison-arm construction,
provenance requirements, and analysis gates. They do not replace a hardware
run.

## 5. Run paper campaigns

Choose a new output directory for every campaign.

Signal capability:

```bash
python3 scripts/run_signal_isolation_campaign.py \
  --output-dir results/signal --reps 2
```

HCAA scale:

```bash
python3 scripts/run_hcaa_scale_campaign.py \
  --out-root results/scale --reps 2
```

Coupling and path-delay sweep:

```bash
python3 scripts/run_summit_v2_campaign.py \
  --out-root results/severity --reps 3
```

Workload breadth:

```bash
python3 scripts/run_generality_campaign.py \
  --out-root results/generality --reps 3
```

Healthy-path overhead:

```bash
python3 scripts/run_overhead_qd_campaign.py \
  --out-root results/overhead --reps 3
```

Use each driver's `--help` output for its full parameter set.

## 6. Accept a run by execution integrity

Campaigns distinguish execution validity from the measured outcome. A run may
be rejected when its process failed, I/O did not start, the shared ring
overflowed, the controller did not publish a stable epoch, the requested
comparison mode did not reach the plugin, or provenance hashes are missing.

Do not reject a run because service, latency, or throughput is worse than
expected. Those values are experimental results.

## 7. Preserve provenance

Keep the generated `manifest.json`, `metrics.json`, controller snapshots,
and per-I/O logs together. Do not combine arms from different source hashes in
one controlled comparison. If a driver or binary changes, start a new campaign
directory.

Raw testbed output can contain machine names and local paths. Review it before
publishing.
