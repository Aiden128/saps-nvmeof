# SAPS: Coupling Path Health and Tenant Allocation for Stable Multi-Tenant NVMe-over-Fabrics

This repository contains the prototype and experiment drivers used by the SAPS
paper. SAPS treats path repair and tenant scheduling as one allocation problem.
It derives a graded path-health signal from NVMe completion behavior, converts
that signal into effective path capacity, and allocates the feasible service
budget with weighted max-min fairness.

The prototype runs the estimator and allocator on NVIDIA BlueField-3 DPA cores.
The host-side SPDK path reads committed decisions and applies them while
submitting I/O.

## Design in one equation

For path `p`, SAPS combines provisioned path capacity `K_p` with estimated
health `h_p`:

```text
e_p = K_p h_p
B   = min(C, sum_p e_p)
```

Here, `C` is the configured service envelope and `B` is the service budget
that can be promised under the current path state. A demand-aware weighted
progressive fill divides `B` among active tenants. Each tenant's rate is then
split across paths in proportion to `e_p`. Excluded paths receive only the
bounded probe traffic needed to test recovery.

## Repository layout

- `src/` contains the DPA program, the host library, and shared data structures.
- `integration/spdk/` contains the SPDK patch used by the prototype; see
  `integration/spdk/README.md`.
- `scripts/` contains the campaign drivers used by the current paper results.
- `experiments/3path_targets/` contains the target setup scripts required by
  those campaigns.
- `experiments/supplementary/` contains the detector-baseline and
  RocksDB experiments; see `experiments/supplementary/README.md`.
- `evaluation/` retains the original experiment wrappers and standalone
  reference tools.
- `docs/` describes the control loop, implementation boundary, runtime
  configuration, and testbed procedure.

The `sapsq` prefix remains in internal symbols and filenames for compatibility
with recorded experiment manifests. It refers to the SAPS controller described
in the paper.

## Build

The DPA application requires NVIDIA DOCA FlexIO and the SPDK integration used by
the testbed.

```bash
cd src
meson setup build
ninja -C build
```

Build the shared-memory inspection tool separately:

```bash
cc -O2 -std=gnu11 -Isrc -o scripts/sapsq_dump scripts/sapsq_dump.c
```

`integration/spdk/0001-saps-spdk-integration.patch` contains the submit,
completion, path-selection, initialization, and target-side fault-injection
changes used in the evaluation. It applies to SPDK commit
`a83e52f1da18807e21b552a0fe35057f8e9ea586`; `integration/spdk/README.md` gives
the build steps.

## Paper campaigns

The current paper is backed by five campaign drivers:

- `scripts/run_signal_isolation_campaign.py` compares completion semantics,
  reachability, queue depth, and request completion time under matched faults.
- `scripts/run_hcaa_scale_campaign.py` compares continuous health-coupled
  allocation with nominal weighted max-min allocation as path and tenant counts
  increase.
- `scripts/run_summit_v2_campaign.py` runs the path-delay and coupling
  comparison.
- `scripts/run_generality_campaign.py` repeats one controller configuration
  across I/O sizes and read-write mixes.
- `scripts/run_overhead_qd_campaign.py` measures healthy-path throughput cost
  across queue depths.

These drivers are the source copies whose hashes are recorded by the experiment
manifests. They retain the testbed's absolute paths, interface names, RPC
sockets, and SSH host aliases. Edit those settings for another deployment.
Raw measurements are intentionally not stored in this repository.

## Local checks

The reference allocator exercises the capacity boundary and weighted
progressive fill without hardware:

```bash
python3 evaluation/sapsq_allocator_ref.py
```

The campaign logic has hardware-independent tests:

```bash
python3 -m unittest \
  scripts/test_run_signal_isolation_campaign.py \
  scripts/test_run_summit_v2_campaign.py \
  scripts/test_run_overhead_qd_campaign.py
```

Syntax-check all published Python sources with:

```bash
python3 -m compileall -q evaluation scripts
```

## Requirements

- NVIDIA BlueField-3 with DPA and FlexIO support
- NVIDIA DOCA SDK 2025.10 or newer
- SPDK built with the SAPS integration
- An NVMe-oF RDMA initiator and target with multiple paths
- Python 3.10 or newer
- NumPy for the severity campaign

See `docs/TESTBED_RUNBOOK.md` for the order of operations.

## License

Apache License 2.0. See `LICENSE`.

## Citation

If you use this artifact, cite:

> Yong-Xuan Huang, Ming-Hung Chen, I-Hsin Chung, and Jerry Chou.
> "SAPS: Coupling Path Health and Tenant Allocation for Stable Multi-Tenant
> NVMe-over-Fabrics." Future Generation Computer Systems, under review, 2026.

A machine-readable record is available in `CITATION.cff`.
