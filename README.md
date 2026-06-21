# SAPS — Semantics-Aware Path Steering for Disaggregated NVMe-over-Fabrics

Reference implementation and evaluation harness for **SAPS**, a completion-aware
NVMe-oF path-steering layer that runs on a SmartNIC datapath accelerator (NVIDIA
BlueField-3 DPA). SAPS folds each fabric path's measured health into a single
weighted max-min capacity allocation, so that steering away from a failing path
and dividing capacity by tenant weight become one computation rather than two
competing mechanisms.

> Companion artifact to the paper *"SAPS: Semantics-Aware Path Steering for
> Disaggregated NVMe-over-Fabrics on the SmartNIC Datapath"* (Future Generation
> Computer Systems). See the paper for the design rationale and the measured results.

## Repository layout

- `src/` — the SAPS plugin
  - `dev/dpa_plugin_dev.c` — DPA signal plane: NEWMA change-point test, Frugal-2U
    tail estimator, per-path FSM, and the health-coupled weighted max-min allocator
  - `host/dpa_plugin.c` — host shim: FlexIO RPC, notify ring, and the two-word
    admission / steer fast path (`*_smoke.c` / `*_test.c` are sanity and accuracy tests)
  - `dpa_plugin.h`, `dpa_plugin_com.h`, `host_saps.h` — public API and shared structs
  - `meson.build` — build orchestration (`dpacc` for the DPA app; host static library)
- `integration/spdk-patches/` — patches that hook the SAPS submit/complete callbacks
  into SPDK's `nvme_rdma.c` fast path, plus bdevperf init and fault-injection
- `evaluation/` — `run_experiments.py` (orchestrator), `aggregate_results.py`
  (result aggregation), `sapsq_dump.c` (shared-memory counter reader),
  `sapsq_allocator_ref.py` (reference allocator for cross-verification), and the
  per-experiment scripts in `experiments/`
- `docs/` — design spec, implementation notes, testbed runbook, env-var reference

## Prerequisites

**Hardware**
- NVIDIA BlueField-3 DPU (DPA / FlexIO capable)
- A two-machine RDMA (RoCEv2) testbed: one NVMe-oF target and one initiator carrying
  the BlueField-3, reachable over multiple fabric paths

**Software**
- NVIDIA DOCA SDK (FlexIO + `dpacc`), 2025.10 or newer
- SPDK (recent `main`) with the patches in `integration/spdk-patches/` applied
- Meson ≥ 0.60 + Ninja, a C11 compiler, Python 3.8+
- Time synchronization (chrony / NTP) between the two machines

## Build

```bash
# 1. DPA app + host library
cd src && meson setup build && ninja -C build      # DPA app object + libdpa_plugin.a

# 2. Patch and rebuild SPDK
cd "$SPDK_ROOT"
for p in /path/to/saps-code/integration/spdk-patches/*.patch; do patch -p1 < "$p"; done
./configure && make -j"$(nproc)"

# 3. Counter reader
cc -O2 -I /path/to/saps-code/src/host -o sapsq_dump /path/to/saps-code/evaluation/sapsq_dump.c
```

See `docs/TESTBED_RUNBOOK.md` for the full procedure and `docs/ENV_VARS.md` for the
runtime toggles (e.g. `DPA_PLUGIN_ENABLED`, the host-resident ablation, the QoS plane).

## Reproduce

Each script under `evaluation/experiments/` drives one experiment through
`run_experiments.py` (SSH orchestration of target + initiator, fault injection, result
aggregation). For example:

```bash
cd evaluation
bash experiments/sapsq_e3_path_degradation.sh   # media-error / fail-slow steering
bash experiments/sapsq_e1_static_fairness.sh    # weighted multi-tenant fairness
bash experiments/sapsq_e5_overhead.sh           # healthy-path overhead
```

Results land as CSV/JSON; `aggregate_results.py` produces the per-experiment tables.
The exact headline numbers, repetition counts (N), and statistics are reported in the paper.

> The experiment scripts and runbook contain **testbed-specific host paths and addresses**.
> See `REVIEW_BEFORE_PUBLISH.md` and adjust them to your environment.

## Design summary

The DPA reads NVMe completion semantics (the `sct/sc` status codes and per-completion
timing) off a notify ring, maintains per-path detectors and a finite-state machine, and
reduces each path to a scalar **health** factor. The allocator scales each path's
capacity by its health and runs one weighted max-min fill: quarantine is the
`health → 0` limit of the same allocation, and the single-tenant case is its `T = 1`
degenerate form. The host fast path only reads the precomputed decision. See
`docs/DESIGN.md` and `docs/IMPLEMENTATION.md`.

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).

## Citation

If you use SAPS in your work, please cite:

> Yong-Xuan Huang, Ming-Hung Chen, I-Hsin Chung, and Jerry Chou.
> "SAPS: Semantics-Aware Path Steering for Disaggregated NVMe-over-Fabrics on the
> SmartNIC Datapath." *Future Generation Computer Systems* (under review), 2026.

A machine-readable citation is provided in [`CITATION.cff`](CITATION.cff); the BibTeX
entry will be updated once the paper is published.

## Acknowledgements

The authors thank the IBM Thomas J. Watson Research Center for providing the
computational resources used in this work, and Wei-Fang Sun (NVIDIA AI Technology
Center, NVAITC) for technical support and insightful critiques. This work was supported
by the National Science and Technology Council (NSTC) under Grant
No. 114-2221-E-007-059-MY3.
