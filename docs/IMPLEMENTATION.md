# Prototype implementation

This document maps the SAPS control loop to the published source tree.

## DPA program

`src/dev/dpa_plugin_dev.c` consumes sampled I/O events, updates path state, and
runs the allocator. The current scheduler path is entered by
`sapsq_scheduler_tick()`.

The scheduler performs five operations:

1. Update per-tenant demand from admitted submissions.
2. Convert provisioned path capacity and observed health into effective
   capacity.
3. Bound the namespace budget by the configured envelope.
4. Run demand-aware weighted progressive filling.
5. Split tenant rates across paths and publish a committed matrix.

The DPA code uses integer and fixed-point arithmetic. Arrays have compile-time
bounds of sixteen tenants and eight paths.

## Shared interface

`src/dpa_plugin_com.h` defines the notify ring, path state, controller
configuration, committed rate matrix, counters, and publication sequence.

The ring has multiple host producers and one DPA consumer. Producers serialize
publication with the shared producer lock. Each producer writes a complete
cache-line entry before advancing the published index.

Allocation state uses two sequence fields:

- `sapsq_epoch_seq` marks the matrix being written.
- `sapsq_epoch_commit_seq` marks the last completely published matrix.

A host reader accepts the matrix only when both values are equal and nonzero.

## Host library

`src/host/dpa_plugin.c` owns initialization, shared-memory setup, FlexIO RPC,
sampling, tenant attachment, and token-bucket enforcement.

The submission path reads the committed row for its tenant. The SPDK
path selector chooses among logical paths in proportion to that row. The
admission hook then consumes the selected path's token budget. A stale or
incomplete epoch follows the configured fallback rather than using a partial
matrix.

## SPDK integration boundary

The prototype adds four classes of hooks to SPDK:

1. Submission and completion callbacks that expose command identity, opcode,
   logical path, size, status, and time.
2. A logical-path mapping at the bdev multipath layer.
3. A path selector that consumes the committed tenant row.
4. Plugin initialization and shutdown in the benchmark process.

`integration/spdk/0001-saps-spdk-integration.patch` implements these hook points.
It applies to SPDK commit `a83e52f1da18807e21b552a0fe35057f8e9ea586`, the tree
used by the testbed; port it by function and data-flow boundary for another
revision.

## Experiment provenance

Each current campaign records source and binary hashes in its manifest. The
drivers reject runs that lack the fields needed by the corresponding analysis.
The shared-memory dump tool exposes the committed health, effective capacity,
rate matrix, selector counters, and ring state used by those checks.

## Build-time dependencies

The DPA target requires the DOCA FlexIO `dpacc` toolchain. The host library
requires FlexIO, libibverbs, and mlx5. Meson receives these dependencies from the
parent DOCA sample build.
