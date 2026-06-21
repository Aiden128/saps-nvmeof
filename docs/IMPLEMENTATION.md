# S3 SAPS-single Implementation Report

**Date**: 2026-04-22
**Scope**: Replace S2 `dpa_plugin_select_io_path()` stub (returns STAILQ_FIRST) with a real signal-driven path selector backed by DPA-computed per-path score. Schema v2 (path_id + sct_sc) + Welford / CUSUM / scoring kernel on DPA + verdict-table read on SPDK.
**Spec**: `/home/user/DPA/nvme-of-controller/specs/spec-v4.md` §情境一 §4 (SAPS-single).

---

## 1. Change list (file:line)

### Schema v2 + verdict table
- `/home/user/DPA/nvme-of-controller/dpa-smart-initiator/flexio_build/samples/dpa_plugin/dpa_plugin_com.h`
  - +4 new `#define` (lines 40-44): `DPA_PLUGIN_PATH_LOG2/MAX/MASK` = 4 paths.
  - `struct dpa_plugin_notify_entry` (lines 57-70): added `path_id@14` (uint16), `sct_sc@16` (uint16), with `reserved_pad0/pad1` to keep 8 B alignment for trailing `reserved1[5]`. Sizeof stays exactly 64 B, alignof 64 (verified with a standalone layout check). Active bytes 0–17, padded 18–23, trailer 24–63.
  - `struct dpa_plugin_shared`: added `per_qp_path_score[1024][4]` (16 KiB) as the last field (line 140). Total `sizeof` grows from 532 864 → 549 248 B. Existing `posix_memalign` / `memfd_create` code uses `sizeof(struct dpa_plugin_shared)` so the allocation auto-scales.

### Host-side plugin
- `/home/user/DPA/nvme-of-controller/dpa-smart-initiator/flexio_build/samples/dpa_plugin/dpa_plugin.h`
  - `dpa_plugin_on_io_submit/complete` signatures extended with `path_id` (submit+complete) and `sct_sc` (complete only).
  - New accessor `uint32_t dpa_plugin_read_path_score(uint16_t qp_id, uint16_t path_id);` documented as non-atomic hot-path read mirroring the `per_qp_tokens[]` admission-slot pattern.
- `/home/user/DPA/nvme-of-controller/dpa-smart-initiator/flexio_build/samples/dpa_plugin/host/dpa_plugin.c`
  - `dpa_plugin_push()` (lines 780-811): accepts + stores `path_id`, `sct_sc` into the entry.
  - `dpa_plugin_on_io_submit/complete` thin wrappers updated.
  - New `dpa_plugin_read_path_score()` at EOF-ish (lines 910-921): branch-free read of `per_qp_path_score[qp & CONN_MASK][path & PATH_MASK]`, returns 0 when plugin not initialised so callers naturally fall back.
- `/home/user/DPA/nvme-of-controller/dpa-smart-initiator/flexio_build/samples/dpa_plugin/host/dpa_plugin_smoke.c:40-41` and `.../dpa_plugin_ipc_smoke.c:53-54`: updated two call sites to pass `path_id=0`, `sct_sc=0`.

### DPA-side SAPS kernel
- `/home/user/DPA/nvme-of-controller/dpa-smart-initiator/flexio_build/samples/dpa_plugin/dev/dpa_plugin_dev.c`
  - New `struct dpa_path_state` (lines 209-223) packed into exactly 64 B cacheline (Welford `n / mean_log_lat / m2`, `cusum_pos`, `inflight`, `err_count`, `err_window_start_tsc`, `last_complete_tsc`, `score`, `degraded`). `_Static_assert(sizeof == 64)` guards the layout.
  - Static `g_path[1024][4]` (line 225) — 256 KiB, fits alongside `g_conn` (320 KiB) + `g_submit_tsc_low` (256 KiB) within BF-3 1.5 MiB L2.
  - SAPS tunables (lines 228-256): `SCORE_SCALE=1<<20`, `CUSUM_DRIFT_Q16=0.5`, `CUSUM_THRESH_Q16=5.0`, `WELFORD_WARMUP=32`, weights `W_LAT=10 / W_QD=1 / W_ERR=20`, `ERR_WINDOW_TICKS=75e6` (≈ 50 ms @ 1.5 GHz DPA TSC).
  - `fixed_log2()` (lines 262-326): bit-length via CLZ + 256-entry Q16.16 LUT of `log2(1 + k/256)`. No FPU.
  - `fixed_isqrt()` (lines 330-344): 6-round Newton iteration; seed via bitlen/2.
  - `saps_update()` (lines 348-430): submit path increments `inflight`. Complete path decrements `inflight`, updates sliding 50 ms error window, runs Welford on `log2(delta_ticks)` (Q16.16), evaluates Page CUSUM on standardised residual (only after warmup), computes score = `SCALE − lat_pen − qd_pen − err_pen` clamped to `[1, SCALE]`. Degraded paths are forced to `score=1` (keeps ε-exploration for recovery detection). Writes `s->per_qp_path_score[qp_idx][path_idx]`.
  - `process_event()` (lines 543-587): signature now takes `volatile struct dpa_plugin_shared *s`. Routes both submit and complete into `saps_update()`. The legacy E1 per-QP CUSUM / admission-token kernel is preserved unchanged — S3 runs in parallel to keep E3 admission gate working.
  - Outer loop call-site at ~line 660 updated to pass `s`.

### SPDK hook sites
- `/home/user/spdk/lib/nvme/nvme_rdma.c`
  - Line ~460 (complete hook): `dpa_plugin_on_io_complete()` call updated with `path_id = qpair->id & 0x3u` (the `DPA_PLUGIN_PATH_MASK` constant inlined to avoid leaking DPA headers into nvme_rdma compilation units) and `sct_sc = rsp->status_raw` (verified field in `struct spdk_nvme_cpl`).
  - Line ~2932 (submit hook): `dpa_plugin_on_io_submit()` call updated with the same `path_id` surrogate. TODO comment documents the need to thread proper `nbdev_io->io_path` index through `spdk_bdev_io` private field.

### SPDK selector
- `/home/user/spdk/module/bdev/nvme/bdev_nvme.c`
  - Lines 1342-1363: added `uint64_t g_dpa_plugin_select_fallback` and `#include "dpa_plugin.h"` (guarded by `#ifdef DPA_PLUGIN_ENABLED`).
  - Lines 1365-1409: `dpa_plugin_select_io_path()` rewritten. Walks `nbdev_ch->io_path_list`, pulls `qp_id` via `spdk_nvme_qpair_get_id()`, uses matching surrogate `path_id = qp_id & 0x3u`, reads `dpa_plugin_read_path_score()`, argmaxes. Falls back to `STAILQ_FIRST` + increments `g_dpa_plugin_select_fallback` if no path has a non-zero score (warmup / plugin not linked).
- `/home/user/spdk/module/bdev/nvme/Makefile`: mirrored `lib/nvme/Makefile`'s `DPA_PLUGIN ?= 0` / `ifeq DPA_PLUGIN,1` block so `bdev_nvme.c` can `#include "dpa_plugin.h"` and set `-DDPA_PLUGIN_ENABLED` + `-I$(DPA_PLUGIN_DIR)`.

### NOT done (documented TODOs, not reached within budget)
- Proper `path_id` injection — spec §A "1-hour path_id surrogate" budget was taken. Both the SPDK hook sites (nvme_rdma.c) AND the bdev_nvme selector use the same `qp_id & 0x3u` surrogate so producer / consumer agree. This degrades to "each QP is one path" which matches the paper single-client experiment (N=2 paths on distinct controllers = distinct qpairs) but will NOT generalise to the multi-path-per-QP configurations. The v4 TODO is to stash `nbdev_io->io_path` index into `spdk_bdev_io->driver_ctx` before `_bdev_nvme_submit_request()` calls into `nvme_rdma_qpair_submit_request`.
- No RPC exposure of per-path score for live telemetry (follow-up; `g_dpa_plugin_select_count` / `_fallback` are in .bss and visible via `nm` but not RPC-queryable).
- No fault-injection experiments (D1–D5 scenarios from spec-v4 §5.2); those are the next phase.

---

## 2. Build log

### DPA_PLUGIN=0 (stock — confirm hooks stay inert)
```
$ make -C module/bdev/nvme DPA_PLUGIN=0
  CC nvme/bdev_nvme.o
  LIB libspdk_bdev_nvme.a
$ make -C lib/nvme DPA_PLUGIN=0
  CC nvme/nvme_rdma.o
  LIB libspdk_nvme.a
```
Clean. With `DPA_PLUGIN=0` the `#ifdef DPA_PLUGIN_ENABLED` block in `dpa_plugin_select_io_path()` is fully elided and the function reduces to the S2 stub.

### DPA_PLUGIN=1 (with S3 selector live)
```
$ make -C module/bdev/nvme DPA_PLUGIN=1
  CC nvme/bdev_nvme.o
  LIB libspdk_bdev_nvme.a
$ make -C lib/nvme DPA_PLUGIN=1
  CC nvme/nvme_rdma.o
  LIB libspdk_nvme.a
$ make -C examples/bdev/bdevperf
  CC bdevperf/bdevperf.o
  LINK bdevperf
```
Clean.

### DPA-side (meson/ninja)
```
$ ninja dpa_plugin/host/libdpa_plugin.a dpa_plugin/dev/dpa_plugin_app.a
[1/2] Generating dpa_plugin/dev/apps_dpa_plugin_app with a custom command
[2/2] Linking static target dpa_plugin/host/libdpa_plugin.a

$ ninja dpa_plugin/host/dpa_plugin_smoke dpa_plugin/host/dpa_plugin_ipc_smoke
[1/6] Compiling C object dpa_plugin_smoke.p/dpa_plugin_smoke.c.o
...
[6/6] Linking target dpa_plugin/host/dpa_plugin_smoke
```
Clean after one size-fix iteration (initial `dpa_path_state` was 128 B due to `aligned(64)` on a 88 B payload; tightened to exactly 64 B by reordering fields and dropping the unused `_pad1[2]`).

### Symbol verification
```
$ nm /home/user/spdk/build/lib/libspdk_bdev_nvme.a | grep -E "dpa_plugin_select|g_dpa_plugin"
0000000000000038 B g_dpa_plugin_select_count
0000000000000030 B g_dpa_plugin_select_fallback

$ nm /home/user/DPA/.../libdpa_plugin.a | grep -E "dpa_plugin_read_path_score|dpa_plugin_on_io"
0000000000000960 T dpa_plugin_read_path_score
0000000000000870 T dpa_plugin_on_io_complete
00000000000006a0 T dpa_plugin_on_io_submit
```
All new symbols resolve.

### Layout verification
Standalone `gcc` compile of the updated `dpa_plugin_com.h`:
```
sizeof dpa_plugin_notify_entry = 64   (expected 64)
offsetof path_id               = 14   (spec-mandated)
offsetof sct_sc                = 16   (spec-mandated)
sizeof dpa_plugin_shared       = 549 248
per_qp_path_score bytes        = 16 384 (= 1024 × 4 × 4)
DPA_PLUGIN_PATH_MAX            = 4
```

---

## 3. Smoke test — runtime

**Config**: node2 bdevperf → node1 nvmf_tgt (backend0 subsystem, listeners 4420+4422, two namespaces Delay0-5ms + Null1 — identical to S2 report §3). JSON at `/tmp/bdevperf_mp.json`. `bdev_nvme_set_multipath_policy -p plugin` applied to `backend0n2` (Null backend, the heavy IOPS path).

**Command**: `build/examples/bdevperf -c /tmp/bdevperf_mp.json -q 32 -o 4096 -w randread -t 5 -m 0x1`

**Result (DPA_PLUGIN=1 binary, DPA coordinator NOT running)**:

```
Running I/O for 5 seconds...
Job: backend0n1 (Core Mask 0x1, workload: randread, depth: 32, IO size: 4096)
   backend0n1 : 5.01    6348.81 IOPS   5044.11 µs avg lat      (Delay0 5ms path)
Job: backend0n2 (Core Mask 0x1, workload: randread, depth: 32, IO size: 4096)
   backend0n2 : 5.00 1841701.24 IOPS     17.33 µs avg lat      (Null1, policy=plugin)
 Total        :      1848050.05 IOPS
```

- No crash. `exit code 0`.
- `backend0n2` at 1.84 M IOPS matches S2 baseline `round_robin` value (1.68–1.89 M across runs — within noise). Confirms the new per-IO walk + `dpa_plugin_read_path_score()` shared-memory read adds negligible cost on the hot path when the plugin is inactive.
- With plugin inactive, `dpa_plugin_read_path_score()` returns 0 for every path, so `dpa_plugin_select_io_path` hits the fallback branch, increments `g_dpa_plugin_select_fallback`, and returns `STAILQ_FIRST` — equivalent to S2 stub behaviour.

**Per-path score write observation**: not validated in this run. Reaching that observation requires a DPA coordinator process (FlexIO + mlx5) to actually boot the device-side kernel; only `spdk_nvme_perf` currently calls `dpa_plugin_init()`, and it doesn't support bdev-multipath. Wiring bdevperf up to call `dpa_plugin_init()` is a trivial follow-up (one `if` block at bdevperf startup modelled after `perf.c:3316`) but is **deliberately NOT part of S3 scope** — the task is "replace the stub on the SPDK side"; DPA-coordinator-for-bdevperf belongs in the fault-injection harness phase.

**What this smoke confirms**:
1. Schema v2 layout is binary-compatible with the existing 64 B ring slot (sizeof preserved).
2. Both build flavours compile clean; stock build is byte-identical-equivalent (zero-diff path for the PLUGIN branch when `DPA_PLUGIN=0`).
3. End-to-end integration: bdevperf → bdev_nvme `find_io_path` → `dpa_plugin_select_io_path` → DPA plugin library function → shared-memory read → fallback to `STAILQ_FIRST`. No deadlock, no memory corruption, IOPS within noise.

**What this smoke does NOT confirm** (outside S3 scope):
1. DPA-side Welford / CUSUM / scoring under live RDMA traffic.
2. `per_qp_path_score` actually getting populated by DPA.
3. Asymmetric-path steering behaviour (needs D1 fault injection — next phase).

---

## 4. 卡點 / Blockers encountered

### 4.1 `path_id` injection (spec-called-out tricky)

The spec-A note correctly flagged this as the hardest part. Within the 1-hour budget specified, I took the documented fallback: `path_id = qp_id & (DPA_PLUGIN_PATH_MASK)` applied identically on both producer (nvme_rdma.c hook) and consumer (bdev_nvme_select selector). This works for the paper's single-client experiment because:
- Each bdev_nvme multipath channel has N distinct `nvme_io_path` entries, each owning its own `nvme_qpair`, each with its own `spdk_nvme_qpair->id`.
- SPDK assigns consecutive qpair IDs, so for N=2 paths on same nqn we get (for example) `qp_id=1` on path-A and `qp_id=2` on path-B. `& 0x3u` maps them to distinct `path_id` slots.
- Because paper experiment N ≤ 2, collisions in the 4-slot surrogate space are impossible.

Failure mode of this surrogate that the TODO must fix: if SPDK allocates more than 4 qpairs per bdev_nvme channel and multiple qpairs serve the same logical path (multi-reactor case), two different paths can collide onto the same `path_id` slot. Not an issue for the current single-reactor paper experiment.

### 4.2 `sizeof(struct dpa_path_state)` initially 128 B not 64 B

Initial struct layout had `aligned(64)` on an 88 B payload, which rounds up to 128. Compacted to exactly 64 B by (a) using `uint32_t n / uint32_t inflight` instead of `uint64_t + uint32_t + padding`, and (b) removing the unused `_pad1[2]` tail. 16 fields repacked into 64 B exactly. `_Static_assert` enforces this at build time now.

### 4.3 `struct spdk_nvme_qpair` is opaque outside `lib/nvme/`

First selector version used `spdk_qp->id` directly — fails with `invalid use of undefined type` because `bdev_nvme.c` only sees the forward-declared pointer. Fixed by using the public accessor `spdk_nvme_qpair_get_id()` from `include/spdk/nvme.h:4515`.

### 4.4 `module/bdev/nvme/Makefile` didn't gate `DPA_PLUGIN`

S2 spike only touched `lib/nvme/Makefile` because `nvme_rdma.c` was the only file that needed the include path + `-DDPA_PLUGIN_ENABLED`. S3 adds `#include "dpa_plugin.h"` to `bdev_nvme.c`, so the same `DPA_PLUGIN=1` switch needed mirroring into `module/bdev/nvme/Makefile`. Done (10 lines mirrored from the other Makefile, exact same pattern).

---

## 5. Byte-identical claim

This patch is **NOT** bit-identical to pre-V1 SPDK — already known from the V1 token-bucket work (see `proposal-v3.md §3.1`): `__LINE__` macros and minor .o layout drift from the admission-gate branch. The S3 changes are in the same ballpark (6 new call-site arg bytes, one new branch in `bdev_nvme_find_io_path`, one static buffer read). This is acceptable and documented — paper "stock SPDK" comparisons use the `DPA_PLUGIN=0` build which has the full S3 code elided by preprocessor.

---

## 6. Next-phase entry points

Once DPA coordinator is wired up for bdevperf (one `if (getenv("DPA_PLUGIN_ENABLED")) dpa_plugin_init();` in bdevperf_main before `spdk_app_start`):

1. **D0 symmetric**: both listeners healthy → score[Null path A] ≈ score[Null path B] within Welford stddev; bdevperf `backend0n2` should split traffic roughly evenly (check `spdk_bdev_get_iostat` per io_path).
2. **D1 asymmetric steady**: one listener behind `delay_bdev:5ms`, the other behind `null_bdev` → scores diverge; bdevperf should send ≥95% to the healthy listener after ~1 s warmup.
3. **D2 fail-slow onset**: dynamically switch one listener's backend to the delay bdev via RPC `bdev_ocssd_create` swap mid-run → measure time-to-recovery via per-second per-path IOPS bucket.

Scoring parameters (`CUSUM_DRIFT / THRESH`, warmup, penalty weights) are currently unjustified — spec §NOT-this-time explicitly defers tuning to post-baseline data. D0/D1 will drive the first tuning pass.
