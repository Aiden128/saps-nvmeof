/* P0.3 dpa_plugin — public C API consumed by the SPDK NVMe-oF RDMA hook.
 *
 * Usage: SPDK builds with -DDPA_PLUGIN_ENABLED, links -ldpa_plugin. The perf
 * app calls dpa_plugin_init() during startup; from that point onwards the
 * two hook points in nvme_rdma.c call dpa_plugin_on_io_submit/complete every
 * IO. A teardown path calls dpa_plugin_shutdown() at exit.
 *
 * Environment variables (read during dpa_plugin_init):
 *   DPA_PLUGIN_DEV      mlx5 device name, e.g. "mlx5_1" (default: mlx5_1)
 *   DPA_PLUGIN_ENABLED  "1" to enable notify on hooks; "0" to load but no-op
 *                       (Config C baseline). Default "1".
 *
 * All functions safe to call with DPA_PLUGIN_ENABLED=0 (they become no-ops).
 * The submit/complete entrypoints are defined as real C functions — the
 * compile-time guard in SPDK (#ifdef DPA_PLUGIN_ENABLED) gates the call site.
 */

#ifndef __DPA_PLUGIN_H__
#define __DPA_PLUGIN_H__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Initialize: open mlx5 device, spawn flexio process, launch DPA polling RPC
 * as a pthread, set up shared ring. Returns 0 on success, -1 otherwise.
 *
 * If the env var DPA_PLUGIN_DISABLE_INIT=1 is set, this returns 0 immediately
 * without touching hardware (pure stock baseline / Config B).
 */
int dpa_plugin_init(void);

/* Fire-and-forget notify: write next ring slot, increment producer.
 * Called from the SPDK reactor polling thread inside the nvme_rdma fast path.
 * When DPA_PLUGIN_ENABLED=0 or init was skipped, this is a compiler no-op via
 * the g_dpa_plugin_active branch.
 *
 * Schema v2 (S3 SAPS-single + SAPS v2):
 *   path_id     — logical path index within bdev_nvme channel. Currently
 *                 injected by the hook caller as (qp_id & DPA_PLUGIN_PATH_MASK)
 *                 as a per-QP surrogate. 0 when the IO is not multipath-steered.
 *   sct_sc      — submit: 0. complete: rsp->status_raw (SCT<<8 | SC).
 *   nbytes      — req->payload.size (v2 SAPS chain E per-opcode / nbytes).
 *   nsid        — req->cmd.nsid (v2 SAPS chain C per-NS exclusion).
 *   retry_count — req->retries / bio->retry_count (v2 SAPS chain A clean sample
 *                 filter — non-zero ⇒ sample excluded from Welford latency).
 */
void dpa_plugin_on_io_submit(uint16_t cmd_id, uint8_t opcode, uint16_t qp_id,
			      uint16_t path_id, uint32_t nbytes, uint32_t nsid,
			      uint8_t retry_count, uint64_t host_tsc);

void dpa_plugin_on_io_complete(uint16_t cmd_id, uint8_t opcode, uint16_t qp_id,
				uint16_t path_id, uint16_t sct_sc,
				uint32_t nbytes, uint32_t nsid,
				uint8_t retry_count, uint64_t host_tsc);

/* Stop DPA polling loop and free all resources. Idempotent. */
void dpa_plugin_shutdown(void);

/* E-4 multi-tenant IPC (Option C — memfd + mmap MPSC ring).
 *
 * Role selection is controlled by env var DPA_PLUGIN_ROLE:
 *
 *   "standalone" (default, or unset): legacy single-process behaviour. Full
 *                init + local ring use. Bit-identical to P0.5.
 *
 *   "coordinator": full init like standalone, BUT the ring backing store is
 *                  a memfd_create region (not posix_memalign). The coordinator
 *                  additionally spawns a Unix Domain Socket server at
 *                  $DPA_PLUGIN_SOCK (default /tmp/dpa_plugin.sock) and hands
 *                  the memfd to each connecting tenant via SCM_RIGHTS. The
 *                  coordinator itself is also a producer on the same ring.
 *
 *   "tenant":    connect()s to $DPA_PLUGIN_SOCK, recv()s the memfd via
 *                SCM_RIGHTS, mmaps it, and uses the shared region as a
 *                producer. Does NOT call ibv_reg_mr / flexio_* — the
 *                coordinator has already done that. The fast-path push uses
 *                an atomic MPSC increment on producer_idx, so multiple
 *                producers (coordinator + N tenants) race-free share one
 *                ring. Returns 0 on successful attach, -1 on any error.
 *
 * Env vars:
 *   DPA_PLUGIN_ROLE   "standalone" | "coordinator" | "tenant"
 *   DPA_PLUGIN_SOCK   UDS path (default "/tmp/dpa_plugin.sock")
 */
int dpa_plugin_attach(int memfd);

/* Test-only accessor: read the current producer_idx from the shared ring.
 * Used by IPC smoke to verify MPSC atomicity (producer_idx should equal
 * the exact sum of all producers' push counts, no torn updates). */
uint64_t dpa_plugin_read_producer_idx(void);

/* E3 admission gate (Yield mechanism).
 *
 * Called from SPDK fast path BEFORE nvme_rdma_req_get(). Returns:
 *   0        — admit IO (caller proceeds to allocate rdma_req + post).
 *   -EAGAIN  — throttle; caller must return -EAGAIN from the transport
 *              submit function so SPDK's existing nvme_qpair_submit_request
 *              catches it and queues into qpair->queued_req for later
 *              resubmission.
 *
 * Zero-overhead when DPA_PLUGIN_ENABLED=0 at compile time (call site gated).
 * When the plugin is loaded but admission is disabled (e.g. init skipped or
 * notify_enabled=0), always returns 0.
 *
 * The check reads a single uint32_t from the shared ring (per_qp_tokens[
 *   qp_id & DPA_PLUGIN_ADM_MASK]) and compares to 0. No atomic, no fence —
 *   worst-case stale read is one cacheline old.
 *
 * SAPS-Q (2026-05-22): qpair pointer is looked up in the host-side
 * qpair→path_id hashmap (see dpa_plugin_register_qp). If `qpair` is NULL or
 * not registered, falls back to (qp_id_fallback & DPA_PLUGIN_PATH_MASK) so
 * M3/M4/M5 callers that don't multipath stay unchanged. The caller MUST
 * still pass qp_id_fallback because M3/M4/M5 use per-tenant env vars and
 * E3 cumulative-credit still hashes by qp_id & DPA_PLUGIN_ADM_MASK.
 *
 * Env-var override for test harness:
 *   DPA_PLUGIN_FORCE_THROTTLE  "none" / unset  → honour DPA per_qp_tokens[]
 *                              "all"           → throttle every IO
 *                              "half"          → throttle odd qp_id
 *
 * SAPS-Q nbytes plumbing (2026-05-22 §4.4): cost in Q16.16 IO units scales
 * with the SQE payload size — 4K = 1 IO, 64K = 16 IO, capped at 256 IO so
 * a single oversized SQE cannot drain the bucket. Callers MUST pass the
 * actual transfer size (req->payload.size in SPDK RDMA transport). Non-SAPS-Q
 * branches (M3/M4/M5/E3) ignore nbytes.
 */
struct spdk_nvme_qpair;
int dpa_plugin_admission_check(struct spdk_nvme_qpair *qpair,
			       uint16_t qp_id_fallback, uint32_t nbytes);

/* Read the current process tenant's committed SAPS-Q per-path budgets.
 *
 * Returns the number of valid path entries copied into budget_q32. Returns 0
 * while SAPS-Q is disabled, before the first budget epoch is committed, or
 * when no non-zero budget is available. The selector uses this row to make
 * its path-choice weights agree with the admission plane that enforces the
 * same row after a qpair has been selected.
 */
int dpa_plugin_read_sapsq_budget_row(uint32_t *budget_q32,
				     uint32_t max_paths);
uint32_t dpa_plugin_read_sapsq_probe_rate_q32(void);

/* SAPS-Q (2026-05-22) — host-side qpair→path_id mapping.
 *
 * Background: SPDK assigns qpair->id per-controller starting at 1, so for
 * a 4-tenant × 3-path multipath deployment all 12 IO qpairs share id=1,
 * collapsing (qp_id & PATH_MASK) to a single slot. The plugin needs a
 * deterministic per-QP path_id derived from the listener port the QP
 * connects to. This pair of register/unregister hooks is called from
 * nvme_rdma.c around qpair connect/disconnect; the lookup
 * (dpa_plugin_path_id_of) is consumed inside dpa_plugin_admission_check
 * and any other host-side site that needs the per-path slot.
 *
 * Path id derivation lives in dpa_plugin_path_id_from_port():
 *   1. If DPA_PLUGIN_PATH_MAP_PORTS env var is set, parse as
 *      "<port>:<path_id>,<port>:<path_id>,..." and use that map.
 *   2. Otherwise, fall back to formula path_id = (port - 4430) / 10,
 *      clamped to [0, DPA_PLUGIN_PATH_MAX - 1]. This matches the
 *      arm-1 setup_arm1_sapsq_4t3p.sh layout where path A uses
 *      4430-4433, path B uses 4440-4443, path C uses 4450-4453.
 *
 * Hashmap implementation: open-addressed linear-probing table of 64
 * slots (DPA_PLUGIN_QPMAP_SIZE), keyed by qpair pointer cast to
 * uintptr_t. Inserts/removes serialised by a single pthread spinlock;
 * lookups are lock-free atomic loads on the slot key. Lookups in the
 * fast path are cheap (1-3 cache lines max for 12 active entries).
 *
 * Sentinel: dpa_plugin_path_id_of() returns DPA_PLUGIN_PATH_MAX
 * (NOT a valid path slot) when the qpair is NULL or unregistered, so
 * callers can fall through to the legacy (qp_id & PATH_MASK) path.
 */
int  dpa_plugin_register_qp(struct spdk_nvme_qpair *qpair, uint16_t path_id);
void dpa_plugin_unregister_qp(struct spdk_nvme_qpair *qpair);
uint16_t dpa_plugin_path_id_of(struct spdk_nvme_qpair *qpair);

/* Derive path_id from a numeric listener port (e.g. parsed from
 * ctrlr->trid.trsvcid). See dpa_plugin_register_qp for the env-var /
 * formula contract. Returns a value in [0, DPA_PLUGIN_PATH_MAX - 1]. */
uint16_t dpa_plugin_path_id_from_port(uint16_t port);

/* Slice 34: read DPA-computed per-opcode score for a given (qp_id, path_id,
 * opcode_class) triple. opcode_class: 0=READ, 1=WRITE, 2=FLUSH.
 *
 * Returns 0 if plugin not initialised, opcode_class is invalid, or the per-opcode
 * estimator has not yet warmed up (caller should fall back to the aggregate
 * dpa_plugin_read_path_score() in that case).
 *
 * Non-atomic read — same semantics as dpa_plugin_read_path_score(). */
uint32_t dpa_plugin_read_path_score_opcode(uint16_t qp_id, uint16_t path_id,
					   uint8_t opcode_class);

/* Slice 34: read raw per-opcode Frugal-2U P99 estimate (Q16.16 log-lat units)
 * for a given (qp_id, path_id, opcode_class) triple. opcode_class: 0=READ,
 * 1=WRITE, 2=FLUSH.
 *
 * Used by host-side cross-path comparison: rescale final_score proportional to
 * min_opc_p99 / path_opc_p99 so a path whose READ P99 is 7× the best peer
 * gets weight ≈ 1/7 for READ IOs.
 *
 * Returns 0 if plugin not initialised, opcode_class is invalid, or warmup incomplete.
 * Caller must treat 0 as "no data" and skip rescaling for that path. */
uint32_t dpa_plugin_read_path_opc_p99(uint16_t qp_id, uint16_t path_id,
				       uint8_t opcode_class);

struct dpa_plugin_path_snapshot {
	uint32_t score;
	uint32_t opcode_score;
	uint32_t opc_p99;
	uint32_t capacity;
	uint16_t fault_type;
	uint16_t state;
};

/* Round 11 D0 hot-path: read all selector fields for one path through a
 * single plugin call. This keeps SAPS selection active while avoiding four
 * separate shared-memory accessor calls in bdev_nvme.c. capacity is filled
 * only when need_capacity is nonzero. */
int dpa_plugin_read_path_snapshot(uint16_t qp_id, uint16_t path_id,
				  uint8_t opcode_class, uint8_t use_opcode_score,
				  uint8_t need_capacity,
				  struct dpa_plugin_path_snapshot *out);

/* S3 SAPS-single: read DPA-computed per-path score for a given (qp_id, path_id)
 * pair. DPA writes per_qp_path_score[qp][path] via the Welford+CUSUM+scoring
 * kernel; SPDK bdev_nvme reads in bdev_nvme_find_io_path() to argmax across
 * paths on a channel.
 *
 * Returns 0 if plugin not initialised / notify disabled (caller treats as
 * equal-score — bdev_nvme falls back to STAILQ_FIRST).
 *
 * Non-atomic read (matches per_qp_tokens[] admission-slot pattern): a stale
 * read is at worst one DPA outer-iter old, which is acceptable for path
 * selection where decisions are coarse. */
uint32_t dpa_plugin_read_path_score(uint16_t qp_id, uint16_t path_id);

/* v2 SAPS: read DPA-classified retry verdict for a given (qp_id, path_id)
 * complete event. Returns one of DPA_SAPS_ACTION_* (see dpa_plugin_com.h):
 *   0 TERMINAL, 1 RETRY_SAME, 2 FAILOVER, 3 NOT_ERROR, 0xFF UNSET (fall-through).
 *
 * Called from SPDK bdev_nvme_check_retry_io() to short-circuit stock retry
 * logic. Returns 0xFF when plugin not initialised so caller falls back to
 * stock behaviour. */
uint8_t dpa_plugin_read_retry_verdict(uint16_t qp_id, uint16_t path_id);

/* Slice 1 — retry-verdict overlay independent toggle.
 *
 * Returns non-zero iff the env var DPA_PLUGIN_RETRY_OVERLAY=1 was set at
 * dpa_plugin_init() time. Used by bdev_nvme_check_retry_io() so that the
 * SAPS retry overlay can be enabled under non-PLUGIN mp_policy values
 * (e.g. round_robin) for the §Evaluation 2x2 factorial design that
 * isolates path-selection contribution from retry-verdict contribution.
 *
 * Default 0 (overlay opt-in only): existing PLUGIN-policy users continue
 * to see overlay because bdev_nvme_check_retry_io() also enables it when
 * mp_policy == PLUGIN, so prior behaviour is preserved. Returns 0 when
 * plugin not initialised. */
int dpa_plugin_retry_overlay_enabled(void);

/* v2 SAPS: read DPA-computed FSM state for a given (qp_id, path_id). Returns
 * one of DPA_SAPS_STATE_* (HEALTHY/DEGRADING/EXCLUDED/RECOVERING). Non-hot
 * path — used by monitoring / test harness. Returns 0 when plugin not
 * initialised. */
uint16_t dpa_plugin_read_path_state(uint16_t qp_id, uint16_t path_id);

/* Slice 19 — read DPA classifier fault_type. Host uses this to differentiate
 * idle-bypass weight: high-confidence drain (PERPETUAL_SLOW/SPARSE_ERROR/
 * BIMODAL_TAIL) gets reduced weight; HEALTHY/FLAP keep full recovery weight.
 * Values: 0=HEALTHY, 1=PERPETUAL_SLOW, 2=SPARSE_ERROR, 3=BIMODAL_TAIL,
 * 4=FLAP, 5=ONSET, 6=QD_DRIFT. Returns 0 (HEALTHY) when plugin not
 * initialised. */
uint16_t dpa_plugin_read_path_fault_type(uint16_t qp_id, uint16_t path_id);

/* Round 10 D8: read DPA-measured per-path capacity score for proportional
 * allocation. The value is relative within one QP row; 0 means unknown. */
uint32_t dpa_plugin_read_path_capacity(uint16_t qp_id, uint16_t path_id);

/* Read active path-fault bitmask for a QP. mask==0 means DPA has not classified
 * any path in that QP as faulty; Round 11 keeps the SAPS selector active unless
 * the explicit legacy fault-mask bypass knob is enabled. */
uint32_t dpa_plugin_read_qp_fault_mask(uint16_t qp_id);

/* Round 11 observability: incremented only when the explicit legacy healthy
 * full-bypass path is enabled and actually returns a stock selector path. */
void dpa_plugin_note_healthy_full_bypass(void);

/* SAPS-Q active query — returns non-zero when the shared ring is mapped and
 * sapsq_enabled is set. Used by bdev_nvme.c saps_try_healthy_bypass to skip
 * the healthy-bypass cache when SAPS-Q admission is governing path selection:
 * the cache always returns STAILQ_FIRST (path 0), preventing traffic from
 * being distributed across all 3 paths per tenant and capping total IOPS at
 * 1/3 of the allocated rate. When SAPS-Q is active, bypass must be disabled
 * so dpa_plugin_select_io_path runs per-IO and round-robins across paths. */
int dpa_plugin_sapsq_active(void);

/* S3 multipath: map an opaque per-path identity pointer (typically the
 * spdk_nvme_ctrlr*) to a stable slot index in [0, DPA_PLUGIN_PATH_MAX).
 *
 * Background: SPDK assigns qpair->id per-controller starting at 1, so all
 * controllers' first IO qpair share id=1. Using (qpair->id & PATH_MASK) as
 * the slot key makes every path collide at [1][1] and the host argmax
 * deadlocks on STAILQ_FIRST. This helper maintains an atomic first-come
 * first-served pointer→slot table so every site that needs to derive a
 * per-path slot (the nvme_rdma.c submit/complete hooks, the bdev_nvme.c
 * plugin selector, the retry-verdict reader) gets the same index for the
 * same controller.
 *
 * Returns slot index 0..DPA_PLUGIN_PATH_MAX-1. On overflow (more distinct
 * controllers than PATH_MAX slots) it reuses the last slot — acceptable
 * for single-client multipath where PATH_MAX=4 covers the designed 2–3
 * paths with headroom. */
uint16_t dpa_plugin_ctrlr_to_path_idx(const void *ctrlr);

/* Release the ctrlr→slot mapping on ctrlr detach/destruct: tombstones the
 * slot(s) owned by `ctrlr` and bumps a generation so per-thread caches drop
 * stale entries. Wire into the SPDK ctrlr-destruct hook so a reused ctrlr
 * pointer does not inherit the dead path's slot/health/score state. */
void dpa_plugin_ctrlr_release(const void *ctrlr);

/* Slice 9.6 Fix 2 — software prefetch for the per-QP score row.
 *
 * dpa_plugin_read_path_score() is called multiple times back-to-back during
 * bdev_nvme select_io_path Pass 1 (once per io_path), each time touching a
 * different path slot inside the SAME row per_qp_path_score[qp_slot][*]. The
 * row is 4 × uint32 = 16 B and fits within one cache line. The DPA polling
 * kernel writes to the row asynchronously, so the line is frequently in a
 * non-resident or shared-with-DPA state from the host's L1/L2 perspective.
 *
 * This call issues a software prefetch (read, high temporal locality) for
 * the row in question so the subsequent N reads of dpa_plugin_read_path_score
 * with the same qp_id and varying path_id hit the L1 cache.
 *
 * Cost: one PRFM instruction (aarch64) or PREFETCH (x86), ~1-2 cycles, no
 * fault or stall. No-op if plugin not initialised.
 *
 * Memory model: g_ctx.ring is host-DRAM (posix_memalign + mlock + ibv_reg_mr;
 * not MMIO). __builtin_prefetch on this region is safe and never triggers a
 * PCIe transaction.
 */
void dpa_plugin_prefetch_path_score_row(uint16_t qp_id);

/* M2 v2 host classifier (2026-05-20):exposed for unit test program。Reads
 * ring->m2_pca_conf_q16[16] and writes ring->per_client_joint_verdict[16] +
 * stderr [M2_VERDICT] log。Production code 透過 m2_classifier_fn pthread 每
 * 10ms 自動呼叫;test program 直接呼叫驗演算法。 */
struct dpa_plugin_shared;
void host_m2_classifier_tick(struct dpa_plugin_shared *ring);

#ifdef __cplusplus
}
#endif

#endif /* __DPA_PLUGIN_H__ */
