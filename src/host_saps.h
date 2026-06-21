/* Phase A — Host-only SAPS classifier (mode b).
 *
 * Co-located with dpa_plugin.h so the existing -I$(DPA_PLUGIN_DIR) flag in
 * lib/nvme/Makefile + module/bdev/nvme/Makefile reaches both headers without
 * touching include paths.
 *
 * Purpose: provide a *parallel* implementation of the SAPS per-path classifier
 * that runs entirely on the host SPDK reactor (i.e. no DPA, no FlexIO, no
 * shared ring). Used as the (b) mode in the dual-axis ablation:
 *
 *   (a) stock SPDK queue_depth          — no SAPS classifier (HOST_SAPS_ENABLED unset,
 *                                          DPA_PLUGIN_ROLE=standalone with notify off).
 *   (b) host_saps                       — full SAPS classifier on host CPU. Validates
 *                                          algorithm without DPA. THIS FILE.
 *   (c) dpa_plugin (current)            — full SAPS classifier on DPA-RP, host reads
 *                                          score/verdict tables.
 *
 * (c) vs (b) measures DPA offload benefit (paper §7.3 E1/E2). The two paths
 * MUST be mutually exclusive at runtime: enabling both would double-count
 * inflight, fight over selector verdicts, and mask the DPA contribution.
 *
 * Activation: env var HOST_SAPS_ENABLED=1 at process start. host_saps_active()
 * returns 1 only if init succeeded; nvme_rdma.c / bdev_nvme.c must check that
 * helper *before* falling through to the dpa_plugin path. Caller in bdevperf
 * must skip dpa_plugin_init() when HOST_SAPS_ENABLED=1 (or set
 * DPA_PLUGIN_DISABLE_INIT=1).
 *
 * Algorithm: bit-identical port of the DPA-resident classifier
 * (dpa_plugin_dev.c) — NEWMA on log-latency, Frugal-2U streaming P99,
 * multi-modal fault classifier with hysteresis, FSM with HEALTHY/DEGRADING/
 * EXCLUDED/RECOVERING, score = QD_BASE - W_QD*inflight + latency_bonus -
 * err_pen, and Slice 9.7 v2 HEALTHY equalisation. See host_saps.c for the
 * authoritative port; do not diverge from dpa_plugin_dev.c without updating
 * both sides.
 */

#ifndef __HOST_SAPS_H__
#define __HOST_SAPS_H__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Init: read HOST_SAPS_ENABLED env var. Returns 0 on success (including the
 * "disabled" path — failure means actively-tried-but-broken). Idempotent.
 * Caller (bdevperf) must call this BEFORE dpa_plugin_init() and must skip
 * dpa_plugin_init() when host_saps_active() returns non-zero. */
int host_saps_init(void);

/* Stop classifier and zero shared tables. Idempotent. */
void host_saps_shutdown(void);

/* Returns 1 iff HOST_SAPS_ENABLED=1 and init succeeded. */
int host_saps_active(void);

/* Per-IO hooks (parallel signatures to dpa_plugin_on_io_submit/complete in
 * dpa_plugin.h). Called from nvme_rdma.c only when host_saps_active(). */
void host_saps_on_io_submit(uint16_t cmd_id, uint8_t opcode, uint16_t qp_id,
			     uint16_t path_id, uint32_t nbytes, uint32_t nsid,
			     uint8_t retry_count, uint64_t host_tsc);

void host_saps_on_io_complete(uint16_t cmd_id, uint8_t opcode, uint16_t qp_id,
			       uint16_t path_id, uint16_t sct_sc,
			       uint32_t nbytes, uint32_t nsid,
			       uint8_t retry_count, uint64_t host_tsc);

/* Score reader (parallel to dpa_plugin_read_path_score). Returns 0 (warmup
 * sentinel) when not active or when (qp_id, path_id) hasn't been sampled yet. */
uint32_t host_saps_read_path_score(uint16_t qp_id, uint16_t path_id);

/* Per-opcode score reader for HOST_SAPS parity with DPA SAPS.
 * opcode_class: 0=READ, 1=WRITE, 2=FLUSH. Returns 0 on warmup / inactive. */
uint32_t host_saps_read_path_score_opcode(uint16_t qp_id, uint16_t path_id,
					  uint8_t opcode_class);

/* Raw per-opcode P99 reader in Q16.16 log-lat units. Returns 0 on warmup /
 * inactive. Used by host selector for cross-path opcode-specific weighting. */
uint32_t host_saps_read_path_opc_p99(uint16_t qp_id, uint16_t path_id,
				     uint8_t opcode_class);

/* Optional prefetch hint — same role as dpa_plugin_prefetch_path_score_row.
 * No-op when not active. Cheap regardless. */
void host_saps_prefetch_path_score_row(uint16_t qp_id);

/* ctrlr→path_idx lookup (parallel to dpa_plugin_ctrlr_to_path_idx). Same
 * algorithm + per-thread cache. host_saps maintains its OWN slot table so
 * mode (b) and mode (c) cannot cross-contaminate. */
uint16_t host_saps_ctrlr_to_path_idx(const void *ctrlr);

/* Release the ctrlr→slot mapping on ctrlr detach/destruct (parallel to
 * dpa_plugin_ctrlr_release): tombstones the slot(s) owned by `ctrlr` and bumps
 * a generation so per-thread caches drop stale entries, preventing a reused
 * ctrlr pointer from inheriting the dead path's slot/health/score state. */
void host_saps_ctrlr_release(const void *ctrlr);

/* Retry verdict reader (parallel to dpa_plugin_read_retry_verdict). Returns
 * 0xFF (DPA_SAPS_ACTION_UNSET) when not active. */
uint8_t host_saps_read_retry_verdict(uint16_t qp_id, uint16_t path_id);

/* FSM state reader (parallel to dpa_plugin_read_path_state). Returns 0
 * (HEALTHY) when not active. */
uint16_t host_saps_read_path_state(uint16_t qp_id, uint16_t path_id);

/* Fault-type reader (parallel to dpa_plugin_read_path_fault_type). Returns 0
 * (HEALTHY) when not active. Values: 0=HEALTHY 1=PERPETUAL_SLOW 2=SPARSE_ERROR
 * 3=BIMODAL_TAIL 4=FLAP 5=ONSET 6=QD_DRIFT — mirrors dpa_plugin_dev.c
 * saps_fault_type enum. */
uint16_t host_saps_read_path_fault_type(uint16_t qp_id, uint16_t path_id);

/* SAPS QoS control-plane evidence (host-resident port of the DPA control
 * plane). host_saps_cp_debug_dump() logs epoch run count + sample budgets so a
 * smoke run can prove the control plane executed on the host reactor (not a
 * stub). host_saps_cp_epoch_count() returns the number of sapsq_allocate epoch
 * invocations (0 when inactive). */
void host_saps_cp_debug_dump(void);
uint64_t host_saps_cp_epoch_count(void);

#ifdef __cplusplus
}
#endif

#endif /* __HOST_SAPS_H__ */
