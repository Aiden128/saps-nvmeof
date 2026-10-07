/* P0.3 dpa_plugin — shared types between host library and DPA polling kernel.
 *
 * The plugin exposes a notify ring from the SPDK fast path (host) to a DPA
 * polling kernel. Each ring slot is 64 bytes (one cacheline) carrying the
 * per-IO event that the fast path wants to report.
 *
 * Layout:
 *   struct notify_entry entries[RING_SIZE]           // producer writes here
 *   volatile uint64_t   producer_idx (cacheline)      // host increments
 *   volatile uint64_t   consumer_idx (cacheline)      // DPA updates after consume
 *   volatile uint64_t   stop         (cacheline)      // host sets to 1 to kill loop
 *
 * RING_SIZE must be a power of 2 so (idx & (RING_SIZE-1)) is cheap.
 *
 * This is a FIRE-AND-FORGET ring for P0.3 overhead measurement. Host does not
 * wait for consumer_idx to catch up; if the ring overflows (producer - consumer
 * > RING_SIZE) we simply overwrite. Paper eval can add back-pressure later.
 */

#ifndef __DPA_PLUGIN_COM_H__
#define __DPA_PLUGIN_COM_H__

#include <stdint.h>

#define DPA_PLUGIN_RING_LOG2  16             /* 2^16 = 65536 slots × 64B = 4 MiB */
#define DPA_PLUGIN_RING_SIZE  (1u << DPA_PLUGIN_RING_LOG2)
#define DPA_PLUGIN_RING_MASK  (DPA_PLUGIN_RING_SIZE - 1u)

/* E1 per-tenant conn_stats direct-map. qp_id is bit-masked into this range.
 * MAX=1024 covers the N=1000 tenant sweep without allocating unbounded state.
 * Sized power-of-2 so hash = qp_id & (N-1) is cheap. */
#define DPA_PLUGIN_CONN_LOG2  10
#define DPA_PLUGIN_CONN_MAX   (1u << DPA_PLUGIN_CONN_LOG2)  /* 1024 */
#define DPA_PLUGIN_CONN_MASK  (DPA_PLUGIN_CONN_MAX - 1u)

/* E1 CUSUM window size. K=32 integer samples per conn. */
#define DPA_PLUGIN_WINDOW_K   32u

/* Maximum logical paths per bdev channel.  The shared-namespace scale
 * campaign exercises up to eight listeners, so every path-indexed plane uses
 * the same power-of-two dimension. */
#define DPA_PLUGIN_PATH_LOG2  3
#define DPA_PLUGIN_PATH_MAX   (1u << DPA_PLUGIN_PATH_LOG2)  /* 8 */
#define DPA_PLUGIN_PATH_MASK  (DPA_PLUGIN_PATH_MAX - 1u)

#define M2_CLIENT_LOG2 4
#define M2_CLIENT_MAX  (1u << M2_CLIENT_LOG2)  /* 16 */
#define M2_CLIENT_MASK (M2_CLIENT_MAX - 1u)
#define M2_FEATURE_DIM 8

#define M2_ETA_Q16 655

#define M2_WARMUP_N 1100u

#define M2_SUBSAMPLE_LOG2 3
#define M2_SUBSAMPLE_MASK ((1u << M2_SUBSAMPLE_LOG2) - 1u)

#define M3_TENANT_LOG2 4
#define M3_TENANT_MAX  (1u << M3_TENANT_LOG2)  /* 16 */
#define M3_TENANT_MASK (M3_TENANT_MAX - 1u)

#define M3_BURST_CAP_Q32  ((int64_t)1000LL << 32)

#define M3_DELTA_TSC_CAP  ((uint64_t)100000000ULL)

#define SAPSQ_TENANT_MAX  M3_TENANT_MAX        /* 16, alias M3_TENANT_MAX */
#define SAPSQ_PATH_MAX    DPA_PLUGIN_PATH_MAX  /* 8,  alias DPA_PLUGIN_PATH_MAX */

/* SAPS-Q M-series unified scheduler dimension macros.
 * SAPSQ_MAX_TENANTS = 16 (matches SAPSQ_TENANT_MAX; kept as explicit named constant
 *   for the new 2D schema fields per specs/research-architecture-consolidated-20260522.md §5.1).
 * SAPSQ_MAX_PATHS aliases the canonical path dimension so classifier,
 * allocation, enforcement, and telemetry cannot silently diverge. */
#define SAPSQ_MAX_TENANTS 16
#define SAPSQ_MAX_PATHS   DPA_PLUGIN_PATH_MAX

#define SAPSQ_HEALTH_HEALTHY_Q16   ((uint32_t)(1u << 16))   /* 65536 */
#define SAPSQ_HEALTH_PROBE_Q16     ((uint32_t)100u)         /* ε ~ 0.0015 */
#define SAPSQ_HEALTH_QUARANTINE    ((uint32_t)0u)

/* Path eligibility encoded as enum bytes for host fast-path branchless lookup. */
#define SAPSQ_ELIG_QUARANTINE  0u
#define SAPSQ_ELIG_PROBE       1u
#define SAPSQ_ELIG_NORMAL      2u

/* Runtime-selectable path-health source for controlled signal isolation.
 * Every source feeds the same HCAA allocator and committed-budget selector.
 * REQUEST_RTT uses request completion time only. It is not an independent
 * network probe. */
enum sapsq_health_source {
	SAPSQ_HEALTH_SOURCE_COMPLETION = 0,
	SAPSQ_HEALTH_SOURCE_QUEUE_DEPTH = 1,
	SAPSQ_HEALTH_SOURCE_REQUEST_RTT = 2,
};

/* Runtime-selectable coupling policy for controlled HCAA comparisons.
 * Every mode uses the same estimator and weighted progressive fill. FIXED
 * keeps admission at the namespace envelope but still uses observed health
 * for path placement. BINARY excludes any path whose health is below one.
 * CONTINUOUS uses graded health for both admission and placement. */
enum sapsq_health_coupling_mode {
	SAPSQ_HEALTH_COUPLING_CONTINUOUS = 0,
	SAPSQ_HEALTH_COUPLING_FIXED = 1,
	SAPSQ_HEALTH_COUPLING_BINARY = 2,
};

#define DPA_PLUGIN_OPC_CLASS_READ   0u
#define DPA_PLUGIN_OPC_CLASS_WRITE  1u
#define DPA_PLUGIN_OPC_CLASS_FLUSH  2u
#define DPA_PLUGIN_OPC_CLASS_MAX    3u

/* Cache-line padded row strides for host-read / DPA-write SAPS tables.
 * Logical path/opcode indexes stay 0..DPA_PLUGIN_PATH_MAX-1 and
 * 0..DPA_PLUGIN_OPC_CLASS_MAX-1; extra slots only isolate neighboring QPs. */
#define DPA_PLUGIN_CL_BYTES             64u
#define DPA_PLUGIN_PATH_U32_CL_SLOTS    (DPA_PLUGIN_CL_BYTES / sizeof(uint32_t))
#define DPA_PLUGIN_PATH_U16_CL_SLOTS    (DPA_PLUGIN_CL_BYTES / sizeof(uint16_t))
#define DPA_PLUGIN_OPC_U32_CL_SLOTS     (DPA_PLUGIN_CL_BYTES / sizeof(uint32_t))

/* Host-side batch size for P0.4 batched notify. Host accumulates this many
 * events in a thread-local staging buffer, then copies the batch to the ring
 * with ONE fence + ONE producer-index bump. Amortizes store-buffer drain
 * cost against SPDK fast path. 64 × 64B = 4 KiB per flush.
 */
#define DPA_PLUGIN_BATCH_LOG2 6
#define DPA_PLUGIN_BATCH_SIZE (1u << DPA_PLUGIN_BATCH_LOG2)

/* One cacheline (64B) per event. Host writes this in the hot path.
 *
 * Schema v2 (S3 SAPS-single + SAPS v2):
 *   offset 14  uint16_t path_id   — logical path index within bdev_nvme channel
 *                                   (0..DPA_PLUGIN_PATH_MAX-1). Currently
 *                                   injected as qp_id & DPA_PLUGIN_PATH_MASK
 *                                   (TODO: proper nbdev_io->io_path index
 *                                   threaded through nvme_rdma.c).
 *   offset 16  uint16_t sct_sc    — complete-only: rsp->status_raw packed
 *                                   (SCT in high nibble, SC in low 8 bits).
 *                                   0 on submit.
 *   offset 20  uint32_t nbytes    — v2 SAPS: req->payload.size in bytes.
 *                                   Chain E (per-opcode / nbytes scoring).
 *   offset 24  uint32_t nsid      — v2 SAPS: req->cmd.nsid. Chain C
 *                                   (per-NS temporary exclusion).
 *   offset 28  uint8_t  retry_cnt — v2 SAPS: req->retries on submit;
 *                                   bio->retry_count on complete. Chain A
 *                                   (clean sample filter — retries excluded
 *                                   from Welford latency statistics).
 */
struct dpa_plugin_notify_entry {
	uint64_t host_tsc;       /* 0..7:   spdk_get_ticks() at event time */
	uint16_t cmd_id;         /* 8..9:   rdma_req->id */
	uint8_t  opcode;         /* 10:     req->cmd.opc */
	uint8_t  kind;           /* 11:     0=submit, 1=complete */
	uint16_t qp_id;          /* 12..13: qpair->id */
	uint16_t path_id;        /* 14..15: v2: logical path index */
	uint16_t sct_sc;         /* 16..17: v2: rsp->status_raw on complete; 0 on submit */
	uint16_t reserved_pad0;  /* 18..19: alignment pad */
	uint32_t nbytes;         /* 20..23: v2 SAPS: req->payload.size */
	uint32_t nsid;           /* 24..27: v2 SAPS: req->cmd.nsid */
	uint8_t  retry_count;    /* 28:     v2 SAPS: req->retries / bio->retry_count */
	uint8_t  reserved_pad2[3]; /* 29..31: alignment pad */
	uint64_t reserved1[4];   /* 32..63: remaining 32B pad to 64B cacheline */
} __attribute__((aligned(64)));

/* E3 admission-control state.
 *
 * DPA writes per_qp_tokens[i] = 1 (admit) or 0 (throttle) based on its
 * per-connection CUSUM state; SPDK hot path reads per_qp_tokens[qp_id &
 * mask] to decide whether to yield. Fire-and-read — no atomic, no fence.
 * A stale read returns at worst a 1 cacheline older decision, which is
 * acceptable for admission control at IO granularity.
 *
 * Host-side test harness (DPA_PLUGIN_FORCE_THROTTLE env var) can override
 * this by xoring a mask into the plugin-read path; DPA writes are still
 * authoritative in production.
 *
 * Size: DPA_PLUGIN_CONN_MAX * 4 = 4 KiB (fits 64 cachelines).
 */
#define DPA_PLUGIN_ADM_LOG2  DPA_PLUGIN_CONN_LOG2
#define DPA_PLUGIN_ADM_SLOTS (1u << DPA_PLUGIN_ADM_LOG2)
#define DPA_PLUGIN_ADM_MASK  (DPA_PLUGIN_ADM_SLOTS - 1u)

/* Cacheline-separated indices.
 *
 * producer_lock serializes the short batch publication critical section used
 * by coordinator and tenant processes. producer_idx is the published head,
 * not a reservation counter. A producer writes and flushes its slots before
 * advancing producer_idx, so the DPA never consumes a reserved but unwritten
 * slot. The lock and each index occupy separate cache lines.
 */
struct dpa_plugin_shared {
	struct dpa_plugin_notify_entry entries[DPA_PLUGIN_RING_SIZE];

	volatile _Atomic uint32_t producer_lock __attribute__((aligned(64)));
	uint8_t pad_producer_lock[64 - sizeof(uint32_t)];

	volatile _Atomic uint64_t producer_idx __attribute__((aligned(64)));
	uint8_t pad0[64 - sizeof(uint64_t)];

	volatile uint64_t consumer_idx;
	uint8_t pad1[64 - sizeof(uint64_t)];

	volatile uint64_t stop;
	uint8_t pad2[64 - sizeof(uint64_t)];

	/* DPA writes consumer count here for debug / DPA-side telemetry. */
	volatile uint64_t dpa_consumed;
	uint8_t pad3[64 - sizeof(uint64_t)];

	/* DPA sets this to 1 just before returning (after the final writeback)
	 * when it exits via stop=1. Not currently polled by host in the
	 * lease-renewal model (DPA returns cleanly, host gets status=0), but
	 * kept as a debug aid. */
	volatile uint64_t dpa_stopped;
	uint8_t pad4[64 - sizeof(uint64_t)];

	/* Host-producer integrity telemetry.  Every producer updates max lag
	 * after reserving a batch and increments overrun_count if the reservation
	 * would place more than one ring of unconsumed entries in flight.  These
	 * counters are monotonic for the lifetime of the shared ring and let an
	 * experiment reject a run even if an overrun occurred between snapshots.
	 */
	volatile _Atomic uint64_t ring_overrun_count;
	volatile _Atomic uint64_t ring_max_lag;
	uint8_t pad_ring_integrity[64 - 2 * sizeof(uint64_t)];

	/* End-to-end tenant-attribution counters. Host producers increment the
	 * first array after stamping a sampled submit. The DPA increments the
	 * second array after reading that submit from the ring. */
	volatile _Atomic uint64_t host_submit_published[SAPSQ_MAX_TENANTS];
	volatile uint64_t dpa_submit_consumed_by_tenant[SAPSQ_MAX_TENANTS];

	/* E3 admission control — DPA writes, host reads.
	 * 0 = throttle this qp, nonzero = admit.
	 *
	 * E1-fix1: interpretation changed to monotonic refill counter.
	 * DPA-side: per_qp_tokens[qp] = cumulative tokens issued so far
	 *           (increments by rate × elapsed-ticks, never decreases).
	 * Host-side: compares per_qp_tokens[qp] against host_admitted[qp];
	 *           admits when (tokens - admitted) > 0, else throttles.
	 * This makes DPA the sole writer of per_qp_tokens and host the sole
	 * writer of host_admitted, avoiding the MPMC race of the
	 * decrement-in-place token bucket. */
	volatile uint32_t per_qp_tokens[DPA_PLUGIN_ADM_SLOTS];

	/* E1-fix1 host-side admission counter. Host increments atomically per
	 * admit, compares against DPA's per_qp_tokens to decide throttle. */
	volatile uint32_t host_admitted[DPA_PLUGIN_ADM_SLOTS];

	/* Host-side counters for E3 bench (host writes, host reads — not
	 * ring memory semantically but colocated for test convenience). */
	volatile uint64_t host_adm_checks;
	volatile uint64_t host_adm_throttles;
	uint8_t pad5[64 - 2 * sizeof(uint64_t)];

	/* S3 SAPS-single verdict table — DPA writes, SPDK bdev_nvme reads.
	 * Indexed [qp_id & CONN_MASK][path_id & PATH_MASK].
	 * DPA computes score from Welford + CUSUM + QD + error window.
	 * Score range: 0..SCORE_SCALE (1<<20). 0 = degraded lock-out only.
	 * bdev_nvme_find_io_path() argmaxes across paths within its channel.
	 * Each QP row is padded to 64B to avoid false sharing between rows. */
	volatile uint32_t per_qp_path_score[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_U32_CL_SLOTS]
		__attribute__((aligned(64)));

	/* Slice 34 — per-opcode score for D5 mixed workload routing.
	 * Index [qp][path][opc] where opc: 0=READ, 1=WRITE, 2=FLUSH.
	 * Host select_io_path queries the current IO opcode so that SAPS routes
	 * around path-opcode combos that are disproportionately slow.
	 * Same scale and sentinel (0=warmup, SAPS_SCORE_MIN=excluded) as
	 * per_qp_path_score. Each path row is padded to 64B. */
	volatile uint32_t per_qp_path_score_opcode[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_MAX][DPA_PLUGIN_OPC_U32_CL_SLOTS]
		__attribute__((aligned(64)));

	/* Slice 34 — raw per-opcode Frugal-2U P99 estimate in Q16.16 log-lat units.
	 * Index [qp][path][opc] where opc: 0=READ, 1=WRITE, 2=FLUSH.
	 * Host uses these for cross-path comparison: argmin(opc_p99) across paths
	 * for each opcode class. 0 = warmup. Each path row is padded to 64B. */
	volatile uint32_t per_qp_path_opc_p99[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_MAX][DPA_PLUGIN_OPC_U32_CL_SLOTS]
		__attribute__((aligned(64)));

	/* v2 SAPS retry verdict table — DPA writes after NVMe semantic
	 * classification, SPDK bdev_nvme_check_retry_io() reads to short-
	 * circuit stock retry/failover logic.
	 *
	 * Values: see enum dpa_saps_action (0=TERMINAL, 1=RETRY_SAME,
	 *         2=FAILOVER, 3=NOT_ERROR, 0xFF=unset / fall-through).
	 *
	 * Size: 1024 * 8 * 1B = 8 KiB. */
	volatile uint8_t per_qp_path_retry_verdict[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_MAX];

	/* v2 SAPS path state table — DPA writes FSM state for monitoring,
	 * SPDK / operator tooling can read it (non-hot-path).
	 *
	 * Values: see enum dpa_saps_state (0=HEALTHY, 1=DEGRADING,
	 *         2=EXCLUDED, 3=RECOVERING).
	 *
	 * Size: 1024 * 8 * 2B = 16 KiB. */
	volatile uint16_t per_qp_path_state[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_MAX];

	/* Slice 19 — DPA writes classifier fault_type for host-side
	 * idle-bypass-weight modulation. Host bdev_nvme.c reads this in
	 * the SAPS_SCORE_MIN handler to differentiate between high-
	 * confidence drain (PERPETUAL_SLOW / SPARSE_ERROR / BIMODAL_TAIL
	 * → small idle-bypass weight) and recoverable false-positives
	 * (HEALTHY / FLAP → large idle-bypass weight to allow recovery).
	 *
	 * Values: see enum saps_fault_type (defined DPA-side in
	 *         dpa_plugin_dev.c — 0=HEALTHY, 1=PERPETUAL_SLOW,
	 *         2=SPARSE_ERROR, 3=BIMODAL_TAIL, 4=FLAP,
	 *         5=ONSET, 6=QD_DRIFT, 7=ANY_THREE_VOTE,
	 *         8=SHARED_FATE, 9=PROPORTIONAL_THROTTLE).
	 *
	 * Each QP row is padded to 64B. */
	volatile uint16_t per_qp_path_fault_type[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_U16_CL_SLOTS]
		__attribute__((aligned(64)));

	/* Round 10 D8 — per-path measured capacity score for proportional split.
	 * DPA writes a relative service-rate estimate per (QP,path); host selector
	 * only uses ratios within the same QP row. 0 means not warmed/unknown.
	 *
	 * Each QP row is padded to 64B. */
	volatile uint32_t per_qp_path_capacity[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_U32_CL_SLOTS]
		__attribute__((aligned(64)));

	/* Round 6 A5 — per-QP active fault bitmask.
	 * Bit path_id is 1 when DPA has classified that path as non-HEALTHY.
	 * The host only uses mask==0 for the explicit legacy bypass mode.
	 * Padded to one cache line per QP; slot [qp][0] carries the mask. */
	volatile uint32_t per_qp_fault_mask[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_U32_CL_SLOTS]
		__attribute__((aligned(64)));

	/* Round 11 D0 hot-path observability counters. DPA increments the first
	 * three while SAPS sampling/classification/scoring remains active; host
	 * increments healthy_full_bypass only if the explicit legacy bypass is used. */
	volatile uint64_t saps_sample_events;
	volatile uint64_t saps_classify_calls;
	volatile uint64_t saps_score_updates;
	volatile uint64_t saps_healthy_full_bypass_count;

	volatile uint64_t d4_state_count[DPA_PLUGIN_PATH_MAX][4];
	volatile uint64_t d4_fault_count[DPA_PLUGIN_PATH_MAX][10];
	/* Transition matrix [from_state][to_state]. Diagonal entries count "no
	 * transition this call" (state stayed). Off-diagonal = real transitions. */
	volatile uint64_t d4_state_transitions[DPA_PLUGIN_PATH_MAX][4][4];

	/* D4 round-2 instrumentation: latest classifier raw state per path.
	 * Written every saps_update() call (overwrite, no accumulation). Lets host
	 * inspect final state of all classifier inputs to find which BIMODAL_TAIL
	 * AND-gate condition is the gatekeeper. All Q16.16 except counts. */
	volatile int64_t  d4_last_max_log_lat[DPA_PLUGIN_PATH_MAX];
	volatile int64_t  d4_last_mean_log_lat[DPA_PLUGIN_PATH_MAX];
	volatile int64_t  d4_last_baseline_log_lat[DPA_PLUGIN_PATH_MAX];
	volatile uint32_t d4_last_tail_count[DPA_PLUGIN_PATH_MAX];
	volatile uint32_t d4_last_bimodal_consec[DPA_PLUGIN_PATH_MAX];
	volatile uint32_t d4_last_recent_sample[DPA_PLUGIN_PATH_MAX];
	volatile int64_t  d4_last_err_rate_ewma[DPA_PLUGIN_PATH_MAX];
	volatile uint32_t d4_last_recent_err_count[DPA_PLUGIN_PATH_MAX];
	volatile uint64_t d4_last_n[DPA_PLUGIN_PATH_MAX];  /* total samples seen */

	/* D4 round-3: raw classifier output histogram per path (before hysteresis
	 * wrapper modifies it). Distinguishes "raw never emits BIMODAL" from
	 * "raw emits BIMODAL but hysteresis suppresses". 10 buckets match
	 * SAPS_FAULT_* enum. Captured by wrapper via out-param. */
	volatile uint64_t d4_raw_fault_count[DPA_PLUGIN_PATH_MAX][10];

	volatile uint32_t saps_m1_single_stream;

	volatile uint32_t saps_m1_reset_request;
	uint8_t pad_m1[64 - 2 * sizeof(uint32_t)];

	volatile uint32_t saps_m2_enabled;          /* 0=off (default), 1=run Oja */
	volatile int64_t  m2_pca_w[16][8];          /* 1024 B — per-client PC */
	volatile int64_t  m2_pca_w_baseline[16][8]; /* 1024 B — warmup snapshot */
	volatile uint32_t m2_pca_n_events[16];      /*   64 B — sub-sample count */
	volatile int32_t  m2_pca_conf_q16[16];      /*   64 B — cosine to baseline */
	volatile uint8_t  per_client_joint_verdict[16]; /* 16 B — host-written */
	uint8_t pad_m2[64 - 16 - sizeof(uint32_t)];

	volatile int64_t  m3_tenant_credits_q32[16];      /* 128 B — DPA writer */
	volatile uint32_t m3_tenant_weights_q16[16];      /*  64 B — host writer */
	volatile uint64_t m3_tenant_last_tsc[16];         /* 128 B — DPA writer */
	volatile uint64_t m3_tenant_iops_served[16];      /* 128 B — DPA writer */
	volatile uint64_t m3_credit_exhaust_count[16];    /* 128 B — DPA writer */
	volatile int64_t  m3_capacity_per_tsc_q32;        /*   8 B — host control plane */
	volatile int64_t  m3_credit_per_io_q32;           /*   8 B — host writer */
	volatile uint32_t m3_enabled;                     /*   4 B — host gate */
	volatile uint32_t m3_v2_admission_gate;           /*   4 B — host: 0=v1 obs, 1=v2 gate */
	volatile uint32_t m2_v2_classifier;               /*   4 B — host: M2 v2 classifier active */
	volatile uint32_t m3_my_tenant_id;                /*   4 B — host writes proc's tenant id */
	volatile uint32_t m3_v3_freeze_gate;              /*   4 B — host: v3 use freeze[] flag */
	uint8_t pad_m3[64 - 8 - 8 - 4 - 4 - 4 - 4 - 4];

	volatile uint8_t  m3_tenant_freeze[16];           /*  16 B — DPA writer, host reader */
	uint8_t pad_m3_freeze[64 - 16];

	volatile uint32_t m4_v2_enabled;                      /*   4 B — host gate */
	volatile uint32_t m4_my_tenant_id;                    /*   4 B — host writes per-proc */
	uint8_t pad_m4_hdr[64 - 8];

	/* DPA writer (refresh + consume), host reader (admission_check).
	 *
	 * Precision design (xverify learning 2026-05-20):
	 *   refresh_per_tsc_q32 = round(r_iops / tsc_freq × 2^32)
	 *     stored uint32; representable range 0..~4M IOPS at 1.5 GHz.
	 *     At 100K IOPS: ~286331 (Q16.16 LSB would have been 4 → 8% truncation,
	 *     Q32 LSB gives sub-0.01% precision).
	 *
	 *   tokens_q16 = current tokens in Q16.16 IO units (uint32, max ~65K IOs).
	 *   burst_cap_q16 = BURST_FACTOR × 2^16 (uint32, IO-units in Q16.16).
	 *
	 *   refill_q16 per refresh = (refresh_per_tsc_q32 × delta_tsc) >> 16
	 *     uint64 intermediate fits safely (max ~3e6 × 1e8 = 3e14, < 2^63). */
	volatile uint32_t m4_tenant_tokens_q16[M3_TENANT_MAX];     /* 64 B — current tokens Q16.16 */
	volatile uint32_t m4_tenant_refresh_per_tsc_q32[M3_TENANT_MAX]; /* 64 B — host writes init (Q32 IO/tsc) */
	volatile uint32_t m4_tenant_burst_cap_q16[M3_TENANT_MAX];  /* 64 B — host writes init */
	volatile uint32_t m4_tenant_reject_flag[M3_TENANT_MAX];    /* 64 B — DPA writer, host reader */
	volatile uint64_t m4_tenant_last_refresh_tsc[M3_TENANT_MAX]; /* 128 B — DPA writer */

	/* Observability counters (DPA writer, host reader) */
	volatile uint64_t m4_tenant_admit_count[M3_TENANT_MAX];    /* 128 B */
	volatile uint64_t m4_tenant_reject_count[M3_TENANT_MAX];   /* 128 B */

	volatile uint32_t m5_drr_enabled;                          /*   4 B — host gate */
	volatile uint32_t m5_my_tenant_id;                         /*   4 B — host writes per-proc */
	uint8_t pad_m5_hdr[64 - 8];

	/* DPA DRR scheduler state (DPA writer, host reader except pending/consumed) */
	volatile uint64_t m5_tenant_grant_count[M3_TENANT_MAX];    /* 128 B — DPA writer, host reader */
	volatile uint64_t m5_tenant_consumed_count[M3_TENANT_MAX]; /* 128 B — host atomic writer */
	volatile int32_t  m5_tenant_deficit_q16[M3_TENANT_MAX];    /*  64 B — DPA writer (Q16.16 signed) */
	volatile uint32_t m5_tenant_quantum_q16[M3_TENANT_MAX];    /*  64 B — host writes init */
	volatile uint32_t m5_tenant_pending[M3_TENANT_MAX];        /*  64 B — host atomic writer */
	volatile uint64_t m5_drr_last_tsc;                         /*   8 B — DPA writer */
	volatile uint64_t m5_drr_interval_tsc;                     /*   8 B — host writes init */
	volatile uint64_t m5_drr_round_count;                      /*   8 B — DPA writer, observability */
	volatile uint32_t m5_cost_per_io_q16;                      /*   4 B — host writes (default 1<<16) */
	uint8_t pad_m5_sched[64 - 8 - 8 - 8 - 4];                 /* pad to 64B */

	/* Observability counters (DPA writer, host reader) */
	volatile uint64_t m5_tenant_admit_count[M3_TENANT_MAX];    /* 128 B — incremented at grant */
	volatile uint64_t m5_tenant_reject_count[M3_TENANT_MAX];   /* 128 B — incremented at throttle */
	volatile uint64_t m5_tenant_idle_rounds[M3_TENANT_MAX];    /* 128 B — DPA: rounds tenant was idle */
	volatile uint64_t m5_tenant_work_conserved[M3_TENANT_MAX]; /* 128 B — DPA: rounds deficit redistributed */

	/* Gates + epoch interval (host writer at init) */
	volatile uint32_t sapsq_enabled;                              /*   4 B — host gate */
	volatile uint32_t sapsq_my_tenant_id;                         /*   4 B — host writes per-proc */
	volatile uint32_t sapsq_epoch_interval_events;                /*   4 B — DPA runs sched after N events (default 1024 ≈ ~10µs @100K IOPS/path) */
	volatile uint32_t sapsq_epoch_stale_us;                       /*   4 B — host fallback threshold (default 1000us) */
	volatile uint32_t sapsq_epoch_interval_tsc_fallback;          /*   4 B — Bug-A fix: TSC-delta fallback threshold (DPA cycles); 0 = disabled */
	uint8_t pad_sapsq_hdr[64 - 20];

	/* Epoch publication (DPA writer, host reader; release-fence protocol) */
	volatile uint64_t sapsq_epoch;                                /*   8 B — next epoch in progress */
	volatile uint64_t sapsq_epoch_commit;                         /*   8 B — committed (safe to read budgets) */
	volatile uint64_t sapsq_last_epoch_tsc;                       /*   8 B — DPA TSC at commit (host staleness check) */
	volatile uint64_t sapsq_epoch_count_events;                   /*   8 B — DPA: events seen this epoch (when reach interval, run scheduler) */
	uint8_t pad_sapsq_epoch[64 - 32];

	/* Per-tenant configuration (host writer, DPA reader) */
	volatile uint32_t sapsq_tenant_weight_q16[SAPSQ_TENANT_MAX];    /* 64 B — Q16.16 weight; 0 = inactive */
	volatile uint32_t sapsq_tenant_demand_q32[SAPSQ_TENANT_MAX];    /* 64 B — host-set or estimated demand in Q32 IOPS-equivalent */

	/* Per-path configuration (host writer, DPA reader) — base capacity + probe budget */
	volatile uint32_t sapsq_path_base_iops_q32[SAPSQ_PATH_MAX];   /*  32 B — base IOPS in Q32 IO/tsc (host init) */
	volatile uint32_t sapsq_probe_rate_q32[SAPSQ_PATH_MAX];       /*  32 B — probe budget on quarantined paths */

	/* Per-path DPA-computed state (DPA writer, host reader) */
	volatile uint32_t sapsq_path_health_q16[SAPSQ_PATH_MAX];      /*  32 B — health_factor Q16.16 */
	volatile uint8_t  sapsq_path_eligibility[SAPSQ_PATH_MAX];     /*   8 B — SAPSQ_ELIG_* */
	uint8_t pad_sapsq_path_state[64 - SAPSQ_PATH_MAX * 5];

	/* Per-(tenant, path) allocated rate (DPA writer, host reader).
	 * Q32 IO/tsc compatible with M4 m4_tenant_refresh_per_tsc_q32 format.
	 * Size: 16 × 8 × 4 B = 512 B (8 cachelines). */
	volatile uint32_t sapsq_tenant_path_rate_q32[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];

	/* Per-(tenant, path) host-side token bucket state (host writer).
	 * Each host process only writes its own row [sapsq_my_tenant_id][*].
	 * Tokens Q16.16 IO units; refresh on every admission_check using host TSC.
	 * Size: 16 × 8 × 4 B = 512 B + 16 × 8 × 8 B = 1024 B = 1536 B. */
	volatile uint32_t sapsq_tenant_path_tokens_q16[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];
	volatile uint64_t sapsq_tenant_path_last_refresh_tsc[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];

	/* Observability counters (host writer on submit; DPA-side may read for demand est).
	 * Size: 16 × 8 × 8 B × 3 = 3072 B. */
	volatile uint64_t sapsq_admit_count[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];
	volatile uint64_t sapsq_reject_count[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];
	volatile uint64_t sapsq_probe_count[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];

	/* Fallback observability — incremented by host when stale epoch forces M4 fallback */
	volatile uint64_t sapsq_stale_epoch_fallback;                 /*   8 B */
	volatile uint64_t sapsq_total_epochs_consumed;                /*   8 B */
	uint8_t pad_sapsq_obs[64 - 16];

	/* === SAPS-Q (M-series unified scheduler) shared schema ===
	 * DPA computes per-tenant/per-path rate budgets every scheduler epoch.
	 * Host fast-path refreshes tokens by (tenant_id, path_id) using local TSC.
	 * See specs/research-architecture-consolidated-20260522.md §5.1.
	 *
	 * These fields use SAPSQ_MAX_TENANTS=16 / SAPSQ_MAX_PATHS=8 and are the
	 * canonical 2D budget plane for SAPS-Q slice 1+.  The earlier sapsq_*
	 * fields above now share the same eight-path dimension.
	 */

	/* DPA-published budget plane (DPA write, host read) */
	volatile uint32_t sapsq_epoch_seq;                                          /*   4 B — monotonic epoch counter; host reads only stable epoch */
	volatile uint32_t sapsq_epoch_commit_seq;                                   /*   4 B — DPA writes this last after rate table fully written */
	uint8_t pad_sapsq_m_epoch[64 - 8];

	volatile uint32_t sapsq_tenant_path_rate_budget_q32[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS]; /* IO/s budget Q0.32 per (tenant,path); DPA write, host read */
	volatile uint32_t sapsq_committed_path_health_factor_q16[SAPSQ_MAX_PATHS];   /* health snapshot used for the committed budget epoch */
	uint8_t pad_sapsq_m_committed_hf[64 - SAPSQ_MAX_PATHS * 4];
	volatile uint64_t sapsq_committed_path_effective_capacity_iops[SAPSQ_MAX_PATHS]; /* K_p h_p used for the committed budget epoch */
	volatile uint8_t  sapsq_path_health[SAPSQ_MAX_PATHS];                       /*   8 B — enum: 0=HEALTHY 1=ONSET 2=PERPETUAL_SLOW 3=BIMODAL_TAIL 4=SPARSE_PATH_ERROR 5=RECOVERING 6=QUARANTINED */
	uint8_t pad_sapsq_m_health[64 - SAPSQ_MAX_PATHS];

	volatile uint32_t sapsq_path_health_factor_q16[SAPSQ_MAX_PATHS];            /*  32 B — Q16.16 [0,1] scale factor applied to path deliverable capacity */
	uint8_t pad_sapsq_m_hf[64 - SAPSQ_MAX_PATHS * 4];

	/* Host-side enforcement plane (host write, DPA read for observability) */
	volatile uint32_t sapsq_tenant_path_tokens_budget_q16[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS]; /* Q16.16 current tokens per (tenant,path); 16×8×4=512 B */
	volatile uint64_t sapsq_tenant_path_refresh_tsc[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS];       /* host TSC of last refresh; 16×8×8=1024 B */
	volatile uint64_t sapsq_tenant_path_admit_count[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS];       /* successful admits; 16×8×8=1024 B */
	volatile uint64_t sapsq_tenant_path_reject_count[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS];      /* EAGAIN returns; 16×8×8=1024 B */

	/* DPA-side demand/served observability (DPA write) */
	volatile uint32_t sapsq_demand_iops[SAPSQ_MAX_TENANTS];                     /*  64 B — EWMA submit attempt rate per tenant */
	volatile uint64_t sapsq_served_iops[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS];   /* completed IO count per (tenant,path); 16×8×8=1024 B */

	/* SAPS-Q runtime config (host init writes once) */
	volatile uint32_t sapsq_num_tenants;                                        /*   4 B — runtime tenant count, <= SAPSQ_MAX_TENANTS */
	volatile uint32_t sapsq_num_paths;                                          /*   4 B — runtime path count, <= SAPSQ_MAX_PATHS */
	volatile uint32_t sapsq_tenant_weight[SAPSQ_MAX_TENANTS];                  /*  64 B — integer weights (e.g. [3,1,1,1]) */
	volatile uint64_t sapsq_link_cap_iops;                                      /*   8 B — namespace service envelope C (IO/s) */
	volatile uint32_t sapsq_epoch_period_us;                                    /*   4 B — DPA scheduler epoch period in microseconds */
	volatile uint32_t sapsq_probe_rate_budget_q32;                              /*   4 B — min probe budget per path for liveness (Q0.32) */
	volatile uint64_t sapsq_host_tsc_freq;                                      /*   8 B — host TSC frequency Hz (for rate-to-token math) */
	volatile uint32_t sapsq_bypass_d_classifier;                               /*   4 B — 0=normal D-driven, 1=bypass (force all paths HEALTHY) */
	volatile uint32_t sapsq_bypass_saps_fsm;                                   /*   4 B — 1=skip SAPS B7 FSM classify+state (E1 baseline testing) */
	volatile uint32_t sapsq_bypass_health_coupling;                            /*   4 B — enum sapsq_health_coupling_mode; legacy field name preserves ABI */
	volatile uint32_t sapsq_health_source;                                      /*   4 B — enum sapsq_health_source */
	uint8_t pad_sapsq_m_cfg[64 - (4 + 4 + 8 + 4 + 4 + 8 + 4 + 4 + 4 + 4)];

	/* Per-path deliverable capacities K_p. These are physical/path-level
	 * limits, not equal shares of the namespace envelope. A redundant path
	 * may therefore have K_p == C, allowing healthy paths to absorb traffic
	 * from a degraded peer without increasing total namespace admission. */
	volatile uint64_t sapsq_path_capacity_iops[SAPSQ_MAX_PATHS];                /*  64 B — host-configured K_p (IO/s) */

	/* Option 1 host-side demand signal (Lane U, 2026-05-28).
	 * Host writes (atomic increment) on every submit attempt;
	 * DPA scheduler tick reads delta to detect idle tenants without
	 * relying on DPA-side g_sapsq_submit_count (producer_idx visibility broken).
	 * Host writer: sapsq_admission_check_2d admit path (and stale-fallback ADMIT).
	 * DPA reader: sapsq_update_demand_ewma (replaces g_sapsq_submit_count).
	 * Size: 16 × 8 B = 128 B (2 cachelines). */
	volatile uint64_t sapsq_host_submit_count[SAPSQ_MAX_TENANTS];             /* 128 B — host writes, DPA reads delta */

} __attribute__((aligned(64)));

/* v2 SAPS action codes (§4.5 semantic classifier output). Shared between
 * DPA kernel writer and SPDK bdev_nvme reader via per_qp_path_retry_verdict[].
 */
enum dpa_saps_action {
	DPA_SAPS_ACTION_TERMINAL   = 0,   /* abort retry, complete as error */
	DPA_SAPS_ACTION_RETRY_SAME = 1,   /* retry on same path (stock default) */
	DPA_SAPS_ACTION_FAILOVER   = 2,   /* force path failover */
	DPA_SAPS_ACTION_NOT_ERROR  = 3,   /* treat as success (e.g. deallocated block) */
	DPA_SAPS_ACTION_UNSET      = 0xFF /* no verdict yet — fall through to stock */
};

enum dpa_m2_verdict {
	DPA_M2_VERDICT_HEALTHY    = 0,    
	DPA_M2_VERDICT_INDIVIDUAL = 1,   /* entropy < log2(N/2),subset signature(K=1..8) */
	DPA_M2_VERDICT_JOINT      = 2,    
	DPA_M2_VERDICT_SUSPECT    = 3,    
	DPA_M2_VERDICT_UNSET      = 0xFF  
};

/* v2 SAPS FSM state codes (§4.4 state machine). */
enum dpa_saps_state {
	DPA_SAPS_STATE_HEALTHY    = 0,
	DPA_SAPS_STATE_DEGRADING  = 1,
	DPA_SAPS_STATE_EXCLUDED   = 2,
	DPA_SAPS_STATE_RECOVERING = 3
};

/* Transfer struct from host to DPA RPC entry. */
struct dpa_plugin_transfer {
	uint32_t window_id;
	uint32_t mkey_id;
	uint64_t haddr;         /* host VA of struct dpa_plugin_shared */
	uint64_t buf_bsize;
} __attribute__((__packed__, aligned(8)));

#endif /* __DPA_PLUGIN_COM_H__ */
