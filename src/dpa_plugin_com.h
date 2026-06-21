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

#define DPA_PLUGIN_RING_LOG2  13             /* 2^13 = 8192 slots × 64B = 512 KiB */
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

/* S3 SAPS-single: max paths per bdev channel. Paper single-client experiment
 * uses 2 paths, but sized to 4 to leave headroom. Must be power-of-2. */
#define DPA_PLUGIN_PATH_LOG2  2
#define DPA_PLUGIN_PATH_MAX   (1u << DPA_PLUGIN_PATH_LOG2)  /* 4 */
#define DPA_PLUGIN_PATH_MASK  (DPA_PLUGIN_PATH_MAX - 1u)

/* M2 streaming PCA: per-client PC vector count。對齊 spike N_CLIENTS=16,
 * client_id = qp_id & (M2_CLIENT_MAX - 1) hash 出來。 */
#define M2_CLIENT_LOG2 4
#define M2_CLIENT_MAX  (1u << M2_CLIENT_LOG2)  /* 16 */
#define M2_CLIENT_MASK (M2_CLIENT_MAX - 1u)
#define M2_FEATURE_DIM 8

/* M2 Oja's rule learning rate η = 0.01 in Q16.16 (spike ETA_OJA=0.01)。
 * 655 / 65536 = 0.00999... (偏 0.055%,遠在 spike convergence tolerance). */
#define M2_ETA_Q16 655

/* M2 warmup sample count(snapshot baseline 之前先收的 events)。
 * Spike extremenoise K=8 converged_to_target_at = 1076 → round up 1100。 */
#define M2_WARMUP_N 1100u

/* M2 sub-sample rate:每 (1 << M2_SUBSAMPLE_LOG2) 個 event 才跑一次 Oja
 * update。每 IO amortized cost = ~113ns / 8 = 14ns,符合 245ns budget。 */
#define M2_SUBSAMPLE_LOG2 3
#define M2_SUBSAMPLE_MASK ((1u << M2_SUBSAMPLE_LOG2) - 1u)

/* M3 WMM credit ledger:per-tenant credit refresh + decrement。Tenant 數
 * 對齊 spike A/B/C/D 都用 4,壓測上限 16。 */
#define M3_TENANT_LOG2 4
#define M3_TENANT_MAX  (1u << M3_TENANT_LOG2)  /* 16 */
#define M3_TENANT_MASK (M3_TENANT_MAX - 1u)

/* Burst credit cap (避免 idle tenant 累積 credit 把對手耗光,對齊 Caladan §3)。
 * 1000 IO worth in Q32.32 = 1000 × (1 << 32) */
#define M3_BURST_CAP_Q32  ((int64_t)1000LL << 32)

/* Δtsc clamp (避免極端 idle gap 觸發溢位),~67 ms @1.5 GHz */
#define M3_DELTA_TSC_CAP  ((uint64_t)100000000ULL)

/* ── SAPS-Q (2026-05-22): DPA-advised, host-enforced per-tenant/per-path scheduler
 *
 * Spec: specs/dm-research-redesign-20260522.md §4。Reuse M3 tenant max + DPA_PLUGIN
 * path max,不另起 dimension。Tenant/path 編號 alias 既有 M3 / per_qp_path 體系。
 */
#define SAPSQ_TENANT_MAX  M3_TENANT_MAX        /* 16, alias M3_TENANT_MAX */
#define SAPSQ_PATH_MAX    DPA_PLUGIN_PATH_MAX  /* 4,  alias DPA_PLUGIN_PATH_MAX */

/* SAPS-Q M-series unified scheduler dimension macros.
 * SAPSQ_MAX_TENANTS = 16 (matches SAPSQ_TENANT_MAX; kept as explicit named constant
 *   for the new 2D schema fields per specs/research-architecture-consolidated-20260522.md §5.1).
 * SAPSQ_MAX_PATHS = 8: future-expansion headroom beyond current 3-path testbed.
 *   Existing fields (sapsq_tenant_path_rate_q32 etc.) use SAPSQ_PATH_MAX=4 and are
 *   NOT resized here — only new §5.1 fields use SAPSQ_MAX_PATHS=8. */
#define SAPSQ_MAX_TENANTS 16
#define SAPSQ_MAX_PATHS   8

/* Health factor in Q16.16 (1.0 = 65536 = HEALTHY full capacity).
 * Mapping from D-classifier (redesign §4.2):
 *   HEALTHY        -> 1.0   * 65536 = 65536
 *   DEGRADING/ONSET-> 0.5   * 65536 = 32768  (caller multiplies by (1-tail_risk))
 *   DEGRADED       -> 0.3   * 65536 = 19660
 *   BIMODAL_TAIL   -> 0.4   * 65536 = 26214  (tail-penalty path)
 *   SPARSE_ERROR   -> SAPSQ_HEALTH_PROBE_Q16 (probe-only)
 *   PERPETUAL_SLOW -> SAPSQ_HEALTH_PROBE_Q16 (probe-only)
 *   EXCLUDED/quarantine -> 0
 * 絕對禁止使用 per_qp_path_capacity[] 做 health_factor (D8 self-reinforcing 已踩過). */
#define SAPSQ_HEALTH_HEALTHY_Q16   ((uint32_t)(1u << 16))   /* 65536 */
#define SAPSQ_HEALTH_PROBE_Q16     ((uint32_t)100u)         /* ε ~ 0.0015 */
#define SAPSQ_HEALTH_QUARANTINE    ((uint32_t)0u)

/* Path eligibility encoded as enum bytes for host fast-path branchless lookup. */
#define SAPSQ_ELIG_QUARANTINE  0u
#define SAPSQ_ELIG_PROBE       1u
#define SAPSQ_ELIG_NORMAL      2u

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
 * E-4 MPSC note: producer_idx is the multi-producer head. With a single
 * producer (standalone/coordinator) it is plain-store + release-fence. With
 * multiple producers (coordinator + tenants sharing the memfd), every
 * producer uses __atomic_fetch_add(&producer_idx, n, __ATOMIC_ACQ_REL) to
 * reserve n slots, writes its entries, then the ACQ_REL on the RMW acts as
 * the publish fence. The field is 8-byte aligned and sits in its own 64B
 * cacheline (pad0 ensures consumer_idx lives in a separate line, preventing
 * false-sharing between producers and the DPA consumer). The entries[] array
 * is 64B-aligned per-entry (see struct dpa_plugin_notify_entry), so MPSC
 * slot writes never collide on the same cacheline once the slot is reserved.
 */
struct dpa_plugin_shared {
	struct dpa_plugin_notify_entry entries[DPA_PLUGIN_RING_SIZE];

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
	 * Size: 1024 * 4 * 1B = 4 KiB. */
	volatile uint8_t per_qp_path_retry_verdict[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_MAX];

	/* v2 SAPS path state table — DPA writes FSM state for monitoring,
	 * SPDK / operator tooling can read it (non-hot-path).
	 *
	 * Values: see enum dpa_saps_state (0=HEALTHY, 1=DEGRADING,
	 *         2=EXCLUDED, 3=RECOVERING).
	 *
	 * Size: 1024 * 4 * 2B = 8 KiB. */
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

	/* D4 trace instrumentation (2026-05-17) — per-path histograms maintained by
	 * DPA inside update_state_machine().
	 *
	 * Goal: 量測 D4 場景下 path B (idx 1) 真實的 FSM dwell time + 分類分佈,
	 * 取代之前 inference-based RCA。Histogram-only (not sequence) is sufficient
	 * because the question is "why doesn't path B reach EXCLUDED" — answered by
	 * state dwell distribution + dominant fault_type.
	 *
	 * Layout: [DPA_PLUGIN_PATH_MAX=4][buckets]. Path index matches existing
	 * per_qp_path_* arrays. Host reads via mmap (no RPC needed).
	 *
	 * State buckets: 0=HEALTHY 1=DEGRADING 2=EXCLUDED 3=RECOVERING
	 * Fault buckets: 0=HEALTHY 1=PERPETUAL_SLOW 2=SPARSE_ERROR 3=BIMODAL_TAIL
	 *                4=FLAP 5=ONSET 6=QD_DRIFT 7=ANY_THREE_VOTE 8=SHARED_FATE
	 *                9=PROPORTIONAL_THROTTLE
	 *
	 * Bumped once per update_state_machine() call (sampled output of classifier).
	 * Single-QP D4 場景下 totals = path 上做過的 classification count。 */
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

	/* M1 spike (2026-05-17): host 在 init 時依 SAPS_M1_SINGLE_STREAM
	 * 環境變數寫 0/1。DPA process_event() 讀此 bit 決定要不要把
	 * (qp_idx, path_idx) 全部 remap 到 (0, 0) 模擬跨 client 聚合。
	 * 預設 0,不影響 M0/M2/M3 路徑。 */
	volatile uint32_t saps_m1_single_stream;

	/* M1 spike (2026-05-19): one-shot reset request。host 在 init 時若
	 * SAPS_M1_SINGLE_STREAM=1 寫 1,DPA 在第一筆 event 時 memset g_path[0][0]
	 * 然後清回 0,避免前一輪 run 的 EWMA / FSM 殘留 (跨 testbed rep 殘留
	 * 而 Slice 31 idle-gap auto-heal 因 cell 間隔 < 60s 不會觸發)。 */
	volatile uint32_t saps_m1_reset_request;
	uint8_t pad_m1[64 - 2 * sizeof(uint32_t)];

	/* ── M2 streaming PCA (Oja's rule) per-client state ────────────────────
	 * 對應 specs/m2-dpa-implementation-plan-20260519.md §1。每 client 維護
	 * 8-feature PC vector w_c (Q16.16 in int64),warmup 結束時 snapshot 成
	 * baseline,之後算 cosine(w_c, w_baseline) 寫到 m2_pca_conf_q16 給 host
	 * non-hot-path 做 entropy 三分類 (HEALTHY/INDIVIDUAL/JOINT)。M2 v1 只當
	 * observability,不接 routing(defensive default)。 */
	volatile uint32_t saps_m2_enabled;          /* 0=off (default), 1=run Oja */
	volatile int64_t  m2_pca_w[16][8];          /* 1024 B — per-client PC */
	volatile int64_t  m2_pca_w_baseline[16][8]; /* 1024 B — warmup snapshot */
	volatile uint32_t m2_pca_n_events[16];      /*   64 B — sub-sample count */
	volatile int32_t  m2_pca_conf_q16[16];      /*   64 B — cosine to baseline */
	volatile uint8_t  per_client_joint_verdict[16]; /* 16 B — host-written */
	uint8_t pad_m2[64 - 16 - sizeof(uint32_t)];

	/* ── M3 WMM credit ledger per-tenant state ──────────────────────────
	 * 對應 specs/m3-dpa-implementation-plan-20260519.md §1.2。Tenant 對應
	 * spike A/B/C/D 四 client(壓測場景延伸到 16)。Hot-path:每 IO complete
	 * 跑一次 credit refresh + decrement(~5 ns 加總)。v1 只當 observability,
	 * 不接 admission gate(out-of-scope v2 才接)。 */
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

	/* M3 v3 (2026-05-20):per-tenant freeze flag。DPA 寫 1 表示 credit < 0,
	 * host fast-path submit 之前 check,1 → spin-wait or yield。比 per_qp_tokens
	 * 的 cumulative-credit gap 直接(後者在 credit=0 wrap 成 huge 不會 throttle)。 */
	volatile uint8_t  m3_tenant_freeze[16];           /*  16 B — DPA writer, host reader */
	uint8_t pad_m3_freeze[64 - 16];

	/* ── M3 v2 (M4 namespace, 2026-05-20):in-DPA per-tenant token bucket。
	 *
	 * 取代 v1 (host SSH daemon, Jain 0.5216, 3.6s/tick RPC wall) 用獨立 Q16.16
	 * fixed-point token bucket。 DPA RP process_event hot path 每事件 refresh
	 * + consume,host admission_check 讀 m4_tenant_reject_flag 立刻 -EAGAIN。
	 *
	 * 避開 v2/v3 失敗 root cause:
	 *   - 不碰 per_qp_tokens(過去 uint32 wrap 在 (credit-admitted) underflow → admit)
	 *     → 用獨立 Q16.16 token counter
	 *   - 不用 binary freeze flag(per-IO 翻太快 host 看不到 sticky)
	 *     → 用 continuous Q16.16 token，refresh + consume 在同 hot path
	 *   - 不走 immediate EAGAIN retry loop
	 *     → host 回 -EAGAIN 後 SPDK queued_req 路徑,resubmit 跟著 completion poll
	 *
	 * Refresh rate: r_i = (w_i / Σw) × link_iops, refresh_per_tsc_q16 = r_i / tsc_freq × 2^16
	 * Burst cap: BURST_FACTOR IOs worth (default 1000 IOs = 1000 << 16 in Q16.16) */
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

	/* ── M5 v3 DPA-side proactive DRR scheduler (2026-05-20) ──────────────
	 *
	 * Alt B design:DPA RP 每 process_event round 跑 Deficit Round Robin,
	 * 把 per-tenant grant_count (monotonic) 往上推。Host admission_check
	 * 比較 grant_count > consumed_count → atomic bump consumed → admit。
	 * 不需要 winner ring,不需要 io_token,不改 hook signature。
	 *
	 * Work-conserving redistribution:若 tenant t 的 pending_io_count == 0
	 * (idle), 本 round 的 deficit[t] 不清零(保留給下輪),而是把這一輪
	 * 多出來的 deficit 標記成 idle,等 active tenant 下輪繼承。
	 * 簡化實作:DPA 每 round 對 idle tenant 不消耗 deficit(已累積即保留);
	 * active tenant 吃 deficit 的同時可以借:deficit cap = 2× quantum(最多
	 * 借一輪),超過的部分截掉以保 O(1) per-IO 複雜度。
	 *
	 * Pending detection:host 每次 submit 時 atomic++ m5_tenant_pending[t],
	 * DPA admit 時 atomic-- m5_tenant_pending[t]。DPA 看 pending > 0 = active。
	 *
	 * State layout:
	 *   grant_count[t]    — DPA 累積發給 tenant t 的 IO 許可數 (uint64, monotonic)
	 *   consumed_count[t] — host 累積消費的 IO 許可數 (uint64, atomic, host writer)
	 *   deficit_q16[t]    — DPA per-tenant DRR deficit accumulator (Q16.16, int32)
	 *   quantum_q16[t]    — DPA per-tenant quantum (weight × BASE_QUANTUM, Q16.16, uint32, host writes)
	 *   pending[t]        — host 端 outstanding 未 admit IO 數 (uint32, host atomic writer)
	 *   last_drr_tsc      — DPA 上次 DRR round 的 TSC (uint64, DPA writer)
	 *   drr_round_count   — DPA 跑過的 DRR round 數 (uint64, observability)
	 *
	 * Admission check:
	 *   tenant_id = m5_my_tenant_id
	 *   if m5_tenant_grant_count[t] > m5_tenant_consumed_count[t]:
	 *     __atomic_fetch_add(&m5_tenant_consumed_count[t], 1, __ATOMIC_ACQ_REL)
	 *     m5_tenant_admit_count[t]++
	 *     return 0
	 *   else:
	 *     m5_tenant_reject_count[t]++
	 *     return -EBUSY  (SPDK queued_req)
	 *
	 * DRR scheduler (DPA process_event, time-driven via TSC delta):
	 *   BASE_QUANTUM = m5_base_quantum_q16 (default 1000 IOs Q16.16 = 1000<<16)
	 *   Each quantum_q16[t] = round(weight[t] / Σweight × BASE_QUANTUM_PER_ROUND)
	 *   每次 elapsed_tsc > m5_drr_interval_tsc 時跑一輪 DRR:
	 *     for t in 0..M5_TENANT_MAX-1:
	 *       if pending[t] > 0 (active tenant):
	 *         deficit[t] += quantum[t]
	 *         grant = min(deficit[t] / cost_per_io_q16, pending[t])
	 *         grant_count[t] += grant
	 *         deficit[t] -= grant × cost_per_io_q16
	 *         clamp deficit[t] to [0, 2×quantum[t]] (prevent unbounded accumulation)
	 *       else (idle):
	 *         deficit[t] += quantum[t]  // 保留累積,work-conserving 下輪多借
	 *         clamp deficit[t] to [0, 2×quantum[t]]
	 *
	 * DRR interval 設計:每 ~1ms (1.5G tsc → 1.5M tsc/ms);與 v2 refill per-IO 不同,
	 * DRR 是 time-sliced,避免 event-driven deadlock (v2 §2 root cause)。
	 *
	 * Env vars:
	 *   SAPS_M5_DRR_ENABLED=1
	 *   SAPS_M5_LINK_IOPS        (default 200000)
	 *   SAPS_M5_WEIGHTS          "w0,w1,w2,..." (default "1,1,1,1")
	 *   SAPS_M5_TENANT_ID        per-proc tenant id
	 *   SAPS_M5_BASE_QUANTUM_IOS (default 1000, IOs per round per unit weight)
	 *   SAPS_M5_DRR_INTERVAL_US  (default 1000 µs = 1 ms)
	 */
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

	/* ── SAPS-Q (2026-05-22): per-tenant × per-path scheduler ─────────────
	 *
	 * Spec: specs/dm-research-redesign-20260522.md §4。
	 *
	 * Plane split:
	 *   - DPA RP writes: epoch, path_health, tenant_path_rate_q32, eligibility
	 *   - Host writes:   config (weights, base IOPS, probe), token bucket state,
	 *                    observability counters
	 *
	 * Stable-epoch publication protocol (DPA):
	 *   1. write all sapsq_tenant_path_rate_q32[t][p] and sapsq_path_health[p]
	 *   2. release fence (full memory barrier on DPA-RP)
	 *   3. sapsq_epoch_commit = ++sapsq_epoch
	 *   4. sapsq_last_epoch_tsc = current DPA-side TSC reference
	 *
	 * Stable-epoch read protocol (host admission_check):
	 *   1. epoch_snapshot = sapsq_epoch_commit (acquire load)
	 *   2. if (host_tsc - sapsq_last_epoch_tsc_host_view) > stale_tsc:
	 *        fall back to M4 static rates  (host has its own tsc proxy
	 *        because DPA TSC is not the same clock; see host code)
	 *   3. otherwise refresh + consume against sapsq_tenant_path_rate_q32
	 *
	 * Anti-deadlock: token refresh is host-TSC-driven on every submit attempt,
	 * not completion-driven (per redesign §4.4)。
	 */

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
	volatile uint32_t sapsq_path_base_iops_q32[SAPSQ_PATH_MAX];   /*  16 B — base IOPS in Q32 IO/tsc (host init) */
	volatile uint32_t sapsq_probe_rate_q32[SAPSQ_PATH_MAX];       /*  16 B — probe budget on quarantined paths */
	uint8_t pad_sapsq_path_cfg[64 - 32];

	/* Per-path DPA-computed state (DPA writer, host reader) */
	volatile uint32_t sapsq_path_health_q16[SAPSQ_PATH_MAX];      /*  16 B — health_factor Q16.16 */
	volatile uint8_t  sapsq_path_eligibility[SAPSQ_PATH_MAX];     /*   4 B — SAPSQ_ELIG_* */
	uint8_t pad_sapsq_path_state[64 - 20];

	/* Per-(tenant, path) allocated rate (DPA writer, host reader).
	 * Q32 IO/tsc compatible with M4 m4_tenant_refresh_per_tsc_q32 format.
	 * Size: 16 × 4 × 4 B = 256 B (4 cachelines). */
	volatile uint32_t sapsq_tenant_path_rate_q32[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];

	/* Per-(tenant, path) host-side token bucket state (host writer).
	 * Each host process only writes its own row [sapsq_my_tenant_id][*].
	 * Tokens Q16.16 IO units; refresh on every admission_check using host TSC.
	 * Size: 16 × 4 × 4 B = 256 B + 16 × 4 × 8 B = 512 B = 768 B. */
	volatile uint32_t sapsq_tenant_path_tokens_q16[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];
	volatile uint64_t sapsq_tenant_path_last_refresh_tsc[SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX];

	/* Observability counters (host writer on submit; DPA-side may read for demand est).
	 * Size: 16 × 4 × 8 B × 3 = 1536 B. */
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
	 * fields above (using SAPSQ_TENANT_MAX × SAPSQ_PATH_MAX=4) remain for
	 * backward compatibility and reviewer transparency.
	 */

	/* DPA-published budget plane (DPA write, host read) */
	volatile uint32_t sapsq_epoch_seq;                                          /*   4 B — monotonic epoch counter; host reads only stable epoch */
	volatile uint32_t sapsq_epoch_commit_seq;                                   /*   4 B — DPA writes this last after rate table fully written */
	uint8_t pad_sapsq_m_epoch[64 - 8];

	volatile uint32_t sapsq_tenant_path_rate_budget_q32[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS]; /* IO/s budget Q0.32 per (tenant,path); DPA write, host read */
	volatile uint8_t  sapsq_path_health[SAPSQ_MAX_PATHS];                       /*   8 B — enum: 0=HEALTHY 1=ONSET 2=PERPETUAL_SLOW 3=BIMODAL_TAIL 4=SPARSE_PATH_ERROR 5=RECOVERING 6=QUARANTINED */
	uint8_t pad_sapsq_m_health[64 - SAPSQ_MAX_PATHS];

	volatile uint32_t sapsq_path_health_factor_q16[SAPSQ_MAX_PATHS];            /*  32 B — Q16.16 [0,1] scale factor applied to path base capacity */
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
	volatile uint64_t sapsq_link_cap_iops;                                      /*   8 B — total link capacity hint (IO/s) */
	volatile uint32_t sapsq_epoch_period_us;                                    /*   4 B — DPA scheduler epoch period in microseconds */
	volatile uint32_t sapsq_probe_rate_budget_q32;                              /*   4 B — min probe budget per path for liveness (Q0.32) */
	volatile uint64_t sapsq_host_tsc_freq;                                      /*   8 B — host TSC frequency Hz (for rate-to-token math) */
	volatile uint32_t sapsq_bypass_d_classifier;                               /*   4 B — 0=normal D-driven, 1=bypass (force all paths HEALTHY) */
	volatile uint32_t sapsq_bypass_saps_fsm;                                   /*   4 B — 1=skip SAPS B7 FSM classify+state (E1 baseline testing) */
	uint8_t pad_sapsq_m_cfg[64 - (4 + 4 + 8 + 4 + 4 + 8 + 4 + 4)];

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

/* M2 PCA joint-anomaly verdict (host-side entropy 三分類 寫到
 * per_client_joint_verdict[client_id])。M2 v1 只當 observability,不接 routing。 */
enum dpa_m2_verdict {
	DPA_M2_VERDICT_HEALTHY    = 0,   /* baseline cosine ≥ H_JOINT,正常 */
	DPA_M2_VERDICT_INDIVIDUAL = 1,   /* entropy < log2(N/2),subset signature(K=1..8) */
	DPA_M2_VERDICT_JOINT      = 2,   /* entropy ≈ log2(N),全災(K=16 pattern) */
	DPA_M2_VERDICT_SUSPECT    = 3,   /* cosine in [H_JOINT, H_JOINT+δ] 邊界 */
	DPA_M2_VERDICT_UNSET      = 0xFF /* warmup 未完成 */
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
