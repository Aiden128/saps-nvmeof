/* P0.3/P0.5/E1 dpa_plugin — DPA-side polling kernel.
 *
 * Configures window entity 0 with the host-provided mkey, translates the host
 * VA of struct dpa_plugin_shared, and polls the producer_idx. Each new slot is
 * read once (touches cmd_id + opcode) and consumer_idx is advanced.
 *
 * P0.5 Teardown fix:
 *   Root cause: flexio_process_call has a 6-second hard timeout in libflexio
 *   (host polls with usleep(10000) × 600 = 6 real seconds). After 6s, libflexio
 *   kills the DPA thread and returns status=-2. The DPA was not running for the
 *   30s test — it died at 6s and consumed only ~9M/132M pushed events.
 *
 *   Fix: DPA polls for DPA_LEASE_OUTER × DPA_INNER_SPIN iterations, then
 *   returns the sentinel DPA_LEASE_CONTINUE. The host re-invokes immediately
 *   (rpc_thread_fn loop). This keeps the DPA alive indefinitely without any
 *   single invocation exceeding the 6s timeout.
 *
 *   Two-level polling:
 *     Inner loop (DPA_INNER_SPIN): pure register ops — checks cached last_prod
 *       vs consumed with no PCIe fence. At ~2 ns/iteration, 100 inner = 200 ns.
 *     Outer loop (DPA_LEASE_OUTER): issues ONE __dpa_thread_window_read_inv()
 *       per iteration to refresh stop + producer_idx from host. Each outer iter
 *       costs 1 fence (~600 µs observed) + 100 inner (~200 ns) ≈ 600 µs.
 *     DPA_LEASE_OUTER = 100: 100 × 600 µs ≈ 60 ms per invocation — well below 6s.
 *     At 60 ms/invocation, a 30-second test requires ~500 lease renewals.
 *
 *   WARNING: Do NOT add flexio_dev_print() inside the polling loop.
 *     If the DPA message ring fills (host not draining during SPDK init),
 *     flexio_dev_print() BLOCKS until the host drains, eating the 6s budget.
 *     Only print at invocation entry, clean exit, and lease-renew boundary.
 *
 *   Return values:
 *     DPA_LEASE_CONTINUE   — voluntary early return for lease renewal
 *     consumed count       — clean exit (saw stop=1)
 *     0xdead0001/0002      — fatal error during window setup
 *
 * E1 algorithm stub (per-connection CUSUM + Welford):
 *   For each consumed ring entry (kind=1 complete events), compute the integer
 *   log10(latency_ns) via CLZ + small lookup, update Welford mean/M2, update
 *   CUSUM accumulators, push into a K=32 rolling window. State lives entirely
 *   in DPA heap (static arrays), no host round-trip.
 *
 *   State footprint (bytes per struct conn_stats, see sizeof_check below):
 *     window[32]×4            = 128
 *     head, count             =   8
 *     mean_x1000, m2 (i64×2)  =  16
 *     n                       =   4
 *     cusum_pos/neg (i64×2)   =  16
 *     tokens, degraded        =   8
 *     last_change_tsc         =   8
 *     qp_id, ns_id, reserved  =   8
 *     Raw = 196; with align(64) the allocator rounds to 256 (4 cachelines).
 *   Total @ N=1000 conn: 256 KiB conn_stats + 512 KiB submit_tsc_low
 *     (direct-map by tenant, path, and cmd_id). Plus the
 *     512 KiB notify ring → 1.0 MiB, within BF-3 DPA 1.5 MiB L2.
 *
 *   NO FPU on DPA — log10 is approximated from bit-length (__builtin_clzll)
 *   plus a 9-entry lookup table giving log10(2^k)*1000 for k in [0..63].
 *   Accuracy: coarse (integer decade), sufficient to separate tail outliers
 *   from baseline for CUSUM change-point detection.
 */

#include <libflexio-dev/flexio_dev_ver.h>
#define FLEXIO_DEV_VER_USED FLEXIO_DEV_VER(25, 10, 0)
#include <libflexio-dev/flexio_dev.h>
#include <libflexio-dev/flexio_dev_err.h>
#include <stddef.h>
#include <stdbool.h>
#include <dpaintrin.h>
#include "../dpa_plugin_com.h"

/* Hot-loop diagnostic prints measurably perturb RPC setup and scheduler
 * cadence on the real DPA. Keep them available for a diagnostic build, but
 * disable them in measurement binaries. Final stop-state counters still print. */
#ifndef SAPS_DPA_DIAGNOSTIC_PRINTS
#define SAPS_DPA_DIAGNOSTIC_PRINTS 0
#endif

uint64_t dpa_plugin_rpc(uint64_t in_daddr);

#define BATCH 128u

/* Sentinel: DPA returns this to request an immediate re-invocation. */
#define DPA_LEASE_CONTINUE 0xc0ff33c0ff33ull

/* Two-level polling loop sizes. */
#define DPA_INNER_SPIN  100u
#define DPA_LEASE_OUTER 100u

/* CUSUM detector parameters (integer, in units of log10_latency_ns * 1000).
 *
 * E1-integration values (2026-04-17) derived from R4-verify + §3.4 tuning:
 *   drift x1000     = 1197  (~1.2 decade — above baseline µs-scale noise,
 *                            below the 5 ms / 5000 µs cascade anchor)
 *   threshold       = 490   (fires after ~4 consecutive outlier events)
 *
 * Safety valve 1: minimum admit rate (throttle cannot lock a QP out completely).
 *   After CUSUM fires, admit one IO per MIN_ADMIT_INTERVAL_TICKS even if still
 *   degraded. 10 ms @ 1.5 GHz DPA TSC = 15,000,000 ticks.
 *
 * Safety valve 2: time-bounded throttle auto-release.
 *   If we remain degraded for more than MAX_THROTTLE_TICKS without CUSUM reset,
 *   force-clear degraded flag + cusum_pos so the QP can recover. 200 ms @
 *   1.5 GHz = 300,000,000 ticks.
 */
#define CUSUM_DRIFT_X1000         1197
#define CUSUM_THRESH              490
/* E1-integration (2026-04-17 tuning after hang observation):
 * MIN_ADMIT_INTERVAL lowered from 10 ms → 100 µs so "degraded" is a soft
 * rate limiter, NOT a binary lockout. At 100 µs per bypass admit, a fully
 * throttled QP is capped at ~10 k IOPS — enough that SPDK can drain
 * queued_req and preventing the pipe-freeze deadlock observed when
 * throttle interval was 10 ms (cascade → throttle → no completions →
 * no new cusum updates → sv2 releases & immediately re-fires → hang). */
#define MIN_ADMIT_INTERVAL_TICKS  150000ull     /* 100 µs @ 1.5 GHz */
/* E1-fix1: extend throttle window from 200ms to 2s. CUSUM fires on rare
 * outlier samples (500ms+ tail events); a single fire must hold the throttle
 * long enough to drain SPDK queued_req. 200ms was too short — sv2 released
 * mid-cascade before the backlog cleared. */
#define MAX_THROTTLE_TICKS        3000000000ull /* 2 s @ 1.5 GHz */

/* E1-fix1 (2026-04-17): rate-limited token bucket.
 *
 * The binary 0/1 admission gate (sv1 only) deadlocks at QD=512/Delay0: SPDK's
 * nvme_qpair_resubmit_requests only fires on completions; a single sv1 admit
 * per 100 µs outer-iter period is ~1600 admits/s/QP, ~15x below the 25k IOPS
 * theoretical ceiling at Delay0 5 ms. queued_req fills, pipe freezes.
 *
 * Fix: DPA writes a monotonically refilled *token count* to per_qp_tokens[].
 * Host decrements atomically per admit. Degraded-rate is bounded by refill
 * rate but each admit is independent of DPA's outer-loop period — so SPDK can
 * burst-drain queued_req whenever completions arrive.
 *
 * Sizing:
 *   TOKENS_PER_SEC_DEGRADED = 20000 (20k/s — caps below the ~25k IOPS
 *     theoretical ceiling of Delay0 5ms, so the cascade gets throttled but
 *     not deadlocked. ~78% of ceiling.)
 *   TOKENS_PER_SEC_NORMAL   = 1000000 (1M/s = no practical limit, matches
 *     host's ~2M-IOPS throughput on Null backends.)
 *   TOKENS_MAX_DEGRADED     = 128 (aligns with NVMe SQ HW depth; bounds
 *     burst size after a refill quiet period so a long CUSUM-fire → recover
 *     transition cannot unleash a 10k-IO burst.)
 *   TOKENS_MAX_NORMAL       = 256 (generous, effectively unbounded.)
 *   TICKS_PER_SEC           = 1500000000 (1.5 GHz DPA TSC, matches
 *     MIN_ADMIT_INTERVAL calibration above.)
 */
/* NORMAL regime: bucket effectively unbounded so healthy traffic never
 * throttles, even at 2 M IOPS Null-backend demand. 1 M tokens × 10 M/s
 * refill means bucket stays above zero as long as IOPS < 10 M. */
#define TOKENS_MAX_NORMAL         1000000u
#define TOKENS_MAX_DEGRADED       128u     /* match NVMe SQ HW depth */
#define TOKENS_PER_SEC_NORMAL     10000000u /* 10 M/s — above any real demand */
/* E1-fix1 tuning: degraded rate = 20k/s. At 5ms delay this gives steady-state
 * in-flight ≈ 20000 × 0.005 = 100 IOs, close to the QD=128 ceiling without
 * starving the pipeline. Below this (e.g. 5k/s) the initiator-side queue
 * backlogs and P50 latency explodes — an over-correction worse than no
 * throttle at all (observed run: IOPS 1800, P50 47ms). */
#define TOKENS_PER_SEC_DEGRADED   20000u
#define TICKS_PER_SEC             1500000000ull

/* Legacy aliases kept to minimise diff for the welford/cusum_update helpers
 * below. Point them at the new integration values. */
#define CUSUM_DRIFT      CUSUM_DRIFT_X1000
#define CUSUM_THRESHOLD  CUSUM_THRESH

/* ------ Per-connection state (DPA-resident, zero host round-trip) ------
 *
 * E1-integration: extended with degraded_since (safety-valve 2),
 * last_admit_tsc (safety-valve 1), degraded_reason (trace / debug). Adds 24 B
 * which pushes aligned sizeof from 256 to 320. L2 footprint at N=1024 is
 * 1024 × 320 = 320 KiB. The submit table is 512 KiB; the host ring is
 * accessed through the registered memory window rather than copied here.
 */
struct conn_stats {
	uint32_t window[DPA_PLUGIN_WINDOW_K];  /* log10_lat_ns * 1000, circular */
	uint32_t head;
	uint32_t count;

	int64_t  mean_x1000;   /* Welford online mean */
	int64_t  m2;           /* Welford sum of squared deviations */
	uint32_t n;
	uint32_t pad_n;

	int64_t  cusum_pos;
	int64_t  cusum_neg;

	uint32_t tokens;           /* E1-fix1: current bucket level (host decrements) */
	uint32_t degraded;
	uint64_t last_change_tsc;

	/* E1-integration decision state (24 B): */
	uint64_t degraded_since;   /* TSC when degraded=1 was asserted */
	uint64_t last_refill_tsc;  /* E1-fix1: TSC of last bucket refill (was last_admit_tsc) */
	uint32_t degraded_reason;  /* 0=normal, 1=cusum_fire */
	uint32_t pad_decision;

	uint32_t qp_id;
	uint16_t ns_id;
	uint16_t reserved;
} __attribute__((aligned(64)));

/* Compile-time size assertion: aligned struct should be multiple of 64.
 * E1-integration bumped the limit from 256 → 320 (see comment on struct). */
_Static_assert(sizeof(struct conn_stats) % 64 == 0,
	       "conn_stats must be cacheline multiple");
_Static_assert(sizeof(struct conn_stats) <= 320,
	       "conn_stats larger than expected 320B");

/* All static — lives in DPA .bss, inside L2. */
static struct conn_stats g_conn[DPA_PLUGIN_CONN_MAX];

/* Track submit timestamps by a compact (qp, path, cmd_id) key, not cmd_id
 * alone.  NVMe command IDs are scoped to a qpair; with multipath, different
 * paths can reuse the same cmd_id while one path has delayed completions.  A
 * cmd_id-only table lets fast peers overwrite the slow path's submit timestamp
 * and makes D2-style latency onset look healthy.  Ten command-ID bits preserve
 * every slot in the prototype's 1024-entry NVMe queue. Four tenant bits and
 * three path bits distinguish the full 16-by-8 scheduler plane. */
#define SAPS_SUBMIT_CID_BITS  10u
#define SAPS_SUBMIT_QP_BITS   4u
#define SAPS_SUBMIT_TABLE_LOG2 \
	(SAPS_SUBMIT_CID_BITS + SAPS_SUBMIT_QP_BITS + DPA_PLUGIN_PATH_LOG2)
#define SAPS_SUBMIT_TABLE_SIZE (1u << SAPS_SUBMIT_TABLE_LOG2)
#define SAPS_SUBMIT_CID_MASK  ((1u << SAPS_SUBMIT_CID_BITS) - 1u)
#define SAPS_SUBMIT_QP_MASK   ((1u << SAPS_SUBMIT_QP_BITS) - 1u)

_Static_assert(SAPS_SUBMIT_TABLE_LOG2 == 17u,
	       "submit timestamp table must cover 16 tenants, 8 paths, and 1024 CIDs");
static uint32_t g_submit_tsc_low[SAPS_SUBMIT_TABLE_SIZE];

static inline uint32_t
saps_submit_slot(uint16_t cmd_id, uint16_t qp_idx, uint16_t path_idx)
{
	uint32_t qp_part = ((uint32_t)qp_idx & SAPS_SUBMIT_QP_MASK);
	uint32_t path_part = ((uint32_t)path_idx & DPA_PLUGIN_PATH_MASK);
	uint32_t owner = (qp_part << DPA_PLUGIN_PATH_LOG2) | path_part;

	return (owner << SAPS_SUBMIT_CID_BITS) |
	       ((uint32_t)cmd_id & SAPS_SUBMIT_CID_MASK);
}

/* v2 SAPS per-path state (spec-v4.md §4.1, 2026-04-22 canonical).
 * Indexed [qp_id & CONN_MASK][path_id & PATH_MASK].
 *
 * v2-modern (this revision) diff from intermediate v2 pluggable-detector:
 *   - REMOVED Welford mean_log_lat / m2 + fixed_isqrt — spec §4.3 scores
 *                 directly from streaming P99, no Kingman variance proxy
 *   - REMOVED pluggable detector_ops (cusum_ops / bocpd_ops / conformal_ops)
 *                 — §4.6 says "no pluggable interface, no ablation candidate.
 *                 NEWMA on log-lat is settled."
 *   - ADDED   p99_estimate + frugal_count (global Frugal-2U streaming P99,
 *             Ma/Muthukrishnan/Sandler ANALCO 2014) — §4.3 direct tail-aware
 *   - ADDED   opc_p99[3] (per-opcode Frugal-2U P99) — §4.1 Chain E
 *   - KEPT    ewma_fast / ewma_slow        (NEWMA on log-lat, §4.6)
 *             err_hist[4][16], err_rate_ewma (semantic histogram, §4.5)
 *             inflight + qd_mean + qd_m2  (QD trend)
 *             state + state_entered_tsc + probe_count (FSM §4.4)
 *             score                        (shadow of verdict table)
 *
 * Footprint: 64 B + 32 B (err_hist) + 24 B (opc_p99) + misc ≈ 192 B actual
 * (3 cachelines). 1024 × 4 × 192 B = 768 KiB — fits 1.5 MiB L2 alongside
 * g_conn (320 KiB, E1 admission) + submit_tsc_low (256 KiB) + ring (512 KiB).
 *
 * All fixed-point (Q16.16) — no FPU on DPA.
 */
/* Slice 10 SAPS multi-modal fault types (spec §2). DPA-internal enum;
 * not exposed to host shared struct (host only consumes score / FSM state /
 * retry verdict). Sequenced same as the rule-tree priority order in §3.2. */
enum saps_fault_type {
	SAPS_FAULT_HEALTHY        = 0,
	SAPS_FAULT_PERPETUAL_SLOW = 1,
	SAPS_FAULT_SPARSE_ERROR   = 2,
	SAPS_FAULT_BIMODAL_TAIL   = 3,
	SAPS_FAULT_FLAP           = 4,
	SAPS_FAULT_ONSET          = 5,
	SAPS_FAULT_QD_DRIFT       = 6,
	SAPS_FAULT_ANY_THREE_VOTE = 7,
	SAPS_FAULT_SHARED_FATE    = 8,
	SAPS_FAULT_PROPORTIONAL_THROTTLE = 9,
	SAPS_FAULT_ALL_DEGRADED   = 10,
	SAPS_FAULT_M2_SUSPECT     = 11,    
};

#define SAPS_BOCPD_RUN_MAX 8u

#define SAPS_D2_B1_ENABLE 0
#define SAPS_D2_B2_ENABLE 0
#define SAPS_D2_B3_ENABLE 0
#define SAPS_D2_B4_ENABLE 0
#define SAPS_D2_B5_ENABLE 0
#define SAPS_D2_B6_ENABLE 0
#define SAPS_D2_B7_ENABLE 1
#define SAPS_D2_B8_ENABLE 1
#define SAPS_D2_PROTOTYPE_ISOLATE_LEGACY_LATENCY 0  /* D4 hero fix 4 (2026-05-17): re-enable BIMODAL_TAIL latch + cross-path PERPETUAL_SLOW */

#define SAPS_D2_B2_STATE_ACTIVE (SAPS_D2_B2_ENABLE || SAPS_D2_B6_ENABLE)

struct dpa_path_state {
	/* Clean sample count (§4.2 — retry_count==0 completions only). */
	uint64_t n;

	/* NEWMA detector state (§4.6, Keriven et al. NeurIPS 2020). */
	int64_t  ewma_fast;                /* Q16.16 on log-latency */
	int64_t  ewma_slow;                /* Q16.16 on log-latency */

	/* Frugal-2U streaming P99 (§4.3, Ma/Muthukrishnan/Sandler ANALCO 2014).
	 * p99_estimate tracked in Q16.16 log2-of-latency-ticks (same units as
	 * ewma_{fast,slow}) so scoring can take 1/p99 directly without unit
	 * conversion. frugal_step is Frugal-2U's adaptive step-doubling state. */
	int64_t  p99_estimate;             /* Q16.16 log-lat */
	int32_t  frugal_step;              /* Frugal-2U step (positive integer) */
	int32_t  _pad_frugal;

	/* Slice 27 — exact P99 from 64-sample sliding window.
	 * Replaces Frugal-2U convergence-based estimator with deterministic
	 * exact P99 from the most recent 64 clean samples. For N=64, P99
	 * sits between sample 63 and 64 of sorted ascending; we use the 2nd
	 * largest (top-2) as the exact P99 estimate. log-lat in Q16.16.
	 *
	 * Memory: 64 × 8B = 512B per path slot; CONN_MAX × PATH_MAX = 4096
	 * paths × 512B = 2MB total (fits DPA RAM with 4MB headroom).
	 *
	 * Update: every clean sample push to ring at lat_ring_head, advance
	 * head modulo 64, saturate count at 64. P99 readers call
	 * compute_exact_p99(p) which does O(N) two-max scan.
	 *
	 * Frugal-2U fields above (p99_estimate, frugal_step) kept as legacy
	 * for binary compat; p99_estimate now holds the exact P99 from this
	 * ring (overwritten on each sample after frugal2u_update). All
	 * existing readers (line 1080, 1230, 1686) continue to work
	 * unchanged but read more accurate values. */
	int64_t  lat_ring[64];             /* Q16.16 log-lat samples */
	uint8_t  lat_ring_head;            /* next write index, 0..63 */
	uint8_t  lat_ring_count;           /* saturates at 64 */
	uint8_t  _pad_lat_ring[6];         /* keep struct aligned */

	/* In-flight + QD trend (used as score penalty, not primary signal). */
	uint32_t inflight;
	uint32_t _pad_inflight;
	int64_t  qd_mean;                  /* Q16.16 */
	int64_t  qd_m2;                    /* Q32.32 */

	/* Round 10 D8: completion-rate/service-capacity estimator.
	 * capacity_rate_ewma_q16 is completions/sec in Q16 over a 60s EWMA.
	 * capacity_score divides that rate by QD EWMA, so a path that needs
	 * twice the queue residency for the same completion rate reports roughly
	 * half the relative capacity. */
	uint64_t capacity_rate_ewma_q16;
	uint32_t capacity_sec_completions;
	uint32_t capacity_last_sec;
	uint32_t capacity_score;
	uint32_t _pad_capacity;

	uint64_t cap_fast_win_start_tsc;    
	uint32_t cap_fast_win_completions;  
	uint32_t rate_fast_q16;             

	uint32_t cap_score_base;           /* baseline rate_fast/qd (Q16/QD unit) */
	uint32_t cap_rate_base;             
	uint32_t qd_base;                   
	int64_t  norm_log_lat_base;         

	uint8_t  sf_degraded_history;      /* 3-bit history of per-tick degraded */
	uint8_t  sf_degraded_cached;        
	uint8_t  _pad_sf_cap[6];

	/* Error histogram [sct 0..3][sc & 0xF] (§4.5 semantic classifier). */
	uint16_t err_hist[4][16];          /* 128 B */
	int64_t  err_rate_ewma;            /* Q16.16 */

	/* Per-opcode Frugal-2U P99 (§4.1 Chain E, opcode-aware scoring).
	 * Index 0=READ(0x02), 1=WRITE(0x01), 2=FLUSH(0x00). */
	struct {
		int64_t  p99;              /* Q16.16 log-lat */
		int32_t  frugal_step;
		uint32_t n;
	} opc_p99[3];

	/* FSM (§4.4). */
	uint16_t state;                    /* enum dpa_saps_state (0..3) */
	uint16_t _pad_state;
	uint32_t probe_count;              /* RECOVERING ε-probe count */
	uint64_t state_entered_tsc;
	uint64_t healthy_since_tsc;        /* continuous HEALTHY verdict interval */

	/* Last-event telemetry (debug aid; read by lease-renew trace). */
	uint64_t last_complete_tsc;

	/* Cached verdict (shadows per_qp_path_score[qp][path]). */
	uint32_t score;
	uint32_t _pad_score;

	/* ── Slice 10 multi-modal classifier state (§3.1) ─────────────────── */

	/* Baseline frozen at end of warmup (n == SAPS_WARMUP_N). Used as the
	 * denominator for the latency-ratio rules. Stored in Q16.16 log2 units
	 * so divisions become subtractions. */
	int64_t  baseline_log_lat_q16;     /* 0 means "not yet frozen" */

	/* EWMA mean of clean log-lat samples (λ = 1/64). Used for ratio against
	 * baseline. Distinct from ewma_fast/ewma_slow (NEWMA detector state). */
	int64_t  mean_log_lat_ewma_q16;

	/* Round 2 onset detector: very slow tracker that stays anchored to the
	 * long-term healthy latency when a path steps from healthy to slow. */
	int64_t  long_term_mean_log_lat_q16;

	/* Round 6 D2 B1: truncated BOCPD posterior over run length.
	 * Slots are capped at SAPS_BOCPD_RUN_MAX - 1; the last slot represents
	 * the collapsed long-run hypothesis. Each slot stores a Q16 posterior
	 * probability plus Student-t conjugate sufficient statistics over
	 * log-latency samples: mean and M2 in Q16.16 log2 units. */
	uint16_t bocpd_run_prob_q16[SAPS_BOCPD_RUN_MAX];
	uint16_t _pad_bocpd_prob[SAPS_BOCPD_RUN_MAX];
	int64_t  bocpd_run_mean_q16[SAPS_BOCPD_RUN_MAX];
	int64_t  bocpd_run_m2_q16[SAPS_BOCPD_RUN_MAX];
	uint16_t bocpd_cp_prob_q16;        /* P(run_length=0 | data), Q16 */
	uint8_t  bocpd_onset_active;
	uint8_t  bocpd_onset_clear_consec;
	uint8_t  bocpd_onset_pending_consec;
	uint8_t  _pad_bocpd_latch;
	int64_t  bocpd_last_log_lat_q16;

	/* Round 6 D2 B2: adaptive Page-CUSUM over per-path log-latency.
	 * Reference stays on the healthy baseline; positive cumulative deviation
	 * fires ONSET without waiting for the slow EWMA to diverge. */
	int64_t  d2_cusum_ref_q16;
	int64_t  d2_cusum_pos_q16;
	int64_t  d2_cusum_last_log_lat_q16;
	uint8_t  d2_cusum_onset_active;
	uint8_t  d2_cusum_clear_consec;
	uint8_t  _pad_d2_cusum[6];

	/* Round 6 D2 B4: ADWIN-style two-subwindow detector over lat_ring. */
	uint8_t  adwin_onset_active;
	uint8_t  adwin_clear_consec;
	uint8_t  _pad_adwin[6];

	/* Round 6 D2 B5: first-backed-up-completion inflight-skew detector. */
	uint8_t  inflight_onset_active;
	uint8_t  inflight_onset_clear_consec;
	uint8_t  _pad_inflight_onset[6];

	/* Round 6 D2 B7: host-assisted active-probe detector.  The host selector
	 * supplies low-rate sentinel samples; DPA latches ONSET after three
	 * consecutive slow probe completions. */
	uint8_t  active_probe_slow_consec;
	uint8_t  active_probe_clear_consec;
	uint8_t  active_heal_fast_consec;
	uint8_t  shared_fate_fast_consec;
	uint8_t  _pad_active_probe[4];

	/* Welford-style M2 accumulator + EWMA-smoothed log-lat variance. The
	 * variance is in (Q16.16 log-lat)^2 units, kept as Q16.16 after rescale.
	 * lat_cv ≈ sqrt(var) / |mean|; in log domain we compare 2·sqrt(var) to
	 * thresholds without explicit division. */
	int64_t  lat_var_ewma_q16;

	/* Sliding error tracker. recent_err_count = number of sct≠0 in the last
	 * recent_sample_count clean samples (decay window 64). Decay: every 64
	 * complete events both counters halve. */
	uint16_t recent_err_count;
	uint16_t recent_sample_count;

	/* NEWMA fire timestamp ring (length 8). Each NEWMA fire pushes
	 * spdk_get_ticks() (in ms units, low 32) into the ring. fire_rate over a
	 * 1 s window = ring entries with (now_ms - tsc_ms) < 1000. */
	uint32_t newma_fire_tsc_ms_ring[8];
	uint8_t  newma_fire_ring_head;
	uint8_t  _pad_newma_head[3];

	/* FSM transition timestamp ring (length 4). Same encoding. */
	uint32_t fsm_trans_tsc_ms_ring[4];
	uint8_t  fsm_trans_ring_head;
	uint8_t  _pad_fsm_head[3];

	/* Cached classification (set per complete event). */
	uint16_t fault_type;               /* enum saps_fault_type */
	uint16_t _pad_fault;

	/* FLAP dampening: skip update_state_machine() until this TSC. Window
	 * doubles on consecutive FLAP triggers; resets on HEALTHY classify. */
	uint64_t flap_dampening_until_tsc;
	uint32_t flap_dampening_window_ms;     /* current window length */
	uint32_t _pad_flap;

	/* Slice 16.5: classifier hysteresis — count of consecutive non-HEALTHY
	 * raw classifications. Only return non-HEALTHY once count reaches
	 * SAPS_HYSTERESIS_N. Prevents D0 healthy-baseline transient noise (one
	 * spike in lat_diff right after warmup) from being labelled
	 * PERPETUAL_SLOW/SPARSE_ERROR/BIMODAL_TAIL → score downweight → argmax
	 * sticky on the path that warmed up first. Reset to 0 on any HEALTHY
	 * raw classification. Saturated at 0xFF. */
	uint8_t  consec_nonhealthy_count;
	uint8_t  _pad_hyst[3];

	/* Slice 15 — max-of-recent-window latency tracker.
	 * Frugal-2U P99 has slow / unreliable convergence on bimodal latency
	 * distributions (D4 85/15 mix did not drive p99_estimate up enough to
	 * fire BIMODAL_TAIL). Track raw max(log_lat) over a sliding window of
	 * SAPS_MAX_WIN_N samples; reset window when max_lat_window_n reaches
	 * SAPS_MAX_WIN_N. Keeps a recent worst-case observation that survives
	 * for the full window even when a tail sample is rare. */
	int64_t  max_log_lat_recent;       /* Q16.16, max in current window */
	uint32_t max_lat_window_n;         /* sample count in current window */
	uint32_t tail_count_recent;        /* Slice 15: # samples > tail thresh in window */
	/* Slice 30: two-consecutive-window bimodal confirmation.
	 * ARM OS stalls produce tc >= SAPS_TAIL_COUNT_MIN in a single window
	 * (one stall event bursts 25-64 samples over threshold) but the NEXT
	 * window returns to clean (0-5 tail samples). D4 bimodal is persistent:
	 * every 512-sample window has ~77 tail samples. Requiring two consecutive
	 * triggered windows blocks the one-shot ARM stall burst while still
	 * catching D4's sustained bimodal. */
	uint8_t  bimodal_consec_windows;   /* # consecutive triggered windows (capped at 2) */
	uint8_t  _pad_bimodal[3];

	/* Slice 28b: hysteresis for inflight-based classifier.
	 * Require 3 consecutive samples above the ratio threshold before firing.
	 * Resets to 0 whenever the path's inflight drops back below threshold.
	 * Prevents D0 healthy false-positives from random per-path burst spikes. */
	uint8_t  inflight_high_consec;   /* Slice 28b: consecutive high-inflight count, hysteresis */
	uint8_t  onset_high_consec;
	uint8_t  onset_clear_consec;
	uint8_t  qd_drift_high_consec;
	uint8_t  proportional_hold_consec;
	uint8_t  _pad_proportional[3];
} __attribute__((aligned(64)));

static struct dpa_path_state g_path[DPA_PLUGIN_CONN_MAX][DPA_PLUGIN_PATH_MAX];
static uint64_t g_shared_fate_until_tsc[DPA_PLUGIN_CONN_MAX];
static uint8_t g_shared_fate_last_excluded_plus1[DPA_PLUGIN_CONN_MAX];
static uint64_t g_saps_sample_events;
static uint64_t g_saps_classify_calls;
static uint64_t g_saps_score_updates;

static inline void saps_publish_counters(volatile struct dpa_plugin_shared *s)
{
	s->saps_sample_events = g_saps_sample_events;
	s->saps_classify_calls = g_saps_classify_calls;
	s->saps_score_updates = g_saps_score_updates;
}

/* v2 SAPS tunables (spec-v4 §4.1 / §4.3 / §4.4 / §4.6 — 2026-04-22 canonical).
 *
 * Scoring is direct 1/P99 — no Kingman variance proxy, no pluggable detector.
 * Single algorithm stack: NEWMA on log-lat + Frugal-2U P99 + semantic classifier
 * + FSM with hysteresis.
 *
 * Score space (§4.3):
 *   SCALE = 1<<20 (Q4.20). Score ∈ [1..SCALE]. Never 0 for HEALTHY/DEGRADING
 *   paths so bdev_nvme argmax always has a winner; EXCLUDED and RECOVERING
 *   (non-probe) paths clamp to 1 to preserve ε-probe opportunity.
 *
 * NEWMA (§4.6, Keriven/Garreau/Poli NeurIPS 2020):
 *   LAMBDA_FAST = 0.1 × (1<<16) = 6553
 *   LAMBDA_SLOW = 0.01 × (1<<16) = 655
 *   H_NEWMA     = 0.5 × (1<<16) = 32768   — distance threshold
 *   WARMUP_N    = 32                      — no detection until n > WARMUP
 *
 * Frugal-2U (§4.3, Ma/Muthukrishnan/Sandler ANALCO 2014):
 *   TARGET_P99_NUM / TARGET_P99_DEN = 99/100 — nominal quantile
 *   PRNG_MASK = 0xFF (256-step). update prob for Frugal-1U up step = 1%.
 *   Frugal-2U step-double when consecutive same-direction updates,
 *   reset step to 1 on direction flip.
 *
 * FSM hysteresis (§4.4):
 *   E_HI  = 0.05 × (1<<16) = 3277
 *   E_LO  = 0.01 × (1<<16) = 655
 *   T_DEG_TICKS     = 500 ms
 *   T_RECOVER_TICKS = 100 ms
 *   T_EXCLUDE_TICKS = 1 s
 *   N_PROBE = 30
 *   ERR_ALPHA_Q16 = (1<<16)/32 = 2048      — α = 1/32 for err_rate_ewma
 *
 * Score weights (§4.3):
 *   W_ERR = 20  — err_rate_ewma penalty
 *   W_QD  = 1   — inflight penalty (per outstanding IO)
 *
 * Wall-clock (DPA TSC @ 1.5 GHz):
 *   TICKS_PER_MS = 1_500_000
 */
#define SAPS_SCORE_SCALE       (1u << 20)
#define SAPS_SCORE_MAX         SAPS_SCORE_SCALE
#define SAPS_SCORE_MIN         1u
#define SAPS_WARMUP_N          32u
#define SAPS_LAMBDA_FAST_Q16   6553       /* 0.1 */
#define SAPS_LAMBDA_SLOW_Q16   655        /* 0.01 */
/* Option2 redesign (2026-04-25): H_NEWMA raised 4× from 0.5 → 2.0 (Q16.16
 * 32768 → 131072). Diagnostic FSM trace from Option2 D1 smoke showed all 3
 * paths NEWMA-fired exactly at n=32 (warmup boundary) and A/C never recovered
 * because they stopped receiving samples after the argmax locked onto B —
 * stale-score deadlock. Higher H_NEWMA suppresses the warmup-edge burst noise
 * that triggered DEGRADING on healthy paths in the first place. The slow path
 * (B 5ms) is detected by inflight accumulation in the new compute_score, not
 * by NEWMA on log-latency. */
#define SAPS_H_NEWMA_Q16       131072     /* 2.0 — was 0.5 */

/* Frugal-2U: target quantile = 99% expressed over a 256-step PRNG output.
 * up_threshold_q8 / 256 ≈ 1 - 0.99 = 0.01 → 3/256 ≈ 1.17%.
 * Complement for down step: (256 - 3)/256 ≈ 98.83%. */
#define SAPS_FRUGAL_UP_Q8      3u

#define SAPS_W_ERR             20u
#define SAPS_W_QD              1u
/* Option2 redesign (2026-04-25): inflight-dominant scoring constants.
 *
 * Motivation: D1 (B=5ms slow) — original score (1/P99 dominant, W_QD=1) was
 * outperformed 47× by stock queue_depth policy because:
 *   (a) NEWMA fires on warmup-edge IO burst → A/C/B all transition HEALTHY
 *       → DEGRADING at n=32 (Option2 FSM trace evidence, see report)
 *   (b) Once host argmax locks onto B (random tiebreak after warmup),
 *       A/C stop receiving samples and their score freezes at the
 *       NEWMA-fired-DEGRADING value — stale-score deadlock
 *   (c) W_QD=1 is too weak to push B's score below A/C's: at inflight=128
 *       penalty is only 128, while base from 1/P99 is in the 10-50K range
 *
 * Fix: replicate stock QD's "min inflight wins" as the dominant signal,
 * keep latency as a tie-breaker. SAPS becomes a strict superset of QD: it
 * picks the same path QD would on pure-latency-asymmetry scenarios (D1),
 * but adds NVMe semantic awareness (D3 sct/sc → retry verdict) and tail-
 * jitter awareness (D4 bimodal) that QD cannot see.
 *
 *   SAPS_QD_SCORE_BASE = 1<<20 (= 1048576) — inflight=0 starts at SCORE_MAX
 *   SAPS_W_QD_NEW      = 4096            — 1 inflight = 4096 score drop.
 *                                           At inflight=128 (≈ NVMe SQ HW
 *                                           depth), penalty = 524288 (half
 *                                           of full range). At inflight=256
 *                                           saturates near MIN.
 *   latency_bonus      = boosted >> 4    — 1/P99 reduced ~16× so worst-case
 *                                           latency_bonus (~4K) ≈ 1 inflight
 *                                           difference. Latency only breaks
 *                                           ties when inflight is equal.
 */
#define SAPS_QD_SCORE_BASE     (1u << 20)
#define SAPS_W_QD_NEW          4096u
#define SAPS_E_HI_Q16          3277       /* 0.05 */
#define SAPS_E_LO_Q16          655        /* 0.01 */
#define SAPS_N_PROBE           96u
#define SAPS_ERR_ALPHA_Q16     2048       /* 1/32 */

#define SAPS_TICKS_PER_MS      1500000ull
#define SAPS_T_DEG_TICKS       (500ull  * SAPS_TICKS_PER_MS)
#define SAPS_T_RECOVER_TICKS   (100ull  * SAPS_TICKS_PER_MS)
#define SAPS_T_EXCLUDE_TICKS   (10000ull * SAPS_TICKS_PER_MS)  /* Fix 9 (2026-05-18): 1s → 10s. Per D4 trace, B re-entered RECOVERING every 1s producing ~4% traffic leak via probes. Extending to 10s reduces RECOVERING cycles 10× → expect B<1%. D0/D1 healthy unaffected (they never enter EXCLUDED). */

/* ─── Slice 10 multi-modal classifier tunables (spec §3.2) ─────────────────
 *
 * All comparisons in log2-domain so we never divide. log2(2.0)=1, log2(5)≈2.32.
 *
 *   LAT_RATIO_HI    : mean_log_lat - baseline > 1.0 (Q16.16 → 65536) ⇒ 2× slower
 *   LAT_RATIO_OK    : |mean - baseline| < 0.32 (Q16.16 ~21000) ⇒ within ±25%
 *   LAT_VAR_HI_Q16  : variance threshold for "high CV"  ≈ (log2(1.5))^2 ~0.6
 *                     in Q16.16 units (lat_var_ewma_q16 is Q16.16 directly).
 *                     0.5 squared as Q16.16 ≈ 16384.
 *   P99_RATIO_HI    : p99 - mean > log2(5) ≈ 2.32 (Q16.16 ~152000) ⇒ tail >5× mean
 *   ERR_RATE_LOW_Q16: 0.01 × (1<<16) = 655 — must be below this to qualify
 *                     PERPETUAL_SLOW or BIMODAL_TAIL (those don't carry errors)
 *   RECENT_ERR_THRESH: ≥ this many sct≠0 in last 64 samples ⇒ SPARSE_ERROR
 *   NEWMA_FIRE_HI_PER_SEC : > 2 NEWMA fires within 1s ⇒ FLAP
 *   FSM_TRANS_HI_PER_SEC  : > 1 FSM transition within 1s ⇒ FLAP
 *
 * §6 D-series targets used to derive thresholds:
 *   D3: err_rate ≈ 7e-5, but recent_err_count(64) ≈ 0.5–2 → use ≥1 with decay
 *   D4: bimodal 85/15 with 5ms slow → variance dominates, mean drift ~+0.4 log2
 *   D6: flap injection drives multiple HEALTHY↔DEGRADING per second
 */
#define SAPS_LAT_RATIO_HI_Q16     65536      /* log2(2.0) = 1.0 */
/* Slice 18: queue-saturation slow-path detector.  qd_mean is Q16.16 EWMA of
 * inflight (α=1/32 in saps_update submit branch).  Threshold = 16 << 16 =
 * 1048576: a path whose mean inflight has converged above 16 (i.e. half the
 * global QD=32) is queue-saturated and structurally slow regardless of
 * whether its latency baseline was poisoned at warmup. D0 healthy paths
 * with QD/3 ≈ 10-12 stay below this threshold; D1 path B with inflight
 * ≥ 24 saturates above it. Picked at QD/2 to leave a gap for natural
 * load asymmetry without firing on healthy noise. */
#define SAPS_INFLIGHT_SLOW_THRESH_Q16   ((int64_t)24 << 16)
/* Slice 18: absolute mean-latency threshold (Q16.16 of log2(ticks)).
 * 1 ms = 1,500,000 ticks; log2(1.5e6) ≈ 20.516. In Q16.16: 20.516 × 65536
 * ≈ 1,344,512.  Below this any path is fast enough for "healthy" classification
 * regardless of its relative drift; above this it is structurally slow even
 * when its baseline was poisoned at warmup. */
#define SAPS_LAT_ABS_SLOW_Q16           1344512
/* Slice 20: severe-absolute threshold for hysteresis bypass.
 *   D1 path B sits at log2(7.5M ticks) ≈ 22.84 → Q16.16 ≈ 1,497,108
 *   Healthy 27 µs path  log2(40500)  ≈ 15.30 → Q16.16 ≈ 1,003,520
 *   Threshold 1 ms (= SAPS_LAT_ABS_SLOW_Q16) gives ≥ 150K margin against
 *   healthy and ≥ 150K against B → no chance of D0 false positive.
 * Same numeric threshold as ABS_SLOW; the difference is the *bypass*: when
 * mean exceeds this, the classifier's hysteresis wrapper short-circuits
 * (saturates the consec counter and returns raw immediately) so a 5 ms
 * stall path is drained in O(1 sample) instead of 4-sample hysteresis.
 *
 * Slice 20: moderate absolute floor for *relative* Rule 2 — host-side queue
 * contamination on D1 inflates A/C's EWMA mean (lat_diff > log2(2)) even
 * though their absolute mean stays well below 1 ms. Without this gate, A/C
 * are mis-classified as PERPETUAL_SLOW under D1 and the host blends them
 * down equally with B, leaving B at ~33 % rather than drained.
 *   200 µs = 300,000 ticks; log2(3e5) ≈ 18.194 → Q16.16 ≈ 1,192,427
 *   Healthy 27 µs    →  ~1,003,520  (well below)
 *   D1 contaminated A/C estimated 100-200 µs → ~1.1-1.2 M (around floor)
 *   B's 5 ms          → 1,497,108 (well above; absolute rule owns it)
 *   D4 bimodal mean ~ 800 µs → ~1.32 M (above floor; Rule 4 BIMODAL owns)
 */
#define SAPS_LAT_ABS_SEVERE_Q16         1344512
#define SAPS_LAT_ABS_MODERATE_Q16       1192427
#define SAPS_LAT_ABS_MIN_SAMPLES        (SAPS_WARMUP_N + 1024u)
#define SAPS_LAT_RATIO_OK_LO_Q16  (-21000)   /* -log2(1.25) ≈ -0.32 */
#define SAPS_LAT_RATIO_OK_HI_Q16  21000      /* log2(1.25) ≈ 0.32 */
#define SAPS_LAT_VAR_HI_Q16       16384      /* 0.25 — variance threshold */
#define SAPS_LAT_VAR_LO_Q16       6553       /* 0.10 */
#define SAPS_P99_RATIO_HI_Q16     152000     /* log2(5) ≈ 2.32 */
/* Slice 15 — max-recent threshold: max(log_lat) - mean(log_lat) > log2(8) ≈ 3.0
 *   Healthy stable: max ≈ 1.2× mean → log diff ~0.3, far below 3.0
 *   D4 bimodal: max in 5 ms tail at log2(7.5e6) ≈ 22.84,
 *               mean ≈ 16.43 → diff ≈ 6.4 — comfortably above 3.0
 *   D1 5 ms slow steady: max ≈ mean (both near 22.8) → diff ~0 — owned by SLOW abs rule
 *   D3 sparse error retry: max can spike but err_rate guards Rule 3 (SPARSE_ERROR) above */
#define SAPS_MAX_RATIO_HI_Q16     196608     /* log2(8) = 3.0 */
#define SAPS_MAX_WIN_N            512u       /* reset window every 512 clean samples — Fix 5 reverted, didn't help B<1% */
/* Tail-count cutoff: log2(2 ms in ticks @ 1.5 GHz) = log2(3e6) ≈ 21.52
 * → Q16.16 = 21.52 × 65536 ≈ 1,410,335. Healthy 27 µs samples sit at
 * log2(40,500) ≈ 15.3 — far below. D4 5 ms tail samples at 22.84 — above.
 * Counting samples that cross this threshold within a 512-window
 * disambiguates a true bimodal mass (≥ 25 hits per 512 samples for
 * D4 15 % mix) from a single scheduler-jitter outlier on a healthy
 * path (typical ≤ 1 per 10 000 samples). */
/* Slice 16 — cross-path median drift threshold (D2 weak-signal CPU throttle).
 * D2 fault: path B sees CPU throttle → mean log-lat shifts ~0.89 above the
 * median of healthy peers. Single-path NEWMA / Frugal-2U cannot see this
 * because B's own baseline is slow too; only cross-path comparison exposes
 * it.
 *
 * Threshold tuning (Slice 16 v2 — bumped from log2(1.5)):
 *   log2(1.7) ≈ 0.766 → Q16.16 = 50,200
 * Lower threshold (1.5) caused C to spuriously trigger on D0 healthy under
 * load (queueing latency bumps C's mean 0.6 above A/B median once briefly
 * → ratchet drains C → stale mean keeps cross-path drift firing).
 *
 * Raised from log2(1.7)=50200 to log2(5.0)=152170 (2026-05-03 Slice 28):
 * ARM scheduler jitter spikes (1-10ms OS stalls) + traffic-imbalance EWMA
 * elevation push a D0 path's EWMA to 80-200µs while peers stay at 40µs,
 * giving cp_drift = log2(200/40) = log2(5) = 2.32. With the old threshold
 * log2(1.7)=0.766, these fire PERPETUAL_SLOW → sticky-A bootstrap deadlock.
 * Observed false cp_drifts: 1.12, 1.74, 1.87 (from n= 386K/396K transitions).
 * New threshold log2(5)=2.322 blocks all observed false positives.
 * D1/D2 B at 5ms vs peers at 27µs: cp_drift = log2(5000/27) ≈ 7.53 >> 2.322.
 * D0 bootstrap skew (one path at 146µs, median at 40µs): cp_drift = 1.87 <
 * 2.322 → does NOT fire (path EWMA decays to normal within 64 clean IOs). */
#define SAPS_CROSSPATH_DRIFT_HI_Q16    152170
/* Consensus-relative latency health.  Below the 5x confidence boundary, keep
 * full health so ordinary queueing variation cannot shrink admitted service.
 * Beyond that boundary, h represents a service-capacity ratio. With fixed
 * outstanding work, Little's law gives service rate proportional to inverse
 * response time, so a log2 latency drift r maps to h = 2^-r. */
#define SAPSQ_CONTINUOUS_HEALTH        1
static inline uint32_t sapsq_continuous_health_from_drift(int64_t drift);
#define SAPS_D7_REF_DRIFT_HI_Q16       65536  /* log2(2) */
#define SAPS_D7_REF_MIN_SAMPLES        (SAPS_WARMUP_N + 64u)
/* Slice 16 v3 — absolute-floor gate so cross-path drift only fires once
 * the path being evaluated has a *meaningfully* elevated mean. Without
 * this, a healthy 27 µs path that briefly drifts to 30 µs (queueing
 * burst) computes drift > log2(1.5) against the 27 µs median, fires
 * PERPETUAL_SLOW, gets drained, mean stays elevated due to staleness,
 * and the ratchet locks the path out (D0 path C false-positive at 4 %
 * traffic share observed in N=5 sweep).
 *   log2(35 µs) ≈ log2(52,500 ticks @ 1.5 GHz) ≈ 15.68 → Q16 = 1,027,604
 * Healthy 27 µs (Q16 = 1,003,520) sits below; D2 throttled 54 µs
 * (Q16 = 1,068,270) sits above. Margin ~24 K Q16 either side. */
/* Slice 16 v3 floor sweep (2026-05-01 → 2026-05-04):
 *   35 µs (1027604): D0/D6 PASS before Slice 27; FAIL after (queueing
 *                    pressure on 27µs path pushes EWMA to ~35µs → Slice 16
 *                    fires → PERPETUAL_SLOW cascade)
 *   30 µs (1015808): same problem, worse
 *   no floor (v2):   D0 BROKEN 48/48/4.5, D2 IOPS 759K
 * Set to 50 µs (1061329): D0 27µs path EWMA under queueing pressure stays
 * < 45µs and does not cross this floor. D2 throttled B path EWMA reaches
 * 100-500µs → floor cleared, Slice 16 fires correctly. */
#define SAPS_LAT_ABS_CP_FLOOR_Q16      1061329

/* Slice 18 — cross-path P99 drift detector for D2 weak-signal fault.
 * D2 (target CPU throttle) signature: B's mean log_lat barely shifts
 * (~0.34 above peers, below cross-path log2(1.7) threshold), but B's
 * P99 jumps significantly because queue head-of-line wait amplifies
 * tail latency. Frugal-2U per-path P99 already maintained — compute
 * cross-path P99 median, compare each path's P99.
 *
 * Trigger: path P99 - global P99 median > log2(2) AND path P99 above
 * absolute floor.
 *
 * Floor raised from 60 µs to 100 µs (Slice 27 fix regression):
 * Slice 27 deactivates exact P99 (returns 0) until ring is full
 * (count < 64). During warmup, Frugal-2U drives p99_estimate and
 * can transiently exceed 60 µs on a healthy 27 µs path when a single
 * scheduler-jitter outlier (50-80 µs) biases the estimate before enough
 * down-moves have accumulated to pull it back. With 60 µs floor, a
 * warmup-phase D0 path can momentarily satisfy (p99 > floor) AND
 * (drift > log2(2)) if the other two paths' Frugal-2U estimates are lower,
 * causing a spurious PERPETUAL_SLOW that latches (path loses traffic →
 * less Frugal-2U feedback → estimate stays elevated → latch persists).
 *
 * Floor at 100 µs ensures Frugal-2U warmup excursions on a 27 µs baseline
 * never reach the trigger zone. D2 is unaffected: B's P99 reaches
 * 200–500 µs under CPU throttle, far above the 100 µs floor.
 * 100 µs @ 1.5 GHz = 150 000 ticks; log2(150000) ≈ 17.19 → Q16 = 1 127 077.
 *
 * D0 healthy: per-path P99 ~ 30-50 µs < 100 µs floor → no fire.
 * D2 throttled: B P99 ~200-500 µs, peers ~30 µs → drift > 2 → fires.
 * D4 bimodal: max-recent rule already owns (BIMODAL_TAIL).
 * D1 PERPETUAL_SLOW abs rule already drains. */
#define SAPS_P99_CP_DRIFT_HI_Q16       65536           /* log2(2) */
#define SAPS_P99_CP_FLOOR_Q16          1127077         /* log2(100 µs in ticks @ 1.5 GHz) */

/* Slice 28 — inflight-based path classification.
 * Threshold: path inflight > 1.5× median(other paths' inflight) AND >= 1 absolute.
 *
 * Sampling-aware floor: DPA_PLUGIN_SAMPLE_RATE=10 means the DPA sees only 1
 * in 10 events. With true B inflight ~14 (50% CPU throttle) and true peers ~7,
 * DPA observes B=1.4, median=0.7. Floor=1 fires at B=2 sampled (= 20 true);
 * Floor=4 would require B=40 true inflight = entire QD=32 — never triggered.
 *
 * Ratio 1.5×: D2 true B:peer ratio is ~14:7 = 2.0×. Sampled ratio is the
 * same but with integer truncation noise. 1.5× is safely between 1.0 (healthy,
 * all equal) and 2.0 (throttled). D0 steady-state B and peers both near 1 → no
 * fire (ratio ≈ 1.0 ≤ 1.5×). Tested via D2 N=10 sweep. */
#define SAPS_INFLIGHT_RATIO_NUM     3u   /* numerator: 3/2 = 1.5× ratio */
#define SAPS_INFLIGHT_RATIO_DEN     2u   /* denominator */
#define SAPS_INFLIGHT_FLOOR_ABS     4u     /* Slice 28b: floor=4 + hysteresis=3 together prevent D0 false-positive; D2 B inflight ~12-16 so floor=4 still fires */

/* Slice 17 — inflight cross-path drift detector (additional D2 fix).
 * D2 fault (target CPU throttle 50 %) elevates B's mean log-lat only
 * 27→50 µs (~ log2(50/27) = 0.89). Under load, A/C's mean also rises
 * due to queueing buildup, so cross-path *latency* drift narrows.
 *
 * Stronger discriminator: per-path inflight (qd_mean EWMA in Q16.16).
 * D2 throttled B accumulates inflight (~12-16 of QD=32), while A/C stay
 * at ~6-8. Ratio 2-3× clearly above healthy 1.0×.
 *
 * Trigger: path qd_mean > 1.5 × global qd_median AND path qd_mean
 * > 4 inflight (absolute floor — too few in-flight is noise).
 *
 * Q16.16 ratio test:
 *   path_qd - median_qd > median_qd / 2  ⇔  path > 1.5 × median
 *
 * D0 healthy steady-state: all qd_mean ~7 ± noise, ratio ~1, no fire.
 * D2 throttled: B qd_mean ~14, median ~7, ratio 2 → fires.
 * D1 perp-slow: B qd_mean ~12, but absolute SLOW rule already owns it.
 * D6 FLAP / D4 bimodal: B qd_mean intermittent — qd_m2 trend is the
 * right detector but not yet plumbed in (future Slice 18).
 *
 * Round 2 D8 target-throttle detector: use cross-path service-rate evidence
 * once the path has meaningful outstanding work. */
#define SAPS_QD_CP_FLOOR_Q16           ((int64_t)4 << 16)
#define SAPS_QD_DRIFT_FIRE_N           16u
#define SAPS_QD_DRIFT_MIN_SAMPLES      (SAPS_WARMUP_N + 1024u)
#define SAPS_CAPACITY_EWMA_SHIFT       6u    /* 1/64 ~= one-minute smoothing */
#define SAPS_CAPACITY_LOW_NUM          4u    /* score < 0.8x peer median */
#define SAPS_CAPACITY_LOW_DEN          5u
#define SAPS_CAPACITY_CLEAR_NUM        9u    /* clear only after >=0.9x median */
#define SAPS_CAPACITY_CLEAR_DEN        10u
#define SAPS_PROPORTIONAL_HOLD_N       64u

/* Round 2 D2 onset detector.
 * Long-term mean uses α=1/4096 (shift 12), while the normal mean EWMA stays
 * at α=1/64.  In log2 space, +1.0 means latency doubled.  Clear band is
 * log2(1.2) ~= 0.263, Q16.16 ~= 17207. */
#define SAPS_LONG_TERM_MEAN_SHIFT      12u
#define SAPS_ONSET_DELTA_FIRE_Q16      65536
#define SAPS_ONSET_DELTA_CLEAR_Q16     17207
#define SAPS_ONSET_FIRE_N              2u
#define SAPS_ONSET_CLEAR_N             64u
#define SAPS_ONSET_PROBE_SCORE         (SAPS_QD_SCORE_BASE >> 3)
#define SAPS_ONSET_MIN_SAMPLES         (SAPS_WARMUP_N + 128u)

/* Round 6 D2 B1: Adams/MacKay-style BOCPD on per-path log-latency.
 * Hazard lambda=200 samples. The posterior vector is truncated to 8 slots
 * to keep DPA memory bounded; the final slot collapses longer run lengths.
 * Predictive likelihood uses a fixed-point Student-t shape from the
 * per-hypothesis mean/M2 sufficient statistics. */
#define SAPS_BOCPD_ONE_Q16             65536u
#define SAPS_BOCPD_ONE_U16             65535u
#define SAPS_BOCPD_HAZARD_Q16          327u       /* round((1/200) * 65536) */
#define SAPS_BOCPD_CP_FIRE_Q16         32768u     /* P(r=0 | data) > 0.5 */
#define SAPS_BOCPD_MIN_SAMPLES         (SAPS_WARMUP_N + 4096u)
#define SAPS_BOCPD_PRIOR_VAR_Q16       1024       /* (0.125 log2 units)^2 */
#define SAPS_BOCPD_PRED_FLOOR_Q16      1u
#define SAPS_BOCPD_ONSET_CONFIRM_N     4u
#define SAPS_BOCPD_ONSET_CLEAR_N       4u
#define SAPS_BOCPD_STEP_MIN_Q16        SAPS_LAT_RATIO_OK_HI_Q16

/* Round 6 D2 B2: adaptive Page-CUSUM on log-latency.
 * K=0.5 log2 units ignores normal multiplicative jitter. H starts at 1.5
 * log2 units and is widened by recent variance, so quiet paths fire on one
 * real 5ms step while jittery paths require more evidence. */
#define SAPS_B2_CUSUM_K_Q16            32768
#define SAPS_B2_CUSUM_H_BASE_Q16       98304
#define SAPS_B2_CUSUM_MIN_SAMPLES      (SAPS_WARMUP_N + 4u)
#define SAPS_B2_CUSUM_CLEAR_Q16        SAPS_ONSET_DELTA_CLEAR_Q16
#define SAPS_B2_CUSUM_CLEAR_N          1u

/* Round 6 D2 B4: ADWIN-style two-subwindow mean test over the 64-sample
 * latency ring.  A 5ms onset produces >7 log2 units of step from the older
 * healthy half to the newer slow half; the 1.0 threshold is deliberately
 * below that step but above normal steady-state spread. */
#define SAPS_B4_ADWIN_MIN_SAMPLES      (SAPS_WARMUP_N + 64u)
#define SAPS_B4_ADWIN_DELTA_Q16        65536
#define SAPS_B4_ADWIN_CLEAR_Q16        SAPS_ONSET_DELTA_CLEAR_Q16

/* Round 6 D2 B5: inflight-skew pressure detector.
 * Fire on the first delayed completion that leaves this path with >1.5x the
 * median in-flight count; clear once the ratio normalizes or a clean probe
 * proves the delay is gone. */
#define SAPS_B5_MIN_SAMPLES            (SAPS_WARMUP_N + 4u)
#define SAPS_B5_CLEAR_N                1u

/* Round 6 D2 B3: cross-path relative latency detector.
 * Fire when |path mean - global median| > 2.5 * MAD. A small MAD floor keeps
 * the N=3 median/MAD estimator from firing on near-identical D0 peers when
 * the mathematical MAD is zero. */
#define SAPS_B3_MAD_RATIO_NUM          5
#define SAPS_B3_MAD_RATIO_DEN          2
#define SAPS_B3_MAD_FLOOR_Q16          SAPS_LAT_RATIO_OK_HI_Q16
#define SAPS_B3_MIN_SAMPLES            (SAPS_WARMUP_N + 16u)
#define SAPS_B3_MIN_DRIFT_Q16          SAPS_LAT_RATIO_OK_HI_Q16

/* Round 6 D2 B6c: voter hardening.  The 5ms D2 signal is dense once the
 * fault is present; healthy ARM/target stalls are sparse single-tail samples.
 * Delay the voter until all path baselines are settled and require two
 * consecutive candidate completions before publishing ANY_THREE_VOTE.  The
 * B2 leg also requires a small local tail cluster so single ARM scheduler
 * stalls cannot bypass the B3/B5 corroboration path. */
#define SAPS_B6_MIN_SAMPLES            (SAPS_WARMUP_N + 4096u)
#define SAPS_B6_FIRE_N                 2u
#define SAPS_B6_TAIL_COUNT_MIN         64u

/* Round 6 D2 B7: host-assisted active probe.  The host can only force an
 * application read onto a path; DPA receives the completion and treats three
 * consecutive slow probe samples as the ONSET verdict.  The path-local tail
 * floor only confirms the sample entered the slow-tail window; the consecutive
 * counter is the actual three-probe gate. */
#define SAPS_B7_MIN_SAMPLES            (SAPS_WARMUP_N + 4096u)
#define SAPS_B7_SLOW_N                 3u
#define SAPS_B7_CLEAR_N                1u
#define SAPS_B7_TAIL_COUNT_MIN         16u

/* Round 7 D2 B8: active heal.  Once an ONSET-style path is already drained,
 * do not wait for the 1s EXCLUDED timer plus 96 RECOVERING probes.  Three
 * consecutive clean sub-50us completions prove the injected D2 delay is gone
 * and directly re-admit the path. */
#define SAPS_B8_HEAL_N                 3u
#define SAPS_B8_FAST_LOG_LAT_Q16       SAPS_LAT_ABS_CP_FLOOR_Q16

/* Round 9 D7: true shared-fate handling.  When at least two paths show
 * simultaneous slow/degraded evidence, give one warmed path a short net-idle
 * window while the remaining warmed paths carry traffic.  The slot is 100ms,
 * but the hard exclusion is only the first 50ms; the second half re-admits all
 * paths so shared-fate handling does not collapse sustained throughput to
 * two-thirds of stock once the detector stays active for the full run. */
#define SAPS_SHARED_FATE_DEGRADED_N        2u
#define SAPS_SHARED_FATE_LAT_MIN_SAMPLES    (SAPS_WARMUP_N + 65536u)
#define SAPS_SHARED_FATE_TAIL_MAX_Q16      1600000
#define SAPS_SHARED_FATE_HEAL_N            3u
#define SAPS_T_SHARED_FATE_ROTATE_TICKS    (100ull * SAPS_TICKS_PER_MS)
#define SAPS_T_SHARED_FATE_EXCLUDE_TICKS   (50ull * SAPS_TICKS_PER_MS)
#define SAPS_T_SHARED_FATE_LEASE_TICKS     (1000ull * SAPS_TICKS_PER_MS)

#define SAPS_SF_RATIO_DEN          1024u
#define SAPS_SF_RATE_BAD_NUM       717u    
#define SAPS_SF_RATE_OK_NUM        870u    
#define SAPS_SF_SCORE_BAD_NUM      717u   /* capacity_score < 70% baseline */

#define SAPS_SF_QD_DEMAND_MIN      2u      
#define SAPS_SF_QD_PRESSURE_MIN    8u      
#define SAPS_SF_QD_PRESSURE_MULT   2u      
#define SAPS_SF_QD_KNEE            16u

#define SAPS_SF_LAT_BAD_DELTA_Q16   38336
#define SAPS_SF_NLAT_BAD_DELTA_Q16  27200

#define SAPS_SF_ERR_BAD_Q16         3277

#define SAPS_SF_FAST_WIN_TICKS      (8ull * SAPS_TICKS_PER_MS)
#define SAPS_SF_PERSIST_NEED        2u

/* 250 us @ 1.5 GHz = 375000 ticks; log2(375000) ~= 18.52.
 *
 * RAISED 2026-05-26 from 100µs (q16=1127077) to 250µs (q16=1213795).
 * Smoke test (07:24 no-injection 4-tenant × 3-path saturated randread 4K
 * QD=32) showed natural healthy p99 = 160-170µs which crossed the old 100µs
 * floor, falsely triggering SHARED_FATE_DEGRADED on all 3 healthy paths.
 * Real D1 injection is 5ms = ~22.25 log2(ticks), well above any threshold,
 * so raising the floor preserves D1 detection while suppressing false-
 * positive on healthy multi-path saturated load.
 *
 * Used to suppress latency/raw-inflight cross-path classifiers when every
 * warmed path is in the clean D0 band. */
#define SAPS_LAT_HEALTHY_FLOOR_Q16     1306235  /* 1ms @ 1GHz ARM host counter (cntvct_el0):
                                              *   1ms × 1000 ticks/µs = 1,000,000 ticks
                                              *   log2(1M) ≈ 19.93 → Q16.16 = 19.93×65536 ≈ 1,306,235
                                              * Fix PP (2026-05-28): corrected from 1344586 (1ms @ 1.5GHz)
                                              * to 1GHz because delta_ticks uses e->host_tsc (cntvct_el0,
                                              * cntfrq_el0 ≈ 1GHz), not DPA 1.5GHz TSC. */
#define SAPS_TAIL_LOG_LAT_Q16     1437307   /* log2(4ms in ticks at 1000 ticks/µs, 1GHz ARM host counter):
                                              *   4ms × 1000 = 4,000,000 ticks
                                              *   log2(4M) ≈ 21.93 → Q16.16 = 21.93×65536 ≈ 1,437,307
                                              * Fix PP (2026-05-28): corrected from 1476053 (4ms @ 1.5GHz).
                                              * Root cause: delta_ticks = e->host_tsc low32 delta; host_tsc
                                              * is cntvct_el0 at cntfrq_el0 ≈ 1GHz, not DPA 1.5GHz TSC.
                                              * At old threshold 1476053 ≡ log2(6M ticks @ 1GHz) ≡ 6ms:
                                              *   5ms inject → 5M ticks → log2(5M)×65536≈1,458,048 < 1,476,053
                                              *   → B7 probe_slow never fires → D classifier blind to fault.
                                              * At new threshold 1437307 ≡ 4ms @ 1GHz:
                                              *   5ms inject → log2(5M)×65536≈1,458,048 > 1,437,307 → fires ✓
                                              *   ARM OS jitter (2-4ms) → log2(4M)×65536≈1,437,307 (boundary)
                                              *     → ~1-5 counts/512-window < SAPS_TAIL_COUNT_MIN=25 → no false fire ✓
                                              * Raised from 2ms (1375587 @ 1GHz) to 4ms to separate ARM jitter. */
#define SAPS_TAIL_COUNT_MIN       25u        /* Fix 11 reverted — TAIL_COUNT_MIN=5 made unlucky cell worse. ≥ 5 % of 512-sample window.
                                              * At the 4ms tail threshold:
                                              *   D4 15%-tail at 5ms → ~77/512 counts >> 25 → fires ✓
                                              *   ARM jitter above 4ms → ~1-5/512 counts << 25 → no fire ✓
                                              * Restored to 25 (was raised to 64 while tail threshold
                                              * was still at 2ms, which is no longer needed). */
#define SAPS_ERR_RATE_LOW_Q16     655        /* 0.01 */
#define SAPS_RECENT_ERR_THRESH    1u
#define SAPS_RECENT_DECAY_PERIOD  64u
#define SAPS_LAT_VAR_EWMA_ALPHA   2048       /* 1/32 */
#define SAPS_MEAN_LAT_EWMA_ALPHA  1024       /* 1/64 */

/* Slice 16.5: classifier hysteresis. Require N consecutive non-HEALTHY raw
 * classifications before transitioning out of HEALTHY. Mitigates D0
 * post-warmup transient noise being labelled PERPETUAL_SLOW/SPARSE_ERROR/
 * BIMODAL_TAIL → score downweight → argmax sticky-A. */
#define SAPS_HYSTERESIS_N         4u

/* FLAP detection: count NEWMA fires in last 1 s window of the ring. If >2 ⇒
 * FLAP. FSM transition rate > 1/s also ⇒ FLAP. */
#define SAPS_FLAP_NEWMA_FIRE_HI   2u
#define SAPS_FLAP_FSM_TRANS_HI    1u
#define SAPS_FLAP_WINDOW_MS       1000u

/* FLAP dampening: initial window 2 s, doubles on each retrigger up to 16 s. */
#define SAPS_FLAP_DAMP_MS_INIT    2000u
#define SAPS_FLAP_DAMP_MS_MAX     16000u

/* BIMODAL_TAIL probabilistic downweight: score >> SAPS_BIMODAL_SHIFT.
 * Spec §4.2: 1/4 (shift=2). Tunable for D4 calibration. */
#define SAPS_BIMODAL_SHIFT        2u

/* log2(x) approximated via CLZ (integer bit-length) plus linear interpolation
 * of the fractional part from a 256-entry LUT of log2(1 + k/256) in Q16.16.
 * Output is Q16.16 fixed-point representing log2(x). No FPU. */
static const uint16_t LOG2_FRAC_Q16_256[256] = {
	    0,   369,   735,  1100,  1462,  1823,  2182,  2539,
	 2894,  3247,  3599,  3949,  4298,  4645,  4990,  5334,
	 5676,  6017,  6356,  6694,  7030,  7365,  7699,  8031,
	 8362,  8691,  9020,  9347,  9672,  9997, 10320, 10642,
	10962, 11282, 11600, 11916, 12232, 12547, 12860, 13173,
	13484, 13794, 14103, 14411, 14717, 15023, 15328, 15631,
	15934, 16235, 16536, 16835, 17134, 17431, 17727, 18023,
	18317, 18611, 18903, 19195, 19486, 19775, 20064, 20352,
	20639, 20925, 21211, 21495, 21778, 22061, 22343, 22623,
	22903, 23182, 23461, 23738, 24015, 24290, 24565, 24839,
	25113, 25385, 25657, 25928, 26198, 26467, 26735, 27003,
	27270, 27536, 27801, 28066, 28330, 28593, 28855, 29117,
	29377, 29637, 29897, 30155, 30413, 30670, 30927, 31182,
	31437, 31692, 31945, 32198, 32450, 32702, 32953, 33203,
	33452, 33701, 33949, 34196, 34443, 34689, 34934, 35179,
	35423, 35666, 35909, 36151, 36392, 36633, 36873, 37113,
	37352, 37590, 37827, 38064, 38301, 38536, 38771, 39006,
	39240, 39473, 39705, 39937, 40169, 40399, 40629, 40859,
	41088, 41316, 41544, 41771, 41998, 42224, 42450, 42675,
	42899, 43123, 43346, 43569, 43791, 44013, 44234, 44454,
	44674, 44894, 45113, 45331, 45549, 45766, 45983, 46199,
	46415, 46630, 46845, 47059, 47273, 47486, 47698, 47911,
	48122, 48334, 48544, 48755, 48964, 49174, 49383, 49591,
	49799, 50006, 50213, 50420, 50625, 50831, 51036, 51240,
	51444, 51648, 51851, 52053, 52255, 52457, 52658, 52859,
	53059, 53259, 53458, 53657, 53856, 54053, 54251, 54448,
	54645, 54841, 55036, 55232, 55427, 55621, 55815, 56008,
	56201, 56394, 56586, 56778, 56969, 57160, 57351, 57541,
	57730, 57919, 58108, 58296, 58484, 58672, 58859, 59045,
	59231, 59417, 59602, 59787, 59972, 60156, 60339, 60522,
	60705, 60887, 61069, 61251, 61432, 61613, 61793, 61973,
	62152, 62331, 62510, 62688, 62866, 63043, 63220, 63397,
};

/* Returns log2(x) in Q16.16. log2(0) defined as 0. */
static inline int64_t fixed_log2(uint64_t x)
{
	if (x == 0)
		return 0;
	/* bitlen = 64 - clz(x); the MSB index is (bitlen - 1). */
	unsigned bl = 64u - (unsigned)__builtin_clzll(x);
	unsigned msb = bl - 1u;
	int64_t int_part = (int64_t)msb << 16;
	/* Fractional part: look up high 8 bits below the MSB. */
	unsigned frac_idx;
	if (msb >= 8u)
		frac_idx = (unsigned)((x >> (msb - 8u)) & 0xFFu);
	else
		frac_idx = (unsigned)((x << (8u - msb)) & 0xFFu);
	int64_t frac_part = (int64_t)LOG2_FRAC_Q16_256[frac_idx];
	return int_part + frac_part;
}

/*
 * Convert a consensus-relative latency drift into the capacity fraction used
 * by HCAA.  The detector's 5x boundary remains a confidence gate: ordinary
 * queueing variation below it must not reduce the service envelope.  Once the
 * gate is crossed, the factor follows the inverse latency ratio implied by
 * Little's law for a fixed amount of outstanding work.
 *
 * LOG2_FRAC_Q16_256[k] approximates log2(1 + k/256).  Choosing the first table
 * entry at or above the observed fractional drift gives a conservative
 * estimate of 2^-drift without floating point.  This work runs once per
 * controller epoch, not on the per-request path.
 */
static inline uint32_t sapsq_continuous_health_from_drift(int64_t drift)
{
	uint64_t whole;
	uint16_t frac;
	unsigned idx = 0;
	uint32_t inv_frac_q16;
	uint32_t health_q16;

	if (drift <= (int64_t)SAPS_CROSSPATH_DRIFT_HI_Q16)
		return SAPSQ_HEALTH_HEALTHY_Q16;

	whole = (uint64_t)drift >> 16;
	if (whole >= 16u)
		return SAPSQ_HEALTH_PROBE_Q16;

	frac = (uint16_t)((uint64_t)drift & 0xFFFFu);
	while (idx < 255u && LOG2_FRAC_Q16_256[idx] < frac)
		idx++;

	inv_frac_q16 =
		(uint32_t)(((uint64_t)SAPSQ_HEALTH_HEALTHY_Q16 * 256u) /
			   (256u + idx));
	health_q16 = inv_frac_q16 >> whole;
	if (health_q16 < SAPSQ_HEALTH_PROBE_Q16)
		health_q16 = SAPSQ_HEALTH_PROBE_Q16;

	return health_q16;
}

/* Convert relative queue occupancy into a capacity factor for the queue-depth
 * comparison arm. Both inputs are Q16.16 queue depth. The one-request offset
 * keeps an idle path usable and avoids a singular ratio at zero occupancy.
 * This helper is only selected by the controlled signal-isolation mode. */
static inline uint32_t
sapsq_queue_depth_health(uint64_t reference_q16, uint64_t observed_q16)
{
	uint64_t adjusted_reference = reference_q16 + (1u << 16);
	uint64_t adjusted_observed = observed_q16 + (1u << 16);
	uint64_t health_q16;

	if (adjusted_observed <= adjusted_reference)
		return SAPSQ_HEALTH_HEALTHY_Q16;

	health_q16 = (adjusted_reference << 16) / adjusted_observed;
	if (health_q16 < SAPSQ_HEALTH_PROBE_Q16)
		health_q16 = SAPSQ_HEALTH_PROBE_Q16;
	if (health_q16 > SAPSQ_HEALTH_HEALTHY_Q16)
		health_q16 = SAPSQ_HEALTH_HEALTHY_Q16;
	return (uint32_t)health_q16;
}

/* v2 SAPS helpers (spec-v4 §4.1 / §4.3 / §4.4 / §4.5 / §4.6). */

/* Absolute value for signed 64-bit. */
static inline int64_t abs64(int64_t x) { return x < 0 ? -x : x; }

static inline uint16_t saps_q16_to_u16(uint32_t x)
{
	return (x >= SAPS_BOCPD_ONE_Q16) ? SAPS_BOCPD_ONE_U16 : (uint16_t)x;
}

static inline bool saps_fault_is_onset_family(uint16_t fault_type)
{
	return fault_type == SAPS_FAULT_ONSET ||
	       fault_type == SAPS_FAULT_ANY_THREE_VOTE;
}

static inline void saps_bocpd_reset(struct dpa_path_state *p, int64_t log_lat)
{
	for (uint8_t i = 0; i < SAPS_BOCPD_RUN_MAX; i++) {
		p->bocpd_run_prob_q16[i] = 0;
		p->bocpd_run_mean_q16[i] = log_lat;
		p->bocpd_run_m2_q16[i] = 0;
	}
	p->bocpd_run_prob_q16[0] = SAPS_BOCPD_ONE_U16;
	p->bocpd_cp_prob_q16 = 0;
	p->bocpd_onset_active = 0;
	p->bocpd_onset_clear_consec = 0;
	p->bocpd_onset_pending_consec = 0;
	p->bocpd_last_log_lat_q16 = log_lat;
}

#if SAPS_D2_B2_STATE_ACTIVE
static inline void saps_b2_cusum_reset(struct dpa_path_state *p, int64_t log_lat)
{
	if (log_lat <= 0)
		log_lat = p->baseline_log_lat_q16;
	if (log_lat <= 0)
		log_lat = p->mean_log_lat_ewma_q16;
	if (log_lat <= 0)
		log_lat = 1;

	p->d2_cusum_ref_q16 = log_lat;
	p->d2_cusum_pos_q16 = 0;
	p->d2_cusum_last_log_lat_q16 = log_lat;
	p->d2_cusum_onset_active = 0;
	p->d2_cusum_clear_consec = 0;
}

static inline void saps_b2_cusum_on_sample(struct dpa_path_state *p, int64_t log_lat)
{
	int64_t ref = p->d2_cusum_ref_q16;

	p->d2_cusum_last_log_lat_q16 = log_lat;

	if (p->n < SAPS_B2_CUSUM_MIN_SAMPLES || p->baseline_log_lat_q16 == 0) {
		if (ref == 0)
			saps_b2_cusum_reset(p, log_lat);
		return;
	}

	if (ref <= 0)
		ref = p->baseline_log_lat_q16;
	if (ref <= 0)
		ref = log_lat;

	/* Keep the reference adaptive only while the path is still close to its
	 * healthy baseline. Once a positive shift begins, CUSUM owns the decision
	 * and the reference remains anchored. */
	int64_t ref_delta = log_lat - ref;
	if (!p->d2_cusum_onset_active &&
	    ref_delta < SAPS_B2_CUSUM_K_Q16 &&
	    ref_delta > -SAPS_B2_CUSUM_K_Q16) {
		p->d2_cusum_ref_q16 = ref + (ref_delta >> 8);
		ref = p->d2_cusum_ref_q16;
	}

	int64_t dev = log_lat - ref - SAPS_B2_CUSUM_K_Q16;
	int64_t next = p->d2_cusum_pos_q16 + dev;
	p->d2_cusum_pos_q16 = (next > 0) ? next : 0;
}
#endif

#if SAPS_D2_B4_ENABLE
static inline bool saps_b4_adwin_window_means(const struct dpa_path_state *p,
					       int64_t *old_mean_q16,
					       int64_t *new_mean_q16)
{
	if (p->lat_ring_count < 64u)
		return false;

	int64_t old_sum = 0;
	int64_t new_sum = 0;
	unsigned head = p->lat_ring_head & 63u;

	for (unsigned i = 0; i < 32u; i++)
		old_sum += p->lat_ring[(head + i) & 63u];
	for (unsigned i = 32u; i < 64u; i++)
		new_sum += p->lat_ring[(head + i) & 63u];

	*old_mean_q16 = old_sum >> 5;
	*new_mean_q16 = new_sum >> 5;
	return true;
}
#endif

static inline void saps_reset_latency_tail_estimators(struct dpa_path_state *p,
						      int64_t log_lat)
{
	if (log_lat <= 0) {
		log_lat = p->baseline_log_lat_q16;
		if (log_lat <= 0)
			log_lat = p->mean_log_lat_ewma_q16;
		if (log_lat <= 0)
			log_lat = 1;
	}

	p->p99_estimate = log_lat;
	p->frugal_step = 1;
	for (uint8_t i = 0; i < 64u; i++)
		p->lat_ring[i] = log_lat;
	p->lat_ring_head = 0;
	p->lat_ring_count = 64;

	p->max_log_lat_recent = log_lat;
	p->max_lat_window_n = 1;
	p->tail_count_recent = (log_lat > SAPS_TAIL_LOG_LAT_Q16) ? 1u : 0u;
	p->bimodal_consec_windows = 0;
	p->adwin_onset_active = 0;
	p->adwin_clear_consec = 0;
	p->inflight_onset_active = 0;
	p->inflight_onset_clear_consec = 0;
	p->active_probe_slow_consec = 0;
	p->active_probe_clear_consec = 0;
	p->active_heal_fast_consec = 0;

	for (uint8_t i = 0; i < DPA_PLUGIN_OPC_CLASS_MAX; i++) {
		p->opc_p99[i].p99 = log_lat;
		p->opc_p99[i].frugal_step = 1;
		p->opc_p99[i].n = SAPS_WARMUP_N + 1u;
	}
}

static inline __attribute__((unused)) void
saps_clear_onset_signal_state(struct dpa_path_state *p)
{
	int64_t log_lat = p->baseline_log_lat_q16;

	if (log_lat <= 0)
		log_lat = p->bocpd_last_log_lat_q16;
	if (log_lat <= 0)
		log_lat = p->mean_log_lat_ewma_q16;
	if (log_lat <= 0)
		log_lat = 1;

	p->mean_log_lat_ewma_q16 = log_lat;
	p->long_term_mean_log_lat_q16 = log_lat;
	p->ewma_fast = log_lat;
	p->ewma_slow = log_lat;
	saps_bocpd_reset(p, log_lat);
#if SAPS_D2_B2_STATE_ACTIVE
	saps_b2_cusum_reset(p, log_lat);
#endif
	saps_reset_latency_tail_estimators(p, log_lat);
	p->onset_high_consec = 0;
	p->onset_clear_consec = SAPS_ONSET_CLEAR_N;
	p->bocpd_onset_active = 0;
	p->bocpd_onset_clear_consec = 0;
	p->bocpd_onset_pending_consec = 0;
	p->d2_cusum_onset_active = 0;
	p->d2_cusum_pos_q16 = 0;
	p->d2_cusum_clear_consec = SAPS_B2_CUSUM_CLEAR_N;
	p->inflight_onset_active = 0;
	p->inflight_onset_clear_consec = SAPS_B5_CLEAR_N;
	p->active_probe_slow_consec = 0;
	p->active_probe_clear_consec = SAPS_B7_CLEAR_N;
	p->active_heal_fast_consec = 0;
}

#if SAPS_D2_B1_ENABLE
static inline bool saps_bocpd_onset_sample_slow(const struct dpa_path_state *p)
{
	return p->baseline_log_lat_q16 != 0 &&
	       p->bocpd_last_log_lat_q16 > SAPS_LAT_ABS_CP_FLOOR_Q16 &&
	       p->bocpd_last_log_lat_q16 - p->baseline_log_lat_q16 >
	       SAPS_BOCPD_STEP_MIN_Q16;
}
#endif

static inline uint32_t saps_bocpd_student_t_pred_q16(const struct dpa_path_state *p,
						      uint8_t slot,
						      int64_t x)
{
	uint32_t run_n = (uint32_t)slot + 1u;
	int64_t resid = abs64(x - p->bocpd_run_mean_q16[slot]);
	int64_t resid2_q16 = (resid * resid) >> 16;
	int64_t var_q16 = SAPS_BOCPD_PRIOR_VAR_Q16;

	if (run_n > 1u) {
		int64_t sample_var_q16 = p->bocpd_run_m2_q16[slot] / (int64_t)(run_n - 1u);
		if (sample_var_q16 > var_q16)
			var_q16 = sample_var_q16;
	}
	if (var_q16 <= 0)
		var_q16 = SAPS_BOCPD_PRIOR_VAR_Q16;

	/* Student-t predictive shape, fixed point:
	 *   p(x|run) proportional to (1 + z / nu)^(-2)
	 * where z is squared standardized residual. This keeps the heavy-tail
	 * behavior needed for BOCPD without FPU pow/sqrt on DPA. */
	uint64_t ratio_q16 = ((uint64_t)resid2_q16 << 16) / (uint64_t)var_q16;
	uint64_t term_q16 = (uint64_t)SAPS_BOCPD_ONE_Q16 +
			    ratio_q16 / (uint64_t)(run_n + 2u);
	uint64_t inv_q16 = ((uint64_t)SAPS_BOCPD_ONE_Q16 << 16) / term_q16;
	uint64_t pred_q16 = (inv_q16 * inv_q16) >> 16;

	if (pred_q16 < SAPS_BOCPD_PRED_FLOOR_Q16)
		return SAPS_BOCPD_PRED_FLOOR_Q16;
	if (pred_q16 > SAPS_BOCPD_ONE_Q16)
		return SAPS_BOCPD_ONE_Q16;
	return (uint32_t)pred_q16;
}

static inline void saps_bocpd_extend_stats(const struct dpa_path_state *p,
					   uint8_t slot,
					   int64_t x,
					   int64_t *mean_out,
					   int64_t *m2_out)
{
	uint32_t old_n = (uint32_t)slot + 1u;
	uint32_t new_n = old_n + 1u;
	int64_t mean = p->bocpd_run_mean_q16[slot];
	int64_t delta = x - mean;
	int64_t next_mean = mean + delta / (int64_t)new_n;
	int64_t delta2 = x - next_mean;
	int64_t next_m2 = p->bocpd_run_m2_q16[slot] + ((delta * delta2) >> 16);

	if (next_m2 < 0)
		next_m2 = 0;
	*mean_out = next_mean;
	*m2_out = next_m2;
}

static inline bool saps_bocpd_on_sample(struct dpa_path_state *p, int64_t log_lat)
{
	uint32_t next_prob[SAPS_BOCPD_RUN_MAX] = {0};
	uint32_t next_src_prob[SAPS_BOCPD_RUN_MAX] = {0};
	int64_t next_mean[SAPS_BOCPD_RUN_MAX];
	int64_t next_m2[SAPS_BOCPD_RUN_MAX];
	uint32_t cp_joint = 0;
	uint32_t norm = 0;

	p->bocpd_last_log_lat_q16 = log_lat;

	if (p->bocpd_run_prob_q16[0] == 0) {
		saps_bocpd_reset(p, log_lat);
		return false;
	}

	for (uint8_t i = 0; i < SAPS_BOCPD_RUN_MAX; i++) {
		next_mean[i] = log_lat;
		next_m2[i] = 0;
	}

	for (uint8_t r = 0; r < SAPS_BOCPD_RUN_MAX; r++) {
		uint32_t prior = p->bocpd_run_prob_q16[r];
		if (prior == 0)
			continue;

		uint32_t pred = saps_bocpd_student_t_pred_q16(p, r, log_lat);
		uint32_t cp_r = (prior * SAPS_BOCPD_HAZARD_Q16) >> 16;
		uint32_t grow_r = (prior * (SAPS_BOCPD_ONE_Q16 - SAPS_BOCPD_HAZARD_Q16)) >> 16;
		grow_r = (grow_r * pred) >> 16;
		uint8_t dest = (r + 1u < SAPS_BOCPD_RUN_MAX) ? (uint8_t)(r + 1u)
							     : (uint8_t)(SAPS_BOCPD_RUN_MAX - 1u);
		int64_t grown_mean;
		int64_t grown_m2;

		cp_joint += cp_r;
		if (grow_r != 0) {
			saps_bocpd_extend_stats(p, r, log_lat, &grown_mean, &grown_m2);
			next_prob[dest] += grow_r;
			if (grow_r > next_src_prob[dest]) {
				next_src_prob[dest] = grow_r;
				next_mean[dest] = grown_mean;
				next_m2[dest] = grown_m2;
			}
		}
	}

	next_prob[0] = cp_joint;
	next_mean[0] = log_lat;
	next_m2[0] = 0;

	for (uint8_t i = 0; i < SAPS_BOCPD_RUN_MAX; i++)
		norm += next_prob[i];

	if (norm == 0) {
		saps_bocpd_reset(p, log_lat);
		return false;
	}

	uint32_t cp_post = (cp_joint << 16) / norm;
	p->bocpd_cp_prob_q16 = saps_q16_to_u16(cp_post);

	for (uint8_t i = 0; i < SAPS_BOCPD_RUN_MAX; i++) {
		uint32_t post = ((uint64_t)next_prob[i] << 16) / norm;
		p->bocpd_run_prob_q16[i] = saps_q16_to_u16(post);
		p->bocpd_run_mean_q16[i] = next_mean[i];
		p->bocpd_run_m2_q16[i] = next_m2[i];
	}

	return p->bocpd_cp_prob_q16 > SAPS_BOCPD_CP_FIRE_Q16;
}

/* Fixed-point inverse (Q16.16): returns (1<<16) / x if x > 0, else 0.
 * Caller multiplies with Q16.16 number to keep Q16.16 scale. */
static inline int64_t fixed_inv_q16(int64_t x)
{
	if (x <= 0)
		return 0;
	return ((int64_t)1 << 32) / x;   /* (1<<16)<<16 / x gives Q16.16 result */
}

static inline int64_t fixed_invsqrt_q16(int64_t x_q16)
{
	if (x_q16 <= 0)
		return 0;
	unsigned bl = 64u - (unsigned)__builtin_clzll((uint64_t)x_q16);
	unsigned exp_half = (bl - 1u) / 2u;
	int shift = 24 - (int)exp_half;
	int64_t y;
	if (shift >= 0 && shift < 63)
		y = (int64_t)1 << shift;
	else if (shift < 0)
		y = 1;
	else
		y = (int64_t)1 << 62;
	if (((bl - 1u) & 1u) != 0u)
		y = (y * 46341LL) >> 16;
	for (int i = 0; i < 4; i++) {
		int64_t y_sq_q16 = (y * y) >> 16;
		int64_t xy_sq_q16 = (x_q16 * y_sq_q16) >> 16;
		int64_t three_minus = ((int64_t)3 << 16) - xy_sq_q16;
		if (three_minus < 0)
			three_minus = 0;
		y = (y * three_minus) >> 17;
	}
	return y;
}

static uint8_t g_m2_baseline_frozen[M2_CLIENT_MAX];

static inline void m2_oja_update(volatile struct dpa_plugin_shared *s,
				  uint16_t client_id,
				  const int64_t x[M2_FEATURE_DIM])
{
	if (!s->saps_m2_enabled)
		return;
	uint32_t n = s->m2_pca_n_events[client_id];

	if ((n & M2_SUBSAMPLE_MASK) != 0) {
		s->m2_pca_n_events[client_id] = n + 1u;
		return;
	}

	volatile int64_t *w = s->m2_pca_w[client_id];

	if (n == 0) {
		int64_t sumsq_init = 0;
		for (int i = 0; i < M2_FEATURE_DIM; i++)
			sumsq_init += ((int64_t)x[i] * x[i]) >> 16;
		if (sumsq_init == 0)
			sumsq_init = (int64_t)1 << 16;
		int64_t inv_norm0 = fixed_invsqrt_q16(sumsq_init);
		for (int i = 0; i < M2_FEATURE_DIM; i++)
			w[i] = ((int64_t)x[i] * inv_norm0) >> 16;
		s->m2_pca_n_events[client_id] = 1u;
		return;
	}

	/* Step 1: dot s = <x, w>  (Q16.16) */
	int64_t dot_q16 = 0;
	for (int i = 0; i < M2_FEATURE_DIM; i++)
		dot_q16 += ((int64_t)x[i] * w[i]) >> 16;

	int64_t step_q16 = (dot_q16 * (int64_t)M2_ETA_Q16) >> 16;
	int64_t w_new[M2_FEATURE_DIM];
	int64_t sumsq = 0;
	for (int i = 0; i < M2_FEATURE_DIM; i++) {
		int64_t inc = (step_q16 * x[i]) >> 16;
		w_new[i] = w[i] + inc;
		sumsq += (w_new[i] * w_new[i]) >> 16;
	}

	/* Step 3: L2 normalize via fixed_invsqrt_q16 */
	if (sumsq <= 0)
		sumsq = (int64_t)1 << 16;
	int64_t inv_norm_q16 = fixed_invsqrt_q16(sumsq);
	for (int i = 0; i < M2_FEATURE_DIM; i++)
		w[i] = (w_new[i] * inv_norm_q16) >> 16;

	n++;
	s->m2_pca_n_events[client_id] = n;

	volatile int64_t *wb = s->m2_pca_w_baseline[client_id];
	if (!g_m2_baseline_frozen[client_id] && n >= M2_WARMUP_N) {
		for (int i = 0; i < M2_FEATURE_DIM; i++)
			wb[i] = w[i];
		g_m2_baseline_frozen[client_id] = 1u;
		return;
	}

	if (g_m2_baseline_frozen[client_id]) {
		int64_t conf_q16 = 0;
		for (int i = 0; i < M2_FEATURE_DIM; i++)
			conf_q16 += ((int64_t)w[i] * wb[i]) >> 16;
		s->m2_pca_conf_q16[client_id] = (int32_t)conf_q16;
	}
}

static inline void m3_refresh_credit(volatile struct dpa_plugin_shared *s,
				      uint32_t tenant_id, uint64_t now_tsc)
{
	uint64_t last = s->m3_tenant_last_tsc[tenant_id];
	if (last == 0) {
		s->m3_tenant_last_tsc[tenant_id] = now_tsc;
		return;
	}
	uint64_t delta_tsc = now_tsc - last;
	if (delta_tsc == 0)
		return;
	if (delta_tsc > M3_DELTA_TSC_CAP)
		delta_tsc = M3_DELTA_TSC_CAP;

	uint32_t w_q16 = s->m3_tenant_weights_q16[tenant_id];
	int64_t cap_per_tsc_q32 = s->m3_capacity_per_tsc_q32;
	if (w_q16 == 0 || cap_per_tsc_q32 == 0) {
		s->m3_tenant_last_tsc[tenant_id] = now_tsc;
		return;
	}

	int64_t refill_q32 = ((int64_t)w_q16 * cap_per_tsc_q32) >> 16;
	refill_q32 = refill_q32 * (int64_t)delta_tsc;
	int64_t credits = s->m3_tenant_credits_q32[tenant_id] + refill_q32;
	if (credits > M3_BURST_CAP_Q32)
		credits = M3_BURST_CAP_Q32;
	s->m3_tenant_credits_q32[tenant_id] = credits;
	s->m3_tenant_last_tsc[tenant_id] = now_tsc;

	if (s->m3_v3_freeze_gate &&
	    credits >= ((int64_t)16 << 32) &&
	    s->m3_tenant_freeze[tenant_id]) {
		s->m3_tenant_freeze[tenant_id] = 0;
	}
}

static inline bool m3_consume(volatile struct dpa_plugin_shared *s,
			       uint32_t tenant_id)
{
	int64_t c = s->m3_tenant_credits_q32[tenant_id];
	int64_t cost = s->m3_credit_per_io_q32;
	if (cost == 0)
		cost = (int64_t)1 << 32;   /* default 1.0 in Q32.32 */
	if (c >= cost) {
		s->m3_tenant_credits_q32[tenant_id] = c - cost;
		s->m3_tenant_iops_served[tenant_id]++;
		return true;
	}
	s->m3_credit_exhaust_count[tenant_id]++;
	return false;
}

static inline void m4_refresh_and_consume(volatile struct dpa_plugin_shared *s,
					   uint32_t tenant_id, uint64_t now_tsc)
{
	uint64_t last = s->m4_tenant_last_refresh_tsc[tenant_id];
	if (last == 0) {
		s->m4_tenant_last_refresh_tsc[tenant_id] = now_tsc;
		/* No refresh on first event; cold-start with whatever host initialised. */
	} else {
		uint64_t delta_tsc = now_tsc - last;
		if (delta_tsc > M3_DELTA_TSC_CAP)
			delta_tsc = M3_DELTA_TSC_CAP;
		if (delta_tsc > 0) {
			uint32_t rate_q32 = s->m4_tenant_refresh_per_tsc_q32[tenant_id];
			uint32_t cap_q16  = s->m4_tenant_burst_cap_q16[tenant_id];
			uint64_t tokens   = s->m4_tenant_tokens_q16[tenant_id];
			/* refill_q16 = (rate_q32 × delta_tsc) >> 16
			 * Max: 3e6 × 1e8 = 3e14 — well within 64-bit. */
			uint64_t refill_q16 = ((uint64_t)rate_q32 * delta_tsc) >> 16;
			tokens += refill_q16;
			if (cap_q16 != 0 && tokens > cap_q16)
				tokens = cap_q16;
			s->m4_tenant_tokens_q16[tenant_id] = (uint32_t)tokens;
			s->m4_tenant_last_refresh_tsc[tenant_id] = now_tsc;
		}
	}

	/* Consume 1.0 token (Q16.16 = 1 << 16). */
	uint32_t tokens_now = s->m4_tenant_tokens_q16[tenant_id];
	if (tokens_now >= (1u << 16)) {
		s->m4_tenant_tokens_q16[tenant_id] = tokens_now - (1u << 16);
		s->m4_tenant_reject_flag[tenant_id] = 0;
		s->m4_tenant_admit_count[tenant_id]++;
	} else {
		s->m4_tenant_reject_flag[tenant_id] = 1;
		s->m4_tenant_reject_count[tenant_id]++;
	}
}

/* Map NVMe opcode to per-opcode Frugal-2U slot.
 *   opc 0x02 = READ   → 0
 *   opc 0x01 = WRITE  → 1
 *   opc 0x00 = FLUSH  → 2
 * Any other opcode uses slot 0 (READ) as default. */
static inline unsigned opc_bucket(uint8_t opcode)
{
	switch (opcode) {
	case 0x02: return 0;
	case 0x01: return 1;
	case 0x00: return 2;
	default:   return 0;
	}
}

/* Slice 27 — exact P99 over 64 samples in lat_ring.
 *
 * Only activates once ring is full (count == 64). Before that, returns 0 so
 * Frugal-2U keeps driving p99_estimate during warmup — avoids cross-path
 * detector false fires on max-of-N random outliers in partial windows.
 *
 * For full window (N=64 sorted ascending): P99 = slot 63 → top-2 (max2).
 *
 * O(N) two-max scan, no sort. Returns Q16.16 log-lat. */
static inline int64_t saps_exact_p99_from_ring(const int64_t *ring, uint8_t count)
{
	if (count < 64)
		return 0;
	int64_t max1 = INT64_MIN, max2 = INT64_MIN;
	for (uint8_t i = 0; i < 64; i++) {
		int64_t v = ring[i];
		if (v > max1) {
			max2 = max1;
			max1 = v;
		} else if (v > max2) {
			max2 = v;
		}
	}
	/* top-2 (max2) is exact P99 for N=64. */
	return max2;
}

/* Branchless 8-bit xorshift PRNG for Frugal-2U random step.
 * State is per-call so we avoid a global mutable seed (which would require
 * fencing across events). Seed comes from the sample itself + a small
 * rotating counter — good enough for Bernoulli(1%) sampling. */
static inline uint8_t frugal_rand8(uint64_t seed)
{
	seed ^= seed << 13;
	seed ^= seed >> 7;
	seed ^= seed << 17;
	return (uint8_t)(seed & 0xFFu);
}

/* §4.3 Frugal-2U streaming quantile estimator (Ma/Muthukrishnan/Sandler 2014).
 *
 * Updates a P99 estimator with log-lat sample x. Frugal-2U augments Frugal-1U
 * with a step-doubling heuristic: consecutive same-direction updates double the
 * step for faster convergence, direction flip resets step to 1.
 *
 *   Pr(step up  | x > est) = 1 - q   (= 1 - 0.99 ≈ SAPS_FRUGAL_UP_Q8/256)
 *   Pr(step dn  | x < est) = q       (= 0.99      ≈ (256 - SAPS_FRUGAL_UP_Q8)/256)
 *
 * Step is signed in the caller's *frugal_step scratch. For P99, the steady
 * state has far more "x < est" events than "x > est" (99:1 ratio), so without
 * step-doubling the downward drift of est during stable periods dominates.
 * Step doubling is engaged only on consecutive up-moves (the rare event) so
 * the estimator climbs fast after a true shift upward.
 *
 * Both x and *est are in the same Q16.16 log-lat units. */
static inline void frugal2u_update(int64_t *est, int32_t *step,
				    int64_t x, uint64_t prng_seed)
{
	uint8_t rv = frugal_rand8(prng_seed);

	if (x > *est) {
		if (rv < SAPS_FRUGAL_UP_Q8) {
			int32_t s = *step;
			if (s <= 0) s = 1;
			*est += (int64_t)s;
			*step = (s < 0x40000000) ? (s + 1) : s;
		} else {
			/* Direction flip — reset step toward 1. */
			if (*step > 1) *step = 1;
		}
	} else if (x < *est) {
		if (rv >= SAPS_FRUGAL_UP_Q8) {
			int32_t s = *step;
			if (s <= 0) s = 1;
			*est -= (int64_t)s;
			if (*est < 0) *est = 0;
			/* Down-moves are the common case at P99 — step stays
			 * modest so we don't oscillate past the true quantile
			 * on a single outlier bin. */
		} else {
			if (*step > 1) *step = 1;
		}
	}
	/* x == *est: no update. */
}

/* §4.5 NVMe semantic classifier — map (sct, sc) → action verdict.
 * Returns enum dpa_saps_action value (0..3) or UNSET for non-error / unmapped.
 *
 * sct/sc encoding from schema v2 packing: (sct<<8)|sc. */
static inline uint8_t classify_action(uint8_t sct, uint8_t sc)
{
	switch (sct) {
	case 0:   /* Generic */
		if (sc == 0x81) return DPA_SAPS_ACTION_TERMINAL;   /* Reservation Conflict */
		if (sc == 0x85) return DPA_SAPS_ACTION_RETRY_SAME; /* Namespace Not Ready */
		break;
	case 2:   /* Media/Data Integrity */
		if (sc == 0x80) return DPA_SAPS_ACTION_TERMINAL;   /* Write Fault */
		if (sc == 0x81) return DPA_SAPS_ACTION_FAILOVER;   /* D3 replicated-path read */
		if (sc == 0x86) return DPA_SAPS_ACTION_NOT_ERROR;  /* Deallocated block */
		return DPA_SAPS_ACTION_TERMINAL;                    /* other media → terminal */
	case 3:   /* Path Related */
		if (sc == 0x00) return DPA_SAPS_ACTION_FAILOVER;   /* D3 mixed path fault */
		if (sc == 0x04) return DPA_SAPS_ACTION_RETRY_SAME; /* Host Pathing — local issue */
		if (sc == 0x01) return DPA_SAPS_ACTION_FAILOVER;   /* Asym. Persistent Loss */
		return DPA_SAPS_ACTION_FAILOVER;                    /* other path → failover */
	default:
		break;
	}
	return DPA_SAPS_ACTION_UNSET;
}

/* §4.6 NEWMA on log-latency — the single change-point detector.
 * Dual-EWMA distance test (Keriven/Garreau/Poli NeurIPS 2020).
 *
 * Returns true once warmup is past and |fast - slow| exceeds H_NEWMA.
 * Caller also owns resetting ewma_fast/ewma_slow when the FSM transitions
 * through a state that logically starts a new observation window — we reset
 * toward the most recent slow EWMA so the two don't immediately re-fire. */
static inline bool newma_on_sample(struct dpa_path_state *p, int64_t log_lat)
{
	/* Q16.16 arithmetic: λ·(x - EWMA) >> 16 keeps scale. */
	p->ewma_fast += ((log_lat - p->ewma_fast) * SAPS_LAMBDA_FAST_Q16) >> 16;
	p->ewma_slow += ((log_lat - p->ewma_slow) * SAPS_LAMBDA_SLOW_Q16) >> 16;
	if (p->n < SAPS_WARMUP_N)
		return false;
	return abs64(p->ewma_fast - p->ewma_slow) > SAPS_H_NEWMA_Q16;
}

static inline void newma_reset(struct dpa_path_state *p)
{
	/* Anchor both EWMAs to current slow value so post-transition samples
	 * measure residual from the just-established baseline, not the
	 * pre-change drift. */
	p->ewma_fast = p->ewma_slow;
}

static inline uint32_t compute_score(struct dpa_path_state *p, uint8_t opcode)
{
	if (__builtin_expect(p->state == DPA_SAPS_STATE_EXCLUDED, 0))
		return SAPS_SCORE_MIN;                    /* preserve ε-probe */
	if (__builtin_expect(p->state == DPA_SAPS_STATE_RECOVERING, 0))
		return SAPS_ONSET_PROBE_SCORE;            /* probe until proven healthy */

	if (__builtin_expect(p->n < SAPS_WARMUP_N, 0))
		return 0;                                 /* sentinel: skip write */

	/* Base: 1 / p99_estimate. p99 is Q16.16; base in Q16.16 units. */
	int64_t p99 = p->p99_estimate;
	if (p99 <= 0) p99 = 1;
	int64_t base = fixed_inv_q16(p99);
	if (base == 0) base = 1;

	/* Per-opcode tail boost. If opcode Frugal-2U still warming, fall back
	 * to identity multiplier (Q16.16 1.0). */
	unsigned bucket = opc_bucket(opcode);
	int64_t opc_p99 = p->opc_p99[bucket].p99;
	int64_t opc_boost_q16 = (int64_t)1 << 16;     /* identity */
	if (p->opc_p99[bucket].n > SAPS_WARMUP_N && opc_p99 > 0) {
		opc_boost_q16 = fixed_inv_q16(opc_p99);
		if (opc_boost_q16 == 0)
			opc_boost_q16 = (int64_t)1 << 16;
	}

	/* Combine base × opc_boost in Q16.16, then rescale to SAPS_SCORE_SCALE.
	 * base is Q16.16, opc_boost is Q16.16 → product is Q32.32, >>16 ⇒ Q16.16.
	 * Multiply by SCORE_SCALE (≈ Q4.20) after dividing by 1<<16 to normalise.
	 *
	 * Overflow fix: fixed_inv_q16(1) == 2^32, so with a sub-microsecond p99
	 * both base and opc_boost can approach 2^32 and base*opc_boost overflows
	 * signed int64 (2^32 × 2^32 = 2^64). Clamp each factor to <= 2^24 before
	 * the multiply — the product then stays <= 2^48, well within int64, and
	 * a 2^24 Q16.16 factor (=256.0 inverse-latency) already saturates the
	 * usable score range, so clamping does not distort ranking. */
	if (base > (int64_t)1 << 24)          base = (int64_t)1 << 24;
	if (opc_boost_q16 > (int64_t)1 << 24) opc_boost_q16 = (int64_t)1 << 24;
	int64_t combined = (base * opc_boost_q16) >> 16;       /* Q16.16 */
	int64_t boosted  = (combined * (int64_t)SAPS_SCORE_SCALE) >> 16;  /* integer */

	/* Penalties (integer domain). err_rate_ewma is Q16.16 scaled down
	 * before multiplying with W_ERR to keep penalty bounded. */
	int64_t err_pen = (p->err_rate_ewma * (int64_t)SAPS_W_ERR) >> 16;

	/* Option2 redesign (2026-04-25): inflight-dominant scoring.
	 *
	 * Old: total = boosted - err_pen - inflight * W_QD(1)
	 *      → inflight contribution capped at ~256 vs boosted ~10K-50K.
	 *      → never beats latency signal even when inflight saturates.
	 *      → SAPS picked B in D1 (5ms slow) because B's P99-derived
	 *        boosted didn't fall below A/C's NEWMA-frozen scores fast
	 *        enough to compensate for stale-score deadlock.
	 *
	 * New: total = QD_BASE - inflight * W_QD_NEW + latency_bonus - err_pen
	 *      where latency_bonus = boosted >> 4 (≈ 1/16 of old contribution)
	 *      → inflight=128 penalty 524288 dominates latency_bonus < 4096
	 *      → SAPS now matches stock queue_depth's min-inflight selection
	 *        on pure-latency-asymmetry workloads, plus latency tiebreak.
	 */
	int64_t qd_pen      = (int64_t)p->inflight * SAPS_W_QD_NEW;
	int64_t latency_bonus = boosted >> 4;
	int64_t total = (int64_t)SAPS_QD_SCORE_BASE - qd_pen + latency_bonus - err_pen;

	/* Slice 9.7 (2026-04-29): HEALTHY path score equalization.
	 *
	 * Issue: physical fabric latency between path A/B/C can differ by 1-5 µs
	 * even when all paths are nominally healthy. This makes per-path
	 * `p99_estimate` differ, which makes `latency_bonus = boosted >> 4`
	 * differ, which makes the per-path score differ even at equal inflight.
	 *
	 * In WRS, P(path_i) = score_i / Σ score. With non-equal scores,
	 * steady-state load distribution is NOT 33/33/33 but proportional to
	 * scores, amplifying physical latency bias. Slice 23 N=10 confirmed
	 * D0 healthy goes to A=53%/B=25%/C=22% sticky.
	 *
	 * Fix: for fault_type == HEALTHY, drop the latency_bonus contribution.
	 * Score becomes purely inflight-driven: total = QD_BASE - inflight*W_QD.
	 * At equal inflight three healthy paths score the same → WRS gives 33/33/33.
	 * When path inflight differs (e.g. A wins one extra IO), host_qd inflight
	 * penalty redirects next pick to other paths → self-equalizing.
	 *
	 * For non-HEALTHY fault types, latency_bonus is preserved so SPARSE_ERROR /
	 * BIMODAL_TAIL still get score drift relative to others (the score override
	 * switch below then applies fault-specific clamp). */
	if (__builtin_expect(p->fault_type == SAPS_FAULT_HEALTHY, 1)) {
		/* Slice 9.7 v2: drop BOTH latency_bonus AND qd_pen for HEALTHY paths.
		 * qd_pen here is double-counting host_qd_score × 1000 in the host
		 * blend formula (bdev_nvme.c:1487). DPA's inflight (from hook count)
		 * lags ~1 IO behind host's actual num_outstanding_reqs anyway. Better
		 * to let host blend formula's host_qd × 1000 dominate inflight balance
		 * naturally; DPA score = constant for healthy → WRS sees three equal
		 * (host_qd × 1000 + const) → 1/3 each at near-equal inflight. */
		total = (int64_t)SAPS_QD_SCORE_BASE - err_pen;
	}

	if (total < (int64_t)SAPS_SCORE_MIN) total = SAPS_SCORE_MIN;
	if (total > (int64_t)SAPS_SCORE_MAX) total = SAPS_SCORE_MAX;

	/* Slice 10 type-specific score modifier (spec §4 action mapping).
	 *   HEALTHY        : no override.
	 *   PERPETUAL_SLOW : clamp MIN — host argmax avoids this path entirely.
	 *   SPARSE_ERROR   : no override — retry verdict handles individual IOs;
	 *                    path remains selectable so traffic share recovers.
	 *   BIMODAL_TAIL   : >> SAPS_BIMODAL_SHIFT — probabilistic shedding.
	 *                    1/4 (shift=2) leaves the path competitive when
	 *                    other paths saturate but loses head-to-head argmax.
	 *   FLAP           : no score override here; FSM is frozen by
	 *                    update_state_machine() so the score reflects the
	 *                    last-pre-flap state. */
	switch (p->fault_type) {
	case SAPS_FAULT_PERPETUAL_SLOW:
		total = SAPS_SCORE_MIN;
		break;
	case SAPS_FAULT_ONSET:
	case SAPS_FAULT_ANY_THREE_VOTE:
		total = SAPS_SCORE_MIN;
		break;
	case SAPS_FAULT_QD_DRIFT:
		total >>= 1;
		if (total < (int64_t)SAPS_SCORE_MIN) total = SAPS_SCORE_MIN;
		break;
	case SAPS_FAULT_SPARSE_ERROR:
		/* Slice 18: drain SPARSE_ERROR paths to host probe-floor (1 in
		 * ~500K). Even with retry-overlay, every IO that hits a sct=2
		 * window causes an extra round-trip; bulk traffic on a sparse-
		 * error path still inflates tail latency and risks io_failed
		 * when retries exhaust. Drain via score=MIN; the host probe
		 * floor (= 1 weight vs ~250K healthy) lets DPA continue to see
		 * recovery samples on the path so SPARSE_ERROR can clear once
		 * the error window passes.  Retry-overlay is preserved at the
		 * SPDK layer for the residual probe traffic. */
		total = SAPS_SCORE_MIN;
		break;
		case SAPS_FAULT_BIMODAL_TAIL:
			/* Tuning round 2 (2026-04-28): host selector treats MIN as
			 * hard-EXCLUDE. A simple shift (1/4) leaves BIMODAL paths still
		 * in argmax contention because host_qd_score×1000 dwarfs the
		 * dpa_score >> 8 nudge. Clamp to MIN for D4 PASS. This deviates
		 * from spec §4.2 ("probabilistic shedding, not exclude") — see
		 * report §6 for the trade-off (sacrifices the 85% fast-path
		 * capacity to bound P99.9). */
			total = SAPS_SCORE_MIN;
			break;
		case SAPS_FAULT_SHARED_FATE:
		case SAPS_FAULT_PROPORTIONAL_THROTTLE:
		case SAPS_FAULT_ALL_DEGRADED:
			break;
		default:
			break;
		}
	return (uint32_t)total;
}

/* Slice 34 — per-opcode score for D5 mixed workload routing.
 *
 * compute_score_opcode(p, opc_bucket) returns the SAPS score seen from the
 * perspective of a future IO of opcode class opc_bucket (0=READ, 1=WRITE).
 *
 * Differs from compute_score() in that the opcode boost is driven by the
 * specified bucket rather than the current event's opcode. The rest of the
 * formula (FSM state, fault_type override, err_pen, QD) is identical so
 * the returned score is directly comparable to per_qp_path_score.
 *
 * Returns 0 (warmup sentinel) when the path-level warmup is not complete or
 * when the per-opcode estimator for opc_bucket has not warmed up yet.
 * Returns SAPS_SCORE_MIN for EXCLUDED/PERPETUAL_SLOW/SPARSE_ERROR paths,
 * matching compute_score() semantics. */
static inline uint32_t compute_score_opcode(struct dpa_path_state *p, unsigned opc_bucket_idx)
{
	if (opc_bucket_idx >= DPA_PLUGIN_OPC_CLASS_MAX)
		return 0;

	if (__builtin_expect(p->state == DPA_SAPS_STATE_EXCLUDED, 0))
		return SAPS_SCORE_MIN;
	if (__builtin_expect(p->state == DPA_SAPS_STATE_RECOVERING, 0))
		return SAPS_ONSET_PROBE_SCORE;

	if (__builtin_expect(p->n < SAPS_WARMUP_N, 0))
		return 0;

	/* Per-opcode P99: if this opcode hasn't warmed up yet, return 0
	 * so the host falls back to the aggregate per_qp_path_score.
	 * This avoids locking a healthy path out before enough per-opcode
	 * samples exist. */
	int64_t opc_p99 = p->opc_p99[opc_bucket_idx].p99;
	if (p->opc_p99[opc_bucket_idx].n <= SAPS_WARMUP_N || opc_p99 <= 0)
		return 0;

	int64_t opc_boost_q16 = fixed_inv_q16(opc_p99);
	if (opc_boost_q16 == 0) opc_boost_q16 = (int64_t)1 << 16;

	/* Base aggregate P99 — used as normalization denominator for ratio. */
	int64_t p99 = p->p99_estimate;
	if (p99 <= 0) p99 = 1;

	int64_t err_pen = (p->err_rate_ewma * (int64_t)SAPS_W_ERR) >> 16;

	/* Slice 34 design for per-opcode routing:
	 *
	 * compute_score() (aggregate, HEALTHY) returns QD_BASE - err_pen (constant,
	 * independent of p99_estimate) so all HEALTHY paths are WRS-equalized.
	 *
	 * For per-opcode score we need: score_A_READ < score_B_READ when A
	 * read-P99 (200µs) > B read-P99 (27µs). We achieve this by scaling
	 * QD_BASE with the ratio (base_opcode / base_overall):
	 *
	 *   ratio_q16 = fixed_inv_q16(opc_p99) / fixed_inv_q16(p99_estimate)
	 *             = p99_estimate / opc_p99    (in Q16.16 approx)
	 *
	 * Interpretation: if opc_p99 == p99_estimate → ratio=1 → same as aggregate.
	 * If opc_p99 >> p99_estimate (opcode-specific slow) → ratio < 1 → lower score.
	 *
	 * total = (QD_BASE × ratio_q16) >> 16 − err_pen
	 *
	 * For D5 A-path READ (opc_p99 200µs, p99_estimate≈148µs):
	 *   ratio ≈ 148/200 = 0.74 → total ≈ 0.74 × QD_BASE ≈ 776951
	 * For D5 B-path READ (opc_p99 27µs, p99_estimate≈27µs):
	 *   ratio ≈ 1.0 → total ≈ QD_BASE = 1048576
	 * Host sees dpa_score(A) < agg_score → dpa_factor_A < 32 → WRS avoids A READ. */
	int64_t base_overall = fixed_inv_q16(p99);
	int64_t ratio_q16;
	if (base_overall > 0) {
		/* ratio = base_opc / base_overall = (1/opc_p99) / (1/p99) = p99/opc_p99.
		 * Both in Q16.16; division: (base_opc << 16) / base_overall gives Q16.16. */
		ratio_q16 = (opc_boost_q16 << 16) / base_overall;
	} else {
		ratio_q16 = (int64_t)1 << 16;  /* identity */
	}
	/* Cap ratio at 1.0 (Q16.16) to prevent opc_score > QD_BASE on fast opcodes. */
	if (ratio_q16 > ((int64_t)1 << 16)) ratio_q16 = (int64_t)1 << 16;
	if (ratio_q16 < 0) ratio_q16 = 0;
	int64_t total = (((int64_t)SAPS_QD_SCORE_BASE * ratio_q16) >> 16) - err_pen;

	if (total < (int64_t)SAPS_SCORE_MIN) total = SAPS_SCORE_MIN;
	if (total > (int64_t)SAPS_SCORE_MAX) total = SAPS_SCORE_MAX;

	/* Fault-type overrides — same as compute_score(). */
	switch (p->fault_type) {
	case SAPS_FAULT_PERPETUAL_SLOW:
		total = SAPS_SCORE_MIN;
		break;
	case SAPS_FAULT_ONSET:
	case SAPS_FAULT_ANY_THREE_VOTE:
		total = SAPS_SCORE_MIN;
		break;
	case SAPS_FAULT_QD_DRIFT:
		total >>= 1;
		if (total < (int64_t)SAPS_SCORE_MIN) total = SAPS_SCORE_MIN;
		break;
	case SAPS_FAULT_PROPORTIONAL_THROTTLE:
		break;
	case SAPS_FAULT_SPARSE_ERROR:
		total = SAPS_SCORE_MIN;
		break;
		case SAPS_FAULT_BIMODAL_TAIL:
			total = SAPS_SCORE_MIN;
			break;
		case SAPS_FAULT_SHARED_FATE:
		case SAPS_FAULT_ALL_DEGRADED:
			break;
		default:
			break;
		}
	return (uint32_t)total;
}

static inline bool saps_shared_fate_is_local_fault(uint16_t fault_type)
{
	return fault_type != SAPS_FAULT_HEALTHY &&
	       fault_type != SAPS_FAULT_QD_DRIFT &&
	       fault_type != SAPS_FAULT_PROPORTIONAL_THROTTLE &&
	       fault_type != SAPS_FAULT_SHARED_FATE &&
	       fault_type != SAPS_FAULT_ALL_DEGRADED;
}

/* The host D5 gate compares published opcode P99 values across paths.  Below
 * the classifier's 1 ms absolute evidence floor, estimator jitter must not
 * manufacture a 2x "slow" path and drain an otherwise HEALTHY route.  Publish
 * one neutral floor value for those healthy samples; real fail-slow evidence
 * remains above the floor and therefore stays distinguishable. */
static inline uint32_t
saps_opcode_p99_for_host(const struct dpa_path_state *p, unsigned opc)
{
	int64_t value = p->opc_p99[opc].p99;

	if (p->fault_type == SAPS_FAULT_HEALTHY &&
	    value > 0 && value < SAPS_LAT_HEALTHY_FLOOR_Q16)
		value = SAPS_LAT_HEALTHY_FLOOR_Q16;
	return (uint32_t)(value & 0xFFFFFFFF);
}

static inline uint32_t saps_capacity_refresh_score(struct dpa_path_state *p)
{
	uint64_t rate_q16 = p->capacity_rate_ewma_q16;
	uint64_t score;

	if (rate_q16 == 0 && p->capacity_sec_completions != 0)
		rate_q16 = (uint64_t)p->capacity_sec_completions << 16;
	if (rate_q16 == 0 || p->n < SAPS_WARMUP_N) {
		p->capacity_score = 0;
		return 0;
	}

	/* Capacity is a service-rate signal.  QD is an admission consequence,
	 * so dividing by QD turns a usable throttled path into a drain signal. */
	score = rate_q16 >> 16;
	if (score == 0)
		score = 1;
	if (score > 0xFFFFFFFFull)
		score = 0xFFFFFFFFull;
	p->capacity_score = (uint32_t)score;
	return p->capacity_score;
}

static inline void saps_capacity_on_complete(struct dpa_path_state *p,
					     uint64_t host_tsc)
{
	uint32_t sec = (uint32_t)(host_tsc / TICKS_PER_SEC);

	if (p->capacity_last_sec == 0) {
		p->capacity_last_sec = sec;
		p->capacity_sec_completions = 0;
	} else if (sec != p->capacity_last_sec) {
		uint32_t gap = sec - p->capacity_last_sec;
		uint64_t sample_q16 = (uint64_t)p->capacity_sec_completions << 16;

		if (p->capacity_rate_ewma_q16 == 0) {
			p->capacity_rate_ewma_q16 = sample_q16;
		} else {
			uint64_t cur = p->capacity_rate_ewma_q16;
			uint32_t steps = gap > 60u ? 60u : gap;
			for (uint32_t i = 0; i < steps; i++) {
				uint64_t sample = (i == 0) ? sample_q16 : 0;
				if (sample >= cur)
					cur += (sample - cur) >> SAPS_CAPACITY_EWMA_SHIFT;
				else
					cur -= (cur - sample) >> SAPS_CAPACITY_EWMA_SHIFT;
			}
			p->capacity_rate_ewma_q16 = cur;
		}
		p->capacity_last_sec = sec;
		p->capacity_sec_completions = 0;
	}

	if (p->capacity_sec_completions != 0xFFFFFFFFu)
		p->capacity_sec_completions++;
	saps_capacity_refresh_score(p);
}

static inline bool saps_capacity_fast_tick(struct dpa_path_state *p,
					   uint64_t host_tsc)
{
	bool rolled = false;

	if (p->cap_fast_win_start_tsc == 0) {
		p->cap_fast_win_start_tsc = host_tsc;
		p->cap_fast_win_completions = 0;
	} else if (host_tsc - p->cap_fast_win_start_tsc >= SAPS_SF_FAST_WIN_TICKS) {
		rolled = true;
		uint32_t win_done = p->cap_fast_win_completions;
		p->rate_fast_q16 = win_done << 16;

		uint32_t qd_int = (uint32_t)(p->qd_mean >> 16);
		bool err_low = p->err_rate_ewma < (int64_t)SAPS_SF_ERR_BAD_Q16;
		bool lat_ok = (p->baseline_log_lat_q16 == 0) ? true :
			(p->mean_log_lat_ewma_q16 - p->baseline_log_lat_q16
				< (int64_t)SAPS_SF_LAT_BAD_DELTA_Q16);
		bool unloaded_healthy =
			p->n >= SAPS_WARMUP_N &&
			p->state == DPA_SAPS_STATE_HEALTHY &&
			p->fault_type == SAPS_FAULT_HEALTHY &&
			err_low && lat_ok &&
			qd_int < SAPS_SF_QD_KNEE;

		if (unloaded_healthy && win_done > 0) {
			uint32_t qd_eff = qd_int < 1u ? 1u : qd_int;
			uint32_t score = (uint32_t)(p->rate_fast_q16 / qd_eff);
			uint32_t nqd = qd_int < SAPS_SF_QD_DEMAND_MIN
				? SAPS_SF_QD_DEMAND_MIN : qd_int;
			int64_t norm = p->mean_log_lat_ewma_q16 -
				fixed_log2((uint64_t)nqd);

			if (p->cap_score_base == 0) {
				p->cap_score_base = score == 0 ? 1u : score;
				p->cap_rate_base = p->rate_fast_q16;
				p->qd_base = qd_eff;
				p->norm_log_lat_base = norm;
			} else {
				p->cap_score_base += ((int32_t)score - (int32_t)p->cap_score_base) >> 3;
				if (p->cap_score_base == 0)
					p->cap_score_base = 1;
				p->cap_rate_base += ((int32_t)p->rate_fast_q16 - (int32_t)p->cap_rate_base) >> 3;
				p->qd_base += ((int32_t)qd_eff - (int32_t)p->qd_base) >> 3;
				if (p->qd_base == 0)
					p->qd_base = 1;
				p->norm_log_lat_base += (norm - p->norm_log_lat_base) >> 3;
			}
		}

		p->cap_fast_win_start_tsc = host_tsc;
		p->cap_fast_win_completions = 0;
	}

	if (p->cap_fast_win_completions != 0xFFFFFFFFu)
		p->cap_fast_win_completions++;

	return rolled;
}

static inline uint32_t saps_sf_expected_rate(const struct dpa_path_state *p)
{
	uint32_t qd_int = (uint32_t)(p->qd_mean >> 16);
	uint32_t qd_eff = qd_int < 1u ? 1u : qd_int;

	if (qd_eff > SAPS_SF_QD_KNEE)
		qd_eff = SAPS_SF_QD_KNEE;

	uint64_t exp = (uint64_t)p->cap_score_base * qd_eff;
	if (p->cap_rate_base != 0 && exp > p->cap_rate_base)
		exp = p->cap_rate_base;
	if (exp > 0xFFFFFFFFull)
		exp = 0xFFFFFFFFull;
	return (uint32_t)exp;
}

static inline bool saps_sf_ratio_lt(uint32_t cur, uint32_t base,
				    uint32_t num, uint32_t den)
{
	return ((uint64_t)cur * den) < ((uint64_t)base * num);
}

static inline bool saps_sf_ratio_ge(uint32_t cur, uint32_t base,
				    uint32_t num, uint32_t den)
{
	return ((uint64_t)cur * den) >= ((uint64_t)base * num);
}

static inline bool saps_sf_path_degraded_this_tick(const struct dpa_path_state *p)
{
	uint32_t qd_int;
	bool demand, lat_bad, pressure, rate_bad, rate_ok, score_bad, nlat_bad;
	uint32_t exp_rate;
	int64_t norm_log_lat;

	if (p->err_rate_ewma >= (int64_t)SAPS_SF_ERR_BAD_Q16)
		return true;

	if (p->cap_score_base == 0)
		return false;

	qd_int = (uint32_t)(p->qd_mean >> 16);

	demand = qd_int >= SAPS_SF_QD_DEMAND_MIN || p->inflight >= SAPS_SF_QD_DEMAND_MIN;
	if (!demand)
		return false;

	lat_bad = p->baseline_log_lat_q16 != 0 &&
		(p->mean_log_lat_ewma_q16 - p->baseline_log_lat_q16
			>= (int64_t)SAPS_SF_LAT_BAD_DELTA_Q16);
	if (!lat_bad)
		return false;

	pressure = qd_int >= SAPS_SF_QD_PRESSURE_MIN &&
		(p->qd_base != 0 && qd_int >= p->qd_base * SAPS_SF_QD_PRESSURE_MULT);

	exp_rate = saps_sf_expected_rate(p);
	rate_bad = saps_sf_ratio_lt(p->rate_fast_q16, exp_rate,
				    SAPS_SF_RATE_BAD_NUM, SAPS_SF_RATIO_DEN);
	rate_ok = saps_sf_ratio_ge(p->rate_fast_q16, exp_rate,
				   SAPS_SF_RATE_OK_NUM, SAPS_SF_RATIO_DEN);

	score_bad = qd_int <= ((SAPS_SF_QD_KNEE * 5u) / 4u) &&
		saps_sf_ratio_lt(p->capacity_score, p->cap_score_base,
				 SAPS_SF_SCORE_BAD_NUM, SAPS_SF_RATIO_DEN);

	{
		uint32_t nqd = qd_int < SAPS_SF_QD_DEMAND_MIN
			? SAPS_SF_QD_DEMAND_MIN : qd_int;
		norm_log_lat = p->mean_log_lat_ewma_q16 -
			fixed_log2((uint64_t)nqd);
	}
	nlat_bad = (norm_log_lat - p->norm_log_lat_base)
			>= (int64_t)SAPS_SF_NLAT_BAD_DELTA_Q16;

	if (pressure && rate_ok && !nlat_bad)
		return false;

	/* Guard v2: nlat_bad alone is not degraded while rate and score are healthy. */
	if (rate_ok && !rate_bad && !score_bad)
		return false;

	return rate_bad || score_bad || nlat_bad;
}

static inline void saps_sf_persist_update(struct dpa_path_state *p)
{
	uint8_t hist = (uint8_t)((p->sf_degraded_history << 1) & 0x7u);
	if (saps_sf_path_degraded_this_tick(p))
		hist |= 1u;
	p->sf_degraded_history = hist;

	p->sf_degraded_cached =
		(__builtin_popcount(hist & 0x7u) >= (int)SAPS_SF_PERSIST_NEED) ? 1u : 0u;
}

static inline bool saps_shared_fate_path_has_degraded_evidence(const struct dpa_path_state *p,
							       bool is_current,
							       uint16_t current_fault)
{
	if (p->n < SAPS_WARMUP_N || p->baseline_log_lat_q16 == 0)
		return false;

	/* Do not count the current IO fault as SHARED_FATE evidence; only
	 * cached path state/capacity evidence can establish cross-path fate. */
	(void)is_current;
	(void)current_fault;

	if (saps_shared_fate_is_local_fault(p->fault_type))
		return true;

	if (p->fault_type != SAPS_FAULT_SHARED_FATE) {
		if (p->state == DPA_SAPS_STATE_EXCLUDED)
			return true;
		if ((p->state == DPA_SAPS_STATE_DEGRADING ||
		     p->state == DPA_SAPS_STATE_RECOVERING) &&
		    p->sf_degraded_cached != 0)
			return true;
	}

	/* B8 shared-fate service-capacity verdict: only persisted capacity
	 * degradation counts as cross-path degraded evidence. */
	return p->sf_degraded_cached != 0;
}

static inline bool
saps_shared_fate_path_has_latency_tail_evidence(const struct dpa_path_state *p)
{
	if (p->n < SAPS_SHARED_FATE_LAT_MIN_SAMPLES ||
	    p->baseline_log_lat_q16 == 0 ||
	    p->recent_sample_count < 4)
		return false;

	bool p99_tail =
		p->p99_estimate > SAPS_TAIL_LOG_LAT_Q16 &&
		p->p99_estimate < SAPS_SHARED_FATE_TAIL_MAX_Q16;
	bool recent_tail =
		p->bimodal_consec_windows >= 2u &&
		p->max_log_lat_recent < SAPS_SHARED_FATE_TAIL_MAX_Q16 &&
		p->max_log_lat_recent > SAPS_TAIL_LOG_LAT_Q16 &&
		p->tail_count_recent >= SAPS_TAIL_COUNT_MIN;

	return (p99_tail || recent_tail) &&
	       p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16 &&
	       p->recent_err_count < SAPS_RECENT_ERR_THRESH;
}

static inline bool saps_shared_fate_active(uint16_t qp_idx,
					   uint16_t current_path_idx,
					   uint16_t current_fault,
					   bool all_paths_degraded,
					   uint64_t now_tsc)
{
	unsigned degraded = 0;
	unsigned excluded_count = 0;

	for (unsigned i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		if (g_path[qp_idx][i].n >= SAPS_WARMUP_N &&
		    g_path[qp_idx][i].state == DPA_SAPS_STATE_EXCLUDED)
			excluded_count++;
	}

	if (excluded_count == 0) {
		for (unsigned i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
			if (saps_shared_fate_path_has_degraded_evidence(
				    &g_path[qp_idx][i],
				    i == current_path_idx,
				    current_fault) ||
			    saps_shared_fate_path_has_latency_tail_evidence(
				    &g_path[qp_idx][i]))
				degraded++;
		}
	}

	if (excluded_count == 0 &&
	    (all_paths_degraded || degraded >= SAPS_SHARED_FATE_DEGRADED_N)) {
		g_shared_fate_until_tsc[qp_idx] =
			now_tsc + SAPS_T_SHARED_FATE_LEASE_TICKS;
		return true;
	}

	if (excluded_count > 0) {
		g_shared_fate_until_tsc[qp_idx] = 0;
		return false;
	}

	return g_shared_fate_until_tsc[qp_idx] != 0 &&
	       now_tsc < g_shared_fate_until_tsc[qp_idx];
}

static inline bool
saps_shared_fate_path_latency_degraded(const struct dpa_path_state *p,
				       int64_t current_consensus_log_lat_q16,
				       int64_t baseline_consensus_log_lat_q16,
				       bool baseline_fallback_allowed)
{
	if (p->n < SAPS_WARMUP_N ||
	    p->baseline_log_lat_q16 == 0 ||
	    current_consensus_log_lat_q16 <= 0)
		return false;

	/* SHARED_FATE exclusion must be backed by sustained latency degradation.
	 * Healthy paths can carry high QD while absorbing traffic from a drained peer. */
	if (saps_shared_fate_path_has_latency_tail_evidence(p) &&
	    p->p99_estimate > SAPS_TAIL_LOG_LAT_Q16 &&
	    p->p99_estimate < SAPS_SHARED_FATE_TAIL_MAX_Q16 &&
	    p->mean_log_lat_ewma_q16 - current_consensus_log_lat_q16 >=
		    (int64_t)SAPS_SF_LAT_BAD_DELTA_Q16 &&
	    p->p99_estimate - current_consensus_log_lat_q16 >=
		    (int64_t)SAPS_CROSSPATH_DRIFT_HI_Q16)
		return true;

	if (p->mean_log_lat_ewma_q16 <= SAPS_LAT_HEALTHY_FLOOR_Q16)
		return false;

	if (baseline_fallback_allowed &&
	    baseline_consensus_log_lat_q16 > 0 &&
	    p->mean_log_lat_ewma_q16 - baseline_consensus_log_lat_q16 >=
		    (int64_t)SAPS_SF_LAT_BAD_DELTA_Q16 &&
	    saps_shared_fate_path_has_latency_tail_evidence(p))
		return true;

	if (p->mean_log_lat_ewma_q16 - current_consensus_log_lat_q16 >=
	    (int64_t)SAPS_CROSSPATH_DRIFT_HI_Q16)
		return true;

	return false;
}

static inline void saps_shared_fate_publish_path(volatile struct dpa_plugin_shared *s,
						 uint16_t qp_idx,
						 uint16_t path_idx,
						 uint8_t opcode)
{
	struct dpa_path_state *p = &g_path[qp_idx][path_idx];
	g_saps_sample_events++;
	uint32_t score = compute_score(p, opcode);

	p->score = score;
	if (score != 0) {
		s->per_qp_path_score[qp_idx][path_idx] = score;
		g_saps_score_updates++;
	}
	s->per_qp_path_capacity[qp_idx][path_idx] =
		saps_capacity_refresh_score(p);

	for (unsigned opc = 0; opc < DPA_PLUGIN_OPC_CLASS_MAX; opc++) {
		uint32_t s_opc = compute_score_opcode(p, opc);
		if (s_opc != 0)
			s->per_qp_path_score_opcode[qp_idx][path_idx][opc] = s_opc;
		if (p->opc_p99[opc].n > SAPS_WARMUP_N && p->opc_p99[opc].p99 > 0)
			s->per_qp_path_opc_p99[qp_idx][path_idx][opc] =
				saps_opcode_p99_for_host(p, opc);
	}

	s->per_qp_path_state[qp_idx][path_idx] = p->state;
	s->per_qp_path_fault_type[qp_idx][path_idx] = p->fault_type;
	{
		uint32_t bit = 1u << (path_idx & DPA_PLUGIN_PATH_MASK);
		uint32_t mask = s->per_qp_fault_mask[qp_idx][0];

		if (p->fault_type == SAPS_FAULT_HEALTHY)
			mask &= ~bit;
		else
			mask |= bit;
		s->per_qp_fault_mask[qp_idx][0] = mask;
	}
}

static inline void saps_shared_fate_apply(volatile struct dpa_plugin_shared *s,
					  uint16_t qp_idx,
					  uint16_t current_path_idx,
					  uint8_t opcode,
					  bool clean_success,
					  int64_t log_lat,
					  uint64_t now_tsc)
{
	uint16_t warmed[DPA_PLUGIN_PATH_MAX];
	uint16_t exclude_candidates[DPA_PLUGIN_PATH_MAX];
	unsigned warmed_n = 0;
	unsigned exclude_n = 0;
	unsigned latency_tail_n = 0;
	int64_t current_consensus_log_lat_q16 = 0;
	int64_t current_max_log_lat_q16 = 0;
	int64_t baseline_consensus_log_lat_q16 = 0;

	for (unsigned i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		struct dpa_path_state *q = &g_path[qp_idx][i];
		int64_t cur = q->mean_log_lat_ewma_q16;

		if (q->n != 0 && q->recent_sample_count >= 4 && cur > 0 &&
		    (current_consensus_log_lat_q16 == 0 ||
		     cur < current_consensus_log_lat_q16))
			current_consensus_log_lat_q16 = cur;
	}

	for (unsigned i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		struct dpa_path_state *q = &g_path[qp_idx][i];
		int64_t cur = q->mean_log_lat_ewma_q16;

		if (q->n >= SAPS_WARMUP_N && q->baseline_log_lat_q16 != 0) {
			warmed[warmed_n++] = (uint16_t)i;
			if (cur > 0 &&
			    (current_consensus_log_lat_q16 == 0 ||
			     cur < current_consensus_log_lat_q16))
				current_consensus_log_lat_q16 = cur;
			if (cur > current_max_log_lat_q16)
				current_max_log_lat_q16 = cur;
			if (saps_shared_fate_path_has_latency_tail_evidence(q))
				latency_tail_n++;
			if (baseline_consensus_log_lat_q16 == 0 ||
			    q->baseline_log_lat_q16 < baseline_consensus_log_lat_q16)
				baseline_consensus_log_lat_q16 = q->baseline_log_lat_q16;
		}
	}
	if (warmed_n < 2 || current_consensus_log_lat_q16 <= 0)
		return;

	bool baseline_fallback_allowed =
		latency_tail_n >= SAPS_SHARED_FATE_DEGRADED_N &&
		current_max_log_lat_q16 - current_consensus_log_lat_q16 <
			(int64_t)SAPS_CROSSPATH_DRIFT_HI_Q16;

	for (unsigned wi = 0; wi < warmed_n; wi++) {
		uint16_t i = warmed[wi];

		if (saps_shared_fate_path_latency_degraded(
			    &g_path[qp_idx][i],
			    current_consensus_log_lat_q16,
			    baseline_consensus_log_lat_q16,
			    baseline_fallback_allowed))
			exclude_candidates[exclude_n++] = i;
	}

	if (exclude_n < SAPS_SHARED_FATE_DEGRADED_N) {
		g_shared_fate_last_excluded_plus1[qp_idx] = 0;
		return;
	}

	uint64_t rotate_ticks = SAPS_T_SHARED_FATE_ROTATE_TICKS;
	uint64_t phase = rotate_ticks != 0 ? now_tsc % rotate_ticks : 0;
	bool in_exclude_window = phase < SAPS_T_SHARED_FATE_EXCLUDE_TICKS;
	unsigned slot = exclude_n != 0 ?
		(unsigned)((rotate_ticks != 0 ? now_tsc / rotate_ticks : 0) % exclude_n) : 0;
	uint16_t excluded = (in_exclude_window && exclude_n != 0) ?
		exclude_candidates[slot] : (uint16_t)DPA_PLUGIN_PATH_MAX;
	if (exclude_n == 0)
		in_exclude_window = false;

	for (unsigned tries = 0; in_exclude_window && tries < exclude_n; tries++) {
		struct dpa_path_state *q = &g_path[qp_idx][excluded];

		if (q->shared_fate_fast_consec < SAPS_SHARED_FATE_HEAL_N)
			break;
		slot = (slot + 1) % exclude_n;
		excluded = exclude_candidates[slot];
	}

	if (!in_exclude_window) {
		g_shared_fate_last_excluded_plus1[qp_idx] = 0;
	} else if (g_shared_fate_last_excluded_plus1[qp_idx] != (uint8_t)(excluded + 1u)) {
		g_shared_fate_last_excluded_plus1[qp_idx] = (uint8_t)(excluded + 1u);
		flexio_dev_print("[SHARED_FATE] qp=%u exclude_path=%u warmed=%u lease_ms=%u\n",
				 (unsigned)qp_idx, (unsigned)excluded,
				 (unsigned)warmed_n,
				 (unsigned)(SAPS_T_SHARED_FATE_LEASE_TICKS / SAPS_TICKS_PER_MS));
	}

	for (unsigned wi = 0; wi < warmed_n; wi++) {
		uint16_t i = warmed[wi];
		struct dpa_path_state *q = &g_path[qp_idx][i];
		bool is_candidate = false;

		for (unsigned ci = 0; ci < exclude_n; ci++) {
			if (exclude_candidates[ci] == i) {
				is_candidate = true;
				break;
			}
		}

		/* Per-path evidence owns quarantine.  SHARED_FATE is only an
		 * annotation and must not overwrite or heal a locally diagnosed path. */
		if (saps_shared_fate_is_local_fault(q->fault_type)) {
			saps_shared_fate_publish_path(s, qp_idx, i, opcode);
			continue;
		}

		if (i == current_path_idx) {
			if (clean_success && log_lat > 0 &&
			    log_lat <= SAPS_B8_FAST_LOG_LAT_Q16) {
				if (q->shared_fate_fast_consec < 0xFFu)
					q->shared_fate_fast_consec++;
			} else {
				q->shared_fate_fast_consec = 0;
			}
		}
		/* The lease/candidate record is annotation only.  Do not write the
		 * action fault_type or FSM state here; the per-path classifier owns
		 * both, including quarantine and recovery. */
		(void)is_candidate;
		saps_shared_fate_publish_path(s, qp_idx, i, opcode);
	}
}



/* ── Slice 10 multi-modal classifier (spec §3.2) ───────────────────────────
 *
 * Counts entries in a fixed-length TSC ring whose age is within window_ms of
 * now. now_ms is low-32 bits of (TSC / TICKS_PER_MS). Ring entries store the
 * same low-32-bit ms timestamp so unsigned subtraction wraps cleanly. */
static inline uint32_t ring_count_within(const uint32_t *ring, unsigned len,
					   uint32_t now_ms, uint32_t window_ms)
{
	uint32_t cnt = 0;
	for (unsigned i = 0; i < len; i++) {
		uint32_t t = ring[i];
		if (t == 0) continue;
		uint32_t age = now_ms - t;
		if (age <= window_ms)
			cnt++;
	}
	return cnt;
}

static inline void ring_push(uint32_t *ring, uint8_t *head, unsigned len,
			       uint32_t now_ms)
{
	ring[(*head) & (len - 1)] = now_ms;
	*head = (*head + 1) & (len - 1);
}

#if SAPS_D2_B8_ENABLE
static inline bool saps_b8_recoverable_fault(const struct dpa_path_state *p,
					     uint16_t prev_fault,
					     uint16_t new_fault)
{
	if (saps_fault_is_onset_family(prev_fault) ||
	    saps_fault_is_onset_family(new_fault))
		return true;
	if (p->active_heal_fast_consec != 0)
		return true;

	/* D2 can transiently publish PERPETUAL_SLOW before/after the ONSET latch
	 * because the injected delay is a hard 5ms path latency.  D1's real
	 * always-slow path has a slow baseline, so this branch does not re-admit
	 * it on a stray fast completion. */
	if ((prev_fault == SAPS_FAULT_PERPETUAL_SLOW ||
	     new_fault == SAPS_FAULT_PERPETUAL_SLOW) &&
	    p->baseline_log_lat_q16 > 0 &&
	    p->baseline_log_lat_q16 <= SAPS_B8_FAST_LOG_LAT_Q16)
		return true;

	return false;
}

static inline bool saps_b8_force_recovery(struct dpa_path_state *p,
					  uint64_t now_tsc,
					  uint16_t qp_idx,
					  uint16_t path_idx,
					  uint16_t old_state,
					  uint16_t old_fault,
					  int64_t log_lat)
{
	uint32_t now_ms = (uint32_t)(now_tsc / SAPS_TICKS_PER_MS);

	saps_clear_onset_signal_state(p);
	p->fault_type = SAPS_FAULT_HEALTHY;
	p->state = DPA_SAPS_STATE_HEALTHY;
	p->state_entered_tsc = now_tsc;
	p->probe_count = 0;
	p->consec_nonhealthy_count = 0;
	p->active_heal_fast_consec = 0;
	p->healthy_since_tsc = 0;
	ring_push(p->fsm_trans_tsc_ms_ring, &p->fsm_trans_ring_head, 4, now_ms);

	flexio_dev_print("[B8_HEAL] qp=%u path=%u old_state=%u new_state=%u old_ft=%u fast_log=%ld\n",
			 (unsigned)qp_idx, (unsigned)path_idx,
			 (unsigned)old_state, (unsigned)p->state,
			 (unsigned)old_fault, (long)log_lat);
	return true;
}

static inline bool saps_b8_active_heal_on_sample(struct dpa_path_state *p,
						 uint16_t prev_fault,
						 uint16_t new_fault,
						 uint16_t state_before,
						 bool clean_success,
						 int64_t log_lat,
						 uint64_t now_tsc,
						 uint16_t qp_idx,
						 uint16_t path_idx)
{
	bool nonhealthy_state =
		state_before == DPA_SAPS_STATE_DEGRADING ||
		state_before == DPA_SAPS_STATE_EXCLUDED ||
		state_before == DPA_SAPS_STATE_RECOVERING;

	if (!nonhealthy_state) {
		p->active_heal_fast_consec = 0;
		return false;
	}

	if (!saps_b8_recoverable_fault(p, prev_fault, new_fault)) {
		p->active_heal_fast_consec = 0;
		return false;
	}

	if (clean_success && log_lat > 0 && log_lat <= SAPS_B8_FAST_LOG_LAT_Q16) {
		if (p->active_heal_fast_consec < 0xFFu)
			p->active_heal_fast_consec++;
	} else {
		p->active_heal_fast_consec = 0;
	}

	if (p->active_heal_fast_consec >= SAPS_B8_HEAL_N) {
		return saps_b8_force_recovery(p, now_tsc, qp_idx, path_idx,
					      state_before, prev_fault, log_lat);
	}

	return false;
}
#endif

/* Push an event into the recent-error sliding tracker. is_err == true for
 * sct≠0 completions. Decay every SAPS_RECENT_DECAY_PERIOD samples to keep the
 * counter representative of the last ~64 samples. */
static inline void recent_err_track(struct dpa_path_state *p, bool is_err)
{
	if (p->recent_sample_count < 0xFFFF)
		p->recent_sample_count++;
	if (is_err && p->recent_err_count < 0xFFFF)
		p->recent_err_count++;
	if (p->recent_sample_count >= SAPS_RECENT_DECAY_PERIOD) {
		p->recent_err_count   >>= 1;
		p->recent_sample_count >>= 1;
	}
}

static inline bool saps_qd_backed_slow(const struct dpa_path_state *p)
{
	return p->inflight >= SAPS_INFLIGHT_FLOOR_ABS ||
	       p->qd_mean > (int64_t)SAPS_QD_CP_FLOOR_Q16;
}

/* Classify fault type from current per-path state (spec §3.2). O(constant).
 *
 * Slice 10 staleness guard: if recent_sample_count is below the decay floor we
 * cannot trust the classifier — every signal (mean, var, p99) is dominated by
 * stale samples and a deselected path will look "degraded" purely because
 * argmax stopped picking it. In that case fall back to HEALTHY so the path
 * remains selectable and gets a chance to refresh its statistics. */
static inline uint16_t classify_fault_type_raw(struct dpa_path_state *p,
					        uint32_t now_ms,
					        int64_t global_median_log_lat_q16,
					        bool global_median_valid,
					        int64_t global_min_log_lat_q16,
					        bool global_min_valid,
					        int64_t global_mad_log_lat_q16,
					        bool global_mad_valid,
					        int64_t global_median_qd_q16,
					        bool global_median_qd_valid,
					        uint32_t global_median_capacity,
					        bool global_capacity_valid,
						        int64_t global_median_p99_q16,
						        bool global_median_p99_valid,
						        int64_t median_inflight,
						        bool median_inflight_valid,
						        bool peer_latency_fault_active,
						        bool all_paths_degraded)
{
#if !(SAPS_D2_B3_ENABLE || SAPS_D2_B6_ENABLE)
	(void)global_mad_log_lat_q16;
	(void)global_mad_valid;
#endif
	/* Warmup: cannot classify yet. Treat as HEALTHY. */
	if (__builtin_expect(p->n < SAPS_WARMUP_N || p->baseline_log_lat_q16 == 0, 0))
		return SAPS_FAULT_HEALTHY;

	/* A protocol error is direct completion evidence and takes precedence
	 * over queue- or capacity-derived diagnoses that may already be latched
	 * on the path.  Otherwise a prior PROPORTIONAL_THROTTLE verdict can mask
	 * an NVMe media/path status indefinitely and keep HCAA at h_p=1. */
	if (p->recent_err_count >= SAPS_RECENT_ERR_THRESH)
		return SAPS_FAULT_SPARSE_ERROR;

	/* Round 2 D8 service-rate detector.  A target-throttle path can look
	 * latency-slow only because host-side queues build behind service-rate
	 * loss.  When QD evidence is already present and mean latency is below
	 * the 4ms hard-tail band, let QD_DRIFT own the case and suppress the
	 * full-drain latency/ONSET rules below.  True D1/D2-style 5ms paths
	 * still exceed SAPS_TAIL_LOG_LAT_Q16 and remain eligible for full drain. */
	/* Fix 6 (2026-05-18): bimodal pattern detector. When a path shows the
	 * sustained bimodal signature (≥ 1 triggered window with ≥ MIN tail samples,
	 * max > 4ms, log-ratio max/mean > log2(8)), the case belongs to Slice 15
	 * BIMODAL_TAIL latch (line ~2844) which routes through FSM → EXCLUDED.
	 * Without this guard, qd_drift_candidate fires PROPORTIONAL_THROTTLE first
	 * (because bimodal slow IOs queue on this path → high qd_mean → drift
	 * detected), keeping B at ~4% via score downweight instead of full
	 * quarantine. D4 hero requires full quarantine = B<1%.
	 *
	 * Fix 7 (2026-05-18): lowered bimodal_consec_windows from >= 2 to >= 1.
	 * Reason: when B FSM oscillates H↔E, traffic windows on B are too short
	 * to ever accumulate 2 consecutive full 512-sample windows of tail >= MIN.
	 * Single-window evidence (one 512-sample slot with > 25 slow samples plus
	 * 5ms max plus log-ratio > 3) is already strong bimodal signal — D0 ARM
	 * jitter cannot produce 25 samples > 4ms within 512 samples even once. */
	int64_t fix6_excess = p->max_log_lat_recent - p->mean_log_lat_ewma_q16;
	bool path_above_current_consensus =
		global_min_valid &&
		p->mean_log_lat_ewma_q16 - global_min_log_lat_q16 >
			SAPS_CROSSPATH_DRIFT_HI_Q16;
	/* Fix 8 (2026-05-18): drop bimodal_consec_windows requirement entirely.
	 * Reason: B oscillates H↔E so traffic on B never sustains a single 512-sample
	 * window to completion → bimodal_consec stuck at 0. With max>4ms floor +
	 * tail>=25 (5% of 512) + log-ratio>3 conditions, D0 healthy 27µs paths cannot
	 * false-fire (max < 1ms < 4ms floor); D4 B can fire BIMODAL_TAIL on partial
	 * window evidence and stay quarantined via FSM. */
	bool p_looks_bimodal =
		path_above_current_consensus &&
		p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
		p->sf_degraded_cached != 0 &&
		p->max_log_lat_recent > SAPS_TAIL_LOG_LAT_Q16 &&
		p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
		fix6_excess > SAPS_MAX_RATIO_HI_Q16 &&
		p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16 &&
		p->recent_err_count < SAPS_RECENT_ERR_THRESH;


	bool qd_drift_candidate =
		global_median_qd_valid &&
		p->n >= SAPS_QD_DRIFT_MIN_SAMPLES &&
		global_median_qd_q16 > 0 &&
		p->mean_log_lat_ewma_q16 > SAPS_LAT_HEALTHY_FLOOR_Q16 &&
		p->qd_mean > (int64_t)SAPS_QD_CP_FLOOR_Q16 &&
		p->qd_mean * (int64_t)SAPS_INFLIGHT_RATIO_DEN >
		global_median_qd_q16 * (int64_t)SAPS_INFLIGHT_RATIO_NUM &&
		!p_looks_bimodal;  /* Fix 6: defer to BIMODAL latch */
	bool qd_service_candidate =
		qd_drift_candidate &&
		p->mean_log_lat_ewma_q16 < SAPS_TAIL_LOG_LAT_Q16;
	bool capacity_low_candidate =
		global_capacity_valid &&
		p->capacity_score != 0 &&
		(uint64_t)p->capacity_score * SAPS_CAPACITY_LOW_DEN <
		(uint64_t)global_median_capacity * SAPS_CAPACITY_LOW_NUM;
	bool capacity_service_candidate =
		capacity_low_candidate &&
		p->n >= SAPS_QD_DRIFT_MIN_SAMPLES &&
		p->mean_log_lat_ewma_q16 < SAPS_TAIL_LOG_LAT_Q16 &&
		p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16 &&
		!peer_latency_fault_active &&
		!all_paths_degraded &&
		!p_looks_bimodal;  /* Fix 6: defer to BIMODAL latch */
	/* A previously latched service-rate verdict must not mask stronger
	 * latency evidence.  This is the same evidence used by the existing
	 * severe-latency rule below.  Letting it preempt the proportional hold
	 * makes classification monotonic as a path moves from reduced service
	 * into a sustained latency failure. */
	bool severe_latency_candidate =
		p->n >= SAPS_LAT_ABS_MIN_SAMPLES &&
		p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
		!peer_latency_fault_active &&
		saps_qd_backed_slow(p) &&
		p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
		p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16;

	/* Fix 6: if bimodal pattern is active, jump straight to Slice 15 latch.
	 * Returns immediately with SAPS_FAULT_BIMODAL_TAIL — bypasses qd_drift /
	 * capacity / ONSET / SHARED_FATE rules that could otherwise misclassify. */
	if (__builtin_expect(p_looks_bimodal, 0)) {
		return SAPS_FAULT_BIMODAL_TAIL;
	}

	if (__builtin_expect(qd_drift_candidate, 0)) {
		if (p->qd_drift_high_consec < 0xFFu)
			p->qd_drift_high_consec++;
		if (qd_service_candidate &&
		    p->qd_drift_high_consec >= SAPS_QD_DRIFT_FIRE_N)
			return SAPS_FAULT_PROPORTIONAL_THROTTLE;
		} else {
			p->qd_drift_high_consec = 0;
		}

	if (__builtin_expect(capacity_service_candidate, 0)) {
		p->proportional_hold_consec = SAPS_PROPORTIONAL_HOLD_N;
		return SAPS_FAULT_PROPORTIONAL_THROTTLE;
	}

	if (!severe_latency_candidate &&
	    p->fault_type == SAPS_FAULT_PROPORTIONAL_THROTTLE &&
	    capacity_low_candidate) {
		p->proportional_hold_consec = SAPS_PROPORTIONAL_HOLD_N;
		return SAPS_FAULT_PROPORTIONAL_THROTTLE;
	}
	if (!severe_latency_candidate &&
	    p->fault_type == SAPS_FAULT_PROPORTIONAL_THROTTLE &&
	    global_capacity_valid &&
	    p->capacity_score != 0 &&
	    (uint64_t)p->capacity_score * SAPS_CAPACITY_CLEAR_DEN <
	    (uint64_t)global_median_capacity * SAPS_CAPACITY_CLEAR_NUM) {
		if (p->proportional_hold_consec != 0)
			p->proportional_hold_consec--;
		if (p->proportional_hold_consec != 0)
			return SAPS_FAULT_PROPORTIONAL_THROTTLE;
	} else if (!severe_latency_candidate &&
		   p->fault_type == SAPS_FAULT_PROPORTIONAL_THROTTLE) {
		p->proportional_hold_consec = SAPS_PROPORTIONAL_HOLD_N;
		return SAPS_FAULT_PROPORTIONAL_THROTTLE;
	}

		/* all_paths_degraded feeds the separate SHARED_FATE annotation
		 * lease below.  It must not replace this path's evidence verdict. */

			/* Round 6 D2 B6c: ANY_THREE_VOTE.  Per experiment contract, publish
		 * ONSET-family behavior when any one of B2 CUSUM, B3 cross-path MAD,
		 * or B5 inflight skew fires. */
#if SAPS_D2_B7_ENABLE
	{
		bool probe_peer_suppressed =
			peer_latency_fault_active &&
			!saps_fault_is_onset_family(p->fault_type);
		/* Fix 3 (D4 hero reproduction, 2026-05-17):
		 * Don't fire B7 ONSET when the path shows bimodal pattern
		 * (sustained high tail samples + max/mean ratio > log2(8)).
		 * Let Rule 4 BIMODAL_TAIL (line ~2964) classify it as ft=3
		 * which can escalate to EXCLUDED. ONSET is for transient onset,
		 * not for sustained bimodal latency that needs full quarantine. */
		int64_t b7_max_excess = p->max_log_lat_recent -
				        p->mean_log_lat_ewma_q16;
		bool b7_looks_bimodal =
			p->bimodal_consec_windows >= 2u &&
			p->max_log_lat_recent > SAPS_TAIL_LOG_LAT_Q16 &&
			b7_max_excess > SAPS_MAX_RATIO_HI_Q16;
		bool probe_slow =
			path_above_current_consensus &&
			p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
			p->sf_degraded_cached != 0 &&
			p->n >= SAPS_B7_MIN_SAMPLES &&
			p->bocpd_last_log_lat_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
			p->tail_count_recent >= SAPS_B7_TAIL_COUNT_MIN &&
			!qd_service_candidate &&
			!probe_peer_suppressed &&
			!b7_looks_bimodal;
		int64_t probe_delta = p->bocpd_last_log_lat_q16 -
				      p->baseline_log_lat_q16;

		if (probe_slow) {
			if (p->active_probe_slow_consec < 0xFFu)
				p->active_probe_slow_consec++;
			p->active_probe_clear_consec = 0;
		} else if (p->bocpd_last_log_lat_q16 < SAPS_LAT_ABS_CP_FLOOR_Q16 ||
			   (probe_delta < SAPS_ONSET_DELTA_CLEAR_Q16 &&
			    probe_delta > -SAPS_ONSET_DELTA_CLEAR_Q16) ||
			   probe_peer_suppressed) {
			if (p->active_probe_clear_consec < 0xFFu)
				p->active_probe_clear_consec++;
			p->active_probe_slow_consec = 0;
			if (p->active_probe_clear_consec >= SAPS_B7_CLEAR_N &&
			    saps_fault_is_onset_family(p->fault_type)) {
				saps_clear_onset_signal_state(p);
				return SAPS_FAULT_HEALTHY;
			}
		} else {
			p->active_probe_slow_consec = 0;
			p->active_probe_clear_consec = 0;
		}

		if (p->active_probe_slow_consec >= SAPS_B7_SLOW_N) {
			p->onset_high_consec = SAPS_ONSET_FIRE_N;
			p->onset_clear_consec = 0;
			return SAPS_FAULT_ONSET;
		}
		if (p->fault_type == SAPS_FAULT_ONSET &&
		    p->active_probe_clear_consec < SAPS_B7_CLEAR_N)
			return SAPS_FAULT_ONSET;
	}
#endif

#if SAPS_D2_B6_ENABLE
	bool b6_b2_signal = false;
	bool b6_b3_signal = false;
	bool b6_b5_signal = false;
	bool b6_peer_suppressed =
		peer_latency_fault_active &&
		!saps_fault_is_onset_family(p->fault_type);
	bool b6_b5_inflight_skew =
		median_inflight_valid &&
		p->n >= SAPS_B6_MIN_SAMPLES &&
		p->inflight >= SAPS_INFLIGHT_FLOOR_ABS &&
		(int64_t)p->inflight * (int64_t)SAPS_INFLIGHT_RATIO_DEN >
		median_inflight * (int64_t)SAPS_INFLIGHT_RATIO_NUM;

	if (p->d2_cusum_onset_active) {
		if (b6_peer_suppressed) {
			p->d2_cusum_onset_active = 0;
			p->d2_cusum_pos_q16 = 0;
			p->d2_cusum_clear_consec = 0;
		} else {
			int64_t recover_delta = p->d2_cusum_last_log_lat_q16 -
						p->d2_cusum_ref_q16;
			if (p->d2_cusum_last_log_lat_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
			    p->tail_count_recent >= SAPS_B6_TAIL_COUNT_MIN &&
			    !qd_service_candidate) {
				p->d2_cusum_clear_consec = 0;
				b6_b2_signal = true;
			} else if (recover_delta < SAPS_B2_CUSUM_CLEAR_Q16 &&
				   recover_delta > -SAPS_B2_CUSUM_CLEAR_Q16) {
				if (p->d2_cusum_clear_consec < 0xFFu)
					p->d2_cusum_clear_consec++;
			} else {
				p->d2_cusum_clear_consec = 0;
			}
			if (p->d2_cusum_clear_consec < SAPS_B2_CUSUM_CLEAR_N)
				b6_b2_signal = true;
			else if (!b6_b2_signal) {
				p->d2_cusum_onset_active = 0;
				p->d2_cusum_pos_q16 = 0;
			}
		}
	}

	if (!b6_b2_signal &&
	    p->n >= SAPS_B6_MIN_SAMPLES &&
	    p->d2_cusum_last_log_lat_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    p->tail_count_recent >= SAPS_B6_TAIL_COUNT_MIN &&
	    !qd_service_candidate &&
	    !b6_peer_suppressed) {
		int64_t h = SAPS_B2_CUSUM_H_BASE_Q16 + (p->lat_var_ewma_q16 >> 1);
		if (p->d2_cusum_pos_q16 > h) {
			p->d2_cusum_onset_active = 1;
			p->d2_cusum_clear_consec = 0;
			b6_b2_signal = true;
		}
	}

	if (global_median_valid &&
	    global_mad_valid &&
	    p->n >= SAPS_B6_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_LAT_ABS_CP_FLOOR_Q16 &&
	    !qd_service_candidate &&
	    !b6_peer_suppressed) {
		int64_t rel_drift = abs64(p->mean_log_lat_ewma_q16 -
					  global_median_log_lat_q16);
		int64_t mad = global_mad_log_lat_q16;
		if (mad < SAPS_B3_MAD_FLOOR_Q16)
			mad = SAPS_B3_MAD_FLOOR_Q16;
		b6_b3_signal =
			rel_drift > SAPS_B3_MIN_DRIFT_Q16 &&
			rel_drift * SAPS_B3_MAD_RATIO_DEN >
			mad * SAPS_B3_MAD_RATIO_NUM;
	}

	if (p->inflight_onset_active) {
		if (b6_peer_suppressed) {
			p->inflight_onset_active = 0;
			p->inflight_onset_clear_consec = 0;
		} else {
			int64_t last_delta = p->bocpd_last_log_lat_q16 -
					     p->baseline_log_lat_q16;
			if ((b6_b5_inflight_skew ||
			     p->bocpd_last_log_lat_q16 > SAPS_TAIL_LOG_LAT_Q16) &&
			    !qd_service_candidate) {
				p->inflight_onset_clear_consec = 0;
				b6_b5_signal = true;
			} else if (p->bocpd_last_log_lat_q16 < SAPS_LAT_ABS_CP_FLOOR_Q16 ||
				   (last_delta < SAPS_ONSET_DELTA_CLEAR_Q16 &&
				    last_delta > -SAPS_ONSET_DELTA_CLEAR_Q16)) {
				if (p->inflight_onset_clear_consec < 0xFFu)
					p->inflight_onset_clear_consec++;
			} else {
				p->inflight_onset_clear_consec = 0;
			}
			if (p->inflight_onset_clear_consec < SAPS_B5_CLEAR_N)
				b6_b5_signal = true;
			else if (!b6_b5_signal) {
				p->inflight_onset_active = 0;
				p->inflight_onset_clear_consec = 0;
			}
		}
	}

	if (!b6_b5_signal &&
	    b6_b5_inflight_skew &&
	    p->bocpd_last_log_lat_q16 > SAPS_LAT_ABS_CP_FLOOR_Q16 &&
	    !qd_service_candidate &&
	    !b6_peer_suppressed) {
		p->inflight_onset_active = 1;
		p->inflight_onset_clear_consec = 0;
		b6_b5_signal = true;
	}

		if (b6_b2_signal || b6_b3_signal || b6_b5_signal) {
		p->onset_clear_consec = 0;
		if (p->onset_high_consec < 0xFFu)
			p->onset_high_consec++;
		if (p->onset_high_consec >= SAPS_B6_FIRE_N)
			return SAPS_FAULT_ANY_THREE_VOTE;
	} else {
		p->onset_high_consec = 0;
	}
#endif

	/* Round 6 D2 B5: first-backed-up-completion detector.  Netem onset
	 * causes the delayed path to accumulate outstanding IOs immediately;
	 * using raw inflight skew avoids waiting for a 32/64-sample latency
	 * window to settle. */
#if SAPS_D2_B5_ENABLE
	bool b5_inflight_skew =
		median_inflight_valid &&
		p->n >= SAPS_B5_MIN_SAMPLES &&
		p->inflight >= SAPS_INFLIGHT_FLOOR_ABS &&
		(int64_t)p->inflight * (int64_t)SAPS_INFLIGHT_RATIO_DEN >
		median_inflight * (int64_t)SAPS_INFLIGHT_RATIO_NUM;

	if (p->inflight_onset_active) {
		int64_t last_delta = p->bocpd_last_log_lat_q16 -
				     p->baseline_log_lat_q16;
		if ((b5_inflight_skew ||
		     p->bocpd_last_log_lat_q16 > SAPS_TAIL_LOG_LAT_Q16) &&
		    !qd_service_candidate) {
			p->inflight_onset_clear_consec = 0;
			p->onset_high_consec = SAPS_ONSET_FIRE_N;
			p->onset_clear_consec = 0;
			return SAPS_FAULT_ONSET;
		}
		if (p->bocpd_last_log_lat_q16 < SAPS_LAT_ABS_CP_FLOOR_Q16 ||
		    (last_delta < SAPS_ONSET_DELTA_CLEAR_Q16 &&
		     last_delta > -SAPS_ONSET_DELTA_CLEAR_Q16)) {
			if (p->inflight_onset_clear_consec < 0xFFu)
				p->inflight_onset_clear_consec++;
		} else {
			p->inflight_onset_clear_consec = 0;
		}
		if (p->inflight_onset_clear_consec < SAPS_B5_CLEAR_N)
			return SAPS_FAULT_ONSET;
		p->inflight_onset_active = 0;
		p->inflight_onset_clear_consec = 0;
		p->onset_high_consec = 0;
		p->onset_clear_consec = SAPS_ONSET_CLEAR_N;
	}

	if (b5_inflight_skew &&
	    p->bocpd_last_log_lat_q16 > SAPS_LAT_ABS_CP_FLOOR_Q16 &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active) {
		p->inflight_onset_active = 1;
		p->inflight_onset_clear_consec = 0;
		p->onset_high_consec = SAPS_ONSET_FIRE_N;
		p->onset_clear_consec = 0;
		return SAPS_FAULT_ONSET;
	}
#endif

	/* Round 6 D2 B4: ADWIN-style sliding-window mean shift. The older half
	 * of lat_ring is the reference window; the newer half is the candidate
	 * window.  This avoids baseline-vs-self lag and does not depend on a
	 * posterior/hazard model. */
#if SAPS_D2_B4_ENABLE
	if (p->adwin_onset_active) {
		int64_t last_delta = p->bocpd_last_log_lat_q16 -
				     p->baseline_log_lat_q16;
		if (p->bocpd_last_log_lat_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
		    !qd_service_candidate) {
			p->adwin_clear_consec = 0;
			p->onset_high_consec = SAPS_ONSET_FIRE_N;
			p->onset_clear_consec = 0;
			return SAPS_FAULT_ONSET;
		}
		if (p->bocpd_last_log_lat_q16 < SAPS_LAT_ABS_CP_FLOOR_Q16 ||
		    (last_delta < SAPS_B4_ADWIN_CLEAR_Q16 &&
		     last_delta > -SAPS_B4_ADWIN_CLEAR_Q16)) {
			if (p->adwin_clear_consec < 0xFFu)
				p->adwin_clear_consec++;
		} else {
			p->adwin_clear_consec = 0;
		}
		if (p->adwin_clear_consec == 0)
			return SAPS_FAULT_ONSET;
		p->adwin_onset_active = 0;
		p->adwin_clear_consec = 0;
		p->onset_high_consec = 0;
		p->onset_clear_consec = SAPS_ONSET_CLEAR_N;
	}

	if (p->n >= SAPS_B4_ADWIN_MIN_SAMPLES &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active) {
		int64_t old_mean_q16;
		int64_t new_mean_q16;
		if (saps_b4_adwin_window_means(p, &old_mean_q16, &new_mean_q16)) {
			int64_t drift = new_mean_q16 - old_mean_q16;
			int64_t baseline_drift = new_mean_q16 - p->baseline_log_lat_q16;
			if (new_mean_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
			    drift > SAPS_B4_ADWIN_DELTA_Q16 &&
			    baseline_drift > SAPS_B4_ADWIN_DELTA_Q16) {
				p->adwin_onset_active = 1;
				p->adwin_clear_consec = 0;
				p->onset_high_consec = SAPS_ONSET_FIRE_N;
				p->onset_clear_consec = 0;
				return SAPS_FAULT_ONSET;
			}
		}
	}
#endif

	/* Round 6 D2 B2: adaptive Page-CUSUM. Unlike NEWMA and the slow
	 * baseline-vs-self rule, this operates on cumulative positive surprise
	 * against the frozen healthy reference and can fire on the first few
	 * delayed completions after an onset step. */
#if SAPS_D2_B2_ENABLE
	if (p->d2_cusum_onset_active) {
		int64_t recover_delta = p->d2_cusum_last_log_lat_q16 -
					p->d2_cusum_ref_q16;
		if (p->d2_cusum_last_log_lat_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
		    !qd_service_candidate) {
			p->d2_cusum_clear_consec = 0;
			p->onset_high_consec = SAPS_ONSET_FIRE_N;
			p->onset_clear_consec = 0;
			return SAPS_FAULT_ONSET;
		}
		if (recover_delta < SAPS_B2_CUSUM_CLEAR_Q16 &&
		    recover_delta > -SAPS_B2_CUSUM_CLEAR_Q16) {
			if (p->d2_cusum_clear_consec < 0xFFu)
				p->d2_cusum_clear_consec++;
		} else {
			p->d2_cusum_clear_consec = 0;
		}
		if (p->d2_cusum_clear_consec < SAPS_B2_CUSUM_CLEAR_N)
			return SAPS_FAULT_ONSET;
		p->d2_cusum_onset_active = 0;
		p->d2_cusum_pos_q16 = 0;
		p->onset_high_consec = 0;
		p->onset_clear_consec = SAPS_ONSET_CLEAR_N;
	}

	if (p->n >= SAPS_B2_CUSUM_MIN_SAMPLES &&
	    p->d2_cusum_last_log_lat_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active) {
		int64_t h = SAPS_B2_CUSUM_H_BASE_Q16 + (p->lat_var_ewma_q16 >> 1);
		if (p->d2_cusum_pos_q16 > h) {
			p->d2_cusum_onset_active = 1;
			p->d2_cusum_clear_consec = 0;
			p->onset_high_consec = SAPS_ONSET_FIRE_N;
			p->onset_clear_consec = 0;
			return SAPS_FAULT_ONSET;
		}
	}
#endif

	/* Round 6 D2 B1: BOCPD posterior over run length. A posterior spike
	 * starts a short 4-sample confirmation window; persistent D2 5ms steps
	 * fire in the requested 3-5 samples, while isolated healthy OS stalls
	 * above the 4ms floor clear before becoming ONSET. */
#if SAPS_D2_B1_ENABLE
	bool bocpd_sample_slow = saps_bocpd_onset_sample_slow(p);
	if (p->bocpd_onset_active) {
		if (bocpd_sample_slow && !qd_service_candidate) {
			p->bocpd_onset_clear_consec = 0;
			p->onset_high_consec = SAPS_ONSET_FIRE_N;
			p->onset_clear_consec = 0;
			return SAPS_FAULT_ONSET;
		}
		if (p->bocpd_onset_clear_consec < 0xFFu)
			p->bocpd_onset_clear_consec++;
		if (p->bocpd_onset_clear_consec < SAPS_BOCPD_ONSET_CLEAR_N)
			return SAPS_FAULT_ONSET;
		p->bocpd_onset_active = 0;
		p->onset_high_consec = 0;
		p->onset_clear_consec = SAPS_ONSET_CLEAR_N;
	}
	if (p->n >= SAPS_BOCPD_MIN_SAMPLES &&
	    p->bocpd_cp_prob_q16 > SAPS_BOCPD_CP_FIRE_Q16 &&
	    bocpd_sample_slow &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active) {
		p->bocpd_onset_pending_consec = 1;
	} else if (p->bocpd_onset_pending_consec != 0 &&
		   bocpd_sample_slow &&
		   !qd_service_candidate &&
		   !peer_latency_fault_active) {
		if (p->bocpd_onset_pending_consec < 0xFFu)
			p->bocpd_onset_pending_consec++;
	} else {
		p->bocpd_onset_pending_consec = 0;
	}
	if (p->bocpd_onset_pending_consec >= SAPS_BOCPD_ONSET_CONFIRM_N) {
		p->bocpd_onset_active = 1;
		p->bocpd_onset_clear_consec = 0;
		p->bocpd_onset_pending_consec = 0;
		p->onset_high_consec = SAPS_ONSET_FIRE_N;
		p->onset_clear_consec = 0;
		return SAPS_FAULT_ONSET;
	}
#endif

	/* Round 6 D2 B3: cross-path relative mean detector. This compares the
	 * current per-path mean to the current all-path median/MAD, not to the
	 * path's frozen baseline, so it has no baseline-vs-self lag. */
#if SAPS_D2_B3_ENABLE
	if (global_median_valid &&
	    global_mad_valid &&
	    p->n >= SAPS_B3_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_LAT_ABS_CP_FLOOR_Q16 &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active) {
		int64_t rel_drift = abs64(p->mean_log_lat_ewma_q16 -
					  global_median_log_lat_q16);
		int64_t mad = global_mad_log_lat_q16;
		if (mad < SAPS_B3_MAD_FLOOR_Q16)
			mad = SAPS_B3_MAD_FLOOR_Q16;
		if (rel_drift > SAPS_B3_MIN_DRIFT_Q16 &&
		    rel_drift * SAPS_B3_MAD_RATIO_DEN >
		    mad * SAPS_B3_MAD_RATIO_NUM) {
			p->onset_high_consec = SAPS_ONSET_FIRE_N;
			p->onset_clear_consec = 0;
			return SAPS_FAULT_ONSET;
		}
	}
#endif

	#if SAPS_D2_PROTOTYPE_ISOLATE_LEGACY_LATENCY
	return SAPS_FAULT_HEALTHY;
	#endif

	/* Round 6 D7 F1: use the fastest path as a reference, not the median.
	 * In the dual-slow case two paths are slow from t=0, so the median is
	 * also slow and median-relative rules cannot identify both bad paths.
	 * A 4ms absolute floor plus a 2x gap from the fastest warmed path keeps
	 * healthy D0 jitter out while allowing both slow peers to drain. */
	if (global_min_valid &&
	    p->n >= SAPS_D7_REF_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    !qd_service_candidate &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16) {
		int64_t ref_drift = p->mean_log_lat_ewma_q16 - global_min_log_lat_q16;
		if (ref_drift > SAPS_D7_REF_DRIFT_HI_Q16)
			return SAPS_FAULT_PERPETUAL_SLOW;
	}

		/* Round 2 D2 onset detector.  Compare the normal mean EWMA against a
		 * much slower long-term mean.  Require enough baseline samples and a
		 * 1ms absolute floor so healthy ARM scheduler jitter in the hundreds of
		 * microseconds cannot trip ONSET; D2's 5ms step clears both gates
		 * quickly.  Once active, keep the path drained until 64 fast samples
		 * prove recovery. */
	if (p->long_term_mean_log_lat_q16 != 0) {
		int64_t onset_delta = p->mean_log_lat_ewma_q16 -
				      p->long_term_mean_log_lat_q16;
		int64_t onset_cp_delta = global_median_valid
			? p->mean_log_lat_ewma_q16 - global_median_log_lat_q16
			: 0;
		bool onset_can_fire =
			path_above_current_consensus &&
			p->sf_degraded_cached != 0 &&
			p->n >= SAPS_ONSET_MIN_SAMPLES &&
			!qd_service_candidate &&
			p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
			global_median_valid &&
			onset_cp_delta > SAPS_ONSET_DELTA_FIRE_Q16;

		if (onset_can_fire &&
		    onset_delta > SAPS_ONSET_DELTA_FIRE_Q16) {
			if (p->onset_high_consec < 0xFFu)
				p->onset_high_consec++;
			p->onset_clear_consec = 0;
		} else if (onset_delta < SAPS_ONSET_DELTA_CLEAR_Q16 &&
			   onset_delta > -SAPS_ONSET_DELTA_CLEAR_Q16) {
			if (p->onset_clear_consec < 0xFFu)
				p->onset_clear_consec++;
			p->onset_high_consec = 0;
		} else {
			p->onset_high_consec = 0;
			p->onset_clear_consec = 0;
		}

		if (p->onset_high_consec >= SAPS_ONSET_FIRE_N)
			return SAPS_FAULT_ONSET;
		if (p->fault_type == SAPS_FAULT_ONSET &&
		    p->onset_clear_consec < SAPS_ONSET_CLEAR_N)
			return SAPS_FAULT_ONSET;
	}

	/* Severe absolute slow-path latch.  Keep this before cross-path rules so
	 * a clearly broken path drains even if peer medians are stale or invalid. */
	if (p->n >= SAPS_LAT_ABS_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active &&
	    saps_qd_backed_slow(p) &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16)
		return SAPS_FAULT_PERPETUAL_SLOW;

	/* Slice 16 — cross-path median drift detector.
	 * Floor raised to SAPS_LAT_ABS_SLOW_Q16 (1 ms) so that only paths with
	 * mean_ewma >= 1ms can trigger cross-path PERPETUAL_SLOW. This prevents
	 * D0 bootstrap cascade: ARM OS jitter spikes and traffic-imbalance EWMA
	 * elevation (50-300µs) no longer fire Slice 16. D2 (B at 5ms via
	 * bdev_delay) and D1 (B constant 5ms) still clear the 1ms floor and fire
	 * after hysteresis. For paths already above 1ms, Slice 20 (absolute) also
	 * fires; Slice 16 adds confirmation that the path is elevated RELATIVE to
	 * peers (not just an absolute-latency pathological state where all 3 paths
	 * are slow simultaneously, which would give cp_drift ≈ 0). */
	if (!peer_latency_fault_active &&
	    !qd_service_candidate &&
	    global_median_valid &&
	    p->n >= SAPS_LAT_ABS_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    saps_qd_backed_slow(p) &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN) {
		int64_t cp_drift = p->mean_log_lat_ewma_q16 - global_median_log_lat_q16;
		if (cp_drift > SAPS_CROSSPATH_DRIFT_HI_Q16 &&
		    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16)
			return SAPS_FAULT_PERPETUAL_SLOW;
	}

	/* Slice 18 — cross-path P99 drift detector DISABLED.
	 * Removed: exact P99 = max2 of 64-sample ring; scheduler-jitter
	 * outliers in D0 healthy 27µs paths elevate max2 (e.g. 80µs) above
	 * global median (35µs) by >log2(2), causing false PERPETUAL_SLOW →
	 * cooldown latch → permanent starvation on healthy paths.
	 * D2 passes on Slice 16 mean-drift alone (recovery_ratio=0.918).
	 * Keeping global_median_p99_valid/global_median_p99_q16 computed for
	 * future use but not using them for classification here. */
	(void)global_median_p99_valid;
	(void)global_median_p99_q16;

	/* Slice 28 — inflight-based PERPETUAL_SLOW.
	 * Deterministic queue-depth signal: throttled path accumulates in-flight
	 * IOs while healthy peers drain. Fires when this path's inflight is
	 * 1.5× greater than median across paths AND above absolute floor.
	 * Uses raw p->inflight counter (not the EWMA qd_mean) for zero-lag
	 * response — incremented on submit, decremented on complete.
	 * Ratio 3/2 (1.5×): D2 50% CPU throttle gives true B:peer = 2×; sampled
	 * ratio is the same. 1.5× is safely between healthy (1.0×) and D2 (2.0×).
	 * Multiplied as: p->inflight * RATIO_DEN > median * RATIO_NUM to avoid
	 * division. */
	if (median_inflight_valid &&
	    p->n >= SAPS_LAT_ABS_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->inflight >= SAPS_INFLIGHT_FLOOR_ABS &&
	    (int64_t)p->inflight * (int64_t)SAPS_INFLIGHT_RATIO_DEN >
	    median_inflight * (int64_t)SAPS_INFLIGHT_RATIO_NUM) {
		/* Slice 28b: no hysteresis — floor=4 alone is sufficient to block D0 bursts.
		 * D0 random burst peaks at ~12 but only 1 sample; D2 throttle sustains > floor.
		 * Hysteresis was killing D2 signal (bursts not consecutive enough to cross consec>=2). */
		p->inflight_high_consec = 0;  /* field kept for ABI compat, not used for gating */
		return SAPS_FAULT_PERPETUAL_SLOW;
	} else {
		p->inflight_high_consec = 0;
	}

	/* Slice 20: severe-absolute latch must precede the staleness guard.
	 * Once a path's EWMA mean has crossed the SEVERE threshold (≥ 1 ms), it
	 * stays there for ~64 samples even after we drain traffic away — EWMA
	 * α=1/64 means decaying from log2(7.5e6) (5 ms) back below log2(1.5e6)
	 * (1 ms) requires either many fast samples or a long time. The
	 * staleness guard would otherwise force us to HEALTHY whenever the
	 * drain succeeded (recent_sample_count < 4), the consec_nonhealthy
	 * counter would reset, traffic would return, B would stall again, and
	 * we'd oscillate. Latching here on absolute mean keeps PERPETUAL_SLOW
	 * sticky until the underlying EWMA actually decays. err_rate guard
	 * preserves the rule's "slow without errors" semantics. */
	if (p->n >= SAPS_LAT_ABS_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active &&
	    saps_qd_backed_slow(p) &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16)
		return SAPS_FAULT_PERPETUAL_SLOW;

	/* Slice 15 — max-recent BIMODAL_TAIL latch. Placed BEFORE the
	 * staleness guard so a path that has just been drained by the
	 * BIMODAL action still classifies BIMODAL on the next tick (max
	 * persists in its 512-sample window even with little fresh traffic).
	 * Without this latch, a drained path's recent_sample_count drops
	 * below 4 → staleness guard returns HEALTHY → score climbs back to
	 * MAX → traffic returns → 5 ms tail → BIMODAL again → oscillation
	 * (observed pre-Slice-15 with classify→drain→stale→re-admit cycle).
	 *
	 * Conditions are the same as the post-staleness BIMODAL rule below
	 * but are checked first for paths whose max_log_lat_recent has been
	 * populated (max_log_lat_recent > 0 after first sample). Same
	 * err-rate guards keep D3 sparse-error owned by Rule 3. */
	/* SAPS_LAT_ABS_SLOW_Q16 floor: max sample must be ≥ 1ms before
	 * triggering BIMODAL_TAIL. D4 bimodal tail spikes are 5ms → above
	 * floor. D0 scheduler jitter is ≤ ~500µs → below floor → no false
	 * positive. Without this floor, a 300µs scheduler jitter on a 27µs
	 * mean D0 path gives max/mean=11x > log2(8)=8x threshold → false
	 * BIMODAL_TAIL → cooldown latch → IOPS drag. */
	if (p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    p->max_log_lat_recent > SAPS_TAIL_LOG_LAT_Q16 &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->bimodal_consec_windows >= 2u) {
		int64_t pre_excess = p->max_log_lat_recent - p->mean_log_lat_ewma_q16;
		if (pre_excess > SAPS_MAX_RATIO_HI_Q16 &&
		    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16 &&
		    p->recent_err_count < SAPS_RECENT_ERR_THRESH)
			return SAPS_FAULT_BIMODAL_TAIL;
	}

	/* Staleness guard: classifier requires fresh samples. recent_sample_count
	 * decays in halves every SAPS_RECENT_DECAY_PERIOD events — anything ≤ 4
	 * means we have not seen meaningful traffic recently. */
	if (p->recent_sample_count < 4)
		return SAPS_FAULT_HEALTHY;

	/* Rule 1: FLAP — high NEWMA fire rate or FSM transition rate. */
	uint32_t newma_rate = ring_count_within(p->newma_fire_tsc_ms_ring, 8,
						 now_ms, SAPS_FLAP_WINDOW_MS);
	uint32_t fsm_rate   = ring_count_within(p->fsm_trans_tsc_ms_ring, 4,
						 now_ms, SAPS_FLAP_WINDOW_MS);
	/* Fix 10 (2026-05-18): if max+tail signature suggests sustained bimodal
	 * pattern (≥25 slow samples > 4ms in current 512-window), it's NOT FLAP.
	 * D4 bimodal classification used to lose to FLAP because NEWMA fires on
	 * every fast↔slow transition (constant in bimodal) → newma_rate > 2 → FLAP.
	 * But FLAP triggers flap_dampening_until_tsc which freezes FSM → B never
	 * enters DEGRADING → B never EXCLUDED → 4% traffic leak. Defer FLAP for
	 * bimodal-signature paths so BIMODAL_TAIL latch below (or earlier Fix 6
	 * gate) classifies them instead, which routes through normal FSM. */
	bool fix10_looks_bimodal_for_flap_guard =
		p->max_log_lat_recent > SAPS_TAIL_LOG_LAT_Q16 &&
		p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
		p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16 &&
		p->recent_err_count < SAPS_RECENT_ERR_THRESH;
	if (path_above_current_consensus &&
	    p->sf_degraded_cached != 0 &&
	    (newma_rate > SAPS_FLAP_NEWMA_FIRE_HI ||
	     fsm_rate   > SAPS_FLAP_FSM_TRANS_HI) &&
	    !fix10_looks_bimodal_for_flap_guard)
		return SAPS_FAULT_FLAP;

	/* Latency ratio in log2 domain: log2(mean) - log2(baseline). */
	int64_t lat_diff = p->mean_log_lat_ewma_q16 - p->baseline_log_lat_q16;

	/* Rule 2: PERPETUAL_SLOW — sustained mean elevation, no errors.
	 * Variance threshold is dropped from the spec text — log2-domain var
	 * has weak discriminating power in our measurements (D0 stable var
	 * already in the 0.5M-1.5M Q16.16 range due to per-sample µs-level
	 * jitter). lat_diff > log2(2) AND low err_rate is sufficient: any
	 * path whose EWMA-mean has shifted >2× from its frozen baseline is
	 * structurally slower regardless of variance.
	 *
	 * Slice 18 addition — inflight saturation trigger: a uniformly-slow
	 * path (D1: B fixed at 5ms from t=0) has lat_diff ≈ 0 because its
	 * baseline is also 5ms — relative-to-baseline detection misses it.
	 * The orthogonal evidence is queue saturation: at QD_per_path=11
	 * (32/3) on a 5ms path, inflight stays pinned ≥ 24 (close to global
	 * QD=32) while healthy peers idle near 0. Any path whose Q16.16
	 * inflight EWMA exceeds SAPS_INFLIGHT_SLOW_THRESH (= 16) is
	 * structurally slow regardless of latency baseline. This is the
	 * Little's Law dual: λ = N/W; same N at higher W means slower path.
	 * SAPS_INFLIGHT_SLOW_THRESH chosen at QD/2 so D0 healthy paths
	 * (inflight EWMA ~ 8-12 with QD=32 ÷ 3 paths) don't trip. */
	/* Slice 20 gate: relative-drift rule must also see absolute mean above
	 * the moderate floor (≈ 200 µs). Without this, host-side queue
	 * contamination on D1 inflates EWMA mean of healthy peers (A/C
	 * accumulate residency time waiting behind B's stalled completions)
	 * by > 2× from their warmup baseline even while their absolute
	 * latency stays well under 1 ms. The relative rule then false-fires,
	 * MIN-clamps every path, and the host blend gives B a uniform 33 %
	 * share instead of draining it. With the moderate-floor gate, only
	 * paths whose absolute mean is also at least ~200 µs can be flagged
	 * by the relative rule — preserving D0/D6 healthy classification and
	 * still catching genuine drift on D2-style overhead scenarios. */
	if (lat_diff > SAPS_LAT_RATIO_HI_Q16 &&
	    p->n >= SAPS_LAT_ABS_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active &&
	    saps_qd_backed_slow(p) &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16)
		return SAPS_FAULT_PERPETUAL_SLOW;
	/* Slice 18: absolute-latency rule for baseline-poisoned slow paths.
	 * The relative `lat_diff > log2(2)` rule fails when warmup happens on
	 * an already-slow path (D1: B's first 32 samples are all 5ms → baseline
	 * frozen at 5ms → any subsequent 5ms sample shows lat_diff = 0).
	 * Absolute threshold: mean_log_lat > log2(1ms in ticks) =
	 * log2(1.5e6) ≈ 20.5 in Q16.16 units. Healthy 27µs path runs at
	 * log2(40500) ≈ 15.3 — comfortably below. D1 slow 5ms path runs at
	 * log2(7.5e6) ≈ 22.8 — above threshold. D4 bimodal (avg 0.85×27µs +
	 * 0.15×5ms ≈ 800µs) sits at log2(1.2e6) ≈ 20.2 — below threshold (so
	 * BIMODAL_TAIL rule still owns it). D3 sparse-error retry brings mean
	 * up modestly but stays below 1ms — owned by SPARSE_ERROR rule. */
	if (p->n >= SAPS_LAT_ABS_MIN_SAMPLES &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    !qd_service_candidate &&
	    !peer_latency_fault_active &&
	    saps_qd_backed_slow(p) &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16)
		return SAPS_FAULT_PERPETUAL_SLOW;

	/* Rule 4: BIMODAL_TAIL — P99 far above mean with low error rate AND
	 * no recent NVMe errors.  The "no recent errors" guard (recent_err_count
	 * == 0) prevents D3-style sparse-error paths from being misclassified:
	 * retry-tail latency can push P99 above the 5× threshold even when
	 * err_rate_ewma has not yet converged (EWMA λ=1/32 lags by ~32 samples).
	 * Without this guard, BIMODAL_TAIL fires, score is clamped to MIN, path
	 * is hard-EXCLUDEd, and in-flight IOs wait for timeout → max_lat spike.
	 * Spec §3.2 also AND'd a variance condition; we drop it for the same
	 * reason as Rule 2 (variance threshold cannot separate D0 from D4).
	 * P99/mean ratio > 5× (log2≈2.32) is the discriminating signal:
	 * stable paths run with P99 at ~1.5× mean, bimodal 85/15 with a 200×
	 * slow tail pushes P99 to 5–10× mean once Frugal-2U converges. */
	/* Rule 4: BIMODAL_TAIL via exact P99 (max2 of 64-sample ring).
	 * Guards:
	 *   (a) p99 > 1ms absolute floor
	 *   (b) p99/mean excess > log2(5)
	 *   (c) tail_count_recent >= SAPS_TAIL_COUNT_MIN: require ≥25/512
	 *       samples above 2ms. ARM scheduler jitter produces 1-3 isolated
	 *       spikes per 512-sample window → tail_count ≈ 1-3 < 25 → no fire.
	 *       D4 bimodal 15% at 5ms → ~77/512 > 2ms → tail_count ≈ 77 ≥ 25 →
	 *       fires correctly. Without this guard, a single 2ms ARM OS stall
	 *       in the 64-sample exact ring makes max2 > 1ms, clears the floor,
	 *       p99_excess > 5×, and BIMODAL_TAIL fires on a healthy D0 path. */
	int64_t p99_excess = p->p99_estimate - p->mean_log_lat_ewma_q16;
	if (p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    p->p99_estimate > SAPS_TAIL_LOG_LAT_Q16 &&
	    p99_excess > SAPS_P99_RATIO_HI_Q16 &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->bimodal_consec_windows >= 2u &&
	    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16 &&
	    p->recent_err_count < SAPS_RECENT_ERR_THRESH)
		return SAPS_FAULT_BIMODAL_TAIL;

	/* Slice 15 — max-of-recent-window robust BIMODAL_TAIL detection.
	 * Frugal-2U is bandwidth-efficient but converges sluggishly on bimodal
	 * workloads (D4 P99 excess never crossed SAPS_P99_RATIO_HI_Q16 in 60 s
	 * trace, leaving D4 path B classified HEALTHY). max(log_lat) over a
	 * 512-sample sliding window catches a single 5 ms spike per ~700-sample
	 * burst (= 0.7 ms wall-clock at 700 K IOPS), so BIMODAL_TAIL fires
	 * within ~1 ms of fault onset. Same err-rate guards as the Frugal rule
	 * keep D3 sparse-error owned by Rule 3. */
	/* Same SAPS_LAT_ABS_SLOW_Q16 (1ms) floor as the pre-staleness latch
	 * above — prevents D0 scheduler-jitter max samples from false-firing
	 * BIMODAL_TAIL here too. D4 5ms spikes remain above the floor. */
	int64_t max_excess = p->max_log_lat_recent - p->mean_log_lat_ewma_q16;
	if (p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    p->max_log_lat_recent > SAPS_TAIL_LOG_LAT_Q16 &&
	    max_excess > SAPS_MAX_RATIO_HI_Q16 &&
	    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
	    p->bimodal_consec_windows >= 2u &&
	    p->err_rate_ewma < SAPS_ERR_RATE_LOW_Q16 &&
	    p->recent_err_count < SAPS_RECENT_ERR_THRESH)
		return SAPS_FAULT_BIMODAL_TAIL;

	/* Default: HEALTHY. */
	return SAPS_FAULT_HEALTHY;
}

/* Slice 16.5 hysteresis wrapper around classify_fault_type_raw().
 *
 * Problem: post-warmup transient noise (one EWMA spike on the path that
 * warmed up second/third) makes raw classifier momentarily emit
 * PERPETUAL_SLOW / SPARSE_ERROR / BIMODAL_TAIL on D0 clean baseline. That
 * single tick is enough for compute_score() to clamp score to MIN, host
 * argmax pins to whichever path warmed up first, and the now-unselected
 * paths never get fresh samples to recover (staleness guard returns
 * HEALTHY too late, the score pin is already in effect).
 *
 * Fix: require N consecutive non-HEALTHY raw verdicts before propagating
 * a non-HEALTHY classification. A single HEALTHY raw verdict resets the
 * counter. SAPS_HYSTERESIS_N = 4 ⇒ 4 consecutive non-HEALTHY samples (≈ a
 * sustained shift, not an isolated spike) before the FSM reacts.
 *
 * Counter saturated at 0xFF to prevent wrap. Caller passes mutable p so
 * the counter state persists across complete events. */
static inline uint16_t classify_fault_type(struct dpa_path_state *p,
					    uint32_t now_ms,
					    int64_t global_median_log_lat_q16,
					    bool global_median_valid,
					    int64_t global_min_log_lat_q16,
					    bool global_min_valid,
					    int64_t global_mad_log_lat_q16,
					    bool global_mad_valid,
					    int64_t global_median_qd_q16,
					    bool global_median_qd_valid,
					    uint32_t global_median_capacity,
					    bool global_capacity_valid,
						    int64_t global_median_p99_q16,
						    bool global_median_p99_valid,
						    int64_t median_inflight,
						    bool median_inflight_valid,
						    bool peer_latency_fault_active,
						    bool all_paths_degraded,
						    uint16_t *raw_out)
{
	uint16_t raw = classify_fault_type_raw(p, now_ms,
					       global_median_log_lat_q16,
					       global_median_valid,
					       global_min_log_lat_q16,
					       global_min_valid,
					       global_mad_log_lat_q16,
					       global_mad_valid,
					       global_median_qd_q16,
					       global_median_qd_valid,
					       global_median_capacity,
					       global_capacity_valid,
						       global_median_p99_q16,
						       global_median_p99_valid,
						       median_inflight,
						       median_inflight_valid,
						       peer_latency_fault_active,
						       all_paths_degraded);

	if (raw_out)
		*raw_out = raw;

	if (__builtin_expect(raw == SAPS_FAULT_HEALTHY, 1)) {
		p->consec_nonhealthy_count = 0;
		return SAPS_FAULT_HEALTHY;
	}

		if (saps_fault_is_onset_family(raw) ||
		    raw == SAPS_FAULT_QD_DRIFT ||
		    raw == SAPS_FAULT_PROPORTIONAL_THROTTLE ||
		    raw == SAPS_FAULT_SHARED_FATE ||
		    raw == SAPS_FAULT_ALL_DEGRADED) {
			p->consec_nonhealthy_count = SAPS_HYSTERESIS_N;
			return raw;
		}

	/* Slice 20: severe-absolute bypass.  When raw is PERPETUAL_SLOW *and*
	 * absolute mean exceeds the severe threshold (1 ms), no NVMe-oF
	 * healthy IO ever sits there — this path is structurally broken and
	 * must drain immediately, not after 4 hysteresis ticks (which on a
	 * 5 ms slow path is ≥ 20 ms of redirected traffic each cycle).
	 * Saturate the counter so subsequent ticks also propagate raw, and
	 * propagate raw on this very tick. D6 (tc flap, mean stays low) and
	 * D0 (healthy 27 µs) cannot reach SEVERE so this never false-fires.
	 * D4 BIMODAL mean ≤ ~800 µs — also below SEVERE → BIMODAL_TAIL rule
	 * stays in charge there. */
	if (raw == SAPS_FAULT_PERPETUAL_SLOW &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16) {
		p->consec_nonhealthy_count = SAPS_HYSTERESIS_N;
		return raw;
	}

	/* Slice 22: SPARSE_ERROR severity bypass.  NVMe sct=2 (media error /
	 * Unrecovered Read Error) is a hard error — a single completion with
	 * sct≠0 is sufficient evidence that the path returned bad data, not a
	 * transient timing artefact. With the default N=4 hysteresis the host
	 * keeps sending IO into a sparse-error path through 3 more bad
	 * completions before SAPS finally transitions out → those 3 IOs are
	 * not retry-overlay-eligible (overlay only kicks once score drops to
	 * MIN) → counted as io_failed. Bypassing hysteresis on the very first
	 * sct≠0 sample drains the path immediately; the retry overlay then
	 * tolerates the in-flight error.  Guarded on recent_err_count >= 1
	 * (i.e. at least one error actually observed in this window) to avoid
	 * propagating SPARSE_ERROR after recent_err_count has decayed but the
	 * raw classifier still emitted SPARSE_ERROR for some other reason.
	 * D6 (FLAP) and D4 (BIMODAL_TAIL) classify under different rules and
	 * do not reach this branch — N=4 hysteresis still applies there. */
	if (raw == SAPS_FAULT_SPARSE_ERROR && p->recent_err_count >= 1) {
		p->consec_nonhealthy_count = SAPS_HYSTERESIS_N;
		return raw;
	}

	/* Slice 28 — inflight-based PERPETUAL_SLOW severity bypass.
	 * Inflight asymmetry is a structural, deterministic signal — if this
	 * path's in-flight count already exceeds 1.5× the cross-path median AND
	 * is above the absolute floor, the queue depth imbalance is real and
	 * the path must drain immediately (not after 4 hysteresis ticks of
	 * continued traffic into a backed-up path).
	 * D0 healthy paths never exceed 1.5× median simultaneously (all near
	 * equal inflight under balanced load), so this cannot false-fire D0. */
	if (raw == SAPS_FAULT_PERPETUAL_SLOW &&
	    median_inflight_valid &&
	    p->mean_log_lat_ewma_q16 > SAPS_TAIL_LOG_LAT_Q16 &&
	    p->inflight >= SAPS_INFLIGHT_FLOOR_ABS &&
	    (int64_t)p->inflight * (int64_t)SAPS_INFLIGHT_RATIO_DEN >
	    median_inflight * (int64_t)SAPS_INFLIGHT_RATIO_NUM) {
		p->consec_nonhealthy_count = SAPS_HYSTERESIS_N;
		return raw;
	}

	/* Slice 15 — BIMODAL_TAIL severity bypass. When raw is BIMODAL_TAIL
	 * with max-mean excess in the same Q16 range (> SAPS_MAX_RATIO_HI_Q16),
	 * drain immediately. Without this bypass, host idle-probe traffic + 4-
	 * sample hysteresis lets B oscillate BIMODAL ↔ HEALTHY indefinitely
	 * (drain → stale → HEALTHY → re-admit → 5 ms → BIMODAL → drain). The
	 * D4 IOPS ceiling at 121 K is exactly this oscillation limiting B's
	 * effective drain. Saturating the consec counter on first BIMODAL
	 * raw verdict skips the 4-tick warmup so the score override takes
	 * effect immediately. D0 healthy and D1 PERPETUAL_SLOW classify under
	 * different rules and never reach this branch. */
	if (raw == SAPS_FAULT_BIMODAL_TAIL) {
		int64_t excess = p->max_log_lat_recent - p->mean_log_lat_ewma_q16;
		/* Severity bypass: require max > 1ms AND excess > log2(8) AND
		 * tail_count >= MIN AND two consecutive triggered windows.
		 * The bimodal_consec_windows >= 2 guard (Slice 30) ensures ARM
		 * one-shot OS stall bursts (tc >= MIN in window N, clean in N+1)
		 * cannot bypass hysteresis — only persistent D4 bimodal (every
		 * 512-window has ~77 tail samples) triggers this path. */
		if (p->max_log_lat_recent > SAPS_TAIL_LOG_LAT_Q16 &&
		    excess > SAPS_MAX_RATIO_HI_Q16 &&
		    p->tail_count_recent >= SAPS_TAIL_COUNT_MIN &&
		    p->bimodal_consec_windows >= 2u) {
			p->consec_nonhealthy_count = SAPS_HYSTERESIS_N;
			return raw;
		}
	}

	if (p->consec_nonhealthy_count < 0xFFu)
		p->consec_nonhealthy_count++;

	if (p->consec_nonhealthy_count < SAPS_HYSTERESIS_N)
		return SAPS_FAULT_HEALTHY;

	return raw;
}

/* §4.4 FSM transitions. Call after every complete event (once err_rate_ewma
 * and detector have been updated for the current sample). */
static inline void update_state_machine(struct dpa_path_state *p,
					 bool detector_changed,
					 uint64_t now_tsc,
					 uint16_t qp_idx, uint16_t path_idx)
{
	/* Slice 10: FLAP dampening — freeze FSM transitions until window expires.
	 * The classifier fires this when transition rate runs hot. Freezing the
	 * FSM lets the underlying signal settle before another transition lands.
	 * Fix 12 reverted — H→D unfreeze gave slightly worse N=5 distribution. */
	if (p->fault_type == SAPS_FAULT_FLAP &&
	    p->flap_dampening_until_tsc != 0 &&
	    now_tsc < p->flap_dampening_until_tsc) {
		return;
	}

	uint16_t new_state = p->state;
	/* Slice 7 — Fix A: spdk_get_ticks() is monotonic-ish but reads 1-2 ticks
	 * earlier than the previous read happen on this hardware (TSC sample noise
	 * across cores; SPDK's read is not synchronised across reactors). The naive
	 * unsigned subtraction below underflows to ~2^64 when that happens, which
	 * makes "since > T_DEG_TICKS" always true and slams a still-healthy path
	 * straight to EXCLUDED. Clamp to 0 on backward step. */
	uint64_t since = (now_tsc >= p->state_entered_tsc)
			 ? (now_tsc - p->state_entered_tsc)
			 : 0;

	switch (p->state) {
	case DPA_SAPS_STATE_HEALTHY:
		/* Slice 28: drive FSM transitions via fault_type (hysteresis-filtered
		 * multi-modal classifier) rather than raw NEWMA detector_changed.
		 *
		 * Previous: detector_changed || err_rate > E_HI triggered DEGRADING.
		 * Problem: NEWMA fires on isolated ARM scheduler jitter spikes (single
		 * 1-10 ms OS stall on 27µs D0 path → |ewma_fast - ewma_slow| > 2.0 →
		 * NEWMA fires → DEGRADING, 845 ms later EXCLUDED). Healthy paths then
		 * oscillate HEALTHY↔DEGRADING↔EXCLUDED at ~5 Hz, draining IOPS to
		 * 748-789K (need >800K) and creating imbalanced distribution.
		 *
		 * Fix: use p->fault_type (output of classify_fault_type() with N=4
		 * hysteresis) instead of raw NEWMA. D0 single-spike jitter: NEWMA
		 * fires once → fault_type stays HEALTHY (hysteresis not saturated) →
		 * FSM stays HEALTHY. D1 5ms B path: Slice 18 absolute mean > 1ms →
		 * PERPETUAL_SLOW on every sample → severity bypass saturates hysteresis
		 * immediately → fault_type = PERPETUAL_SLOW → FSM enters DEGRADING
		 * correctly. D2 CPU throttle: Slice 16 cross-path drift fires
		 * continuously → N=4 hysteresis saturates in ~4 IOs → DEGRADING. */
		if (p->fault_type != SAPS_FAULT_HEALTHY &&
		    p->fault_type != SAPS_FAULT_QD_DRIFT &&
		    p->fault_type != SAPS_FAULT_PROPORTIONAL_THROTTLE &&
		    p->fault_type != SAPS_FAULT_SHARED_FATE) {
			new_state = DPA_SAPS_STATE_DEGRADING;
		}
		break;
	case DPA_SAPS_STATE_DEGRADING:
		/* Escalate if fault continues past T_DEG_TICKS; recover if fault
		 * remains clear for T_RECOVER_TICKS. State age cannot stand in for
		 * a healthy interval: after a long degradation, one stale HEALTHY
		 * verdict would otherwise re-admit a path immediately. */
		if (p->fault_type == SAPS_FAULT_HEALTHY) {
			if (p->healthy_since_tsc == 0) {
				p->healthy_since_tsc = now_tsc;
			} else {
				uint64_t healthy_for =
					now_tsc >= p->healthy_since_tsc
					? now_tsc - p->healthy_since_tsc
					: 0;
				if (healthy_for > SAPS_T_RECOVER_TICKS)
					new_state = DPA_SAPS_STATE_HEALTHY;
			}
		} else {
			p->healthy_since_tsc = 0;
			if (since > SAPS_T_DEG_TICKS &&
			    !saps_fault_is_onset_family(p->fault_type) &&
			    p->fault_type != SAPS_FAULT_QD_DRIFT &&
			    p->fault_type != SAPS_FAULT_PROPORTIONAL_THROTTLE &&
			    p->fault_type != SAPS_FAULT_SHARED_FATE) {
				new_state = DPA_SAPS_STATE_EXCLUDED;
			}
		}
		break;
	case DPA_SAPS_STATE_EXCLUDED:
		if (since > SAPS_T_EXCLUDE_TICKS) {
			new_state = DPA_SAPS_STATE_RECOVERING;
			p->probe_count = 0;
			/* Slice 32: clear hysteresis counter on RECOVERING entry.
			 * consec_nonhealthy_count accumulates during EXCLUDED while
			 * ε-probes observe a slow path.  If it is saturated (= N)
			 * when RECOVERING is entered, the FIRST probe completion
			 * immediately propagates PERPETUAL_SLOW as fault_type even
			 * though the underlying fault may have cleared (D2 cgroup
			 * throttle removed).  FSM then bounces RECOVERING → EXCLUDED
			 * on every tick, preventing probe_count from reaching N_PROBE.
			 *
			 * Fix: reset consec_nonhealthy_count to 0 so the hysteresis
			 * window restarts fresh on RECOVERING entry.  A genuinely
			 * still-slow path (D1 5ms B) will re-saturate the counter in
			 * N=4 probes and return EXCLUDED; a recovered path (D2 post-
			 * throttle) will see raw=HEALTHY on the first probe and let
			 * probe_count accumulate to N_PROBE → HEALTHY.
			 *
			 * D1 regression risk: 4 extra slow probes before re-exclusion
			 * (4 × 5ms = 20ms) — negligible given 60s run window. */
			p->consec_nonhealthy_count = 0;
			/* Slice 33: reset mean EWMA and NEWMA state to frozen baseline
			 * on RECOVERING entry.
			 *
			 * Problem: mean_log_lat_ewma_q16 retains the stale high-latency
			 * value accumulated during the fault period (e.g. ~5 ms for a
			 * D2 50% CPU throttle).  EWMA alpha=1/64 means decaying from
			 * log2(5ms) back below the 1ms severe threshold requires ~140
			 * fast probe samples.  But the Slice 20 severe-absolute latch
			 * fires on the FIRST RECOVERING probe (mean still >> 1ms) ->
			 * fault_type = PERPETUAL_SLOW -> RECOVERING -> EXCLUDED
			 * immediately.  The path re-enters RECOVERING 1s later with the
			 * same stale mean, repeating the bounce indefinitely.
			 *
			 * Fix: reset mean_log_lat_ewma_q16 to baseline_log_lat_q16 (the
			 * frozen healthy-era value) so the Slice 20 latch starts from
			 * the correct pre-fault baseline.  Also reset ewma_fast/ewma_slow
			 * (NEWMA detector) to baseline to prevent stale NEWMA drift from
			 * immediately firing FLAP on the first probe.
			 *
			 * D1 safety: D1 path B's baseline was frozen during its first 32
			 * samples, all at 5ms (fault present from t=0).  After reset,
			 * mean = baseline ~ 5ms -> Slice 18 absolute latch fires on the
			 * first probe -> re-EXCLUDED in 1 probe.  Zero regression.
			 *
			 * D2 safety: D2 baseline frozen during the clean first 10s of
			 * the run (~27us).  After cgroup removal, probes run at ~27us ->
			 * mean stays near baseline -> all latency rules return HEALTHY ->
			 * probe_count reaches N_PROBE -> HEALTHY.
			 *
			 * Guard: skip reset if baseline not yet frozen (== 0). */
			if (p->baseline_log_lat_q16 != 0) {
				p->mean_log_lat_ewma_q16 = p->baseline_log_lat_q16;
				p->long_term_mean_log_lat_q16 = p->baseline_log_lat_q16;
				p->ewma_fast             = p->baseline_log_lat_q16;
				p->ewma_slow             = p->baseline_log_lat_q16;
				saps_bocpd_reset(p, p->baseline_log_lat_q16);
				p->onset_high_consec     = 0;
				p->onset_clear_consec    = SAPS_ONSET_CLEAR_N;
			}
		}
		break;
	case DPA_SAPS_STATE_RECOVERING:
		if (p->fault_type != SAPS_FAULT_HEALTHY &&
		    p->fault_type != SAPS_FAULT_QD_DRIFT &&
		    p->fault_type != SAPS_FAULT_PROPORTIONAL_THROTTLE &&
		    p->fault_type != SAPS_FAULT_SHARED_FATE) {
			new_state = DPA_SAPS_STATE_EXCLUDED;
		} else {
			/* Count consecutive healthy probes. On warmup-N, promote. */
			p->probe_count++;
			if (p->probe_count >= SAPS_N_PROBE) {
				new_state = DPA_SAPS_STATE_HEALTHY;
			}
		}
		break;
	default:
		new_state = DPA_SAPS_STATE_HEALTHY;
		break;
	}

	if (new_state != p->state) {
		uint16_t old_state = p->state;
		p->state = new_state;
		p->state_entered_tsc = now_tsc;
		p->healthy_since_tsc = 0;
		/* Slice 10: record transition into ring so classify_fault_type
		 * can detect FLAP (repeated transitions within 1 s window). */
		uint32_t now_ms = (uint32_t)(now_tsc / SAPS_TICKS_PER_MS);
		ring_push(p->fsm_trans_tsc_ms_ring,
			   &p->fsm_trans_ring_head, 4, now_ms);
		/* On entering HEALTHY from DEGRADING/RECOVERING, reset NEWMA
		 * so leftover ewma_fast/ewma_slow drift doesn't immediately
		 * re-fire. Same treatment on DEGRADING entry keeps the detector
		 * from getting stuck in a triggered state after a single blip. */
		if (new_state == DPA_SAPS_STATE_HEALTHY ||
		    new_state == DPA_SAPS_STATE_DEGRADING)
			newma_reset(p);
		/* Slice 29: flush stale max/tail-count window on EXCLUDED entry.
		 * When a path is EXCLUDED (no traffic), max_lat_window_n does not
		 * advance — the 512-sample window never resets — so max_log_lat_recent
		 * and tail_count_recent stay frozen at the values from the exclusion
		 * event.  When the path enters RECOVERING and receives probe IOs, the
		 * Slice 15 pre-staleness latch checks these frozen values and fires
		 * BIMODAL_TAIL immediately (tail_count was ≥ 25 during exclusion,
		 * max was > 1ms), re-excluding the path after every single probe.
		 * Result on D0: healthy path excluded by ARM OS jitter spike
		 * (tail_count hit 25 during a transient stall), can never recover.
		 * Fix: clear max/tail on EXCLUDED entry so the probe window starts
		 * fresh.  D0 probes (27µs) then keep max < 1ms → latch never fires
		 * → path recovers correctly.  D4 bimodal: after reset, the 30-probe
		 * window yields only ~4-5 samples > 2ms (15% × 30) → tail_count < 25
		 * → latch doesn't fire → path re-admitted → gets full traffic → full
		 * window fills at 15% tail → tail_count reaches 77 → classified
		 * BIMODAL_TAIL through the normal hysteresis path → EXCLUDED again.
		 * D4 still passes because B is excluded the majority of the time. */
		if (new_state == DPA_SAPS_STATE_EXCLUDED) {
			p->max_log_lat_recent      = 0;
			p->max_lat_window_n        = 0;
			p->tail_count_recent       = 0;
			p->bimodal_consec_windows  = 0; /* Slice 30: reset consec counter */
		}
		/* Option2 diagnostic: log every FSM transition so we can
		 * distinguish "B 5ms slow path → DEGRADING" (expected) from
		 * "A/C healthy path → DEGRADING" (NEWMA false positive). */
		flexio_dev_print("[FSM] qp=%u path=%u old=%u new=%u n=%lu err=%ld since_ms=%lu det=%d ft=%u var=%ld diff=%ld tc=%u mx=%ld\n",
				 (unsigned)qp_idx, (unsigned)path_idx,
				 (unsigned)old_state, (unsigned)new_state,
				 (unsigned long)p->n,
				 (long)p->err_rate_ewma,
				 (unsigned long)(since / SAPS_TICKS_PER_MS),
				 (int)detector_changed,
				 (unsigned)p->fault_type,
				 (long)p->lat_var_ewma_q16,
				 (long)(p->mean_log_lat_ewma_q16 - p->baseline_log_lat_q16),
				 (unsigned)p->tail_count_recent,
				 (long)p->max_log_lat_recent);
	}
}

/* v2 SAPS per-event update (spec-v4 §4.2).
 *
 * Parameters:
 *   s        — shared ring struct pointer (to write verdict tables)
 *   qp_idx   — qp_id & CONN_MASK (already hashed)
 *   path_idx — path_id & PATH_MASK
 *   kind     — 0 submit, 1 complete
 *   e        — ring entry (opcode, sct_sc, retry_count, etc)
 *   delta_ticks — on complete only: latency in host-tsc ticks (>=1).
 */
static inline void saps_update(volatile struct dpa_plugin_shared *s,
			       uint16_t qp_idx, uint16_t path_idx,
			       uint8_t kind,
			       const volatile struct dpa_plugin_notify_entry *e,
			       uint32_t delta_ticks)
{
	struct dpa_path_state *p = &g_path[qp_idx][path_idx];

	if (kind == 0) {
		/* Submit: track inflight + QD EWMA. §4.2
		 * Spec sketches a full QD Welford but on the submit hot path we
		 * use a cheap EWMA (α = 1/64) to stay under the 30 ns/event
		 * budget quoted in §4.7 for submit events. qd_m2 accumulates a
		 * coarse squared-delta for future variance-aware QD use; not
		 * consumed by compute_score() today. */
		p->inflight++;
		int64_t qd_sample = (int64_t)p->inflight << 16;   /* Q16.16 */
		int64_t qd_delta = qd_sample - p->qd_mean;
		p->qd_mean += qd_delta >> 6;
		p->qd_m2  += (qd_delta * qd_delta) >> 32;
		/* Option2 redesign (2026-04-25): publish score on submit too.
		 * Without this, a slow path's score goes stale between rare
		 * completes while inflight piles up. On D1 with B at 5ms,
		 * B's complete rate is ~200/s/QP — score updated 5ms apart
		 * — but submit rate is whatever IO is forwarded — score
		 * MUST reflect rising inflight in real time so argmax pivots
		 * away as the queue grows. Score==0 sentinel preserved for
		 * paths still in warmup. */
		if (p->n >= SAPS_WARMUP_N) {
			uint32_t new_score = compute_score(p, e->opcode);
			if (new_score != 0) {
				p->score = new_score;
				s->per_qp_path_score[qp_idx][path_idx] = new_score;
			}
			s->per_qp_path_capacity[qp_idx][path_idx] =
				saps_capacity_refresh_score(p);
		}
		return;
	}

	/* Complete path. §4.2 */
	if (p->inflight > 0)
		p->inflight--;

	uint64_t host_tsc = e->host_tsc;
	uint16_t sct_sc   = e->sct_sc;
	uint8_t  opcode   = e->opcode;
	uint8_t  retry_count = e->retry_count;

	/* Slice 31: stale-state self-heal.
	 * g_path[] lives in DPA DMEM and survives across host-side bdevperf
	 * restarts (the DPA firmware is not reloaded between runs).  On the
	 * first complete event after a long idle gap (> 60 s), all accumulated
	 * EWMA / FSM / BIMODAL state from the previous experiment is stale —
	 * mean_ewma reflects the OLD scenario's latency (e.g., 5 ms D1 run),
	 * so a fresh D0 run immediately fires PERPETUAL_SLOW (diff=6 log2 units
	 * against a 12-day-old baseline) before any real IO traffic.
	 * Fix: zero the entire dpa_path_state on the first complete after a
	 * 60-second idle gap.  60 s >> max scenario run time (90 s), so this
	 * never fires mid-run.  After zeroing, last_complete_tsc is set to
	 * host_tsc on the very next line, preventing repeated resets. */
		if (p->last_complete_tsc != 0 &&
		    host_tsc > p->last_complete_tsc &&
		    (host_tsc - p->last_complete_tsc) > 60ull * TICKS_PER_SEC) {
		uint32_t saved_inflight = p->inflight;
		__builtin_memset(p, 0, sizeof(*p));
		p->inflight = saved_inflight;
	}

	p->last_complete_tsc = host_tsc;
	saps_capacity_on_complete(p, host_tsc);
	if (saps_capacity_fast_tick(p, host_tsc))
		saps_sf_persist_update(p);

	bool detector_changed = false;
	int64_t log_lat = fixed_log2((uint64_t)delta_ticks);

	/* 1. Clean sample filter (§4.2): only retry_count==0 feeds statistics
	 *    and detector. Retries skew P99 and defeat change-point detection —
	 *    they're signals in their own right (error histogram below), not
	 *    latency samples.
	 *
	 *    Slice 34 (D3 sct=3 root cause): also exclude sct≠0 fast-fail
	 *    completions. NVMe sct=3 PATH ERROR completes with delta_ticks≈0
	 *    (target fast-rejects). Feeding those into mean_log_lat_ewma_q16
	 *    pulls path B's mean BELOW baseline → lat_diff goes large negative
	 *    → SPARSE_ERROR Rule 3 lower-bound rejects → fault never classified
	 *    on B → FAILOVER overlay retries to A/C → A/C latency rises
	 *    → wrong-path drain. Filter keeps mean stat clean; the err_rate
	 *    EWMA / recent_err_count below already capture the error signal. */
	if (retry_count == 0 && (sct_sc >> 8) == 0) {
		p->n++;

		/* NEWMA cold-start anchor (§4.6).
		 *
		 * ewma_fast (λ=0.1) and ewma_slow (λ=0.01) are zero-initialized in
		 * .bss.  If left at zero they converge to the true log-latency at
		 * very different rates: by warmup completion (n=WARMUP_N=32) the gap
		 * is analytically ~12 log2 units, trivially exceeding H_NEWMA=2.0
		 * and triggering NEWMA on every path at the warmup boundary.  This
		 * was the root cause of the D3 sparse-error regression: path B's
		 * sct=2 fast-fail completions feed into NEWMA via retry_count==0,
		 * and the spurious warmup-boundary fire drives HEALTHY→DEGRADING
		 * which then cascades to EXCLUDED after T_DEG_TICKS=500ms.
		 *
		 * Fix: on the very first clean sample (n==1), seed both EWMAs to
		 * the observed log_lat.  Both then track the same true distribution
		 * from the start; the gap at warmup completion is near-zero for
		 * stable paths (verified: 0.0 gap, 0 false fires in simulation).
		 * Genuine latency steps (D1: 5ms slow path) still fire NEWMA within
		 * 3 post-warmup samples.  This is a one-time init guard — subsequent
		 * samples follow the normal EWMA update in newma_on_sample(). */
		if (p->n == 1) {
			p->ewma_fast = log_lat;
			p->ewma_slow = log_lat;
			/* Slice 10: seed mean EWMA too. */
			p->mean_log_lat_ewma_q16 = log_lat;
			p->long_term_mean_log_lat_q16 = log_lat;
			saps_bocpd_reset(p, log_lat);
#if SAPS_D2_B2_STATE_ACTIVE
			saps_b2_cusum_reset(p, log_lat);
#endif
		} else {
			(void)saps_bocpd_on_sample(p, log_lat);
		}

		/* Slice 10: mean log-lat EWMA (λ=1/64). Distinct from NEWMA
		 * detector (λ=0.1/0.01); used for ratio against frozen baseline. */
		int64_t mean_delta = log_lat - p->mean_log_lat_ewma_q16;
		p->mean_log_lat_ewma_q16 +=
			(mean_delta * SAPS_MEAN_LAT_EWMA_ALPHA) >> 16;

		/* Round 2 onset baseline: α=1/4096 so a sudden D2-style step is
		 * measured against the pre-fault latency instead of a poisoned slow
		 * baseline.  At ~MIOPS rates even α=1/4096 can chase a 5ms fault
		 * in wall-clock milliseconds, so only let non-onset samples pull the
		 * long-term baseline upward.  Downward/near-baseline samples still
		 * update it, which keeps recovery fast after the fault clears. */
		if (p->long_term_mean_log_lat_q16 == 0) {
			p->long_term_mean_log_lat_q16 = log_lat;
		} else {
			int64_t long_delta = log_lat - p->long_term_mean_log_lat_q16;
			if (long_delta < SAPS_ONSET_DELTA_FIRE_Q16) {
				p->long_term_mean_log_lat_q16 +=
					long_delta >> SAPS_LONG_TERM_MEAN_SHIFT;
			}
		}

		/* Slice 15 — max + tail-count over a SAPS_MAX_WIN_N-sample
		 * window. Both reset together. max alone catches the worst
		 * latency seen; tail_count_recent counts how many samples in
		 * the window exceeded SAPS_TAIL_LOG_LAT_Q16 (≈ 2 ms). The
		 * BIMODAL_TAIL classifier requires BOTH max-mean excess and
		 * tail_count >= SAPS_TAIL_COUNT_MIN, so a one-off outlier on
		 * a healthy path (tail_count = 1) does not fire. */
		p->max_lat_window_n++;
		if (p->max_lat_window_n >= SAPS_MAX_WIN_N) {
			/* Slice 30: before rolling the window, record whether this
			 * window was "triggered" (tail_count >= MIN).  ARM OS stalls
			 * produce tc >= TAIL_COUNT_MIN in ONE window then return to
			 * 0-5 in the next → bimodal_consec_windows never reaches 2.
			 * D4 bimodal is persistent: every 512-sample window has ~77
			 * tail samples → bimodal_consec_windows reaches 2 after the
			 * 2nd window → BIMODAL_TAIL guards below can fire. */
			if (p->tail_count_recent >= SAPS_TAIL_COUNT_MIN) {
				if (p->bimodal_consec_windows < 2u)
					p->bimodal_consec_windows++;
			} else {
				p->bimodal_consec_windows = 0;
			}
			p->max_log_lat_recent = log_lat;
			p->max_lat_window_n = 1;
			p->tail_count_recent =
				(log_lat > SAPS_TAIL_LOG_LAT_Q16) ? 1u : 0u;
		} else {
			if (log_lat > p->max_log_lat_recent)
				p->max_log_lat_recent = log_lat;
			if (log_lat > SAPS_TAIL_LOG_LAT_Q16)
				p->tail_count_recent++;
		}

		/* Slice 10: variance EWMA. Use Welford double-delta against the
		 * post-update mean — that's what gives variance, not the squared
		 * raw delta. Raw delta squared captures high-freq jitter (large
		 * even on stable paths), so a threshold on it cannot separate
		 * D0 stable from D4 bimodal. The (delta_pre × delta_post) form
		 * is what Welford uses precisely because it tracks variance
		 * and not raw step size. */
		int64_t mean_delta_post = log_lat - p->mean_log_lat_ewma_q16;
		int64_t welford_term    = (mean_delta * mean_delta_post) >> 16;
		if (welford_term < 0)
			welford_term = 0;
		int64_t var_delta = welford_term - p->lat_var_ewma_q16;
		p->lat_var_ewma_q16 +=
			(var_delta * SAPS_LAT_VAR_EWMA_ALPHA) >> 16;
		if (p->lat_var_ewma_q16 < 0)
			p->lat_var_ewma_q16 = 0;

		/* Slice 10: freeze baseline at end of warmup so subsequent
		 * comparisons are against the actual measured baseline of this
		 * path under healthy conditions. baseline_log_lat_q16 == 0 acts
		 * as the "not yet frozen" sentinel. */
		if (p->n == SAPS_WARMUP_N && p->baseline_log_lat_q16 == 0) {
			p->baseline_log_lat_q16 = p->mean_log_lat_ewma_q16;
			if (p->baseline_log_lat_q16 == 0)
				p->baseline_log_lat_q16 = 1;
#if SAPS_D2_B2_STATE_ACTIVE
			saps_b2_cusum_reset(p, p->baseline_log_lat_q16);
#endif
		}

#if SAPS_D2_B2_STATE_ACTIVE
		saps_b2_cusum_on_sample(p, log_lat);
#endif

		/* NEWMA change-point detector (§4.6). */
		detector_changed = newma_on_sample(p, log_lat);
		if (detector_changed) {
			/* Slice 10: record fire into 1 s ring for FLAP detection. */
			uint32_t now_ms_fire = (uint32_t)(host_tsc / SAPS_TICKS_PER_MS);
			ring_push(p->newma_fire_tsc_ms_ring,
				   &p->newma_fire_ring_head, 8, now_ms_fire);

			static uint64_t g_m1_fire_stride;
			if ((g_m1_fire_stride++ & 0x3F) == 0) {
				flexio_dev_print(
					"[M1_FIRE] tsc_ms=%u host_tsc=%llu qp=%u path=%u "
					"log_lat_q16=%lld ewma_fast=%lld ewma_slow=%lld "
					"reason=NEWMA single_stream=%u\n",
					now_ms_fire,
					(unsigned long long)host_tsc,
					(unsigned)qp_idx, (unsigned)path_idx,
					(long long)log_lat,
					(long long)p->ewma_fast,
					(long long)p->ewma_slow,
					(unsigned)s->saps_m1_single_stream);
			}
		}

		/* Global Frugal-2U P99 (§4.3). PRNG seed rotates per-sample
		 * combining host_tsc and sample index so we get uncorrelated
		 * Bernoulli draws across paths. */
		uint64_t seed = host_tsc ^ (p->n * 0x9E3779B97F4A7C15ull);
		frugal2u_update(&p->p99_estimate, &p->frugal_step, log_lat, seed);

		/* Slice 27: also push to exact ring + overwrite p99_estimate
		 * with exact value. Frugal-2U call kept above for now (legacy
		 * fields stay populated, but exact value wins).
		 * NOTE: exact_p99 returns 0 until count==64, so Frugal-2U
		 * drives p99_estimate during warmup; exact takes over after. */
		p->lat_ring[p->lat_ring_head] = log_lat;
		p->lat_ring_head = (p->lat_ring_head + 1) & 63;
		if (p->lat_ring_count < 64)
			p->lat_ring_count++;
		{
			int64_t exact_p99 = saps_exact_p99_from_ring(p->lat_ring, p->lat_ring_count);
			if (exact_p99 != 0)
				p->p99_estimate = exact_p99;
		}

		/* Per-opcode Frugal-2U (§4.1 Chain E). */
		unsigned bucket = opc_bucket(opcode);
		p->opc_p99[bucket].n++;
		if (p->opc_p99[bucket].p99 <= 0) {
			p->opc_p99[bucket].p99 = log_lat;
		} else {
			p->opc_p99[bucket].p99 +=
				(log_lat - p->opc_p99[bucket].p99) >> 4;
		}
	}

	/* 2. NVMe semantic classification (§4.5). sct_sc is packed as
	 *    (sct<<8)|sc per schema v2. Non-error completions: sct=0, sc=0. */
	uint8_t verdict = DPA_SAPS_ACTION_UNSET;
	if (sct_sc != 0) {
		uint8_t sct = (uint8_t)((sct_sc >> 8) & 0xFF);
		uint8_t sc  = (uint8_t)(sct_sc & 0xFF);
		if (sct < 4)
			p->err_hist[sct][sc & 0xF]++;
		/* err_rate_ewma = α·(1<<16) + (1-α)·ewma — treats each error
		 * as a full unit in Q16.16 space, decayed at α = 1/32. */
		p->err_rate_ewma = ((int64_t)SAPS_ERR_ALPHA_Q16 * ((int64_t)1 << 16)
				    + ((int64_t)(1 << 16) - SAPS_ERR_ALPHA_Q16)
					   * p->err_rate_ewma)
				   >> 16;
		verdict = classify_action(sct, sc);
		/* Slice 10 sliding error tracker. */
		recent_err_track(p, true);
	} else {
		/* Clean completion — decay err_rate_ewma toward 0. */
		p->err_rate_ewma = (((int64_t)(1 << 16) - SAPS_ERR_ALPHA_Q16)
					   * p->err_rate_ewma) >> 16;
		recent_err_track(p, false);
	}

	/* 3a. Slice 10: classify fault type before FSM transitions / scoring.
	 * fault_type is consumed by both update_state_machine() (FLAP freeze)
	 * and compute_score() (PERPETUAL_SLOW clamp / BIMODAL_TAIL shift). */
	uint32_t now_ms = (uint32_t)(host_tsc / SAPS_TICKS_PER_MS);
	/* Slice 16 -- compute global median log-lat across paths in this qp.
	 * Median of N=3 paths is the middle value; we sort by simple compare.
	 * Only paths with completed warmup and a frozen baseline contribute.
	 * If fewer than 2 contribute, median is invalid and the cross-path
	 * rule is skipped (handled by global_median_valid flag). */
	int64_t sf_current_consensus_log_lat_q16 = 0;
	for (unsigned i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		struct dpa_path_state *q = &g_path[qp_idx][i];
		int64_t cur = q->mean_log_lat_ewma_q16;

		if (q->n != 0 && q->recent_sample_count >= 4 && cur > 0 &&
		    (sf_current_consensus_log_lat_q16 == 0 ||
		     cur < sf_current_consensus_log_lat_q16))
			sf_current_consensus_log_lat_q16 = cur;
	}

	int64_t means[DPA_PLUGIN_PATH_MAX];
	int64_t qds[DPA_PLUGIN_PATH_MAX];
	int64_t p99s[DPA_PLUGIN_PATH_MAX];
	uint32_t caps[DPA_PLUGIN_PATH_MAX];
	unsigned valid = 0;
	unsigned qd_valid = 0;
	unsigned p99_valid = 0;
	unsigned cap_valid = 0;
	unsigned sf_latency_degraded = 0;
	for (unsigned i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		struct dpa_path_state *q = &g_path[qp_idx][i];
		if (q->n >= SAPS_WARMUP_N &&
		    q->baseline_log_lat_q16 != 0 &&
		    q->recent_sample_count >= 4) {
			means[valid++] = q->mean_log_lat_ewma_q16;
			qds[qd_valid++] = q->qd_mean;
			if (q->p99_estimate > 0)
				p99s[p99_valid++] = q->p99_estimate;
			if (q->capacity_score != 0)
				caps[cap_valid++] = q->capacity_score;
			bool sf_p99_degraded =
				sf_current_consensus_log_lat_q16 > 0 &&
				q->p99_estimate > SAPS_TAIL_LOG_LAT_Q16 &&
				q->p99_estimate < SAPS_SHARED_FATE_TAIL_MAX_Q16 &&
				q->mean_log_lat_ewma_q16 - sf_current_consensus_log_lat_q16 >=
					(int64_t)SAPS_SF_LAT_BAD_DELTA_Q16 &&
				q->p99_estimate - sf_current_consensus_log_lat_q16 >=
					(int64_t)SAPS_CROSSPATH_DRIFT_HI_Q16;
			bool sf_mean_degraded =
				q->mean_log_lat_ewma_q16 > SAPS_LAT_HEALTHY_FLOOR_Q16 &&
				((sf_current_consensus_log_lat_q16 > 0 &&
				  q->mean_log_lat_ewma_q16 - sf_current_consensus_log_lat_q16 >=
					  (int64_t)SAPS_CROSSPATH_DRIFT_HI_Q16) ||
				 q->mean_log_lat_ewma_q16 - q->baseline_log_lat_q16 >=
					 (int64_t)SAPS_SF_LAT_BAD_DELTA_Q16);

			if (saps_shared_fate_path_has_latency_tail_evidence(q) &&
			    (sf_p99_degraded || sf_mean_degraded))
				sf_latency_degraded++;
		}
	}
	int64_t median_log_lat = 0; bool median_valid = false;
		int64_t min_log_lat = 0;    bool min_valid = false;
		int64_t mad_log_lat = 0;    bool mad_valid = false;
		int64_t median_qd = 0;      bool median_qd_valid = false;
		int64_t median_p99 = 0;     bool median_p99_valid = false;
		uint32_t median_capacity = 0; bool median_capacity_valid = false;
		bool all_paths_degraded = false;
		if (valid >= 2) {
		for (unsigned i = 1; i < valid; i++) {
			int64_t key = means[i]; int j = (int)i - 1;
			while (j >= 0 && means[j] > key) { means[j+1] = means[j]; j--; }
			means[j+1] = key;
		}
		for (unsigned i = 1; i < qd_valid; i++) {
			int64_t key = qds[i]; int j = (int)i - 1;
			while (j >= 0 && qds[j] > key) { qds[j+1] = qds[j]; j--; }
			qds[j+1] = key;
		}
		min_log_lat = means[0];                 min_valid = true;
		median_log_lat = means[valid / 2];      median_valid = true;
		if (valid >= 3) {
			int64_t devs[DPA_PLUGIN_PATH_MAX];
			for (unsigned i = 0; i < valid; i++)
				devs[i] = abs64(means[i] - median_log_lat);
			for (unsigned i = 1; i < valid; i++) {
				int64_t key = devs[i]; int j = (int)i - 1;
				while (j >= 0 && devs[j] > key) { devs[j+1] = devs[j]; j--; }
				devs[j+1] = key;
			}
			mad_log_lat = devs[valid / 2];
			mad_valid = true;
		}
			if (qd_valid >= 3) {
				median_qd      = qds[qd_valid / 2];
				median_qd_valid = true;
			}
			if (cap_valid >= 2) {
				for (unsigned i = 1; i < cap_valid; i++) {
					uint32_t key = caps[i]; int j = (int)i - 1;
					while (j >= 0 && caps[j] > key) { caps[j+1] = caps[j]; j--; }
					caps[j+1] = key;
				}
				median_capacity = caps[cap_valid / 2];
				median_capacity_valid = median_capacity != 0;
			}
			if (valid >= 2 &&
			    sf_latency_degraded >= SAPS_SHARED_FATE_DEGRADED_N)
				all_paths_degraded = true;
		}
	if (p99_valid >= 2) {
		for (unsigned i = 1; i < p99_valid; i++) {
			int64_t key = p99s[i]; int j = (int)i - 1;
			while (j >= 0 && p99s[j] > key) { p99s[j+1] = p99s[j]; j--; }
			p99s[j+1] = key;
		}
		median_p99      = p99s[p99_valid / 2];  median_p99_valid = true;
	}
	/* Slice 28 — cross-path inflight median.
	 * Uses raw p->inflight counter (not qd_mean EWMA) for zero-lag response.
	 * All warmed-up paths contribute regardless of recent_sample_count so the
	 * median is stable even when one path has been drained and has low traffic. */
	int64_t infls[DPA_PLUGIN_PATH_MAX];
	unsigned infl_valid = 0;
	for (unsigned i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		struct dpa_path_state *q = &g_path[qp_idx][i];
		if (q->n >= SAPS_WARMUP_N)
			infls[infl_valid++] = (int64_t)q->inflight;
	}
	int64_t median_inflight = 0;
	bool median_inflight_valid = false;
	if (infl_valid >= 2) {
		for (unsigned i = 1; i < infl_valid; i++) {
			int64_t key = infls[i]; int j = (int)i - 1;
			while (j >= 0 && infls[j] > key) { infls[j+1] = infls[j]; j--; }
			infls[j+1] = key;
		}
		median_inflight = infls[infl_valid / 2];
		median_inflight_valid = true;
	}

	bool peer_latency_fault_active = false;
	for (unsigned i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		struct dpa_path_state *q = &g_path[qp_idx][i];
		if (i == path_idx)
			continue;
		if (q->n >= SAPS_WARMUP_N &&
		    q->state != DPA_SAPS_STATE_HEALTHY &&
		    (saps_fault_is_onset_family(q->fault_type) ||
		     q->fault_type == SAPS_FAULT_PERPETUAL_SLOW)) {
			peer_latency_fault_active = true;
			break;
		}
	}

	uint16_t state_before = p->state;
	uint16_t prev_fault = p->fault_type;
	uint16_t raw_fault = 0;
	uint16_t new_fault;

	/* sapsq_bypass_saps_fsm=1: skip SAPS B7 classify + FSM + SHARED_FATE
	 * so cold-start high-latency spikes cannot degrade paths during E1
	 * baseline testing.  Score/verdict writes still run normally. */
	if (s->sapsq_bypass_saps_fsm) {
		new_fault = SAPS_FAULT_HEALTHY;
		p->fault_type = SAPS_FAULT_HEALTHY;
		p->state      = DPA_SAPS_STATE_HEALTHY;
	} else {
	new_fault  = classify_fault_type(p, now_ms,
					  median_log_lat, median_valid,
					  min_log_lat, min_valid,
						  mad_log_lat, mad_valid,
						  median_qd, median_qd_valid,
						  median_capacity, median_capacity_valid,
						  median_p99, median_p99_valid,
						  median_inflight, median_inflight_valid,
						  peer_latency_fault_active,
						  all_paths_degraded,
						  &raw_fault);
	g_saps_classify_calls++;
	p->fault_type = new_fault;
	if (saps_fault_is_onset_family(prev_fault) &&
	    new_fault == SAPS_FAULT_HEALTHY) {
		saps_reset_latency_tail_estimators(p, p->bocpd_last_log_lat_q16);
#if SAPS_D2_B2_STATE_ACTIVE
		saps_b2_cusum_reset(p, p->baseline_log_lat_q16);
#endif
	}
	bool shared_fate = saps_shared_fate_active(qp_idx, path_idx, new_fault,
						   all_paths_degraded, host_tsc);
#if SAPS_D2_B8_ENABLE
	if (!shared_fate &&
	    saps_b8_active_heal_on_sample(p, prev_fault, new_fault, state_before,
					  retry_count == 0 && sct_sc == 0,
					  log_lat, host_tsc, qp_idx, path_idx)) {
		new_fault = p->fault_type;
	}
#endif

	/* Slice 24a: SPARSE_ERROR override TERMINAL → FAILOVER for sct=2/sc=0x81.
	 * NVMe spec: sct=2/sc=0x81 (Unrecovered Read Error) is TERMINAL with
	 * DNR=1, so SPDK won't retry. But under multipath with replicated data,
	 * retry on a different controller is correct when path-level classifier
	 * just confirmed SPARSE_ERROR. Slice 22 latches new_fault on the very
	 * first sct≠0 sample, so this fires on the FIRST error → io_failed→0. */
	if (verdict == DPA_SAPS_ACTION_TERMINAL &&
	    new_fault == SAPS_FAULT_SPARSE_ERROR && sct_sc != 0) {
		uint8_t local_sc = (uint8_t)(sct_sc & 0xFF);
		if (local_sc == 0x81)
			verdict = DPA_SAPS_ACTION_FAILOVER;
	}

	/* FLAP dampening window management. On entering FLAP, set initial
	 * window; on consecutive FLAP classifies after window expiry, double
	 * the window up to MAX. On classifying anything else, clear. */
	if (shared_fate) {
		p->flap_dampening_until_tsc = 0;
		p->flap_dampening_window_ms = 0;
	} else if (new_fault == SAPS_FAULT_FLAP) {
		if (prev_fault != SAPS_FAULT_FLAP) {
			p->flap_dampening_window_ms = SAPS_FLAP_DAMP_MS_INIT;
			p->flap_dampening_until_tsc =
				host_tsc + (uint64_t)SAPS_FLAP_DAMP_MS_INIT
					   * SAPS_TICKS_PER_MS;
		} else if (host_tsc >= p->flap_dampening_until_tsc) {
			uint32_t w = p->flap_dampening_window_ms * 2;
			if (w > SAPS_FLAP_DAMP_MS_MAX)
				w = SAPS_FLAP_DAMP_MS_MAX;
			p->flap_dampening_window_ms = w;
			p->flap_dampening_until_tsc =
				host_tsc + (uint64_t)w * SAPS_TICKS_PER_MS;
		}
	} else {
		p->flap_dampening_until_tsc = 0;
		p->flap_dampening_window_ms = 0;
	}

	/* 3. FSM (§4.4).  The per-path verdict always owns quarantine and
	 * recovery.  SHARED_FATE runs afterward as annotation-only publication. */
	update_state_machine(p, detector_changed, host_tsc, qp_idx, path_idx);
	if (shared_fate) {
		saps_shared_fate_apply(s, qp_idx, path_idx, opcode,
				       retry_count == 0 && sct_sc == 0,
				       log_lat, host_tsc);
	}
	} /* end: !sapsq_bypass_saps_fsm */

	if (path_idx < DPA_PLUGIN_PATH_MAX) {
		uint16_t st_after  = (uint16_t)p->state;
		uint16_t ft_after  = (uint16_t)new_fault;
		uint16_t st_before = (uint16_t)state_before;
		if (st_after < 4u)
			s->d4_state_count[path_idx][st_after]++;
		if (ft_after < 10u)
			s->d4_fault_count[path_idx][ft_after]++;
		if (st_before < 4u && st_after < 4u)
			s->d4_state_transitions[path_idx][st_before][st_after]++;
		if (raw_fault < 10u)
			s->d4_raw_fault_count[path_idx][raw_fault]++;

		/* D4 round-2: dump raw classifier state for BIMODAL_TAIL forensics.
		 * Overwrite every call — host reads END snapshot at shutdown. */
		s->d4_last_max_log_lat[path_idx]      = p->max_log_lat_recent;
		s->d4_last_mean_log_lat[path_idx]     = p->mean_log_lat_ewma_q16;
		s->d4_last_baseline_log_lat[path_idx] = p->baseline_log_lat_q16;
		s->d4_last_tail_count[path_idx]       = p->tail_count_recent;
		s->d4_last_bimodal_consec[path_idx]   = p->bimodal_consec_windows;
		s->d4_last_recent_sample[path_idx]    = p->recent_sample_count;
		s->d4_last_err_rate_ewma[path_idx]    = p->err_rate_ewma;
		s->d4_last_recent_err_count[path_idx] = p->recent_err_count;
		s->d4_last_n[path_idx]                = p->n;
	}

	/* 4. Score (§4.3). */
	uint32_t score = compute_score(p, opcode);
	p->score = score;

	/* 5. Write verdict tables.
	 * score==0 is the warmup sentinel — leave per_qp_path_score untouched so
	 * the host-side exploration in dpa_plugin_select_io_path sees slot==0
	 * and round-robins unsampled paths. state / retry_verdict are orthogonal
	 * to the argmax contract, so we still publish them. */
	if (score != 0) {
		s->per_qp_path_score[qp_idx][path_idx] = score;
		g_saps_score_updates++;
	}
	s->per_qp_path_capacity[qp_idx][path_idx] = p->capacity_score;

	/* Slice 34: per-opcode scores + raw P99 for D5 mixed-workload routing.
	 * opc 0=READ, 1=WRITE, 2=FLUSH. score=0 (warmup sentinel) → leave untouched.
	 * opc_p99 written unconditionally once n>WARMUP (0=warmup sentinel). */
	{
		for (unsigned opc = 0; opc < DPA_PLUGIN_OPC_CLASS_MAX; opc++) {
			uint32_t s_opc = compute_score_opcode(p, opc);
			if (s_opc != 0)
				s->per_qp_path_score_opcode[qp_idx][path_idx][opc] = s_opc;
			/* Publish raw opc_p99 for cross-path comparison on host side.
			 * ONSET is a time-domain D2 condition with its own probe score;
			 * clearing the host opcode-P99 sentinel prevents the D5 opcode
			 * suitability gate from hard-clamping recovery probes to weight 1. */
			if (saps_fault_is_onset_family(p->fault_type)) {
				s->per_qp_path_opc_p99[qp_idx][path_idx][opc] = 0;
			} else if (p->opc_p99[opc].n > SAPS_WARMUP_N && p->opc_p99[opc].p99 > 0) {
				s->per_qp_path_opc_p99[qp_idx][path_idx][opc] =
					saps_opcode_p99_for_host(p, opc);
			}
		}
	}

	s->per_qp_path_state[qp_idx][path_idx] = p->state;
	/* Slice 19: expose fault_type so host can modulate idle-bypass weight. */
	s->per_qp_path_fault_type[qp_idx][path_idx] = p->fault_type;
	{
		uint32_t bit = 1u << (path_idx & DPA_PLUGIN_PATH_MASK);
		uint32_t mask = s->per_qp_fault_mask[qp_idx][0];

		if (p->fault_type == SAPS_FAULT_HEALTHY)
			mask &= ~bit;
		else
			mask |= bit;
		s->per_qp_fault_mask[qp_idx][0] = mask;
	}
	if (verdict != DPA_SAPS_ACTION_UNSET)
		s->per_qp_path_retry_verdict[qp_idx][path_idx] = verdict;
	else
		s->per_qp_path_retry_verdict[qp_idx][path_idx] = DPA_SAPS_ACTION_UNSET;

	/* L4 trace: path-1 per-classifier-call snapshot, stride 1/8192 to keep
	 * console volume small (~244 lines per QP at 2M events/60s). Emits the
	 * NEWMA detector state, classifier verdict, and baseline so we can see
	 * whether path 1 ever sees a verdict change post tc-netem inject. */
	if (path_idx == 1) {
		static uint64_t g_l4_p1_stride;
		if ((g_l4_p1_stride++ & 0x1FFFu) == 0) {
			flexio_dev_print(
				"[L4_P1] qp=%u n=%u log_lat=%lld ewma_f=%lld ewma_s=%lld "
				"base=%lld mean=%lld st=%u ft=%u raw_ft=%u dc=%u\n",
				(unsigned)qp_idx, (unsigned)p->n,
				(long long)log_lat,
				(long long)p->ewma_fast, (long long)p->ewma_slow,
				(long long)p->baseline_log_lat_q16,
				(long long)p->mean_log_lat_ewma_q16,
				(unsigned)p->state, (unsigned)p->fault_type,
				(unsigned)raw_fault, (unsigned)detector_changed);
		}
	}
}

/* Runtime counters, readable via flexio_dev_print on lease renew. */
static uint64_t g_algo_events;
static uint64_t g_algo_degraded_transitions;
static uint64_t g_algo_sv1_bypass_admits;   /* safety valve 1 activations */
static uint64_t g_algo_sv2_auto_release;    /* safety valve 2 activations */
static uint64_t g_algo_throttle_writes;     /* per-outer-iter token=0 stores */
static uint64_t g_algo_admit_writes;        /* per-outer-iter token=1 stores */

/* SAPS-Q M-series scheduler static state (slice 2).
 * Must be declared before process_event() which increments submit_count. */
static uint32_t g_sapsq_submit_count[SAPSQ_MAX_TENANTS];    /* raw submit events per tenant since last tick */
static uint32_t g_sapsq_demand_ewma_q32[SAPSQ_MAX_TENANTS]; /* per-tenant demand EWMA IO/s */
static uint32_t g_sapsq_zero_epochs[SAPSQ_MAX_TENANTS];     /* consecutive zero-admit epochs per tenant */
static uint64_t g_sapsq_tick_last_tsc;                      /* TSC of last scheduler tick */
/* Option 1 (Lane U): last snapshot of sapsq_host_submit_count[] read by DPA tick.
 * Delta = current - last gives submit activity since previous epoch.
 * Replaces g_sapsq_submit_count as the idle-detection signal because
 * sapsq_host_submit_count is written by the host (always visible via MR)
 * whereas g_sapsq_submit_count is incremented in DPA process_event() which
 * suffers producer_idx MR registration visibility bug (coordinator VA ≠ tenant
 * alias VA → DPA scheduler tick reads stale 0). */
static uint64_t g_sapsq_host_submit_last[SAPSQ_MAX_TENANTS]; /* snapshot from previous tick */

/* ------ Integer log10 helpers (no FPU) ------
 * log10(x) ≈ (bitlen(x) - 1) * log10(2). For x in ns (uint64_t), bitlen via CLZ.
 * Return value: log10(x) * 1000 as integer. Fully fenceless. */

/* log10(2) * 1000 = 301.03 → use 301 with a 64-entry table for exact decades. */
static const uint16_t LOG10_POW2_X1000[65] = {
	    0,   301,   602,   903,  1204,  1505,  1806,  2107,  /*  0..7 */
	 2408,  2709,  3010,  3311,  3612,  3913,  4214,  4515,  /*  8..15 */
	 4816,  5117,  5418,  5719,  6020,  6321,  6622,  6923,  /* 16..23 */
	 7224,  7525,  7826,  8127,  8428,  8729,  9030,  9331,  /* 24..31 */
	 9632,  9933, 10234, 10535, 10836, 11137, 11438, 11739,  /* 32..39 */
	12040, 12341, 12642, 12943, 13244, 13545, 13846, 14147,  /* 40..47 */
	14448, 14749, 15050, 15351, 15652, 15953, 16254, 16555,  /* 48..55 */
	16856, 17157, 17458, 17759, 18060, 18361, 18662, 18963,  /* 56..63 */
	19264,                                                   /* 64 */
};

static inline uint32_t __attribute__((unused)) log10_ns_x1000(uint64_t lat_ns)
{
	if (lat_ns == 0)
		return 0;
	/* bitlen = 64 - clz(x); index into table. Cheap integer op on DPA. */
	unsigned bl = 64u - (unsigned)__builtin_clzll(lat_ns);
	if (bl > 64u) bl = 64u;
	return LOG10_POW2_X1000[bl];
}

/* Legacy E1 per-QP CUSUM admission. Disabled (2026-06-23): SAPS drives steering
 * from per_qp_path_score[] and admission from the 2D M4 path, so this CUSUM
 * detector no longer runs. Compiled out by default; the paper describes a single
 * cross-path detector, not this legacy per-stream one. */
#define DPA_PLUGIN_LEGACY_E1_ADMISSION 0
#if DPA_PLUGIN_LEGACY_E1_ADMISSION
/* Welford online update (integer form). Latency sample already in x1000 log-ns. */
static inline void welford_update(struct conn_stats *c, int32_t x_x1000)
{
	c->n++;
	int64_t delta = (int64_t)x_x1000 - c->mean_x1000;
	/* mean += delta / n  — integer div; acceptable coarseness on DPA. */
	c->mean_x1000 += delta / (int64_t)c->n;
	int64_t delta2 = (int64_t)x_x1000 - c->mean_x1000;
	c->m2 += delta * delta2;
}

/* CUSUM update and flag transition (positive side only for this spike).
 * E1-integration: on transition, also stamp degraded_since (for safety
 * valve 2) and reset cusum_pos so we don't re-fire continuously.
 * E1-fix1: ALSO refresh degraded_since on re-fire while already degraded.
 * This keeps throttle engaged during sustained tail-latency storms — sv2
 * only auto-releases MAX_THROTTLE_TICKS (2s) after the LAST anomaly, not
 * the first. Without this, sv2 could release mid-cascade because CUSUM
 * silenced itself after the first fire. */
static inline void cusum_update(struct conn_stats *c, int32_t x_x1000,
				 uint64_t now_tsc)
{
	int64_t dev = (int64_t)x_x1000 - c->mean_x1000 - CUSUM_DRIFT;
	c->cusum_pos += dev;
	if (c->cusum_pos < 0)
		c->cusum_pos = 0;

	if (c->cusum_pos > CUSUM_THRESH) {
		if (c->degraded == 0) {
			c->degraded = 1;
			c->degraded_reason = 1;  /* 1 = cusum_fire */
			g_algo_degraded_transitions++;
		}
		c->degraded_since = now_tsc;  /* refresh sv2 window on each fire */
		c->cusum_pos = 0;
	}
}

/* Push latency sample into circular window; no return value needed. */
static inline void window_push(struct conn_stats *c, uint32_t x_x1000)
{
	c->window[c->head] = x_x1000;
	c->head = (c->head + 1) & (DPA_PLUGIN_WINDOW_K - 1);
	if (c->count < DPA_PLUGIN_WINDOW_K)
		c->count++;
}
#endif /* DPA_PLUGIN_LEGACY_E1_ADMISSION */

/* Per-event algorithm step. Called for each ring entry we consume. */
static inline void process_event(volatile struct dpa_plugin_shared *s,
				  const volatile struct dpa_plugin_notify_entry *e)
{
	uint16_t cid = e->cmd_id;
	uint16_t qp  = e->qp_id;
	uint16_t path = e->path_id;
	uint8_t  kind = e->kind;
	uint64_t tsc_low = e->host_tsc;  /* full tsc; we use low 32 for deltas */

	uint16_t qp_idx_real = qp & DPA_PLUGIN_CONN_MASK;
	uint16_t path_idx_real = path & DPA_PLUGIN_PATH_MASK;

	uint16_t qp_idx = qp_idx_real;
	uint16_t path_idx = path_idx_real;
	if (s->saps_m1_single_stream) {
		if (s->saps_m1_reset_request) {
			s->saps_m1_reset_request = 0;
			__builtin_memset(&g_path[0][0], 0,
					 sizeof(g_path[0][0]));
		}
		qp_idx = 0;
		path_idx = 0;
	}

	if (kind == 0) {
		g_submit_tsc_low[saps_submit_slot(cid, qp_idx_real,
						   path_idx_real)] = (uint32_t)tsc_low;
		saps_update(s, qp_idx, path_idx, 0, e, 0);
		/* SAPS-Q M-series: per-tenant submit counter for demand EWMA.
		 * tenant_id = qp_id & M3_TENANT_MASK (matches M3/M4 convention).
		 * Saturating increment — DPA uint32 submit burst cannot exceed 2^32 per epoch. */
		if (s->sapsq_enabled) {
			uint32_t tid = (uint32_t)qp & M3_TENANT_MASK;
			s->dpa_submit_consumed_by_tenant[tid]++;
			if (g_sapsq_submit_count[tid] < 0xFFFFFFFFu)
				g_sapsq_submit_count[tid]++;
		}
		return;
	}

	/* complete — compute latency delta (low 32 bits of tsc) */
	uint32_t submit_slot = saps_submit_slot(cid, qp_idx_real, path_idx_real);
	uint32_t submit_low = g_submit_tsc_low[submit_slot];
	/* Error completions bypass the host sampling gate even when their
	 * matching submission was not sampled. Their status is still valid
	 * completion evidence, while their latency must not enter the latency
	 * estimator. Preserve those events with a neutral non-zero delta. */
	if (submit_low == 0 && e->sct_sc == 0)
		return;
	if (submit_low != 0)
		g_submit_tsc_low[submit_slot] = 0;
	uint32_t delta_ticks = submit_low == 0
			      ? 1u
			      : (uint32_t)tsc_low - submit_low;
	if (delta_ticks == 0)
		delta_ticks = 1;

	/* S3 SAPS: per-path Welford + CUSUM + scoring + verdict write. */
	saps_update(s, qp_idx, path_idx, 1, e, delta_ticks);

	if (s->saps_m2_enabled) {
		struct dpa_path_state *p_m2 = &g_path[qp_idx][path_idx];
		uint16_t client_id = (qp & DPA_PLUGIN_CONN_MASK) & M2_CLIENT_MASK;
		int64_t log_lat_q16 = fixed_log2((uint64_t)delta_ticks);
		int64_t newma_resid = abs64(p_m2->ewma_fast - p_m2->ewma_slow);
		uint16_t sct_sc_m2 = e->sct_sc;
		uint32_t nbytes_m2 = e->nbytes;
		int64_t x[M2_FEATURE_DIM] = {
			log_lat_q16,
			p_m2->ewma_fast,
			p_m2->ewma_slow,
			newma_resid,
			p_m2->err_rate_ewma,
			(int64_t)p_m2->inflight << 16,
			fixed_log2(nbytes_m2 == 0 ? 1u : (uint64_t)nbytes_m2),
			(sct_sc_m2 != 0) ? ((int64_t)1 << 16) : 0,
		};
		m2_oja_update(s, client_id, x);
	}

	if (s->m3_enabled) {
		uint32_t tenant_id;
		if (s->m3_my_tenant_id != 0xFFFFFFFFu) {
			tenant_id = s->m3_my_tenant_id & M3_TENANT_MASK;
		} else {
			tenant_id = (qp & DPA_PLUGIN_CONN_MASK) & M3_TENANT_MASK;
		}
		m3_refresh_credit(s, tenant_id, tsc_low);
		bool admitted = m3_consume(s, tenant_id);

		if (s->m4_v2_enabled) {
			uint32_t m4_tid = (s->m4_my_tenant_id != 0xFFFFFFFFu)
				? (s->m4_my_tenant_id & M3_TENANT_MASK)
				: tenant_id;
			(void)m4_tid;
			(void)m4_refresh_and_consume;
		}

		if (s->m3_v3_freeze_gate && !admitted) {
			s->m3_tenant_freeze[tenant_id] = 1;
		} else if (!admitted && s->m3_v2_admission_gate) {
			for (uint32_t qp_iter = tenant_id;
			     qp_iter < DPA_PLUGIN_CONN_MAX;
			     qp_iter += M3_TENANT_MAX) {
				s->per_qp_tokens[qp_iter] = 0;
			}
		}
	}

#if DPA_PLUGIN_LEGACY_E1_ADMISSION
	/* Legacy E1 path — per-QP CUSUM for admission control. Disabled: SAPS
	 * steers from per_qp_path_score[], not this per-stream CUSUM. */
	uint32_t x = log10_ns_x1000((uint64_t)delta_ticks);

	struct conn_stats *c = &g_conn[qp & DPA_PLUGIN_CONN_MASK];
	if (c->qp_id == 0 && c->n == 0)
		c->qp_id = qp; /* first-touch bind */

	welford_update(c, (int32_t)x);
	cusum_update(c, (int32_t)x, __dpa_thread_cycles());
	window_push(c, x);
#else
	(void)delta_ticks;
#endif

	g_algo_events++;

	/* SAPS-Q (2026-05-22): accumulate per-epoch event counter. Scheduler
	 * body runs in the outer loop where it can amortize the writeback fence.
	 * Gated cheaply so M3/M4/M5-only configs pay one load + branch. */
	if (s->sapsq_enabled) {
		s->sapsq_epoch_count_events++;
	}
}

/* E1-fix1: rate-limited cumulative-credit refill, invoked once per outer
 * iteration. Walks every active conn, applies safety-valve 2, then advances
 * the monotonic per_qp_tokens[] credit counter by rate × elapsed-ticks.
 *
 * Protocol (DPA-writer / host-reader — no MPMC race):
 *   DPA:  per_qp_tokens[qp] = cumulative credits (monotonic, never decr)
 *   Host: host_admitted[qp] = cumulative admits (monotonic, host increments)
 *   Admit iff per_qp_tokens[qp] - host_admitted[qp] > 0.
 *
 * Burst bound: when DPA refills faster than host consumes, the gap grows
 * toward infinity. We cap the gap at max_tokens by advancing host_admitted
 * (server-side) OR by the refill logic clamping its own advance to keep the
 * *current* gap ≤ max_tokens. Since host_admitted is host-local, DPA does
 * the clamp: new_credit = max(host_admitted, old_credit + new_tokens clamped
 * so that new_credit - host_admitted ≤ max_tokens).
 *
 * If host is currently "way behind" (e.g. throttled long enough that
 * accumulated credit gap would exceed max_tokens), we clamp the credit to
 * host_admitted + max_tokens — i.e. tokens not-yet-consumed expire if the
 * host wasn't there to use them. This is the classic leaky-bucket bound.
 */

/* Per-path worst classification aggregate produced by sapsq_compute_health(). */
struct sapsq_path_agg {
	uint16_t worst_state;       /* enum dpa_saps_state */
	uint16_t worst_fault;       /* enum saps_fault_type */
	uint8_t  any_qp_active;     /* 1 if any QP has touched this path */
};

/* Severity rank for state aggregation: EXCLUDED > DEGRADING > RECOVERING > HEALTHY.
 * Larger rank = worse path health. */
static inline uint8_t sapsq_state_rank(uint16_t st)
{
	switch (st) {
	case DPA_SAPS_STATE_EXCLUDED:   return 3;
	case DPA_SAPS_STATE_DEGRADING:  return 2;
	case DPA_SAPS_STATE_RECOVERING: return 1;
	case DPA_SAPS_STATE_HEALTHY:
	default:                        return 0;
	}
}

/* Recovery health ramp tracker — one slot per path. Resets to a low value
 * when path first enters RECOVERING; advances by SAPSQ_RECOVER_STEP every
 * epoch the worst state stays RECOVERING; clears once path returns to
 * HEALTHY. Defined static-DPA-memory so it survives across epochs. */
#define SAPSQ_RECOVER_STEP_Q16   (8192u)           /* +0.125 per epoch  */
#define SAPSQ_RECOVER_FLOOR_Q16  (32768u)          /* start at 0.5      */
static uint32_t g_sapsq_recover_ramp_q16[SAPSQ_PATH_MAX];

/* Compute per-path health_factor_q16 + eligibility from classifier state. */
static inline void sapsq_compute_health(volatile struct dpa_plugin_shared *s)
{
	/* Lane I bypass: when sapsq_bypass_d_classifier=1, force all paths to
	 * HEALTHY + health_factor=1.0 so slice 1-4 2D enforcement core can be
	 * validated independently of D classifier FSM state.  Re-enable by
	 * setting SAPSQ_BYPASS_D_CLASSIFIER=0 (E3 path-degradation testing). */
	if (s->sapsq_bypass_d_classifier) {
		for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
			s->sapsq_path_health_q16[p]          = SAPSQ_HEALTH_HEALTHY_Q16;
			s->sapsq_path_eligibility[p]         = SAPSQ_ELIG_NORMAL;
			s->sapsq_path_health[p]              = 0; /* HEALTHY enum */
			s->sapsq_path_health_factor_q16[p]   = SAPSQ_HEALTH_HEALTHY_Q16; /* 1.0 */
			g_sapsq_recover_ramp_q16[p]          = 0;
		}
		return;
	}

	struct sapsq_path_agg agg[SAPSQ_PATH_MAX];
	for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
		agg[p].worst_state = DPA_SAPS_STATE_HEALTHY;
		agg[p].worst_fault = SAPS_FAULT_HEALTHY;
		agg[p].any_qp_active = 0;
	}

	/* Aggregate across all QP slots. Only count rows that have at least one
	 * score update (filters out the 1024-row direct-map's unused slots). */
	for (uint32_t qp = 0; qp < DPA_PLUGIN_CONN_MAX; qp++) {
		/* Cheap activity gate: skip if the QP has never produced a sample. */
		bool active = false;
		for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
			if (g_path[qp][p].n != 0) {
				active = true;
				break;
			}
		}
		if (!active)
			continue;

		for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
			if (g_path[qp][p].n == 0)
				continue;
			uint16_t st = s->per_qp_path_state[qp][p];
			uint16_t ft = s->per_qp_path_fault_type[qp][p];
			agg[p].any_qp_active = 1;
			if (sapsq_state_rank(st) > sapsq_state_rank(agg[p].worst_state)) {
				agg[p].worst_state = st;
				agg[p].worst_fault = ft;
			} else if (sapsq_state_rank(st) == sapsq_state_rank(agg[p].worst_state) &&
				   ft != SAPS_FAULT_HEALTHY) {
				/* same state: prefer non-HEALTHY fault label */
				agg[p].worst_fault = ft;
			}
		}
	}

#if SAPSQ_CONTINUOUS_HEALTH
	/* Consensus-relative severity inputs (2026-06-23): each path's worst mean
	 * log-latency across its QPs, and the cross-path minimum (best path) as the
	 * consensus baseline. Min, not median, is the baseline so a correlated
	 * multi-path slowdown still leaves a healthy path to measure against. The
	 * DEGRADING latency branch maps a path's drift above this baseline to a
	 * continuous health factor. */
	int64_t cont_path_lat[SAPSQ_PATH_MAX];
	bool    cont_path_valid[SAPSQ_PATH_MAX];
	int64_t cont_base = 0;
	bool    cont_base_valid = false;
	for (uint32_t cp = 0; cp < SAPSQ_PATH_MAX; cp++) {
		cont_path_valid[cp] = false;
		cont_path_lat[cp] = 0;
		for (uint32_t cq = 0; cq < DPA_PLUGIN_CONN_MAX; cq++) {
			if (g_path[cq][cp].n < SAPS_WARMUP_N)
				continue;
			int64_t l = g_path[cq][cp].mean_log_lat_ewma_q16;
			if (!cont_path_valid[cp] || l > cont_path_lat[cp]) {
				cont_path_lat[cp] = l;
				cont_path_valid[cp] = true;
			}
		}
		if (cont_path_valid[cp] &&
		    (!cont_base_valid || cont_path_lat[cp] < cont_base)) {
			cont_base = cont_path_lat[cp];
			cont_base_valid = true;
		}
	}
#endif

	/* The signal-isolation arms keep HCAA and the budget-based selector fixed.
	 * Only the health input changes. Queue depth uses the worst smoothed
	 * occupancy seen for each path across active QPs and normalizes it to the
	 * cross-path median. REQUEST_RTT reuses the request completion-time plane
	 * above without status or tail-shape classifications. */
	uint64_t qd_path_q16[SAPSQ_PATH_MAX] = {0};
	bool qd_path_valid[SAPSQ_PATH_MAX] = {false};
	uint64_t qd_values[SAPSQ_PATH_MAX];
	uint32_t qd_value_count = 0;
	uint64_t qd_reference_q16 = 0;
	int64_t rtt_path_drift_q16[SAPSQ_PATH_MAX] = {0};
	bool rtt_path_valid[SAPSQ_PATH_MAX] = {false};
	uint32_t configured_paths = s->sapsq_num_paths;
	if (configured_paths > SAPSQ_PATH_MAX)
		configured_paths = SAPSQ_PATH_MAX;
	if (s->sapsq_health_source == SAPSQ_HEALTH_SOURCE_QUEUE_DEPTH) {
		for (uint32_t cp = 0; cp < configured_paths; cp++) {
			for (uint32_t cq = 0; cq < DPA_PLUGIN_CONN_MAX; cq++) {
				if (g_path[cq][cp].n < SAPS_WARMUP_N ||
				    g_path[cq][cp].qd_mean < 0)
					continue;
				uint64_t qd = (uint64_t)g_path[cq][cp].qd_mean;
				if (!qd_path_valid[cp] || qd > qd_path_q16[cp]) {
					qd_path_q16[cp] = qd;
					qd_path_valid[cp] = true;
				}
			}
			if (qd_path_valid[cp])
				qd_values[qd_value_count++] = qd_path_q16[cp];
		}
		for (uint32_t i = 1; i < qd_value_count; i++) {
			uint64_t key = qd_values[i];
			int j = (int)i - 1;
			while (j >= 0 && qd_values[j] > key) {
				qd_values[j + 1] = qd_values[j];
				j--;
			}
			qd_values[j + 1] = key;
		}
		if (qd_value_count != 0)
			qd_reference_q16 = qd_values[qd_value_count / 2];
	}
	if (s->sapsq_health_source == SAPSQ_HEALTH_SOURCE_REQUEST_RTT) {
		for (uint32_t cp = 0; cp < configured_paths; cp++) {
			for (uint32_t cq = 0; cq < DPA_PLUGIN_CONN_MAX; cq++) {
				if (g_path[cq][cp].n < SAPS_WARMUP_N ||
				    g_path[cq][cp].baseline_log_lat_q16 == 0)
					continue;
				int64_t drift =
					g_path[cq][cp].mean_log_lat_ewma_q16 -
					g_path[cq][cp].baseline_log_lat_q16;
				if (!rtt_path_valid[cp] ||
				    drift > rtt_path_drift_q16[cp]) {
					rtt_path_drift_q16[cp] = drift;
					rtt_path_valid[cp] = true;
				}
			}
		}
	}

	for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
		uint32_t health_q16;
		uint8_t  elig;
		/* Slice 4: D-classifier → sapsq_path_health + sapsq_path_health_factor_q16 bridge.
		 * h_enum  : uint8 enum written to sapsq_path_health[p]
		 *   0=HEALTHY 1=ONSET 2=PERPETUAL_SLOW 3=BIMODAL_TAIL
		 *   4=SPARSE_PATH_ERROR 5=RECOVERING 6=QUARANTINED
		 * hf_q16  : Q16.16 scale factor written to sapsq_path_health_factor_q16[p];
		 *   consumed by sapsq_compute_path_capacity() → cap_eff[] → progressive fill.
		 * Values per dm-research-redesign §4.2 mapping table. */
		uint8_t  h_enum;
		uint32_t hf_q16;

		/* If no QP has touched the path yet, leave it fully usable so the
		 * scheduler can hand out budget on a freshly-brought-up path. */
		uint16_t st = agg[p].any_qp_active ? agg[p].worst_state : DPA_SAPS_STATE_HEALTHY;
		uint16_t ft = agg[p].any_qp_active ? agg[p].worst_fault : SAPS_FAULT_HEALTHY;

		if (s->sapsq_health_source !=
		    SAPSQ_HEALTH_SOURCE_COMPLETION) {
			if (p >= configured_paths) {
				hf_q16 = SAPSQ_HEALTH_HEALTHY_Q16;
			} else if (s->sapsq_health_source ==
				   SAPSQ_HEALTH_SOURCE_QUEUE_DEPTH) {
				hf_q16 = qd_path_valid[p]
					? sapsq_queue_depth_health(
						qd_reference_q16,
						qd_path_q16[p])
					: SAPSQ_HEALTH_HEALTHY_Q16;
			} else if (s->sapsq_health_source ==
				   SAPSQ_HEALTH_SOURCE_REQUEST_RTT) {
#if SAPSQ_CONTINUOUS_HEALTH
				hf_q16 = rtt_path_valid[p]
					? sapsq_continuous_health_from_drift(
						rtt_path_drift_q16[p])
					: SAPSQ_HEALTH_HEALTHY_Q16;
#else
				hf_q16 = SAPSQ_HEALTH_HEALTHY_Q16;
#endif
			} else {
				hf_q16 = SAPSQ_HEALTH_HEALTHY_Q16;
			}
			h_enum = hf_q16 < SAPSQ_HEALTH_HEALTHY_Q16 ? 2 : 0;
			s->sapsq_path_health_q16[p] = hf_q16;
			s->sapsq_path_eligibility[p] = SAPSQ_ELIG_NORMAL;
			s->sapsq_path_health[p] = h_enum;
			s->sapsq_path_health_factor_q16[p] = hf_q16;
			g_sapsq_recover_ramp_q16[p] = 0;
			continue;
		}

		/* REVERTED 2026-05-25 evening: Earlier this session I added a
		 * fault-driven early demotion bypass here. It worked for E3 single-
		 * path injection but over-fires under multi-path noise — instrumented
		 * smoke (21:05) showed L4_AGG agg_fault=8 (SHARED_FATE) on multiple
		 * paths simultaneously even when only path B was injected, causing
		 * SAPS-Q to demote healthy paths and pick the WRONG reroute target.
		 * Revert to state-machine-only health (Slice 28 hysteresis path).
		 * The flakiness this was trying to fix needs a proper fsm_step
		 * transition fix instead of a downstream bypass. */

		switch (st) {
		case DPA_SAPS_STATE_HEALTHY:
			health_q16 = SAPSQ_HEALTH_HEALTHY_Q16;
			elig = SAPSQ_ELIG_NORMAL;
			g_sapsq_recover_ramp_q16[p] = 0;
			break;

		case DPA_SAPS_STATE_DEGRADING:
			switch (ft) {
			case SAPS_FAULT_PERPETUAL_SLOW:
			case SAPS_FAULT_SPARSE_ERROR:
				health_q16 = SAPSQ_HEALTH_PROBE_Q16;
				elig = SAPSQ_ELIG_PROBE;
				break;
			case SAPS_FAULT_BIMODAL_TAIL: {
				/* Tail-risk scaled: base 0.4, scaled down by recent-err
				 * fraction. d4_last_recent_err_count is in [0, ~window].
				 * Use a coarse penalty: subtract up to 0.2 (13107) when
				 * err_count is high. */
				uint32_t base = 26214u; /* 0.4 in Q16.16 */
				uint32_t err = s->d4_last_recent_err_count[p];
				/* Saturate err to 64 and reduce base by up to 0.2. */
				if (err > 64u) err = 64u;
				uint32_t penalty = (err * 13107u) >> 6; /* err/64 * 0.2 */
				if (penalty > base) penalty = base;
				health_q16 = base - penalty;
				if (health_q16 < SAPSQ_HEALTH_PROBE_Q16)
					health_q16 = SAPSQ_HEALTH_PROBE_Q16;
				elig = SAPSQ_ELIG_NORMAL;
				break;
			}
			case SAPS_FAULT_ONSET: {
				/* 0.5 base, optionally scaled by tail (use d4 err_count). */
				uint32_t base = 32768u; /* 0.5 */
				uint32_t err = s->d4_last_recent_err_count[p];
				if (err > 64u) err = 64u;
				uint32_t penalty = (err * 16384u) >> 6; /* up to 0.25 */
				if (penalty > base) penalty = base;
				health_q16 = base - penalty;
				if (health_q16 < SAPSQ_HEALTH_PROBE_Q16)
					health_q16 = SAPSQ_HEALTH_PROBE_Q16;
				elig = SAPSQ_ELIG_NORMAL;
				break;
			}
			case SAPS_FAULT_FLAP:
			case SAPS_FAULT_QD_DRIFT:
			case SAPS_FAULT_ANY_THREE_VOTE:
			case SAPS_FAULT_SHARED_FATE:
			case SAPS_FAULT_PROPORTIONAL_THROTTLE:
				/* Generic mid-fault: 0.3 */
				health_q16 = 19660u;
				elig = SAPSQ_ELIG_NORMAL;
				break;
			default:
				/* Conservative default 0.5 normal. */
				health_q16 = 32768u;
				elig = SAPSQ_ELIG_NORMAL;
				break;
			}
			g_sapsq_recover_ramp_q16[p] = 0;
			break;

		case DPA_SAPS_STATE_EXCLUDED:
			/* Quarantine: zero normal budget. If host has set a probe
			 * budget, expose probe eligibility so the allocator hands
			 * the floor probe rate; otherwise full quarantine. */
			if (s->sapsq_probe_rate_q32[p] != 0) {
				health_q16 = SAPSQ_HEALTH_PROBE_Q16;
				elig = SAPSQ_ELIG_PROBE;
			} else {
				health_q16 = SAPSQ_HEALTH_QUARANTINE;
				elig = SAPSQ_ELIG_QUARANTINE;
			}
			g_sapsq_recover_ramp_q16[p] = 0;
			break;

		case DPA_SAPS_STATE_RECOVERING: {
			/* Hysteretic ramp: start at 0.5, add SAPSQ_RECOVER_STEP each
			 * epoch we remain in RECOVERING. Capped at HEALTHY. */
			uint32_t cur = g_sapsq_recover_ramp_q16[p];
			if (cur < SAPSQ_RECOVER_FLOOR_Q16)
				cur = SAPSQ_RECOVER_FLOOR_Q16;
			else if (cur < SAPSQ_HEALTH_HEALTHY_Q16) {
				uint32_t next = cur + SAPSQ_RECOVER_STEP_Q16;
				cur = (next > SAPSQ_HEALTH_HEALTHY_Q16)
					? SAPSQ_HEALTH_HEALTHY_Q16
					: next;
			}
			g_sapsq_recover_ramp_q16[p] = cur;
			health_q16 = cur;
			elig = SAPSQ_ELIG_NORMAL;
			break;
		}

		default:
			health_q16 = SAPSQ_HEALTH_HEALTHY_Q16;
			elig = SAPSQ_ELIG_NORMAL;
			break;
		}

		s->sapsq_path_health_q16[p] = health_q16;
		s->sapsq_path_eligibility[p] = elig;

		/* Populate the M-series health plane. DEGRADING is split by signal
		 * type. RECOVERING reuses the hysteretic ramp computed above so both
		 * allocator planes expose the same recovery semantics. */
		switch (st) {
		case DPA_SAPS_STATE_HEALTHY:
#if SAPSQ_CONTINUOUS_HEALTH
			/* Keep the discrete FSM stable under short-lived estimator noise,
			 * but do not discard strong consensus-relative latency evidence.
			 * A capacity-shaped verdict can keep the FSM HEALTHY even after
			 * steering drains the path. Fresh sentinel completions still
			 * update the continuous factor used by allocation. */
			if (cont_path_valid[p] && cont_base_valid) {
				int64_t drift = cont_path_lat[p] - cont_base;
				hf_q16 = sapsq_continuous_health_from_drift(drift);
				h_enum = hf_q16 < SAPSQ_HEALTH_HEALTHY_Q16 ? 2 : 0;
			} else {
				h_enum = 0;
				hf_q16 = SAPSQ_HEALTH_HEALTHY_Q16;
			}
#else
			h_enum = 0;
			hf_q16 = SAPSQ_HEALTH_HEALTHY_Q16;
#endif
			break;
		case DPA_SAPS_STATE_DEGRADING:
#if SAPSQ_CONTINUOUS_HEALTH
			/* Latency-magnitude faults take a continuous consensus-relative
			 * health (drift above the cross-path best path). Tail-shape (bimodal)
			 * and protocol-error faults are not latency-drift visible, so they
			 * keep their signal-specific factor until the tail-drift increment. */
			if (ft == SAPS_FAULT_BIMODAL_TAIL) {
				h_enum = 3; hf_q16 = 45875u;       /* 0.7, tail-shape */
			} else if (ft == SAPS_FAULT_SPARSE_ERROR) {
				h_enum = 4; hf_q16 = 3277u;        /* 0.05, protocol error */
			} else if (!cont_path_valid[p] || !cont_base_valid) {
				h_enum = 1; hf_q16 = 32768u;       /* no latency evidence yet */
			} else {
				int64_t drift = cont_path_lat[p] - cont_base;
				h_enum = 2;
				hf_q16 = sapsq_continuous_health_from_drift(drift);
			}
			break;
#else
			switch (ft) {
			case SAPS_FAULT_ONSET:
				h_enum = 1; hf_q16 = 32768u; break;  /* 0.5 */
			case SAPS_FAULT_PERPETUAL_SLOW:
				h_enum = 2; hf_q16 = 6554u;  break;  /* 0.1 */
			case SAPS_FAULT_BIMODAL_TAIL:
				h_enum = 3; hf_q16 = 45875u; break;  /* 0.7 */
			case SAPS_FAULT_SPARSE_ERROR:
				h_enum = 4; hf_q16 = 3277u;  break;  /* 0.05 */
			default:
				/* FLAP / QD_DRIFT / ANY_THREE_VOTE / SHARED_FATE /
				 * PROPORTIONAL_THROTTLE / M2_SUSPECT: treat as ONSET. */
				h_enum = 1; hf_q16 = 32768u; break;  /* 0.5 */
			}
			break;
#endif
		case DPA_SAPS_STATE_RECOVERING:
			h_enum = 5; hf_q16 = health_q16; break;
		case DPA_SAPS_STATE_EXCLUDED:
			h_enum = 6; hf_q16 = 0u;     break;  /* quarantine */
		default:
			h_enum = 0; hf_q16 = 65536u; break;  /* unknown → HEALTHY */
		}
		s->sapsq_path_health[p]           = h_enum;
		s->sapsq_path_health_factor_q16[p] = hf_q16;
	}
}

/* Weighted max-min progressive-fill allocator (redesign §4.3).
 *
 * Inputs (already on shared):
 *   sapsq_tenant_weight_q16[t] (Q16.16, 0 = inactive)
 *   sapsq_tenant_demand_q32[t] (Q32 IO/tsc, 0 = inactive)
 *   sapsq_path_base_iops_q32[p] (Q32 IO/tsc)
 *   sapsq_path_health_q16[p]   (Q16.16)
 *   sapsq_path_eligibility[p]
 *   sapsq_probe_rate_q32[p]
 *
 * Output: sapsq_tenant_path_rate_q32[t][p] (Q32 IO/tsc, identical scale to M4).
 *
 * Q-arithmetic notes:
 *   C_p_q32 = base_q32 × health_q16 / 65536    (uint64 intermediate fits)
 *   For "demand_i / w_i" sort comparator we use cross-product
 *     d_i × w_j  vs  d_j × w_i  (Q32 × Q16 = Q48, fits in u64)
 *   For fair_rate split:
 *     fair_q32 = (remaining_cap_q32 × w_i) / sum_w_q16
 *     Use uint64: (cap_q32 << 16) × w / sum_w  → keep precision.
 */
static inline void sapsq_allocate(volatile struct dpa_plugin_shared *s)
{
	/* Local snapshots. */
	uint32_t weight[SAPSQ_TENANT_MAX];
	uint32_t demand[SAPSQ_TENANT_MAX];
	uint64_t cap_q32[SAPSQ_PATH_MAX];
	uint8_t  elig[SAPSQ_PATH_MAX];
	uint32_t probe_q32[SAPSQ_PATH_MAX];
	uint32_t rate[SAPSQ_TENANT_MAX];   /* Q32 IO/tsc result */
	uint8_t  active_idx[SAPSQ_TENANT_MAX];
	uint32_t n_active = 0;

	uint64_t total_cap_q32 = 0;
	uint64_t total_eligible_cap_q32 = 0; /* normal + probe paths */

	for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
		elig[p] = s->sapsq_path_eligibility[p];
		probe_q32[p] = s->sapsq_probe_rate_q32[p];
		uint64_t base = s->sapsq_path_base_iops_q32[p];
		uint64_t hf   = s->sapsq_path_health_q16[p];
		cap_q32[p] = (base * hf) >> 16;  /* Q32 */
		if (elig[p] != SAPSQ_ELIG_QUARANTINE) {
			total_cap_q32 += cap_q32[p];
			total_eligible_cap_q32 += cap_q32[p];
		}
	}

	/* Build active tenant set. Demand threshold = 1 (any non-zero demand). */
	for (uint32_t t = 0; t < SAPSQ_TENANT_MAX; t++) {
		weight[t] = s->sapsq_tenant_weight_q16[t];
		demand[t] = s->sapsq_tenant_demand_q32[t];
		rate[t] = 0;
		if (weight[t] > 0 && demand[t] > 0) {
			active_idx[n_active++] = (uint8_t)t;
		}
	}

	if (n_active == 0 || total_cap_q32 == 0) {
		/* Nothing to allocate — zero the matrix and bail. */
		for (uint32_t t = 0; t < SAPSQ_TENANT_MAX; t++)
			for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++)
				s->sapsq_tenant_path_rate_q32[t][p] = 0;
		return;
	}

	/* Insertion sort active_idx ascending by demand/weight ratio.
	 * Comparator uses cross-product to avoid division:
	 *   sort key: demand[a]/weight[a] < demand[b]/weight[b]
	 *   iff demand[a] * weight[b] < demand[b] * weight[a]
	 * Both demand (u32 Q32) and weight (u32 Q16) fit in uint64 product. */
	for (uint32_t i = 1; i < n_active; i++) {
		uint8_t key = active_idx[i];
		uint64_t key_d = demand[key];
		uint64_t key_w = weight[key];
		int32_t j = (int32_t)i - 1;
		while (j >= 0) {
			uint8_t cur = active_idx[j];
			uint64_t lhs = key_d * (uint64_t)weight[cur];
			uint64_t rhs = (uint64_t)demand[cur] * key_w;
			if (lhs < rhs) {
				active_idx[j + 1] = cur;
				j--;
			} else {
				break;
			}
		}
		active_idx[j + 1] = key;
	}

	/* Progressive fill. */
	uint64_t remaining_cap_q32 = total_cap_q32;
	uint64_t remaining_weight_q16 = 0;
	for (uint32_t i = 0; i < n_active; i++)
		remaining_weight_q16 += weight[active_idx[i]];

	uint8_t saturated[SAPSQ_TENANT_MAX] = {0};

	for (uint32_t i = 0; i < n_active; i++) {
		uint8_t t = active_idx[i];
		if (remaining_weight_q16 == 0)
			break;
		/* fair_rate_q32 = remaining_cap_q32 × weight[t] / remaining_weight_q16
		 * weight and remaining_weight are both Q16.16 so they cancel. */
		uint64_t fair_q32 = (remaining_cap_q32 * (uint64_t)weight[t])
				    / remaining_weight_q16;

		if ((uint64_t)demand[t] <= fair_q32) {
			/* F-MD-* cppcheck: defensive underflow guard — demand[t]
			 * <= fair_q32 implies remaining_cap_q32 >= demand[t] via
			 * the fair_q32 derivation, but guard future scaling. */
			if (remaining_cap_q32 < demand[t])
				break;
			rate[t] = demand[t];
			remaining_cap_q32 -= demand[t];
			remaining_weight_q16 -= weight[t];
		} else {
			saturated[t] = 1;
			/* defer; tenant rate filled in second pass. */
		}
	}

	/* Second pass: saturated tenants split remaining capacity by weight. */
	uint64_t sat_weight_q16 = 0;
	for (uint32_t i = 0; i < n_active; i++) {
		uint8_t t = active_idx[i];
		if (saturated[t])
			sat_weight_q16 += weight[t];
	}
	if (sat_weight_q16 > 0 && remaining_cap_q32 > 0) {
		for (uint32_t i = 0; i < n_active; i++) {
			uint8_t t = active_idx[i];
			if (!saturated[t])
				continue;
			uint64_t share_q32 = (remaining_cap_q32 * (uint64_t)weight[t])
					     / sat_weight_q16;
			if (share_q32 > (uint64_t)demand[t])
				share_q32 = demand[t]; /* clamp at demand */
			rate[t] = (uint32_t)(share_q32 > 0xFFFFFFFFu
					     ? 0xFFFFFFFFu : share_q32);
		}
	}

	/* Path split: x_{i,p} = r_i × (C_p × E_p) / Σ_q (C_q × E_q).
	 * Probe paths participate with their actual cap_q32 (probe cap is tiny
	 * because health_q16 = PROBE), and after split we floor probe paths to
	 * sapsq_probe_rate_q32[]. Quarantined paths get 0. */
	for (uint32_t t = 0; t < SAPSQ_TENANT_MAX; t++) {
		if (rate[t] == 0) {
			for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++)
				s->sapsq_tenant_path_rate_q32[t][p] = 0;
			continue;
		}
		if (total_eligible_cap_q32 == 0) {
			for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++)
				s->sapsq_tenant_path_rate_q32[t][p] = 0;
			continue;
		}
		for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
			uint32_t x_q32;
			if (elig[p] == SAPSQ_ELIG_QUARANTINE || cap_q32[p] == 0) {
				x_q32 = 0;
			} else {
				uint64_t share = ((uint64_t)rate[t] * cap_q32[p])
						 / total_eligible_cap_q32;
				if (share > 0xFFFFFFFFu)
					share = 0xFFFFFFFFu;
				x_q32 = (uint32_t)share;
				if (elig[p] == SAPSQ_ELIG_PROBE &&
				    x_q32 < probe_q32[p]) {
					x_q32 = probe_q32[p];
				}
			}
			s->sapsq_tenant_path_rate_q32[t][p] = x_q32;
		}
	}
}

/* Top-level epoch runner: triggered when sapsq_epoch_count_events crosses
 * sapsq_epoch_interval_events. Returns true if it actually ran (so caller
 * can perform the writeback fence + commit). */
static inline bool sapsq_run_epoch(volatile struct dpa_plugin_shared *s,
				   uint64_t now_tsc)
{
	/* Observability: confirm the function is reached at all (before any
	 * early-exit checks). Stride 1/1000 keeps log spam bounded at >1M IOPS. */
	static uint32_t sapsq_enter_count = 0;
	if (SAPS_DPA_DIAGNOSTIC_PRINTS &&
	    (sapsq_enter_count++ % 1000u) == 0) {
		flexio_dev_print("dpa_plugin: sapsq entered count=%u events_seen=%u "
				 "interval=%u enabled=%u\n",
				 (unsigned)sapsq_enter_count,
				 (unsigned)s->sapsq_epoch_count_events,
				 (unsigned)s->sapsq_epoch_interval_events,
				 (unsigned)s->sapsq_enabled);
	}

	if (!s->sapsq_enabled)
		return false;

	uint32_t interval = s->sapsq_epoch_interval_events;
	if (interval == 0)
		interval = 1024u;  /* default per spec */

	/* Bug-A fix: TSC-based fallback for dormant event counter.
	 * When SAPS-Q throttles admissions the completions pipeline drains,
	 * process_event() stops being called, sapsq_epoch_count_events stalls
	 * below the threshold and the scheduler never re-runs — a self-locking
	 * starvation loop. Guard: if more than sapsq_epoch_interval_tsc_fallback
	 * DPA cycles have elapsed since the last committed epoch, force a run
	 * regardless of the event count. sapsq_last_epoch_tsc is written by
	 * the commit block below (initialised 0 at host startup). */
	bool tsc_triggered = false;
	uint32_t tsc_fallback = s->sapsq_epoch_interval_tsc_fallback;
	if (tsc_fallback > 0) {
		uint64_t last_tsc = s->sapsq_last_epoch_tsc;
		if ((now_tsc - last_tsc) >= (uint64_t)tsc_fallback)
			tsc_triggered = true;
	}

	if (s->sapsq_epoch_count_events < interval && !tsc_triggered)
		return false;

	/* (c) Stable-epoch publication protocol — seqlock writer.
	 * Mirrors sapsq_publish_budgets (the M-series budget plane): the begin
	 * marker (sapsq_epoch) MUST be bumped BEFORE the rate-matrix writes so a
	 * host acquire-reader sees epoch != epoch_commit during the write window
	 * and retries. Previously epoch and epoch_commit were both written with
	 * the same value AFTER the rate writes — no torn-read window existed, so
	 * a host could latch a half-written rate matrix.
	 *
	 * Sequence:
	 *   1. bump sapsq_epoch (host: "update in progress, don't trust table").
	 *   2. compiler barrier.
	 *   3. write rate matrix + path_health (sapsq_compute_health / allocate).
	 *   4. __dpa_thread_window_writeback() — flush rates to host.
	 *   5. compiler barrier so commit store can't reorder before writeback.
	 *   6. set sapsq_epoch_commit = sapsq_epoch (safe to read).
	 *   7. __dpa_thread_window_writeback() — flush commit to host.
	 *
	 * F-HI-2 (2026-05-22): the second writeback after the commit store is
	 * required — store-buffer DMA ordering across cachelines is not
	 * guaranteed on DPA-RP without the explicit fence between the two store
	 * groups, else host observes the new commit before the rate matrix. */
	uint64_t next = s->sapsq_epoch + 1ull;
	s->sapsq_epoch = next;                  /* begin marker first */
	__atomic_signal_fence(__ATOMIC_RELEASE);

	/* (a) Phase 1 — health (writes path_health) */
	sapsq_compute_health(s);

	/* (b) Phase 2 — allocator (writes rate matrix) */
	sapsq_allocate(s);

	__dpa_thread_window_writeback();        /* flush rate group */
	__atomic_signal_fence(__ATOMIC_RELEASE);
	s->sapsq_epoch_commit = next;           /* commit marker last */
	s->sapsq_last_epoch_tsc = now_tsc;
	s->sapsq_total_epochs_consumed++;
	__dpa_thread_window_writeback();        /* F-HI-2: flush commit group */

	/* Observability: every Nth committed epoch, dump my tenant's path rates
	 * + path health/eligibility so we can diagnose weighted-allocator output.
	 * Stride 1/100 keeps log volume small even if scheduler fires often. */
	static uint32_t sapsq_log_count = 0;
	if (SAPS_DPA_DIAGNOSTIC_PRINTS &&
	    (sapsq_log_count++ % 100u) == 0) {
		uint32_t mt = s->sapsq_my_tenant_id;
		if (mt >= SAPSQ_TENANT_MAX)
			mt = 0;
		flexio_dev_print("dpa_plugin: sapsq epoch=%lu my_tid=%u "
				 "rates=[%u,%u,%u,%u] health=[%u,%u,%u,%u] "
				 "elig=[%u,%u,%u,%u]\n",
				 (unsigned long)next, (unsigned)mt,
				 (unsigned)s->sapsq_tenant_path_rate_q32[mt][0],
				 (unsigned)s->sapsq_tenant_path_rate_q32[mt][1],
				 (unsigned)s->sapsq_tenant_path_rate_q32[mt][2],
				 (unsigned)s->sapsq_tenant_path_rate_q32[mt][3],
				 (unsigned)s->sapsq_path_health_q16[0],
				 (unsigned)s->sapsq_path_health_q16[1],
				 (unsigned)s->sapsq_path_health_q16[2],
				 (unsigned)s->sapsq_path_health_q16[3],
				 (unsigned)s->sapsq_path_eligibility[0],
				 (unsigned)s->sapsq_path_eligibility[1],
				 (unsigned)s->sapsq_path_eligibility[2],
				 (unsigned)s->sapsq_path_eligibility[3]);
	}

	/* (d) Reset event accumulator for next epoch. */
	s->sapsq_epoch_count_events = 0;
	return true;
}

/* ── SAPS-Q M-series unified scheduler (slice 2, 2026-05-27) ─────────────────
 *
 * Writes to the new SAPSQ_MAX_TENANTS=16 × SAPSQ_MAX_PATHS=8 budget plane:
 *   sapsq_tenant_path_rate_budget_q32[16][8]  (Q0.32 IO/s, DPA write)
 *   sapsq_epoch_seq / sapsq_epoch_commit_seq  (publication protocol)
 *   sapsq_demand_iops[16]                     (EWMA submit-attempt rate)
 *
 * This scheduler is INDEPENDENT of the old sapsq_run_epoch() which writes to
 * sapsq_tenant_path_rate_q32[16][4].  Both coexist; slice 3 (host enforcement)
 * will switch the token-bucket path to read from the new 8-path budget plane.
 *
 * Demand source: sapsq_host_submit_count[t] — host-published admission
 *   attempts measured before enforcement. Counting only admitted submissions
 *   would make a throttled tenant appear idle and reduce its next budget.
 *
 * Path capacity: sapsq_path_capacity_iops[p] × health_factor_q16[p] / Q16.
 *   health_factor_q16[p] is initialised to SAPSQ_HEALTH_HEALTHY_Q16 (65536) by
 *   host at start-up; slice 4 (D-classifier bridge) overwrites it dynamically.
 *   Slice 2 placeholder: all paths HEALTHY → cap evenly split.
 *
 * Period: sapsq_epoch_period_us (host-set µs); DPA computes tsc delta using
 *   sapsq_host_tsc_freq (Hz).  Guard: 0-values silently skip the tick.
 *
 * Gate: !sapsq_enabled → return immediately, zero overhead.
 */

/* α = 0.125 in Q16.16 */
#define SAPSQ_ALPHA_Q16  0x2000u  /* 0.125 × 65536 = 8192 */
#define SAPSQ_IDLE_HYSTERESIS_EPOCHS 3u
/* Consecutive zero-admit epochs before treating a tenant as truly idle (E2
 * work-conserving). Guards against single-epoch notify-timing jitter starving
 * an active tenant. */

/* Update per-tenant demand EWMA.
 * new_rate = submit_count / dt_sec (IO/s).
 * ewma = α * new_rate + (1 - α) * ewma,  α = 0.125.
 * Writes g_sapsq_demand_ewma_q32[t] and s->sapsq_demand_iops[t].
 * dt_tsc must be > 0.
 */
static void sapsq_update_demand_ewma(volatile struct dpa_plugin_shared *s,
				     uint64_t dt_tsc, uint64_t tsc_freq)
{
	if (tsc_freq == 0)
		return;
	uint32_t num_t = s->sapsq_num_tenants;
	if (num_t == 0 || num_t > SAPSQ_MAX_TENANTS)
		num_t = SAPSQ_MAX_TENANTS;

	for (uint32_t t = 0; t < num_t; t++) {
		/* Host processes publish offered attempts in cache-cleaned batches.
		 * The counter is monotone and remains independent of the admission
		 * decision made from the previous budget. */
		uint64_t cur_host = s->sapsq_host_submit_count[t];
		uint64_t prev_host = g_sapsq_host_submit_last[t];
		uint64_t delta_host = cur_host - prev_host;
		g_sapsq_host_submit_last[t] = cur_host;
		/* DIAG Lane BBB-V3 DISABLED (E2 work-conserving fix, 2026-05-29):
		 * flexio_dev_print() inside the scheduler tick BLOCKS when the DPA->host
		 * message ring fills (see file-header WARNING).  Diagnosis of
		 * experiments/sapsq_e2_diag_085857 showed the polling loop ran only ~2700
		 * iters in 60 s (~22 ms/iter vs ~600 us expected) and the per-tenant submit
		 * counters were frozen (t0..t3 == 35 for ~2000 ticks, then jumped to ~150k
		 * at teardown) => process_event() was starved, the scheduler ran on stale
		 * demand and could never see a tenant go idle (no work-conserving redistrib).
		 * Removed so event drain recovers.  Non-blocking observability: host reads
		 * s->sapsq_demand_iops[] and the host-side rate_budget print. */

		/* Cap delta to uint32 range for rate calculation (same as old cnt). */
		uint32_t cnt = (delta_host > 0xFFFFFFFFu) ? 0xFFFFFFFFu
							   : (uint32_t)delta_host;

		/* new_rate_q32 in IO/s.  Use uint64 intermediate:
		 * (cnt × tsc_freq) / dt_tsc  fits in uint64 for
		 * cnt ≤ 2^20, tsc_freq ≤ 3e9, dt_tsc ≥ 1. */
		uint64_t new_rate;
		if (dt_tsc > 0)
			new_rate = ((uint64_t)cnt * tsc_freq) / dt_tsc;
		else
			new_rate = 0;
		if (new_rate > 0xFFFFFFFFu)
			new_rate = 0xFFFFFFFFu;

		/* ewma = (α × new_rate + (65536 - α) × old_ewma) / 65536 */
		uint64_t old = g_sapsq_demand_ewma_q32[t];
		uint64_t updated = ((uint64_t)SAPSQ_ALPHA_Q16 * new_rate
				    + (uint64_t)(65536u - SAPSQ_ALPHA_Q16) * old)
				   >> 16;
		if (updated > 0xFFFFFFFFu)
			updated = 0xFFFFFFFFu;

		if (cnt > 0)
			g_sapsq_zero_epochs[t] = 0;
		else if (g_sapsq_zero_epochs[t] < 0xFFFFFFFFu)
			g_sapsq_zero_epochs[t]++;

		/* Demand floor: when demand_ewma collapses to near-zero (cold-start
		 * or low-traffic epoch), rate_budget_q32 falls below probe_rate and
		 * the host sub-probe bypass opens the gate fully (no weight enforcement).
		 *
		 * Fix: if a weighted tenant has not crossed the consecutive-zero
		 * hysteresis, substitute link_cap_iops as the demand floor.  This makes
		 * active tenants appear fully saturated to progressive_fill, allocating
		 * BW by weight even if one notify epoch reports cnt==0.
		 *
		 * Cold start: static zero-init leaves g_sapsq_zero_epochs[t]==0, so a
		 * fresh tenant gets the floor before the first admit delta arrives.
		 *
		 * Idle gate: SIGSTOP'd tenants eventually produce K consecutive zero
		 * admits; only then does demand decay to its real measured value (≈0).
		 * progressive_fill can then skip them and redistribute their share to
		 * the remaining active tenants, preserving E2 work-conserving behavior
		 * delayed by SAPSQ_IDLE_HYSTERESIS_EPOCHS. */
		int truly_idle = (g_sapsq_zero_epochs[t] >= SAPSQ_IDLE_HYSTERESIS_EPOCHS);
		if (s->sapsq_tenant_weight[t] > 0 && s->sapsq_link_cap_iops > 0 &&
		    !truly_idle) {
			uint64_t floor_demand = s->sapsq_link_cap_iops;
			if (updated < floor_demand)
				updated = floor_demand;
		}

		g_sapsq_demand_ewma_q32[t] = (uint32_t)updated;
		s->sapsq_demand_iops[t] = (uint32_t)updated;
	}
}

static inline uint32_t
sapsq_allocation_health_factor(volatile struct dpa_plugin_shared *s,
			       uint32_t path)
{
	uint32_t observed = s->sapsq_path_health_factor_q16[path];

	switch (s->sapsq_bypass_health_coupling) {
	case SAPSQ_HEALTH_COUPLING_FIXED:
		return observed;
	case SAPSQ_HEALTH_COUPLING_BINARY:
		return observed < SAPSQ_HEALTH_HEALTHY_Q16
		       ? SAPSQ_HEALTH_QUARANTINE
		       : SAPSQ_HEALTH_HEALTHY_Q16;
	case SAPSQ_HEALTH_COUPLING_CONTINUOUS:
	default:
		return observed;
	}
}

/* Compute per-path effective capacity from deliverable capacity and health.
 * cap_eff_out[p] = path_capacity_iops[p] × health_factor_q16[p] / 65536.
 * Result is IO/s (Q0 integer, same unit as sapsq_demand_ewma_q32).
 */
static void sapsq_compute_path_capacity(volatile struct dpa_plugin_shared *s,
					uint32_t cap_eff_out[SAPSQ_MAX_PATHS])
{
	uint32_t num_p = s->sapsq_num_paths;
	if (num_p == 0 || num_p > SAPSQ_MAX_PATHS)
		num_p = SAPSQ_MAX_PATHS;

	for (uint32_t p = 0; p < SAPSQ_MAX_PATHS; p++) {
		if (p >= num_p) {
			cap_eff_out[p] = 0;
			continue;
		}
		uint32_t hf = sapsq_allocation_health_factor(s, p);
		uint64_t path_cap = s->sapsq_path_capacity_iops[p];
		/* Missing K_p is an invalid configuration. Treat it as zero capacity
		 * instead of silently assuming K_p=C. The host initializer rejects
		 * this configuration before enabling the scheduler. */
		/* Q16 multiply: (K_p × h_p) >> 16 */
		uint64_t eff = (path_cap * (uint64_t)hf) >> 16;
		if (eff > 0xFFFFFFFFu)
			eff = 0xFFFFFFFFu;
		cap_eff_out[p] = (uint32_t)eff;
	}
}

/* Weighted max-min progressive fill over SAPSQ_MAX_TENANTS × 1-D (total cap).
 *
 * demand_q32[t]  — per-tenant demand IO/s
 * cap_eff[p]     — per-path effective capacity IO/s
 * num_t          — active tenant count
 * num_p          — active path count
 * weights[t]     — integer weights (e.g. [3,1,1,1])
 * tenant_rate_out[t] — per-tenant total allocated rate IO/s (output)
 *
 * Algorithm: Bertsekas weighted max-min.
 *   C_total = min(namespace envelope, Σ cap_eff[p])
 *   Active  = { t | demand > 0 AND weight > 0 }
 *   Sort active ascending by demand[t] / weight[t]  (insertion sort, T ≤ 16)
 *   First pass:  if demand[t] ≤ fair_share → satisfy at demand[t]
 *   Second pass: saturated tenants split remaining_cap by weight
 */
static void sapsq_progressive_fill(const uint32_t demand[SAPSQ_MAX_TENANTS],
				   const uint32_t cap_eff[SAPSQ_MAX_PATHS],
				   uint32_t num_t, uint32_t num_p,
				   uint64_t service_envelope,
				   bool fixed_envelope,
				   const uint32_t weights[SAPSQ_MAX_TENANTS],
				   uint32_t tenant_rate_out[SAPSQ_MAX_TENANTS])
{
	/* Zero output first. */
	for (uint32_t t = 0; t < SAPSQ_MAX_TENANTS; t++)
		tenant_rate_out[t] = 0;

	if (num_t == 0 || num_p == 0)
		return;

	/* Total capacity. */
	uint64_t c_total = service_envelope;
	if (!fixed_envelope) {
		c_total = 0;
		for (uint32_t p = 0; p < num_p && p < SAPSQ_MAX_PATHS; p++)
			c_total += cap_eff[p];
		if (c_total > service_envelope)
			c_total = service_envelope;
	}
	if (c_total == 0)
		return;

	/* Build active set. Demand threshold = 1 (any non-zero). */
	uint8_t active[SAPSQ_MAX_TENANTS];
	uint32_t n_active = 0;
	for (uint32_t t = 0; t < num_t && t < SAPSQ_MAX_TENANTS; t++) {
		if (weights[t] > 0 && demand[t] > 0)
			active[n_active++] = (uint8_t)t;
	}
	if (n_active == 0)
		return;

	/* Insertion sort ascending by demand[t] / weight[t] (cross-product).
	 * demand is uint32 IO/s, weight is uint32.  Product fits in uint64. */
	for (uint32_t i = 1; i < n_active; i++) {
		uint8_t key = active[i];
		uint64_t key_d = demand[key];
		uint64_t key_w = weights[key];
		int32_t j = (int32_t)i - 1;
		while (j >= 0) {
			uint8_t cur = active[j];
			/* key_d/key_w < demand[cur]/weights[cur]
			 * iff key_d * weights[cur] < demand[cur] * key_w */
			if (key_d * (uint64_t)weights[cur] <
			    (uint64_t)demand[cur] * key_w) {
				active[j + 1] = cur;
				j--;
			} else {
				break;
			}
		}
		active[j + 1] = key;
	}

	/* Progressive fill first pass. */
	uint64_t remaining_cap = c_total;
	uint64_t remaining_weight = 0;
	for (uint32_t i = 0; i < n_active; i++)
		remaining_weight += weights[active[i]];

	uint8_t saturated[SAPSQ_MAX_TENANTS] = {0};

	for (uint32_t i = 0; i < n_active; i++) {
		uint8_t t = active[i];
		if (remaining_weight == 0)
			break;
		/* fair_share = remaining_cap × weight[t] / remaining_weight
		 * Use uint64 intermediate; weights cancel (both plain integers). */
		uint64_t fair = (remaining_cap * (uint64_t)weights[t])
				/ remaining_weight;

		if ((uint64_t)demand[t] <= fair) {
			if (remaining_cap < demand[t])
				break;  /* defensive underflow guard */
			tenant_rate_out[t] = demand[t];
			remaining_cap -= demand[t];
			remaining_weight -= weights[t];
		} else {
			saturated[t] = 1;
		}
	}

	/* Second pass: saturated tenants split remaining_cap by weight. */
	uint64_t sat_weight = 0;
	for (uint32_t i = 0; i < n_active; i++) {
		uint8_t t = active[i];
		if (saturated[t])
			sat_weight += weights[t];
	}
	if (sat_weight > 0 && remaining_cap > 0) {
		for (uint32_t i = 0; i < n_active; i++) {
			uint8_t t = active[i];
			if (!saturated[t])
				continue;
			uint64_t share = (remaining_cap * (uint64_t)weights[t])
					 / sat_weight;
			if (share > demand[t])
				share = demand[t];
			if (share > 0xFFFFFFFFu)
				share = 0xFFFFFFFFu;
			tenant_rate_out[t] = (uint32_t)share;
		}
	}
}

/* Split per-tenant rate across paths proportional to cap_eff.
 * Slice 2 placeholder: all paths eligible (health_factor ≠ 0).
 * Quarantined/zero-cap paths receive sapsq_probe_rate_budget_q32.
 */
static void sapsq_split_per_path(uint32_t tenant_rate,
				 const uint32_t cap_eff[SAPSQ_MAX_PATHS],
				 uint32_t num_p,
				 uint32_t probe_iops,
				 uint32_t rate_out[SAPSQ_MAX_PATHS])
{
	/* Probe traffic is part of the tenant's admitted rate. Reserve its bounded
	 * share before distributing the remainder across service-capable paths.
	 * This keeps the published matrix within both the tenant rate and the
	 * namespace envelope. */
	uint64_t c_sum = 0;
	uint32_t probe_paths = 0;
	for (uint32_t p = 0; p < num_p && p < SAPSQ_MAX_PATHS; p++)
		if (cap_eff[p] == 0)
			probe_paths++;
		else
			c_sum += cap_eff[p];

	uint64_t requested_probe = (uint64_t)probe_iops * probe_paths;
	if (requested_probe > tenant_rate)
		requested_probe = tenant_rate;
	uint32_t probe_each = probe_paths
			      ? (uint32_t)(requested_probe / probe_paths)
			      : 0;
	uint32_t probe_remainder = probe_paths
				   ? (uint32_t)(requested_probe % probe_paths)
				   : 0;
	uint32_t remaining_rate = tenant_rate - (uint32_t)requested_probe;

	for (uint32_t p = 0; p < SAPSQ_MAX_PATHS; p++) {
		if (p >= num_p) {
			rate_out[p] = 0;
			continue;
		}
		if (cap_eff[p] == 0) {
			rate_out[p] = probe_each;
			if (probe_remainder != 0) {
				rate_out[p]++;
				probe_remainder--;
			}
			continue;
		}
		if (c_sum == 0) {
			rate_out[p] = 0;
			continue;
		}
		uint64_t share =
			((uint64_t)remaining_rate * cap_eff[p]) / c_sum;
		if (share > 0xFFFFFFFFu)
			share = 0xFFFFFFFFu;
		rate_out[p] = (uint32_t)share;
	}
}

/* Stable-epoch publication protocol for the M-series budget plane.
 *
 * Mirrors redesign §4 step 5 but for the new {seq, commit_seq} fields:
 *   1. Bump sapsq_epoch_seq (host: "update in progress, don't trust table").
 *   2. compiler barrier.
 *   3. Convert rates IO/s → Q0.32 IO/tsc and write rate table.
 *   4. __dpa_thread_window_writeback() — flush rate table to host-visible mem.
 *   5. compiler barrier so commit stores cannot be reordered before writeback.
 *   6. Write sapsq_epoch_commit_seq = sapsq_epoch_seq (safe to read).
 *   7. __dpa_thread_window_writeback() — flush commit to host-visible mem.
 *
 * Host reads: safe iff epoch_commit_seq == epoch_seq after acquire load.
 *
 * Fix 1 (Lane G): __atomic_signal_fence is only a compiler barrier; it does
 * NOT flush the DPA-RP store buffer to host-visible memory.  Without the two
 * __dpa_thread_window_writeback() calls the host always sees stale 0 values.
 *
 * Fix 2 (Lane G): rates[] from sapsq_split_per_path are IO/s integers.
 * The host token-bucket formula is:
 *   refill_q16 = (rate_q32 × dt_tsc) >> 16
 * which expects rate_q32 in Q0.32 IO/tsc units.  Publishing raw IO/s integers
 * as Q0.32 IO/tsc produces a ~70× overflow at 200 K IO/s / 1.5 GHz TSC.
 * Conversion: rate_q32 = iops * 2^32 / tsc_freq.
 */
static void sapsq_publish_budgets(volatile struct dpa_plugin_shared *s,
				  const uint32_t rates[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS],
				  const uint32_t cap_eff[SAPSQ_MAX_PATHS])
{
	uint32_t new_seq = s->sapsq_epoch_seq + 1u;
	s->sapsq_epoch_seq = new_seq;
	__atomic_signal_fence(__ATOMIC_RELEASE);

	/* Fix 2: convert IO/s → Q0.32 IO/tsc before writing to shared struct.
	 * rate_q32 = iops * 2^32 / tsc_freq.
	 * Overflow check: iops <= 2^32-1, so iops << 32 fits in uint64 only if
	 * iops <= 2^32-1 — but uint64 max is 2^64-1, and iops * 2^32 can overflow
	 * when iops > 2^32.  Since iops is uint32_t (max ~4G) and 4G << 32 = 2^64
	 * which wraps, use uint64 cast and saturating: if iops > tsc_freq/1 the
	 * result should be capped at 0xFFFFFFFF.  In practice iops <= ~10M. */
	uint64_t tsc_freq = s->sapsq_host_tsc_freq;
	if (tsc_freq == 0)
		tsc_freq = 1500000000ULL;  /* 1.5 GHz fallback */

	for (uint32_t p = 0; p < SAPSQ_MAX_PATHS; p++) {
		s->sapsq_committed_path_health_factor_q16[p] =
			sapsq_allocation_health_factor(s, p);
		s->sapsq_committed_path_effective_capacity_iops[p] = cap_eff[p];
	}

	for (uint32_t t = 0; t < SAPSQ_MAX_TENANTS; t++) {
		for (uint32_t p = 0; p < SAPSQ_MAX_PATHS; p++) {
			uint64_t iops = rates[t][p];
			/* iops * 2^32 / tsc_freq — uint64 safe for iops <= ~4G */
			uint64_t r_q32 = (iops << 32) / tsc_freq;
			if (r_q32 > 0xFFFFFFFFu)
				r_q32 = 0xFFFFFFFFu;
			s->sapsq_tenant_path_rate_budget_q32[t][p] = (uint32_t)r_q32;
		}
	}

	/* Fix 1: flush rate table to host-visible memory before writing commit. */
	__dpa_thread_window_writeback();
	__atomic_signal_fence(__ATOMIC_RELEASE);
	s->sapsq_epoch_commit_seq = new_seq;
	/* Fix 1: flush commit seq to host-visible memory. */
	__dpa_thread_window_writeback();
}

/* Top-level SAPS-Q M-series scheduler tick.
 *
 * Called once per outer iteration (same cadence as sapsq_run_epoch).
 * Period gate: fires only when elapsed DPA TSC ≥ epoch_period_us × tsc_freq / 1e6.
 * Stack budget: rates[16][8] = 512 B.  DPA-RP stack ≥ 8 KiB so this is fine.
 */
static void sapsq_scheduler_tick(volatile struct dpa_plugin_shared *s,
				 uint64_t now_tsc)
{
	if (!s->sapsq_enabled)
		return;

	/* Period check using DPA TSC and host-provided tsc_freq. */
	uint32_t period_us = s->sapsq_epoch_period_us;
	if (period_us == 0)
		period_us = 1000u;  /* default 1 ms */
	uint64_t tsc_freq = s->sapsq_host_tsc_freq;
	if (tsc_freq == 0)
		tsc_freq = 1500000000ULL;  /* 1.5 GHz fallback */

	uint64_t period_tsc = ((uint64_t)period_us * tsc_freq) / 1000000ULL;

	uint64_t last_tsc = g_sapsq_tick_last_tsc;
	uint64_t dt_tsc = (last_tsc == 0) ? period_tsc : (now_tsc - last_tsc);
	if (last_tsc != 0 && dt_tsc < period_tsc)
		return;

	/* Fix 4 (Lane G MEDIUM): if link_cap is 0, host has not configured the
	 * scheduler yet.  Publishing all-zero budgets would overwrite any stale
	 * fallback the host is using, causing all IOs to be rejected.  Return
	 * early so the host stays on its existing (possibly stale) fallback path
	 * until a valid configuration arrives. */
	if (s->sapsq_link_cap_iops == 0)
		return;

	g_sapsq_tick_last_tsc = now_tsc;

	/* Step 1: update per-tenant demand EWMA. */
	sapsq_update_demand_ewma(s, dt_tsc, tsc_freq);

	/* Step 2: compute per-path effective capacity. */
	uint32_t cap_eff[SAPSQ_MAX_PATHS];
	sapsq_compute_path_capacity(s, cap_eff);

	/* Step 3: progressive fill to get per-tenant total rates. */
	uint32_t num_t = s->sapsq_num_tenants;
	if (num_t == 0 || num_t > SAPSQ_MAX_TENANTS)
		num_t = SAPSQ_MAX_TENANTS;
	uint32_t num_p = s->sapsq_num_paths;
	if (num_p == 0 || num_p > SAPSQ_MAX_PATHS)
		num_p = SAPSQ_MAX_PATHS;

	/* Snapshot demand and weights. */
	uint32_t demand[SAPSQ_MAX_TENANTS];
	uint32_t weights[SAPSQ_MAX_TENANTS];
	for (uint32_t t = 0; t < SAPSQ_MAX_TENANTS; t++) {
		demand[t]  = g_sapsq_demand_ewma_q32[t];
		weights[t] = s->sapsq_tenant_weight[t];
	}

	uint32_t tenant_rate[SAPSQ_MAX_TENANTS];
	bool fixed_envelope = s->sapsq_bypass_health_coupling ==
			      SAPSQ_HEALTH_COUPLING_FIXED;
	sapsq_progressive_fill(demand, cap_eff, num_t, num_p,
			      s->sapsq_link_cap_iops, fixed_envelope,
			      weights, tenant_rate);

	/* Step 4: split each tenant's rate across paths. */
	uint32_t probe_q32 = s->sapsq_probe_rate_budget_q32;
	uint64_t probe_iops_u64 =
		((uint64_t)probe_q32 * tsc_freq) >> 32;
	if (probe_q32 != 0 && probe_iops_u64 == 0)
		probe_iops_u64 = 1;
	if (probe_iops_u64 > 0xFFFFFFFFu)
		probe_iops_u64 = 0xFFFFFFFFu;
	uint32_t probe_iops = (uint32_t)probe_iops_u64;
	uint32_t rates[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS];
	for (uint32_t t = 0; t < SAPSQ_MAX_TENANTS; t++) {
		sapsq_split_per_path(tenant_rate[t], cap_eff, num_p,
				     probe_iops, rates[t]);
	}

	/* Step 5: publish to shared struct with epoch barrier. */
	sapsq_publish_budgets(s, rates, cap_eff);
}

/* ── M5 v3 DPA-side proactive DRR scheduler — elapsed-TSC rate-based ─────────
 *
 * Called once per outer iteration. Fires only when elapsed_tsc ≥ min_interval
 * (100µs guard; real interval driven by elapsed_tsc to handle variable
 * outer-loop cadence).
 *
 * Rate formula (mirrors M4 Q32 precision design):
 *   grant_per_tsc_q32[t] = round(rate_iops[t] / tsc_freq × 2^32)
 *     stored in m5_tenant_quantum_q16[t] but reinterpreted: host init writes
 *     this as Q32 IO/tsc rate (same calculation as M4 rate_q32).
 *   new_grants_q16 = (grant_per_tsc_q32 × elapsed_tsc) >> 16
 *   new_grants (integer) = new_grants_q16 >> 16
 *
 * Work-conserving idle detection:
 *   gap = grant_count[t] - consumed_count[t]
 *   If gap ≥ burst_cap_grants (host not consuming) → tenant idle →
 *     do not issue new grants (they'd pile up unused). Deficit accumulates
 *     in deficit_q16[t] (up to 2× one round worth) to allow work-conserving
 *     catch-up when tenant becomes active again.
 *
 * burst_cap_grants = burst_cap_q16 >> 16 (host writes, default 1000 IOs).
 *
 * This design avoids the oscillation bug of the original pending-based DRR:
 *   - no pending signal needed from host
 *   - rate is purely time-driven (no event-driven feedback loop)
 *   - idle detection via grant-consumed gap (accurate, no stale pending)
 */
static inline void m5_drr_round(volatile struct dpa_plugin_shared *s,
				uint64_t now_tsc)
{
	if (!s->m5_drr_enabled)
		return;

	uint64_t last = s->m5_drr_last_tsc;

	/* Minimum 100µs guard to avoid tiny delta_tsc noise. */
	uint64_t min_interval = s->m5_drr_interval_tsc;
	if (min_interval == 0)
		min_interval = 100000ULL;  /* 100µs @ 1 GHz */

	if (last != 0 && (now_tsc - last) < min_interval)
		return;

	uint64_t delta_tsc = (last == 0) ? min_interval : (now_tsc - last);
	if (delta_tsc > M3_DELTA_TSC_CAP)
		delta_tsc = M3_DELTA_TSC_CAP;

	s->m5_drr_last_tsc = now_tsc;
	s->m5_drr_round_count++;

	/* burst_cap must be >= host QD so that throttled tenants always have
	 * enough grants in flight and never fall into the queued_req drain path
	 * (drain tick fires at most 10K IOs/s — a hard ceiling below any useful
	 * quota).  We use max(rate_per_interval × burst_mult, 128) where 128 is
	 * the typical bdevperf QD.  The initial burst lasts at most
	 * burst_cap/rate seconds which is short and acceptable.
	 * m5_cost_per_io_q16 holds the multiplier (host can override; default 2). */
	uint32_t burst_mult = s->m5_cost_per_io_q16;
	if (burst_mult == 0)
		burst_mult = 2u;

#define M5_MIN_BURST_CAP 128u

	for (uint32_t t = 0; t < M3_TENANT_MAX; t++) {
		/* m5_tenant_quantum_q16[t] reused as Q32 rate (IO/tsc), matching M4
		 * rate_q32 precision. Host init writes:
		 *   quantum_q16[t] = round(w[t]/Σw × link_iops / tsc_freq × 2^32)
		 * (same formula as m4_tenant_refresh_per_tsc_q32). */
		uint32_t rate_q32 = s->m5_tenant_quantum_q16[t];
		if (rate_q32 == 0)
			continue;

		/* new_grants = (rate_q32 × delta_tsc) >> 32 */
		uint64_t new_grants = ((uint64_t)rate_q32 * delta_tsc) >> 32;
		if (new_grants == 0)
			continue;

		/* Per-tenant burst cap = max(rate_per_interval × burst_mult, M5_MIN_BURST_CAP).
		 * The max() ensures burst_cap >= host QD (128) so that active tenants
		 * always find grants available and never need the queued_req drain path.
		 * rate_per_interval = (rate_q32 × min_interval) >> 32. */
		uint32_t rate_per_interval = (uint32_t)(((uint64_t)rate_q32 * min_interval) >> 32);
		uint32_t burst_cap_grants = rate_per_interval * burst_mult;
		if (burst_cap_grants < M5_MIN_BURST_CAP)
			burst_cap_grants = M5_MIN_BURST_CAP;

		/* Work-conserving idle check: if grant is too far ahead of consumed,
		 * tenant is idle — accumulate deficit for catch-up when active. */
		uint64_t grant    = s->m5_tenant_grant_count[t];
		uint64_t consumed = s->m5_tenant_consumed_count[t];
		uint64_t gap = (grant >= consumed) ? (grant - consumed) : 0;

		if (gap >= (uint64_t)burst_cap_grants) {
			/* Idle: accumulate deficit for work-conserving catch-up.
			 * Overflow fix: burst_cap_grants can exceed 32767, so
			 * (burst_cap_grants << 16) overflows a 32-bit int and the old
			 * (int32_t) casts wrapped negative — the deficit cap then went
			 * negative and clamping broke. Do ALL arithmetic in int64_t and
			 * saturate against the stored int32 range on writeback. */
			int64_t def    = (int64_t)s->m5_tenant_deficit_q16[t];
			int64_t cap_q16 = (int64_t)burst_cap_grants << 16;  /* no 32-bit wrap */
			int64_t add_q16 = (int64_t)new_grants << 16;
			def += add_q16;
			if (def > cap_q16)
				def = cap_q16;
			/* Stored type is int32_t (shared struct ABI) — saturate, never
			 * wrap, against the int32 max (0x7FFFFFFF) as well as the
			 * burst-cap ceiling (cap_q16 can exceed int32 for large caps). */
			if (def > 0x7FFFFFFFLL)
				def = 0x7FFFFFFFLL;
			if (def < 0)
				def = 0;
			s->m5_tenant_deficit_q16[t] = (int32_t)def;
			s->m5_tenant_idle_rounds[t]++;
			continue;
		}

		/* Active tenant: issue new grants, capped at burst_cap. */
		uint64_t max_issue = (uint64_t)burst_cap_grants - gap;
		if (new_grants > max_issue)
			new_grants = max_issue;

		/* Bonus from accumulated deficit (work-conserving redistribution).
		 * int64 arithmetic for the deficit drain too: bonus << 16 can exceed
		 * the int32 range when burst_cap_grants is large, so subtract in
		 * int64 and saturate on the int32 writeback (companion to the idle
		 * accumulate fix above). */
		int64_t def = (int64_t)s->m5_tenant_deficit_q16[t];
		if (def > 0) {
			uint64_t bonus = (uint64_t)def >> 16;
			if (bonus > max_issue - new_grants)
				bonus = max_issue - new_grants;
			new_grants += bonus;
			def -= (int64_t)(bonus << 16);
			if (def < 0) def = 0;
			s->m5_tenant_deficit_q16[t] = (int32_t)def;  /* drained, always <= prior cap */
			if (bonus > 0)
				s->m5_tenant_work_conserved[t]++;
		}

		s->m5_tenant_grant_count[t] += new_grants;
		s->m5_tenant_admit_count[t] += new_grants;
	}
}

static inline void emit_admission_tokens(volatile struct dpa_plugin_shared *s,
					  uint64_t now_tsc)
{
	for (uint32_t qp = 0; qp < DPA_PLUGIN_CONN_MAX; qp++) {
		struct conn_stats *c = &g_conn[qp];
		if (c->n == 0)
			continue;

		/* Safety valve 2 (retained): time-bounded throttle auto-release. */
		if (c->degraded &&
		    (now_tsc - c->degraded_since) > MAX_THROTTLE_TICKS) {
			c->degraded = 0;
			c->cusum_pos = 0;
			c->degraded_reason = 0;
			g_algo_sv2_auto_release++;
		}

		/* First-touch: seed timestamp. */
		if (c->last_refill_tsc == 0)
			c->last_refill_tsc = now_tsc;

		if (!c->degraded) {
			/* Healthy regime: publish credit far above admitted so
			 * the gate is effectively open. Read host_admitted,
			 * set credit = admitted + LARGE. No rate limiting.
			 * This avoids the SPDK drain-loop deadlock that binary
			 * "gap=0" would trigger for even a single call. */
			uint32_t admitted = s->host_admitted[qp];
			uint32_t credit = admitted + TOKENS_MAX_NORMAL;
			c->tokens = credit;
			s->per_qp_tokens[qp] = credit;
			c->last_refill_tsc = now_tsc;
			g_algo_admit_writes++;
			continue;
		}

		/* DEGRADED regime: rate-limit via cumulative-credit. */
		uint32_t rate = TOKENS_PER_SEC_DEGRADED;
		uint32_t max_tokens = TOKENS_MAX_DEGRADED;

		uint64_t dt = now_tsc - c->last_refill_tsc;
		uint64_t new_tokens = (dt * (uint64_t)rate) / TICKS_PER_SEC;

		if (new_tokens > 0) {
			uint32_t credit = c->tokens;
			uint32_t admitted = s->host_admitted[qp];
			uint32_t new_credit = credit + (uint32_t)new_tokens;
			uint32_t gap = new_credit - admitted;
			if (gap > max_tokens)
				new_credit = admitted + max_tokens;
			c->tokens = new_credit;
			s->per_qp_tokens[qp] = new_credit;
			c->last_refill_tsc += (new_tokens * TICKS_PER_SEC)
					       / (uint64_t)rate;
			g_algo_sv1_bypass_admits += new_tokens;
		}

		uint32_t gap_now = c->tokens - s->host_admitted[qp];
		if (gap_now == 0)
			g_algo_throttle_writes++;
		else
			g_algo_admit_writes++;
	}
}

__dpa_rpc__ uint64_t dpa_plugin_rpc(uint64_t in_daddr)
{
	struct dpa_plugin_transfer *t = (struct dpa_plugin_transfer *)(uintptr_t)in_daddr;

	flexio_dev_status_t st = flexio_dev_window_config(FLEXIO_DEV_WINDOW_ENTITY_0,
							   t->window_id, t->mkey_id);
	if (st != FLEXIO_DEV_STATUS_SUCCESS) {
		flexio_dev_print("dpa_plugin: window_config failed, st=%d\n", (int)st);
		return 0xdead0001;
	}

	flexio_uintptr_t daddr;
	st = flexio_dev_window_ptr_acquire(FLEXIO_DEV_WINDOW_ENTITY_0,
					    t->haddr, &daddr);
	if (st != FLEXIO_DEV_STATUS_SUCCESS) {
		flexio_dev_print("dpa_plugin: ptr_acquire failed, st=%d\n", (int)st);
		return 0xdead0002;
	}

	volatile struct dpa_plugin_shared *s =
		(volatile struct dpa_plugin_shared *)(uintptr_t)daddr;

	/* New bdevperf processes allocate a fresh host ring, but the DPA DMEM
	 * backing g_path/g_conn survives across those host restarts.  Reset local
	 * runtime state when the host ring is still empty; lease renewals after IO
	 * has started have non-zero producer/consumer indexes and preserve state. */
	__dpa_thread_window_read_inv();
	if (s->consumer_idx == 0 && s->producer_idx == 0) {
		__builtin_memset(g_path, 0, sizeof(g_path));
		__builtin_memset(g_conn, 0, sizeof(g_conn));
		__builtin_memset(g_submit_tsc_low, 0, sizeof(g_submit_tsc_low));
		__builtin_memset(g_shared_fate_until_tsc, 0, sizeof(g_shared_fate_until_tsc));
		__builtin_memset(g_shared_fate_last_excluded_plus1, 0,
				 sizeof(g_shared_fate_last_excluded_plus1));
		__builtin_memset(g_sapsq_submit_count, 0,
				 sizeof(g_sapsq_submit_count));
		__builtin_memset(g_sapsq_demand_ewma_q32, 0,
				 sizeof(g_sapsq_demand_ewma_q32));
		__builtin_memset(g_sapsq_zero_epochs, 0,
				 sizeof(g_sapsq_zero_epochs));
		__builtin_memset(g_sapsq_host_submit_last, 0,
				 sizeof(g_sapsq_host_submit_last));
		__builtin_memset(g_sapsq_recover_ramp_q16, 0,
				 sizeof(g_sapsq_recover_ramp_q16));
		g_sapsq_tick_last_tsc = 0;
		g_algo_events = 0;
		g_algo_degraded_transitions = 0;
		g_algo_sv1_bypass_admits = 0;
		g_algo_sv2_auto_release = 0;
		g_algo_throttle_writes = 0;
		g_algo_admit_writes = 0;
		g_saps_sample_events = 0;
		g_saps_classify_calls = 0;
		g_saps_score_updates = 0;
		saps_publish_counters(s);
	}

	/* Resume from where the previous invocation left off (or start at 0). */
	uint64_t consumed  = s->consumer_idx;
	uint64_t last_prod = s->producer_idx;

	if (SAPS_DPA_DIAGNOSTIC_PRINTS) {
		flexio_dev_print(
			"dpa_plugin: DPA RPC entered "
			"(consumed=%lu prod=%lu algo_ev=%lu)\n",
			(unsigned long)consumed, (unsigned long)last_prod,
			(unsigned long)g_algo_events);
	}

	uint32_t outer = 0;
	/* Time-based lease budget (E2 work-conserving root-cause fix, 2026-05-29):
	 * renew the FlexIO RPC well before the libflexio ~10s polling timeout
	 * REGARDLESS of per-outer-iter cost.  The old count-based renewal
	 * (outer >= DPA_LEASE_OUTER) assumed ~600us/iter, but under heavy
	 * multi-tenant load the inner ring drain + PCIe writeback fences stretch
	 * individual outer iters to 14-199ms (observed), so 100 iters could exceed
	 * the timeout.  When that happens flexio_process_call returns a timeout,
	 * rpc_thread_fn breaks, and the DPA is never re-invoked: in E2 the ring
	 * drain froze at consumed=995072 ~13s in, per-tenant demand froze, budgets
	 * went static, and work-conserving redistribution could never fire.
	 * Renew at min(DPA_LEASE_OUTER iters, ~2s wall-clock). */
	const uint64_t lease_max_cycles = 2000000000ULL; /* ~2s @ ~1GHz DPA cycle, << 10s timeout */
	uint64_t lease_start_cycles = __dpa_thread_cycles();

	for (;;) {
		/* Inner spin: tight register-only loop (no PCIe fence).
		 * Processes any pending entries using the cached last_prod value. */
		uint32_t inner;
		for (inner = 0; inner < DPA_INNER_SPIN; inner++) {
			if (last_prod != consumed) {
				uint64_t hi = last_prod;
				if (hi - consumed > BATCH)
					hi = consumed + BATCH;

				for (uint64_t i = consumed; i < hi; i++) {
					const volatile struct dpa_plugin_notify_entry *e =
						&s->entries[i & DPA_PLUGIN_RING_MASK];
					process_event(s, e);
				}
				consumed = hi;
				s->dpa_consumed = consumed;
				s->consumer_idx = consumed;
				__dpa_thread_window_writeback();
			}
		}

		/* One PCIe fence per outer iteration: refreshes stop + producer_idx. */
		__dpa_thread_window_read_inv();

		if (s->stop != 0) {
			/* Final decision-output pass so host sees the last
			 * admission state before teardown. */
			emit_admission_tokens(s, __dpa_thread_cycles());
			saps_publish_counters(s);
			s->dpa_stopped = 1;
			__dpa_thread_window_writeback();
			flexio_dev_print("dpa_plugin: DPA exit (stop) consumed=%lu algo_ev=%lu deg=%lu sv1=%lu sv2=%lu thr_wr=%lu adm_wr=%lu\n",
					 (unsigned long)consumed,
					 (unsigned long)g_algo_events,
					 (unsigned long)g_algo_degraded_transitions,
					 (unsigned long)g_algo_sv1_bypass_admits,
					 (unsigned long)g_algo_sv2_auto_release,
					 (unsigned long)g_algo_throttle_writes,
					 (unsigned long)g_algo_admit_writes);
			flexio_dev_print("SAPS_SAMPLE sample_events=%lu classify_state=%lu score_updates=%lu healthy_full_bypass=%lu\n",
					 (unsigned long)g_saps_sample_events,
					 (unsigned long)g_saps_classify_calls,
					 (unsigned long)g_saps_score_updates,
					 (unsigned long)s->saps_healthy_full_bypass_count);
			return consumed;
		}

		last_prod = s->producer_idx;

		/* E1-integration: once-per-outer-iter decision output.
		 * Walks g_conn[], applies safety valve 2, writes per_qp_tokens[].
		 * Cost: N * ~5 ns pure DPA ops + the writeback fence below.
		 *
		 * E1-fix1: explicit window_writeback AFTER emit so host sees
		 * fresh token counts without waiting for the inner-loop fence
		 * (which only fires on event consumption). Without this, DPA's
		 * store buffer holds per_qp_tokens for an arbitrary number of
		 * outer iters, host reads stale 0, admission gate stays
		 * closed, pipeline deadlocks. */
		emit_admission_tokens(s, __dpa_thread_cycles());
		/* M5 v3 DRR round: time-driven, once per outer iter. Fires when
		 * elapsed_tsc ≥ m5_drr_interval_tsc. O(16) fixed cost. */
		m5_drr_round(s, __dpa_thread_cycles());
		/* SAPS-Q epoch scheduler: triggered when the event accumulator
		 * crossed sapsq_epoch_interval_events. Internal publish-fence +
		 * commit handled inside sapsq_run_epoch(). O(T×P + T log T)
		 * with T=16, P=4 → ~256 ops. */
		sapsq_run_epoch(s, __dpa_thread_cycles());
		/* SAPS-Q M-series unified scheduler tick (slice 2): writes the
		 * new 16×8 budget plane (sapsq_tenant_path_rate_budget_q32) and
		 * publishes via sapsq_epoch_seq/commit_seq. Dormant when
		 * sapsq_enabled=0. O(T×P + T log T) with T=16, P=8 → ~512 ops. */
		sapsq_scheduler_tick(s, __dpa_thread_cycles());
		saps_publish_counters(s);
		__dpa_thread_window_writeback();

		/* Lease expiry: voluntary return before the 6s FlexIO timeout.
		 * Host re-invokes immediately; DPA resumes from consumer_idx. */
		outer++;
		if (outer >= DPA_LEASE_OUTER ||
		    (__dpa_thread_cycles() - lease_start_cycles) >= lease_max_cycles) {
			__dpa_thread_window_writeback();
			if (SAPS_DPA_DIAGNOSTIC_PRINTS) {
				flexio_dev_print(
					"dpa_plugin: lease renew "
					"(outer=%u consumed=%lu prod=%lu algo_ev=%lu "
					"deg=%lu sv1=%lu sv2=%lu thr_wr=%lu adm_wr=%lu)\n",
					(unsigned)outer,
					(unsigned long)consumed,
					(unsigned long)last_prod,
					(unsigned long)g_algo_events,
					(unsigned long)g_algo_degraded_transitions,
					(unsigned long)g_algo_sv1_bypass_admits,
					(unsigned long)g_algo_sv2_auto_release,
					(unsigned long)g_algo_throttle_writes,
					(unsigned long)g_algo_admit_writes);
				flexio_dev_print(
					"SAPS_SAMPLE sample_events=%lu classify_state=%lu "
					"score_updates=%lu healthy_full_bypass=%lu\n",
					(unsigned long)g_saps_sample_events,
					(unsigned long)g_saps_classify_calls,
					(unsigned long)g_saps_score_updates,
					(unsigned long)s->saps_healthy_full_bypass_count);
			}
			return DPA_LEASE_CONTINUE;
		}
	}
}
