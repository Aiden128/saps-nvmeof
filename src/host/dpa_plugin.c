/* P0.3/P0.5 dpa_plugin — host-side library.
 *
 * Drives the DPA polling kernel for the SPDK fast-path notify ring.
 *
 * Lifecycle:
 *   dpa_plugin_init()           - open mlx5 dev, allocate ring, register MR,
 *                                 spawn DPA process, launch polling RPC on a
 *                                 pthread. Honors env var DPA_PLUGIN_DEV
 *                                 (default mlx5_1) and DPA_PLUGIN_DISABLE_INIT.
 *   dpa_plugin_on_io_submit/
 *   dpa_plugin_on_io_complete   - fast path: write one 64B slot, increment
 *                                 producer_idx. No locks. No syscalls.
 *   dpa_plugin_shutdown()       - set stop, join DPA RPC pthread, free.
 *
 * P0.5 Teardown fix — lease-based RPC renewal:
 *   flexio_process_call has a hard 10-second timeout in libflexio. The DPA
 *   polling RPC was killed after 10s, so only ~9M/119M events were consumed
 *   and the DPA was dead for the 30s test.
 *
 *   Fix: the DPA RPC now polls for 8s (DPA_LEASE_TICKS) then returns the
 *   sentinel DPA_LEASE_CONTINUE (0xc0ff33c0ff33). rpc_thread_fn re-invokes
 *   immediately, keeping the DPA alive without any single invocation exceeding
 *   the 10s timeout. On stop=1, the DPA exits cleanly and returns consumed.
 */

#define _GNU_SOURCE
#include "../dpa_plugin.h"
#include "../dpa_plugin_com.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <linux/memfd.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>

#include <infiniband/verbs.h>
#include <libflexio/flexio_ver.h>
#define FLEXIO_VER_USED FLEXIO_VER(25, 10, 0)
#include <libflexio/flexio.h>

/* memfd_create wrapper — some libc versions lack a wrapper on older BF3 images. */
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
static inline int dpa_memfd_create(const char *name, unsigned flags)
{
	return (int)syscall(SYS_memfd_create, name, flags);
}

#define DPA_PLUGIN_SOCK_DEFAULT "/tmp/dpa_plugin.sock"

extern flexio_func_t dpa_plugin_rpc;

#define DEV_APP_NAME_STR(x) #x
#define DEV_APP_NAME_XSTR(x) DEV_APP_NAME_STR(x)
#define L2V(l) (1UL << (l))
#define MSG_HOST_BUFF_BSIZE (4 * L2V(FLEXIO_MSG_DEV_LOG_DATA_CHUNK_BSIZE))

/* Sentinel matching DPA_LEASE_CONTINUE in dpa_plugin_dev.c */
#define DPA_LEASE_CONTINUE 0xc0ff33c0ff33ull
#define DPA_PLUGIN_FAULT_PROPORTIONAL_THROTTLE 9u

/* aarch64 cache clean to PoC after writing notify-ring fields that the DPA
 * reads via the FlexIO window (PCIe DMA path).
 *
 * Root cause: __atomic_fetch_add leaves the updated cache line in CPU L1/L2.
 * The NIC DMA engine (used by flexio_dev_window_ptr_acquire) reads physical
 * DRAM, not the CPU cache.  __dpa_thread_window_read_inv() on the DPA side
 * only invalidates DPA's own window cache; it cannot force the host CPU to
 * write back dirty lines.  Without an explicit clean, DPA reads stale 0.
 *
 * Fix: "dc cvac, %0" — Data Cache, Clean by Virtual Address to PoC (Point
 * of Coherency).  This ensures the cache line is written back to DRAM so the
 * NIC DMA sees the updated value.  "dsb ish" is a full inner-shareable domain
 * barrier that serialises the clean before any subsequent memory operations,
 * ensuring the NIC cannot observe the old value after this macro returns.
 *
 * ARM64 atomics enforce CPU-to-CPU ordering but do not guarantee that dirty
 * cache lines have reached DRAM before the DPA reads them. */
#ifdef __aarch64__
/* BB fix: flush a single cache line to DRAM without an ordering barrier.
 * Use DPA_RING_FLUSH_CACHELINE() in a loop over entries[], then call
 * DPA_RING_FLUSH_BARRIER() once after all lines are cleaned.  This amortises
 * the expensive dsb ish across an entire batch. */
#define DPA_RING_FLUSH_CACHELINE(ptr)                                   \
	__asm__ __volatile__("dc cvac, %0" : : "r" (ptr) : "memory")
#define DPA_RING_FLUSH_BARRIER() \
	__asm__ __volatile__("dsb ish" : : : "memory")
#else
/* Non-aarch64 (e.g., x86_64 developer builds): no explicit flush needed;
 * x86 has a strongly-ordered memory model and typically coherent DMA. */
#define DPA_RING_FLUSH_CACHELINE(ptr) \
	__asm__ __volatile__("" ::: "memory")
#define DPA_RING_FLUSH_BARRIER() \
	__asm__ __volatile__("" ::: "memory")
#endif

/* E1: tenant-count knob. DPA_PLUGIN_N_CONN rewrites qp_id to
 * (orig_qp_id ^ (fast_counter % N)) so we simulate N distinct tenants through
 * a single SPDK qpair. Real multi-process sharing is E-4 (out of scope here). */
static uint32_t g_n_conn = 1;
static uint32_t g_n_conn_mask = 0; /* N-1 when N is power of two; else 0 */

/* Slice 11a — sub-sample gate for fast-path notify ring publish.
 *
 * Read once from env var DPA_PLUGIN_SAMPLE_RATE in dpa_plugin_init(). When > 1,
 * dpa_plugin_push() drops events whose cmd_id % rate != 0 before they reach the
 * batch buffer / ring. cmd_id is set by SPDK NVMe layer and identical for a
 * given IO's submit + completion pair, so the gate keeps pairs together (a
 * sampled IO has both its submit and complete published; a skipped IO has
 * neither). This preserves DPA-side latency calculation correctness for the
 * subset that gets through.
 *
 * Error events (sct_sc != 0) bypass the gate — D3 sparse-error classifier
 * accuracy depends on every error completion being visible regardless of rate.
 *
 * Default = 1 (full publish, backward-compatible with Slice 1..10 behaviour).
 *
 * Counter g_sample_skipped is bumped per skipped event for debug only (printed
 * in shutdown stats). Not on hot path of the kept-events branch.
 */
static volatile uint32_t g_sample_rate = 1;
static uint64_t g_sample_skipped;
static uint64_t g_sample_force_pub_errors;
static uint64_t g_sample_submit_seq;
/* One bit per path/command slot records the submission sampling decision so
 * the matching completion follows the same decision. Sampling fixed command
 * IDs biases processes whose active CID range contains no multiple of the
 * configured rate. */
static uint64_t g_sampled_cmd[DPA_PLUGIN_PATH_MAX][UINT16_MAX / 64u + 1u];

/* E4 coordinator/tenant: process-local SAPS-Q tenant id.
 * Each bdevperf tenant process reads SAPSQ_MY_TENANT_ID from env and stores it
 * here so admission_check uses the correct row in the 2D shared ring — without
 * a per-process local copy every tenant would share ring->sapsq_my_tenant_id
 * (set by coordinator) and all tenants would compete on the same token bucket
 * row.
 * 0xFFFFFFFFu = not set (dormant, fall through to ring->sapsq_my_tenant_id). */
static uint32_t g_sapsq_local_tenant_id = 0xFFFFFFFFu;

/* Offered-demand publication for HCAA.
 *
 * The DPA cannot infer offered demand from admitted submissions. Once a tenant
 * is throttled, admitted submissions fall, which would reduce its next budget
 * and create a self-reinforcing starvation loop. Count admission attempts
 * before enforcement and publish them in batches. The cache clean makes the
 * counter visible to the DPA window without placing a barrier on every I/O. */
#define SAPSQ_OFFERED_FLUSH_BATCH 128u
static __thread uint64_t g_sapsq_offered_pending;

/* E3 admission-control state (host side). */
enum dpa_plugin_force_mode {
	DPA_FORCE_NONE = 0,   /* honour DPA per_qp_tokens[] as-is */
	DPA_FORCE_ALL,        /* throttle every qp_id (jam gate closed) */
	DPA_FORCE_HALF,       /* throttle odd qp_id (multi-qp partial gating) */
	DPA_FORCE_TOGGLE,     /* 50% duty cycle — flips gate every N checks */
};
#define DPA_FORCE_TOGGLE_PERIOD 1000u  /* gate state flips every 1000 admission checks */
static volatile int g_force_mode = DPA_FORCE_NONE;
static volatile int g_admission_enabled = 0;  /* 0 = no gating (Mech-D clean) */

/* E-4 role selection. */
enum dpa_plugin_role {
	DPA_ROLE_STANDALONE = 0,  /* legacy P0.5 single-process */
	DPA_ROLE_COORDINATOR,     /* full init + UDS server + memfd backing */
	DPA_ROLE_TENANT,          /* attach-only; no FlexIO resources */
};

struct dpa_plugin_ctx {
	int                          initialized;
	int                          notify_enabled;
	enum dpa_plugin_role         role;
	int                          ring_memfd;    /* coordinator: owns; tenant: received */
	int                          uds_listen_fd; /* coordinator only */
	pthread_t                    uds_thread;
	int                          uds_thread_started;
	volatile int                 uds_stop;
	char                         uds_path[128];

	struct ibv_device          **dev_list;
	struct ibv_context          *ibv_ctx;
	struct flexio_process       *process;
	struct flexio_app           *app;
	struct flexio_msg_stream    *stream;
	struct flexio_window        *window;
	struct ibv_mr               *mr;
	struct dpa_plugin_shared    *ring;
	size_t                       ring_alloc_size;
	flexio_uintptr_t             transfer_daddr;
	pthread_t                    rpc_thread;
	int                          rpc_thread_started;
	uint64_t                     rpc_ret;
	flexio_status                rpc_status;
	int                          rpc_renewals;  /* count of lease renewals */

	/* E1 rate sampler */
	pthread_t                    sampler_thread;
	int                          sampler_started;
	volatile int                 sampler_stop;
	FILE                        *sampler_file;

	/* M2 v2 host-side joint-anomaly classifier (2026-05-20)。讀
	 * ring->m2_pca_conf_q16[16] 算 entropy → 寫 ring->per_client_joint_verdict[16]。
	 * 10ms tick,不在 hot path,純 observability。 */
	pthread_t                    m2_cls_thread;
	int                          m2_cls_started;
	volatile int                 m2_cls_stop;

	/* M3 v4 closed-loop snapshot dumper (2026-05-20)。
	 * 每 100ms 把 per-tenant ledger state (iops_served / credits_q32 /
	 * exhaust_count / last_tsc) JSON dump 到
	 * `/tmp/m3_proc_{tenant_id}_snap.json`,給 host control daemon
	 * 讀做 closed-loop feedback。SAPS_M3_SNAPSHOT=1 啟用,預設 0。
	 * 用 stdlib pthread,不依賴 SPDK thread。 */
	pthread_t                    m3_snap_thread;
	int                          m3_snap_started;
	volatile int                 m3_snap_stop;
	uint32_t                     m3_snap_tenant_id;     /* this proc's tenant id */
	char                         m3_snap_path[128];     /* /tmp/m3_proc_{id}_snap.json */
};

static struct dpa_plugin_ctx g_ctx;

static volatile int g_notify_enabled;

/* Slice 1 — retry-verdict overlay independent toggle.
 *
 * Set from env var DPA_PLUGIN_RETRY_OVERLAY=1 inside dpa_plugin_init(). When
 * 1, bdev_nvme_check_retry_io() consults the SAPS retry verdict regardless
 * of mp_policy (so round_robin policy can also enjoy the short-circuit, used
 * by the 2x2 factorial design that isolates path-selection from retry).
 *
 * Default 0 keeps the legacy semantic: only mp_policy == PLUGIN sees overlay.
 * Reads are a single byte from .data — no atomic / lock needed; the value is
 * written exactly once at init before any IO is submitted. */
static volatile int g_retry_overlay_enabled;

/* Multi-reactor fix (2026-06-10): the completion hook runs on every SPDK
 * reactor thread.  The staging buffer + its fill count MUST be per-thread,
 * otherwise concurrent reactors tear records into the same buffer and
 * double-flush. Each thread stages its own batch and publishes it through the
 * process-shared producer lock. Each reactor therefore owns an independent
 * staging buffer while the shared head remains a write-complete boundary. */
static __thread struct dpa_plugin_notify_entry
	g_batch_buf[DPA_PLUGIN_BATCH_SIZE] __attribute__((aligned(64)));
static __thread uint32_t g_batch_count;
/* Stat totals: read cross-thread by sampler_fn (~:280) and the shutdown
 * report (~:3937/3997), so they stay process-global aggregates; the per-
 * reactor increments are made atomic (RELAXED) to avoid lost updates. */
static uint64_t g_batch_flushes;
static uint64_t g_batch_events_pushed;

/* E1 sampler_fn: every 1s, read producer_idx and dpa_consumed, log rate. */
static void *sampler_fn(void *arg)
{
	struct dpa_plugin_ctx *c = arg;
	uint64_t prev_pushed = 0, prev_consumed = 0;
	uint64_t tick = 0;
	fprintf(c->sampler_file,
		"t_s,unix_s,pushed_total,consumed_total,pushed_delta,consumed_delta,ring_lag,"
		"dem0,dem1,dem2,dem3,bud0,bud1,bud2,bud3,"
		"bud_t0p0,bud_t0p1,bud_t0p2,"
		"hf0,hf1,hf2,chf0,chf1,chf2,eff0,eff1,eff2,ftB\n");
	fflush(c->sampler_file);
	while (!c->sampler_stop) {
		struct timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
		nanosleep(&ts, NULL);
		tick++;
		struct timespec wall;
		clock_gettime(CLOCK_REALTIME, &wall);
		double unix_s = (double)wall.tv_sec + (double)wall.tv_nsec / 1.0e9;
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		uint64_t pushed = g_batch_events_pushed;
		uint64_t consumed = c->ring ? c->ring->dpa_consumed : 0;
		uint64_t prod = c->ring ?
			__atomic_load_n(&c->ring->producer_idx,
					__ATOMIC_ACQUIRE) : 0;
		uint64_t lag = (prod >= consumed) ? prod - consumed : 0;
		/* WC diagnostic: per-tenant demand EWMA (IO/s) + decoded path-0 budget
		 * (rate_budget_q32 → IO/s).  Lets us see whether, during a tenant's idle
		 * window, its demand decays AND the active tenants' budgets grow (true
		 * scheduler-level work-conserving redistribution). */
		/* health_factor (Q16.16→float) for paths 0/1/2 and fault_type for path B(=1) */
		float hf[3] = {0.0f, 0.0f, 0.0f};
		float chf[3] = {0.0f, 0.0f, 0.0f};
		uint32_t eff[3] = {0};
		uint16_t ftB = 0;
		if (c->ring) {
			for (int p = 0; p < 3; p++) {
				hf[p] = (float)c->ring->sapsq_path_health_factor_q16[p] / 65536.0f;
				chf[p] =
					(float)c->ring->sapsq_committed_path_health_factor_q16[p] /
					65536.0f;
				eff[p] =
					c->ring->sapsq_committed_path_effective_capacity_iops[p];
			}
			ftB = c->ring->per_qp_path_fault_type[0][1]; /* coordinator QP=0, path B=1 */
		}
		uint32_t dem[4] = {0}, bud[4] = {0};
		uint32_t bud_t0p[3] = {0};
		if (c->ring) {
			uint64_t tf = c->ring->sapsq_host_tsc_freq
				    ? c->ring->sapsq_host_tsc_freq : 1000000000ull;
			for (int t = 0; t < 4; t++) {
				dem[t] = c->ring->sapsq_demand_iops[t];
				uint64_t q = c->ring->sapsq_tenant_path_rate_budget_q32[t][0];
				bud[t] = (uint32_t)((q * tf) >> 32);
			}
			for (int p = 0; p < 3; p++) {
				uint64_t q =
					c->ring->sapsq_tenant_path_rate_budget_q32[0][p];
				bud_t0p[p] = (uint32_t)((q * tf) >> 32);
			}
		}
		fprintf(c->sampler_file,
			"%lu,%.6f,%lu,%lu,%lu,%lu,%lu,"
			"%u,%u,%u,%u,%u,%u,%u,%u,"
			"%u,%u,%u,"
			"%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,"
			"%u,%u,%u,%u\n",
			(unsigned long)tick,
			unix_s,
			(unsigned long)pushed,
			(unsigned long)consumed,
			(unsigned long)(pushed - prev_pushed),
			(unsigned long)(consumed - prev_consumed),
			(unsigned long)lag,
			dem[0], dem[1], dem[2], dem[3],
			bud[0], bud[1], bud[2], bud[3],
			bud_t0p[0], bud_t0p[1], bud_t0p[2],
			(double)hf[0], (double)hf[1], (double)hf[2],
			(double)chf[0], (double)chf[1], (double)chf[2],
			eff[0], eff[1], eff[2],
			(unsigned)ftB);
		fflush(c->sampler_file);
		prev_pushed = pushed;
		prev_consumed = consumed;
	}
	return NULL;
}

/* ─── M2 v2 host-side joint-anomaly classifier (2026-05-20) ──────────────
 *
 * 對應 specs/m2-dpa-implementation-plan-20260519.md §3 + §4。DPA 端 Oja
 * update 寫 ring->m2_pca_conf_q16[c] = cosine(w_c, w_baseline) (Q16.16 signed)。
 * 此 thread 每 10ms tick 一次:
 *   1. 對 16 個 client 算 deviation L_c = 1.0 - conf_q16[c]/65536
 *   2. 跳過 warmup 未完成 (conf_q16 == 0) 與 noise floor (L_c < 0.05)
 *   3. 算 entropy H = -Σ p_c log2(p_c), p_c = L_c / Σ L_c
 *   4. 三分類 verdict 寫 ring->per_client_joint_verdict[c]:
 *        H ≥ 3.6  (≥ 0.9 × log2(16)) → JOINT (SHARED_FATE)
 *        H < 2.0                     → INDIVIDUAL (K=1 pattern)
 *        2.0 ≤ H < 3.6               → SUSPECT,經 N_SUSPECT_CONFIRM
 *                                       連續 tick 仍邊界才升 JOINT
 *
 * 純 observability:per_client_joint_verdict[] 沒人讀,paper §6 只用 stderr
 * log [M2_VERDICT] grep 拿 detection latency。 */

#define HOST_M2_CLIENT_MAX        16
#define HOST_M2_NOISE_FLOOR_Q16   3277       /* 0.05 in Q16.16 */
#define HOST_M2_H_JOINT_Q16       235930     /* 3.6 in Q16.16 (0.9 × log2(16)) */
#define HOST_M2_H_LOW_Q16         131072     /* 2.0 in Q16.16 */
#define HOST_M2_TICK_NS           10000000ULL  /* 10 ms */
#define HOST_M2_SUSPECT_CONFIRM   200u        /* spec §3.4 */

/* Q16.16 log2 LUT (mirror host_saps.c LOG2_FRAC_Q16_256). */
static const uint16_t HOST_M2_LOG2_FRAC_Q16_256[256] = {
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

/* Return log2(x) in Q16.16 fixed-point. x is Q16.16 (so true value = x/65536). */
static inline int64_t host_m2_log2_q16(int64_t x_q16)
{
	if (x_q16 <= 0)
		return INT64_MIN;
	uint64_t u = (uint64_t)x_q16;
	unsigned bl = 64u - (unsigned)__builtin_clzll(u);
	unsigned msb = bl - 1u;
	/* log2(x_q16) = log2(true * 2^16) = log2(true) + 16
	 * so log2_true_q16 = (msb - 16) << 16 + frac_part
	 */
	int64_t int_part = ((int64_t)msb - 16) << 16;
	unsigned frac_idx;
	if (msb >= 8u) {
		frac_idx = (unsigned)((u >> (msb - 8u)) & 0xFFu);
	} else {
		frac_idx = (unsigned)((u << (8u - msb)) & 0xFFu);
	}
	int64_t frac_part = (int64_t)HOST_M2_LOG2_FRAC_Q16_256[frac_idx];
	return int_part + frac_part;
}

/* Per-client SUSPECT hysteresis counter,thread-local 給 classifier 用。 */
static uint32_t g_host_m2_suspect_count[HOST_M2_CLIENT_MAX];
/* Previous verdict — 用來偵測 verdict 變化以 print log。 */
static uint8_t  g_host_m2_prev_verdict[HOST_M2_CLIENT_MAX];

/* monotonic ms wall-clock,給 log timestamp。 */
static inline uint64_t host_m2_now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)(ts.tv_nsec / 1000000ull);
}

/* 跑一次 classifier tick。寫 ring->per_client_joint_verdict[]。
 * Non-static 給 unit test program 直接呼叫驗演算法。 */
void host_m2_classifier_tick(struct dpa_plugin_shared *ring)
{
	if (!ring || !ring->saps_m2_enabled || !ring->m2_v2_classifier)
		return;

	/* Snapshot all 16 conf_q16,並算 L_c。Skip warmup (conf==0) 與 noise. */
	int64_t L_q16[HOST_M2_CLIENT_MAX] = {0};
	int     active[HOST_M2_CLIENT_MAX] = {0};
	int     n_active = 0;
	int64_t sum_L_q16 = 0;

	for (int c = 0; c < HOST_M2_CLIENT_MAX; c++) {
		int32_t conf = ring->m2_pca_conf_q16[c];
		if (conf == 0) {
			/* warmup 未完成,verdict 維持 UNSET */
			g_host_m2_suspect_count[c] = 0;
			if (g_host_m2_prev_verdict[c] != DPA_M2_VERDICT_UNSET) {
				ring->per_client_joint_verdict[c] = DPA_M2_VERDICT_UNSET;
				g_host_m2_prev_verdict[c] = DPA_M2_VERDICT_UNSET;
			}
			continue;
		}
		/* clip 到 [-65536, 65536] 避免異常溢位 */
		int64_t c_q16 = conf;
		if (c_q16 >  65536) c_q16 =  65536;
		if (c_q16 < -65536) c_q16 = -65536;
		/* L_c = 1.0 - cosine (Q16.16) */
		int64_t l = 65536 - c_q16;
		if (l < 0) l = 0;
		L_q16[c] = l;
		if (l >= HOST_M2_NOISE_FLOOR_Q16) {
			active[c] = 1;
			n_active++;
			sum_L_q16 += l;
		}
	}

	/* 若無 active client,全寫 HEALTHY 並清 suspect counter。 */
	if (n_active == 0 || sum_L_q16 <= 0) {
		for (int c = 0; c < HOST_M2_CLIENT_MAX; c++) {
			int32_t conf = ring->m2_pca_conf_q16[c];
			if (conf == 0) continue;  /* 仍 warmup */
			g_host_m2_suspect_count[c] = 0;
			uint8_t v = DPA_M2_VERDICT_HEALTHY;
			if (g_host_m2_prev_verdict[c] != v) {
				ring->per_client_joint_verdict[c] = v;
				fprintf(stderr,
					"[M2_VERDICT] ts_ms=%lu client=%d H=0.000 n_active=0 verdict=%u prev=%u\n",
					(unsigned long)host_m2_now_ms(), c, v,
					g_host_m2_prev_verdict[c]);
				g_host_m2_prev_verdict[c] = v;
			}
		}
		return;
	}

	/* Entropy H = -Σ p_c log2(p_c) in Q16.16.
	 * p_c = L_c / sum_L,p_c_q16 = (L_c << 16) / sum_L.
	 * log2(p_c) ≤ 0 (since p_c ≤ 1) → -p_c·log2(p_c) ≥ 0.
	 */
	int64_t H_q16 = 0;
	for (int c = 0; c < HOST_M2_CLIENT_MAX; c++) {
		if (!active[c]) continue;
		int64_t p_q16 = (L_q16[c] << 16) / sum_L_q16;
		if (p_q16 <= 0) continue;
		int64_t log_p_q16 = host_m2_log2_q16(p_q16);  /* ≤ 0 */
		int64_t term = (p_q16 * log_p_q16) >> 16;     /* ≤ 0 */
		H_q16 -= term;
	}
	if (H_q16 < 0) H_q16 = 0;

	/* 三分類。SUSPECT hysteresis:邊界區 H ∈ [H_LOW, H_JOINT) 累積 confirm
	 * count,連續 ≥ N_SUSPECT_CONFIRM tick 才升 JOINT。其他狀態立刻 clear count. */
	uint8_t bulk_verdict;
	int     bulk_is_boundary = 0;
	if (H_q16 >= HOST_M2_H_JOINT_Q16) {
		bulk_verdict = DPA_M2_VERDICT_JOINT;
	} else if (H_q16 >= HOST_M2_H_LOW_Q16) {
		bulk_verdict = DPA_M2_VERDICT_SUSPECT;
		bulk_is_boundary = 1;
	} else {
		bulk_verdict = DPA_M2_VERDICT_INDIVIDUAL;
	}

	/* 把 bulk verdict apply 到每個 active client。INDIVIDUAL 場景:只標
	 * dominant client (L_c 最大),其他 active client 保持/降回 HEALTHY。 */
	int dominant_c = -1;
	int64_t dominant_L = -1;
	if (bulk_verdict == DPA_M2_VERDICT_INDIVIDUAL) {
		for (int c = 0; c < HOST_M2_CLIENT_MAX; c++) {
			if (!active[c]) continue;
			if (L_q16[c] > dominant_L) {
				dominant_L = L_q16[c];
				dominant_c = c;
			}
		}
	}

	for (int c = 0; c < HOST_M2_CLIENT_MAX; c++) {
		int32_t conf = ring->m2_pca_conf_q16[c];
		if (conf == 0) continue;  /* warmup */

		uint8_t v;
		if (bulk_verdict == DPA_M2_VERDICT_INDIVIDUAL) {
			v = (c == dominant_c) ? DPA_M2_VERDICT_INDIVIDUAL
					      : DPA_M2_VERDICT_HEALTHY;
			g_host_m2_suspect_count[c] = 0;
		} else if (bulk_verdict == DPA_M2_VERDICT_JOINT) {
			v = active[c] ? DPA_M2_VERDICT_JOINT
				      : DPA_M2_VERDICT_HEALTHY;
			g_host_m2_suspect_count[c] = 0;
		} else {
			/* SUSPECT boundary band */
			if (active[c]) {
				if (g_host_m2_suspect_count[c] < 0xFFFFFFFFu)
					g_host_m2_suspect_count[c]++;
				if (g_host_m2_suspect_count[c] >= HOST_M2_SUSPECT_CONFIRM)
					v = DPA_M2_VERDICT_JOINT;
				else
					v = DPA_M2_VERDICT_SUSPECT;
			} else {
				v = DPA_M2_VERDICT_HEALTHY;
				g_host_m2_suspect_count[c] = 0;
			}
		}

		if (v != g_host_m2_prev_verdict[c]) {
			ring->per_client_joint_verdict[c] = v;
			/* H 用 Q16.16 印小數三位:H = H_q16 / 65536 */
			int H_int = (int)(H_q16 >> 16);
			int H_frac = (int)(((H_q16 & 0xFFFF) * 1000ll) >> 16);
			fprintf(stderr,
				"[M2_VERDICT] ts_ms=%lu client=%d H=%d.%03d n_active=%d "
				"conf_q16=%d L_q16=%ld verdict=%u prev=%u suspect_cnt=%u%s\n",
				(unsigned long)host_m2_now_ms(), c, H_int, H_frac,
				n_active, conf, (long)L_q16[c], v,
				g_host_m2_prev_verdict[c],
				g_host_m2_suspect_count[c],
				bulk_is_boundary ? " boundary" : "");
			g_host_m2_prev_verdict[c] = v;
		}
	}
}

static void *m2_classifier_fn(void *arg)
{
	struct dpa_plugin_ctx *c = arg;
	struct timespec ts = { .tv_sec = 0, .tv_nsec = (long)HOST_M2_TICK_NS };
	while (!c->m2_cls_stop) {
		nanosleep(&ts, NULL);
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		host_m2_classifier_tick(c->ring);
	}
	return NULL;
}

/* M3 v4 closed-loop snapshot dumper (2026-05-20)。
 *
 * 對應 specs/m3-v4-closed-loop-snapshot-impl-20260520.md。
 *
 * 每 100 ms 從 shared ring 讀此 proc 自己的 tenant ledger
 * (m3_tenant_iops_served / m3_tenant_credits_q32 / m3_credit_exhaust_count /
 * m3_tenant_last_tsc),JSON dump 到 /tmp/m3_proc_{tid}_snap.json。
 * Host control daemon (m3_v4_control_daemon.py) 讀檔做 closed-loop
 * feedback (取代 open-loop weight × link_iops 推估)。
 *
 * Atomic write protocol:write 到 "{path}.tmp" 然後 rename 到 path,
 * 確保 reader (daemon) 永遠不會讀到 half-written file。
 *
 * 此 thread 不在 hot path (純 observability),完全 stdlib (pthread_create
 * + nanosleep + fprintf + rename),不依賴 SPDK thread。
 */
static void *m3_snapshot_fn(void *arg)
{
	struct dpa_plugin_ctx *c = arg;
	struct timespec ts = { .tv_sec = 0, .tv_nsec = 100L * 1000L * 1000L }; /* 100 ms */
	char tmp_path[160];
	snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", c->m3_snap_path);

	while (!c->m3_snap_stop) {
		nanosleep(&ts, NULL);
		if (!c->ring)
			continue;
		__atomic_thread_fence(__ATOMIC_ACQUIRE);

		uint32_t tid = c->m3_snap_tenant_id;
		if (tid >= 16)
			continue;   /* invalid tenant id;skip dump */

		uint64_t iops    = c->ring->m3_tenant_iops_served[tid];
		int64_t  credits = c->ring->m3_tenant_credits_q32[tid];
		uint64_t exhaust = c->ring->m3_credit_exhaust_count[tid];
		uint64_t last_tsc = c->ring->m3_tenant_last_tsc[tid];

		/* M4 v2 observability (2026-05-20) */
		uint64_t m4_admit  = c->ring->m4_tenant_admit_count[tid];
		uint64_t m4_reject = c->ring->m4_tenant_reject_count[tid];
		uint32_t m4_tokens = c->ring->m4_tenant_tokens_q16[tid];
		uint32_t m4_rflag  = c->ring->m4_tenant_reject_flag[tid];

		/* timestamp from CLOCK_REALTIME ms — daemon is_stale() 用
		 * time.time()*1000 (wall-clock epoch ms),必須同一時鐘。 */
		struct timespec now;
		clock_gettime(CLOCK_REALTIME, &now);
		uint64_t ts_ms = (uint64_t)now.tv_sec * 1000ULL
				 + (uint64_t)now.tv_nsec / 1000000ULL;

		FILE *fp = fopen(tmp_path, "w");
		if (!fp)
			continue;
		fprintf(fp,
			"{"
			"\"ts_ms\":%lu,"
			"\"tenant_id\":%u,"
			"\"iops_served\":%lu,"
			"\"credits_q32\":%ld,"
			"\"exhaust_count\":%lu,"
			"\"last_tsc\":%lu,"
			"\"m4_admit\":%lu,"
			"\"m4_reject\":%lu,"
			"\"m4_tokens_q16\":%u,"
			"\"m4_reject_flag\":%u"
			"}\n",
			(unsigned long)ts_ms,
			tid,
			(unsigned long)iops,
			(long)credits,
			(unsigned long)exhaust,
			(unsigned long)last_tsc,
			(unsigned long)m4_admit,
			(unsigned long)m4_reject,
			m4_tokens,
			m4_rflag);
		fclose(fp);
		/* Atomic publish:rename = single inode swap,reader 不會見 partial */
		if (rename(tmp_path, c->m3_snap_path) != 0) {
			/* 同 file system 不該失敗,但記 errno 給 debug */
			fprintf(stderr, "dpa_plugin: m3 snapshot rename(%s) failed: %s\n",
				c->m3_snap_path, strerror(errno));
		}
	}
	return NULL;
}

/* 啟動 M3 snapshot dumper:讀 SAPS_M3_SNAPSHOT env(預設 off),決定
 * tenant_id 來源(SAPS_M3_TENANT_ID env 或 ring->m3_my_tenant_id),
 * 設定 snapshot file path,spawn pthread。
 *
 * 此 helper 同時被 coordinator/standalone init 跟 tenant attach 呼叫,
 * 確保 multi-proc bdevperf 每個 proc 都有自己的 snapshot file。
 */
static void m3_snapshot_maybe_start(struct dpa_plugin_ctx *c)
{
	const char *snap_env = getenv("SAPS_M3_SNAPSHOT");
	if (!(snap_env && snap_env[0] == '1'))
		return;   /* 預設 off,backward compat — 不 spawn thread */

	/* 決定 tenant id:env override 優先(per-proc orchestrator 設),
	 * fallback 用 ring->m3_my_tenant_id(coordinator init 寫入)。 */
	uint32_t tid = 0xFFFFFFFFu;
	const char *tid_env = getenv("SAPS_M3_TENANT_ID");
	if (tid_env && tid_env[0]) {
		tid = (uint32_t)atoi(tid_env);
	} else if (c->ring && c->ring->m3_my_tenant_id != 0xFFFFFFFFu) {
		tid = c->ring->m3_my_tenant_id;
	}
	if (tid >= 16) {
		fprintf(stderr,
			"dpa_plugin: M3 snapshot skip — tenant_id invalid (%u)\n",
			tid);
		return;
	}

	c->m3_snap_tenant_id = tid;
	snprintf(c->m3_snap_path, sizeof(c->m3_snap_path),
		 "/tmp/m3_proc_%u_snap.json", tid);
	c->m3_snap_stop = 0;
	if (pthread_create(&c->m3_snap_thread, NULL, m3_snapshot_fn, c)) {
		fprintf(stderr,
			"dpa_plugin: M3 snapshot pthread_create failed: %s\n",
			strerror(errno));
		return;
	}
	c->m3_snap_started = 1;
	fprintf(stderr,
		"dpa_plugin: M3 snapshot started tenant=%u path=%s (100ms cadence)\n",
		tid, c->m3_snap_path);
}

/* Stop + join snapshot thread,unlink snapshot file。idempotent。 */
static void m3_snapshot_stop(struct dpa_plugin_ctx *c)
{
	if (!c->m3_snap_started)
		return;
	c->m3_snap_stop = 1;
	pthread_join(c->m3_snap_thread, NULL);
	c->m3_snap_started = 0;
	if (c->m3_snap_path[0]) {
		unlink(c->m3_snap_path);
		/* unlink tmp 殘留(若 rename 之前 thread 被 join cut off) */
		char tmp_path[160];
		snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", c->m3_snap_path);
		unlink(tmp_path);
	}
	fprintf(stderr, "dpa_plugin: M3 snapshot stopped (tenant=%u)\n",
		c->m3_snap_tenant_id);
}

/* rpc_thread_fn: re-invokes flexio_process_call in a loop whenever the DPA
 * returns DPA_LEASE_CONTINUE (voluntary lease renewal before timeout).
 * Exits when DPA returns any other value (clean exit or error). */
static void *rpc_thread_fn(void *arg)
{
	struct dpa_plugin_ctx *c = arg;
	do {
		c->rpc_status = flexio_process_call(c->process, &dpa_plugin_rpc,
						    &c->rpc_ret, c->transfer_daddr);
		if (c->rpc_status == FLEXIO_STATUS_SUCCESS &&
		    c->rpc_ret == DPA_LEASE_CONTINUE) {
			c->rpc_renewals++;
			/* Immediately re-invoke — DPA picks up from consumer_idx. */
			continue;
		}
		break;
	} while (1);
	return NULL;
}

/* ----- E-4 multi-tenant IPC helpers ----- */

/* Send a single file descriptor over UDS using SCM_RIGHTS. */
static int send_fd(int sock, int fd)
{
	struct msghdr msg = {0};
	char ctrl_buf[CMSG_SPACE(sizeof(int))];
	char dummy = 'F';
	struct iovec iov = { .iov_base = &dummy, .iov_len = 1 };

	memset(ctrl_buf, 0, sizeof(ctrl_buf));
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;
	msg.msg_control = ctrl_buf;
	msg.msg_controllen = sizeof(ctrl_buf);

	struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
	cmsg->cmsg_level = SOL_SOCKET;
	cmsg->cmsg_type = SCM_RIGHTS;
	cmsg->cmsg_len = CMSG_LEN(sizeof(int));
	memcpy(CMSG_DATA(cmsg), &fd, sizeof(int));

	ssize_t n = sendmsg(sock, &msg, 0);
	return (n == 1) ? 0 : -1;
}

/* Receive a single file descriptor over UDS using SCM_RIGHTS. */
static int recv_fd(int sock)
{
	struct msghdr msg = {0};
	char ctrl_buf[CMSG_SPACE(sizeof(int))];
	char dummy;
	struct iovec iov = { .iov_base = &dummy, .iov_len = 1 };

	memset(ctrl_buf, 0, sizeof(ctrl_buf));
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;
	msg.msg_control = ctrl_buf;
	msg.msg_controllen = sizeof(ctrl_buf);

	ssize_t n = recvmsg(sock, &msg, 0);
	if (n != 1)
		return -1;

	struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
	if (!cmsg || cmsg->cmsg_level != SOL_SOCKET ||
	    cmsg->cmsg_type != SCM_RIGHTS ||
	    cmsg->cmsg_len != CMSG_LEN(sizeof(int)))
		return -1;

	int fd;
	memcpy(&fd, CMSG_DATA(cmsg), sizeof(int));
	return fd;
}

/* Coordinator UDS server thread. Blocks on accept(), for each incoming
 * tenant connection sends the memfd + ring size, closes the client sock,
 * and loops. Exits when uds_stop is set (triggered by shutdown path
 * closing the listen fd, which makes accept() return EBADF). */
static void *uds_server_fn(void *arg)
{
	struct dpa_plugin_ctx *c = arg;
	while (!c->uds_stop) {
		int client = accept(c->uds_listen_fd, NULL, NULL);
		if (client < 0) {
			if (c->uds_stop)
				break;
			if (errno == EINTR)
				continue;
			fprintf(stderr, "dpa_plugin: UDS accept failed: %s\n",
				strerror(errno));
			break;
		}

		/* Send memfd + ring size as a single SCM_RIGHTS message.
		 * For simplicity we first send the size as a plain 8 bytes,
		 * then the memfd via SCM_RIGHTS. Tenant reads them in the
		 * same order. */
		uint64_t rsize = (uint64_t)c->ring_alloc_size;
		if (write(client, &rsize, sizeof(rsize)) != (ssize_t)sizeof(rsize)) {
			fprintf(stderr, "dpa_plugin: UDS write(size) failed: %s\n",
				strerror(errno));
			close(client);
			continue;
		}

		if (send_fd(client, c->ring_memfd) < 0) {
			fprintf(stderr, "dpa_plugin: UDS send_fd failed: %s\n",
				strerror(errno));
		} else {
			fprintf(stderr, "dpa_plugin: handed memfd to tenant\n");
		}
		close(client);
	}
	return NULL;
}

static int uds_server_start(struct dpa_plugin_ctx *c, const char *path)
{
	size_t path_len = strlen(path);
	if (path_len >= sizeof(c->uds_path)) {
		fprintf(stderr, "dpa_plugin: UDS path too long: %s\n", path);
		return -1;
	}
	struct sockaddr_un addr = { .sun_family = AF_UNIX };
	if (path_len >= sizeof(addr.sun_path)) {
		fprintf(stderr, "dpa_plugin: UDS path too long for sockaddr: %s\n", path);
		return -1;
	}
	memcpy(c->uds_path, path, path_len + 1);

	/* Remove stale socket file from previous runs. */
	unlink(c->uds_path);

	c->uds_listen_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (c->uds_listen_fd < 0) {
		fprintf(stderr, "dpa_plugin: UDS socket failed: %s\n", strerror(errno));
		return -1;
	}

	memcpy(addr.sun_path, c->uds_path, path_len + 1);
	if (bind(c->uds_listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		fprintf(stderr, "dpa_plugin: UDS bind(%s) failed: %s\n",
			c->uds_path, strerror(errno));
		close(c->uds_listen_fd);
		c->uds_listen_fd = -1;
		return -1;
	}
	if (listen(c->uds_listen_fd, 8) < 0) {
		fprintf(stderr, "dpa_plugin: UDS listen failed: %s\n", strerror(errno));
		close(c->uds_listen_fd);
		c->uds_listen_fd = -1;
		return -1;
	}

	if (pthread_create(&c->uds_thread, NULL, uds_server_fn, c)) {
		fprintf(stderr, "dpa_plugin: UDS pthread_create failed\n");
		close(c->uds_listen_fd);
		c->uds_listen_fd = -1;
		return -1;
	}
	c->uds_thread_started = 1;
	fprintf(stderr, "dpa_plugin: UDS server listening at %s\n", c->uds_path);
	return 0;
}

static int uds_client_get_memfd(const char *path, uint64_t *out_size)
{
	int sock = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (sock < 0) {
		fprintf(stderr, "dpa_plugin: tenant socket() failed: %s\n",
			strerror(errno));
		return -1;
	}
	struct sockaddr_un addr = { .sun_family = AF_UNIX };
	size_t plen = strlen(path);
	if (plen >= sizeof(addr.sun_path)) {
		fprintf(stderr, "dpa_plugin: tenant UDS path too long: %s\n", path);
		close(sock);
		return -1;
	}
	memcpy(addr.sun_path, path, plen + 1);

	/* Retry connect for up to 5 seconds to handle coordinator-starts-slightly-after-tenant. */
	int connected = 0;
	for (int i = 0; i < 50; i++) {
		if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
			connected = 1;
			break;
		}
		if (errno != ENOENT && errno != ECONNREFUSED) {
			fprintf(stderr, "dpa_plugin: tenant connect(%s) failed: %s\n",
				path, strerror(errno));
			close(sock);
			return -1;
		}
		struct timespec ts = { .tv_sec = 0, .tv_nsec = 100 * 1000 * 1000 };
		nanosleep(&ts, NULL);
	}
	if (!connected) {
		fprintf(stderr, "dpa_plugin: tenant connect(%s) timed out\n", path);
		close(sock);
		return -1;
	}

	uint64_t rsize = 0;
	if (read(sock, &rsize, sizeof(rsize)) != (ssize_t)sizeof(rsize)) {
		fprintf(stderr, "dpa_plugin: tenant read(size) failed: %s\n",
			strerror(errno));
		close(sock);
		return -1;
	}
	int memfd = recv_fd(sock);
	close(sock);
	if (memfd < 0) {
		fprintf(stderr, "dpa_plugin: tenant recv_fd failed\n");
		return -1;
	}
	*out_size = rsize;
	return memfd;
}

/* Allocate the ring in a memfd region (coordinator and standalone roles).
 * Replaces posix_memalign so the ring lives on shareable/mappable pages.
 * For coordinator the same physical pages are mmap'd by tenant processes via
 * SCM_RIGHTS fd passing; for standalone the memfd lets the out-of-band
 * sapsq_dump helper map the ring via /proc/<pid>/fd (anonymous heap would be
 * unreadable to the dumper). Ring layout/size are unchanged either way. */
static int alloc_ring_memfd(struct dpa_plugin_ctx *c, size_t ring_alloc_size)
{
	c->ring_memfd = dpa_memfd_create("dpa_plugin_ring", MFD_CLOEXEC);
	if (c->ring_memfd < 0) {
		fprintf(stderr, "dpa_plugin: memfd_create failed: %s\n",
			strerror(errno));
		return -1;
	}
	if (ftruncate(c->ring_memfd, (off_t)ring_alloc_size) < 0) {
		fprintf(stderr, "dpa_plugin: ftruncate(memfd, %zu) failed: %s\n",
			ring_alloc_size, strerror(errno));
		close(c->ring_memfd);
		c->ring_memfd = -1;
		return -1;
	}
	c->ring = mmap(NULL, ring_alloc_size, PROT_READ | PROT_WRITE,
		       MAP_SHARED, c->ring_memfd, 0);
	if (c->ring == MAP_FAILED) {
		fprintf(stderr, "dpa_plugin: mmap(memfd) failed: %s\n",
			strerror(errno));
		close(c->ring_memfd);
		c->ring_memfd = -1;
		c->ring = NULL;
		return -1;
	}
	c->ring_alloc_size = ring_alloc_size;
	return 0;
}

/* Public attach entrypoint for tenants.
 * Maps an already-created memfd into this process and installs it as the
 * ring. Does NOT open IB device, does NOT register MR, does NOT spawn DPA
 * process — those are all coordinator responsibilities. The tenant is a
 * pure producer on the shared ring. */
int dpa_plugin_attach(int memfd)
{
	if (g_ctx.initialized) {
		fprintf(stderr, "dpa_plugin: attach called after init\n");
		return -1;
	}

	struct stat st;
	if (fstat(memfd, &st) < 0) {
		fprintf(stderr, "dpa_plugin: attach fstat failed: %s\n",
			strerror(errno));
		return -1;
	}
	size_t sz = (size_t)st.st_size;
	if (sz < sizeof(struct dpa_plugin_shared)) {
		fprintf(stderr,
			"dpa_plugin: attach memfd size %zu < expected %zu\n",
			sz, sizeof(struct dpa_plugin_shared));
		return -1;
	}

	void *addr = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED,
			  memfd, 0);
	if (addr == MAP_FAILED) {
		fprintf(stderr, "dpa_plugin: attach mmap failed: %s\n",
			strerror(errno));
		return -1;
	}

	g_ctx.ring = (struct dpa_plugin_shared *)addr;
	g_ctx.ring_alloc_size = sz;
	g_ctx.ring_memfd = memfd;
	g_ctx.role = DPA_ROLE_TENANT;
	g_ctx.initialized = 1;
	g_ctx.notify_enabled = 1;
	g_notify_enabled = 1;

	fprintf(stderr, "dpa_plugin: tenant attach ok (ring=%p size=%zu)\n",
		(void *)g_ctx.ring, sz);
	return 0;
}

static int open_ibv_dev(struct dpa_plugin_ctx *c, const char *devname)
{
	c->dev_list = ibv_get_device_list(NULL);
	if (!c->dev_list) {
		fprintf(stderr, "dpa_plugin: ibv_get_device_list failed\n");
		return -1;
	}
	int i;
	for (i = 0; c->dev_list[i]; i++) {
		if (!strcmp(ibv_get_device_name(c->dev_list[i]), devname))
			break;
	}
	if (!c->dev_list[i]) {
		fprintf(stderr, "dpa_plugin: no device named %s\n", devname);
		return -1;
	}
	c->ibv_ctx = ibv_open_device(c->dev_list[i]);
	if (!c->ibv_ctx) {
		fprintf(stderr, "dpa_plugin: ibv_open_device(%s) failed\n", devname);
		return -1;
	}
	return 0;
}

/* Q16.16 unit constant: 1 IO 在 Q16.16 fixed-point 表示為 65536。
 * Declared here (before sapsq_init_shared) so Fix 3 cold-start seed compiles. */
#define SAPSQ_Q16_ONE  ((uint32_t)(1u << 16))

/* sapsq_init_shared — zero-initialize all SAPS-Q M-series §5.1 fields.
 *
 * Called from dpa_plugin_init() after the shared ring is allocated and
 * memset'd to 0.  Explicit per-field zeroing here documents intent and
 * ensures forward-compatibility when new fields are appended: any field
 * added to the struct is covered by the memset in alloc_ring_memfd(), but
 * the explicit loop below makes the 2D budget plane reset visible to readers
 * of this init path.
 *
 * sapsq_enabled is left 0 (disabled) by default; caller sets it to 1 only
 * when SAPS_Q_ENABLED=1 is present and all required config is valid.
 */
static void sapsq_init_shared(struct dpa_plugin_shared *com)
{
	/* Config scalars */
	com->sapsq_epoch_seq          = 0;
	com->sapsq_epoch_commit_seq   = 0;
	com->sapsq_num_tenants        = 0;
	com->sapsq_num_paths          = 0;
	com->sapsq_link_cap_iops      = 0;
	com->sapsq_epoch_period_us    = 0;
	com->sapsq_probe_rate_budget_q32 = 0;
	com->sapsq_host_tsc_freq      = 0;

	/* Per-path health (DPA write) */
	for (uint32_t p = 0; p < SAPSQ_MAX_PATHS; p++) {
		com->sapsq_path_health[p]            = 0; /* HEALTHY */
		com->sapsq_path_health_factor_q16[p] = SAPSQ_HEALTH_HEALTHY_Q16;
		com->sapsq_path_capacity_iops[p]     = 0;
	}

	/* D classifier bypass gate (host init writes, DPA reads in sapsq_compute_health) */
	com->sapsq_bypass_d_classifier = 0;
	/* SAPS B7 FSM bypass gate (host init writes, DPA reads in saps_update complete path) */
	com->sapsq_bypass_saps_fsm = 0;
	/* Continuous HCAA is the production coupling policy. */
	com->sapsq_bypass_health_coupling =
		SAPSQ_HEALTH_COUPLING_CONTINUOUS;
	/* Completion semantics are the production default. */
	com->sapsq_health_source = SAPSQ_HEALTH_SOURCE_COMPLETION;

	/* Per-tenant config (host init) */
	for (uint32_t t = 0; t < SAPSQ_MAX_TENANTS; t++) {
		com->sapsq_tenant_weight[t]       = 0;
		com->sapsq_demand_iops[t]         = 0;
		com->sapsq_host_submit_count[t]   = 0;  /* Option 1 Lane U: host demand signal */
	}

	/* Per-(tenant, path) 2D budget plane */
	for (uint32_t t = 0; t < SAPSQ_MAX_TENANTS; t++) {
		for (uint32_t p = 0; p < SAPSQ_MAX_PATHS; p++) {
			com->sapsq_tenant_path_rate_budget_q32[t][p]  = 0;
			/* Fix 3 (Lane G): pre-seed 1 IO credit so the very first IO
			 * is not rejected during cold-start.  Without this, the first
			 * call sees last_tsc=0 → dt_tsc=0 guard → refill=0 → tokens=0
			 * < SAPSQ_Q16_ONE → REJECT.  The second IO gets a non-zero
			 * dt_tsc but the bucket is still empty, causing a burst of
			 * early rejects until the first scheduler tick fires. */
			com->sapsq_tenant_path_tokens_budget_q16[t][p] = SAPSQ_Q16_ONE;
			com->sapsq_tenant_path_refresh_tsc[t][p]       = 0;
			com->sapsq_tenant_path_admit_count[t][p]        = 0;
			com->sapsq_tenant_path_reject_count[t][p]       = 0;
			com->sapsq_served_iops[t][p]                    = 0;
		}
	}
}

/* ── Slice 3 SAPS-Q M-series host enforcement plane (2026-05-27) ─────────────
 *
 * 三個 static helper 函式 + sapsq_m_init() 實作 spec §4.4 host enforcement。
 * 對應 specs/research-architecture-consolidated-20260522.md §3.2 / §5.3 和
 * specs/dm-research-redesign-20260522.md §4.4。
 *
 * sapsq_read_stable_epoch():   讀取 epoch_seq/commit_seq 雙-seq stable-epoch protocol
 * sapsq_refresh_and_consume(): Q16.16 token bucket refresh + consume for (t,p)
 * sapsq_admission_check_2d():  enforce the budget of the selected path
 * sapsq_m_init():              讀 SAPSQ_* env,初始化 M-series 欄位
 *
 * 命名前綴全用 sapsq_ 避免與既有 m4_/m5_ 衝突。
 * sapsq_enabled=0 時所有函式均 dormant(caller 不呼叫)。
 */

/* Sentinel return from sapsq_admission_check_2d:通知 caller 落 M4 fallback。
 * 用 -EOPNOTSUPP 作為 out-of-band sentinel,不會與 -EAGAIN(reject) 混淆。 */
#define SAPSQ_ADMIT_STALE_FALLBACK  (-EOPNOTSUPP)

/* QUARANTINED health level (per dpa_plugin_com.h comment: 6=QUARANTINED). */
#define SAPSQ_HEALTH_QUARANTINED_VAL  6u

/* sapsq_host_now_tsc: 讀 aarch64 virtual counter (cntvct_el0)。
 * 與現有 M4 / SAPS-Q 路徑用相同 counter,保持時鐘一致性。 */
static inline uint64_t sapsq_host_now_tsc(void)
{
	uint64_t v;
	__asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(v));
	return v;
}

static inline void sapsq_publish_offered_attempt(
	struct dpa_plugin_shared *com, uint32_t tenant_id)
{
	g_sapsq_offered_pending++;
	if (g_sapsq_offered_pending < SAPSQ_OFFERED_FLUSH_BATCH)
		return;

	__atomic_fetch_add(
		&com->sapsq_host_submit_count[tenant_id],
		g_sapsq_offered_pending,
		__ATOMIC_RELAXED);
	DPA_RING_FLUSH_CACHELINE(&com->sapsq_host_submit_count[tenant_id]);
	DPA_RING_FLUSH_BARRIER();
	g_sapsq_offered_pending = 0;
}

/* sapsq_read_stable_epoch: 讀取 sapsq_epoch_seq / sapsq_epoch_commit_seq 雙-seq
 * protocol,確保 rates_snapshot 是 DPA scheduler 完整提交的 epoch。
 *
 * Protocol (對稱 sapsq_publish_budgets on DPA-side):
 *   1. acquire-load epoch_commit_seq  (e1)
 *   2. compiler barrier
 *   3. copy rates_snapshot from sapsq_tenant_path_rate_budget_q32
 *   4. compiler barrier
 *   5. acquire-load epoch_seq         (e2)
 *   6. e1 == e2 → snapshot 一致;否則 DPA 在步驟 3 期間進行了 update,retry。
 *
 * 成功 → true,*out_epoch = epoch 值。
 * 4 次都失敗(DPA 持續 in-flight) → false → caller 用 fallback。
 *
 * 記憶體開銷:rates_snapshot 16×8×4 = 512 B。在 SPDK fast-path 呼叫,
 * 但每次 admission_check 只做一次 memcpy。若 future profiling 顯示 > 50ns
 * overhead,可改成 lazy per-row copy(每次只複製 tenant_id 那 row = 8×4=32B)。
 */
/* Last-good budget cache (E2 cap-binding fix, 2026-05-29).
 * The DPA's publish_budgets holds epoch_seq != epoch_commit_seq for the whole
 * rate-table write + two ~600 us window-writebacks.  A host read landing in
 * that window misses the seqlock; the old code then returned false and the
 * caller fell back to the M4 *unlimited* admission gate.  Under high IOPS those
 * misses are frequent enough that aggregate admission ran to the system ceiling
 * (~171K) instead of the 120K cap — the cap was effectively non-binding, which
 * washed out work-conserving redistribution (idle capacity had nothing to bind
 * against).  Fix: cache the last consistently-read budget table and reuse it on
 * a miss so enforcement stays tight; only genuine cold-start (no successful read
 * ever) falls through to the caller's cold-start path. */
static __thread uint32_t g_sapsq_lastgood_rates[SAPSQ_MAX_TENANTS][SAPSQ_MAX_PATHS];
static __thread uint32_t g_sapsq_lastgood_epoch;
static __thread bool     g_sapsq_lastgood_valid;

/* Epoch-gated admission read (throughput-ceiling fix, 2026-05-29).
 *
 * Profiling at high IOPS showed the per-I/O admission cost — a 512 B full-table
 * memcpy plus a 16-retry seqlock loop inside the old sapsq_read_stable_epoch() —
 * pinned the SAPS-Q multi-tenant aggregate at ~850 K IOPS (~1.17 us/IO), a hard
 * ceiling far below stock SPDK's ~2.8 M.  Two observations remove it:
 *
 *  (1) Caller only ever reads its OWN tenant's row (rates[tenant_id][*]); the
 *      other 15 rows of the 512 B table are dead weight.  Copy one row (32 B).
 *
 *  (2) sapsq_epoch_commit_seq is monotone and advances ONLY when the DPA fully
 *      commits a new budget table (sapsq_publish_budgets step 6).  The DPA tick
 *      publishes ~10^3/s while the host admits ~10^6/s, so >99.9% of reads see
 *      the same committed epoch as the previous validated read.  If commit_seq
 *      equals the cached epoch, the table has not been republished since we last
 *      validated it — the cached row is still authoritative and we skip both the
 *      seqlock retry loop and the table memcpy entirely.  This is safe even when
 *      a publish is mid-flight (epoch_seq = e+1, commit_seq still = e): the new
 *      table is not yet committed, so the last committed table (epoch e) remains
 *      the correct one to enforce; the next read after commit sees e+1, misses
 *      the cache, and re-validates via the slow path.
 *
 * Fills row_out[p] for p in [0, SAPSQ_MAX_PATHS) with the tenant's per-path
 * Q0.32 rate budget.  Returns false only on genuine cold start (no row ever
 * validated), which the caller maps to the M4 fallback gate.
 */
static inline bool sapsq_read_stable_row(
	struct dpa_plugin_shared *com,
	uint32_t tenant_id,
	uint32_t *out_epoch,
	uint32_t row_out[SAPSQ_MAX_PATHS])
{
	/* Fast path: committed epoch unchanged since the last validated read.
	 * Skip the seqlock loop and the 512 B table copy; serve the cached row. */
	uint32_t e0 = __atomic_load_n(&com->sapsq_epoch_commit_seq,
				      __ATOMIC_ACQUIRE);
	if (g_sapsq_lastgood_valid && e0 == g_sapsq_lastgood_epoch) {
		memcpy(row_out, g_sapsq_lastgood_rates[tenant_id],
		       sizeof(uint32_t) * SAPSQ_MAX_PATHS);
		*out_epoch = e0;
		return true;
	}

	/* Slow path: commit_seq advanced (new table) — re-validate via seqlock and
	 * refresh the full-table cache so subsequent tenants hit the fast path. */
	for (int retry = 0; retry < 16; retry++) {
		uint32_t e1 = __atomic_load_n(&com->sapsq_epoch_commit_seq,
					      __ATOMIC_ACQUIRE);
		__asm__ __volatile__("" ::: "memory");  /* compiler barrier */
		memcpy(g_sapsq_lastgood_rates,
		       (const void *)com->sapsq_tenant_path_rate_budget_q32,
		       sizeof(g_sapsq_lastgood_rates));
		__asm__ __volatile__("" ::: "memory");  /* compiler barrier */
		uint32_t e2 = __atomic_load_n(&com->sapsq_epoch_seq,
					      __ATOMIC_ACQUIRE);
		if (e1 == e2) {
			g_sapsq_lastgood_epoch = e1;
			g_sapsq_lastgood_valid = true;
			memcpy(row_out, g_sapsq_lastgood_rates[tenant_id],
			       sizeof(uint32_t) * SAPSQ_MAX_PATHS);
			*out_epoch = e1;
			return true;
		}
	}
	/* Persistent seqlock miss (DPA mid-publish): enforce the last-good budget
	 * rather than bypassing to the unlimited gate, so the cap stays binding. */
	if (g_sapsq_lastgood_valid) {
		memcpy(row_out, g_sapsq_lastgood_rates[tenant_id],
		       sizeof(uint32_t) * SAPSQ_MAX_PATHS);
		*out_epoch = g_sapsq_lastgood_epoch;
		return true;
	}
	return false;
}

int dpa_plugin_read_sapsq_budget_row(uint32_t *budget_q32,
				     uint32_t max_paths)
{
	uint32_t tenant_id;
	uint32_t num_paths;
	uint32_t epoch;
	uint32_t row[SAPSQ_MAX_PATHS];
	bool any_budget = false;

	if (budget_q32 == NULL || max_paths == 0 || g_ctx.ring == NULL ||
	    !g_ctx.ring->sapsq_enabled) {
		return 0;
	}

	tenant_id = (g_sapsq_local_tenant_id < SAPSQ_MAX_TENANTS)
		    ? g_sapsq_local_tenant_id
		    : g_ctx.ring->sapsq_my_tenant_id;
	if (tenant_id >= SAPSQ_MAX_TENANTS ||
	    !sapsq_read_stable_row(g_ctx.ring, tenant_id, &epoch, row) ||
	    epoch == 0) {
		return 0;
	}

	num_paths = g_ctx.ring->sapsq_num_paths;
	if (num_paths == 0 || num_paths > SAPSQ_MAX_PATHS) {
		num_paths = SAPSQ_MAX_PATHS;
	}
	if (num_paths > max_paths) {
		num_paths = max_paths;
	}

	for (uint32_t p = 0; p < num_paths; p++) {
		budget_q32[p] = row[p];
		any_budget |= row[p] != 0;
	}

	return any_budget ? (int)num_paths : 0;
}

uint32_t dpa_plugin_read_sapsq_probe_rate_q32(void)
{
	if (g_ctx.ring == NULL || !g_ctx.ring->sapsq_enabled) {
		return 0;
	}

	return __atomic_load_n(
		&g_ctx.ring->sapsq_probe_rate_budget_q32,
		__ATOMIC_ACQUIRE);
}

/* sapsq_refresh_and_consume: Q16.16 token bucket refresh + consume for (t,p)。
 *
 * 公式:
 *   dt_tsc        = now_tsc - last_refresh_tsc (clamped to M3_DELTA_TSC_CAP)
 *   refill_q16    = (rate_q32 × dt_tsc) >> 16   (rate IO/tsc × tsc = IO, Q16.16)
 *   burst_cap_q16 = rate_q32 / 10               (≈100ms worth,最多不超過)
 *   tokens        = min(tokens + refill_q16, burst_cap_q16)
 *   if tokens >= Q16_ONE: tokens -= Q16_ONE; ADMIT
 *   else:                 REJECT
 *
 * Race 說明:SPDK per-qpair 是 single-thread,M3-v2 既有 assumption = single-process
 * per-tenant。同 (t,p) 不會 cross-thread。函式不需 atomic CAS(與 M4 一致)。
 *
 * 返回 0 = ADMIT,-EAGAIN = REJECT。
 */
static inline int sapsq_refresh_and_consume_m(
	struct dpa_plugin_shared *com,
	uint32_t t, uint32_t p,
	uint32_t rate_q32, uint64_t now_tsc)
{
	uint64_t last_tsc  = com->sapsq_tenant_path_refresh_tsc[t][p];
	uint64_t dt_tsc    = (last_tsc && now_tsc > last_tsc)
			       ? (now_tsc - last_tsc) : 0ULL;
	if (dt_tsc > M3_DELTA_TSC_CAP)
		dt_tsc = M3_DELTA_TSC_CAP;

	/* refill_q16:rate_q32 (Q0.32 IO/tsc) × dt_tsc → Q0.32 IO,>> 16 → Q16.16 IO */
	uint64_t refill_q16 = ((uint64_t)rate_q32 * dt_tsc) >> 16;

	/* burst cap = 100ms worth of tokens, in Q16.16 IO units.
	 *
	 * BUG FIX (2026-05-29): the old formula `rate_q32 / 10` was dimensionally
	 * wrong.  rate_q32 is Q0.32 IO-per-tsc (per-tick), not per-second, so for a
	 * 500K IO/s path (rate_q32 ≈ 2.15e6) it yielded a burst cap of only
	 * 214748 Q16.16 = 3.27 IOs.  bdevperf submits QD-sized bursts (32–128) per
	 * poll; with a 3-IO cap, ~29–125 of each burst hit the token gate, return
	 * -EAGAIN, and are requeued+retried.  That reject→retry churn (visible as
	 * inflated reactor_run/spdk_ring_dequeue in perf, identical in shape to
	 * stock so it hid in plain sight) capped multi-tenant aggregate at ~750K
	 * vs stock's 2.75M — independent of QD, since a larger QD just produces
	 * more rejects.  Correct 100ms token count in Q16.16 is:
	 *   tokens = rate_iops * 0.1 * 2^16,  rate_iops = rate_q32 * tsc_freq / 2^32
	 *          = (rate_q32 * (tsc_freq/10)) >> 16.
	 * Sustained admit rate is still governed by refill (= the DPA budget), so
	 * weighted max-min fairness is unchanged; only short-burst absorption grows
	 * to match the offered QD. */
	uint64_t tsc_freq = com->sapsq_host_tsc_freq;
	if (tsc_freq == 0)
		tsc_freq = 1000000000ULL;  /* 1 GHz fallback (matches DPA publish) */
	uint64_t burst_cap_q16 = ((uint64_t)rate_q32 * (tsc_freq / 10ULL)) >> 16;
	/* Floor: absorb at least a 256-IO burst so any tested QD (≤128) never
	 * spuriously rejects on a low-rate path. */
	if (burst_cap_q16 < (uint64_t)SAPSQ_Q16_ONE * 256ULL)
		burst_cap_q16 = (uint64_t)SAPSQ_Q16_ONE * 256ULL;

	uint64_t tokens = (uint64_t)com->sapsq_tenant_path_tokens_budget_q16[t][p];
	tokens += refill_q16;
	if (tokens > burst_cap_q16)
		tokens = burst_cap_q16;
	if (tokens > 0xFFFFFFFFULL)
		tokens = 0xFFFFFFFFULL;

	/* 更新 TSC — 只在 dt_tsc > 0(refill 確實算過)或 cold-start(last_tsc==0,
	 * 必須 seed anchor)時推進 anchor。
	 * Bug fix: 在 line-rate 下多筆 consume 可能讀到相同的 now_tsc
	 * (host TSC 來源粒度),此時 dt_tsc==0、refill==0。若仍把 anchor
	 * 覆寫成 now_tsc,會把 last_tsc 到下一個真正前進的 now_tsc 之間的
	 * 子粒度時間丟掉 → refill 永遠補不回來 → 在 line rate 下發出假性
	 * -EAGAIN。保留舊 anchor,讓下次 dt_tsc 涵蓋完整經過時間。 */
	if (dt_tsc > 0 || last_tsc == 0)
		com->sapsq_tenant_path_refresh_tsc[t][p] = now_tsc;

	if (tokens >= (uint64_t)SAPSQ_Q16_ONE) {
		tokens -= (uint64_t)SAPSQ_Q16_ONE;
		com->sapsq_tenant_path_tokens_budget_q16[t][p] = (uint32_t)tokens;
		__atomic_fetch_add(
			&com->sapsq_tenant_path_admit_count[t][p],
			1ull, __ATOMIC_RELAXED);
		return 0;  /* ADMIT */
	}

	com->sapsq_tenant_path_tokens_budget_q16[t][p] = (uint32_t)tokens;
	__atomic_fetch_add(
		&com->sapsq_tenant_path_reject_count[t][p],
		1ull, __ATOMIC_RELAXED);
	return -EAGAIN;  /* REJECT */
}

/* sapsq_admission_check_2d: enforce the committed budget for the path selected
 * by SPDK before the RDMA submission hook runs.
 *
 * The admission hook receives an already-selected qpair. It can delay that
 * submission, but it cannot redirect it to another qpair. Token accounting
 * must therefore use the qpair's physical path. Consuming another path's token
 * here would make the budget table disagree with the path that carries the IO.
 *
 * 返回:
 *   0                          — ADMIT
 *   SAPSQ_ADMIT_STALE_FALLBACK — epoch 尚未就緒(cold-start),caller 走 M4
 *   -EAGAIN                    — selected path has no token, retry through SPDK
 */
static int sapsq_admission_check_2d(
	struct dpa_plugin_shared *com,
	uint32_t tenant_id,
	uint8_t selected_path)
{
	if (!com->sapsq_enabled)
		return SAPSQ_ADMIT_STALE_FALLBACK;

	/* Demand is measured before enforcement so a reduced budget cannot make
	 * an active tenant appear idle to the next HCAA epoch. */
	sapsq_publish_offered_attempt(com, tenant_id);

	/* Step 1: stable-epoch read。讀失敗(DPA 持續 in-flight)→ fallback。
	 * epoch_commit_seq == 0 代表 DPA scheduler 尚未執行過任何 tick → cold-start。 */
	uint32_t epoch;
	uint32_t row[SAPSQ_MAX_PATHS];

	if (!sapsq_read_stable_row(com, tenant_id, &epoch, row))
		return SAPSQ_ADMIT_STALE_FALLBACK;

	/* M-series cold-start bypass: if the DPA scheduler_tick has not produced
	 * meaningful rates for this tenant yet, admit freely so the IO queue
	 * stays warm and DPA accumulates demand EWMA data.
	 *
	 * Three cases indicate cold-start / sub-probe rates:
	 *  (a) epoch == 0: scheduler_tick has never committed (first few ms).
	 *  (b) epoch != 0 but all rates for this tenant are 0: demand_ewma still 0.
	 *  (c) epoch != 0 but all rates < probe_rate_budget_q32: demand_ewma is very
	 *      small (bootstrapping from probe IOs).  A rate below probe means the
	 *      token bucket refills slower than probe allows, causing nearly all IOs
	 *      to be rejected and trapping demand_ewma at a low value permanently.
	 *      Treat sub-probe rates as effectively zero and open the gate until
	 *      rates reach probe_rate (enough demand for valid scheduling). */
	{
		bool all_below_probe = true;
		uint32_t num_paths_check = com->sapsq_num_paths;
		if (num_paths_check == 0 || num_paths_check > SAPSQ_MAX_PATHS)
			num_paths_check = SAPSQ_MAX_PATHS;
		uint32_t probe_q32 = com->sapsq_probe_rate_budget_q32;
		if (epoch == 0) {
			all_below_probe = true;  /* epoch==0 is always treated as cold-start */
		} else {
			for (uint32_t p = 0; p < num_paths_check; p++) {
				if (row[p] >= probe_q32) {
					all_below_probe = false;
					break;
				}
			}
		}
		if (all_below_probe) {
			__atomic_fetch_add(&com->sapsq_stale_epoch_fallback,
					   1ull, __ATOMIC_RELAXED);
			return 0;  /* ADMIT all IOs: cold-start or sub-probe rate epoch */
		}
	}

	uint32_t num_paths = com->sapsq_num_paths;
	if (num_paths == 0 || num_paths > SAPSQ_MAX_PATHS)
		num_paths = SAPSQ_MAX_PATHS;
	if (selected_path >= num_paths)
		selected_path = 0;

	uint64_t now_tsc = sapsq_host_now_tsc();
	int rc = sapsq_refresh_and_consume_m(
		com, tenant_id, selected_path, row[selected_path], now_tsc);
	if (rc == 0 &&
	    (com->sapsq_path_health[selected_path] >= SAPSQ_HEALTH_QUARANTINED_VAL ||
	     row[selected_path] <= com->sapsq_probe_rate_budget_q32)) {
		__atomic_fetch_add(
			&com->sapsq_probe_count[tenant_id][selected_path],
			1ull, __ATOMIC_RELAXED);
	}
	return rc;
}

/* sapsq_m_init: 初始化 SAPS-Q M-series 欄位,讀 SAPSQ_* env vars。
 *
 * 與現有 SAPS_Q_* 路徑(舊 4-path 欄位 sapsq_tenant_path_rate_q32)並存:
 *   - SAPS_Q_ENABLED=1 → 舊路徑(sapsq_epoch_commit 單-seq,舊 token bucket)
 *   - SAPSQ_ENABLED=1  → 新 M-series 路徑(epoch_seq/commit_seq 雙-seq,新 2D table)
 *   - 兩者可同時不啟用(sapsq_enabled=0 → dormant)
 *
 * 若 SAPSQ_ENABLED=1,此函式覆寫 sapsq_enabled=1 並設定:
 *   sapsq_num_tenants, sapsq_num_paths, sapsq_link_cap_iops,
 *   sapsq_epoch_period_us, sapsq_tenant_weight[T], sapsq_probe_rate_budget_q32,
 *   sapsq_host_tsc_freq, sapsq_my_tenant_id (via SAPSQ_MY_TENANT_ID env)。
 *
 * DPA scheduler tick 需要 sapsq_host_tsc_freq 做 period 計算;若 SAPSQ_HOST_TSC_FREQ
 * 未設,直接從 cntfrq_el0 讀取(與 M4/M5 一致)。
 */
static void sapsq_m_init(struct dpa_plugin_shared *com)
{
	const char *en_env = getenv("SAPSQ_ENABLED");
	if (!(en_env && en_env[0] == '1'))
		return;  /* SAPSQ_ENABLED 未設或非 1 → dormant,不覆寫 sapsq_enabled */

	/* tenant id — 每 proc 必設,否則 disable。 */
	const char *tid_env = getenv("SAPSQ_MY_TENANT_ID");
	uint32_t my_tid = (tid_env && tid_env[0]) ? (uint32_t)atoi(tid_env) : 0u;
	if (my_tid >= SAPSQ_MAX_TENANTS) {
		fprintf(stderr,
			"dpa_plugin: SAPSQ_M disabled — SAPSQ_MY_TENANT_ID=%u out of range\n",
			my_tid);
		return;
	}

	/* num_tenants */
	const char *nt_env = getenv("SAPSQ_NUM_TENANTS");
	uint32_t num_t = (nt_env && nt_env[0]) ? (uint32_t)atoi(nt_env) : 4u;
	if (num_t == 0 || num_t > SAPSQ_MAX_TENANTS) {
		fprintf(stderr,
			"dpa_plugin: SAPSQ_M disabled — SAPSQ_NUM_TENANTS=%u out of range\n",
			num_t);
		return;
	}

	/* num_paths */
	const char *np_env = getenv("SAPSQ_NUM_PATHS");
	uint32_t num_p = (np_env && np_env[0]) ? (uint32_t)atoi(np_env) : 3u;
	if (num_p == 0 || num_p > SAPSQ_MAX_PATHS) {
		fprintf(stderr,
			"dpa_plugin: SAPSQ_M disabled — SAPSQ_NUM_PATHS=%u out of range\n",
			num_p);
		return;
	}

	/* link_cap_iops */
	const char *lc_env = getenv("SAPSQ_LINK_CAP_IOPS");
	uint64_t link_cap = (lc_env && lc_env[0])
			    ? strtoull(lc_env, NULL, 10) : 200000ULL;
	if (link_cap == 0)
		link_cap = 200000ULL;

	/* Per-path deliverable capacities K_p, formatted as "900000,900000,900000".
	 * K_p is independent of the namespace service envelope C and must be
	 * provisioned explicitly. A silent K_p=C fallback makes the allocator's
	 * capacity boundary depend on an undocumented assumption and can invalidate
	 * health-coupling experiments. */
	uint64_t path_capacity[SAPSQ_MAX_PATHS] = {0};
	const char *pc_env = getenv("SAPSQ_PATH_CAP_IOPS");
	if (!(pc_env && pc_env[0])) {
		fprintf(stderr,
			"dpa_plugin: SAPSQ_M disabled — SAPSQ_PATH_CAP_IOPS is required\n");
		return;
	}
	char pc_buf[256];
	strncpy(pc_buf, pc_env, sizeof(pc_buf) - 1);
	pc_buf[sizeof(pc_buf) - 1] = '\0';
	char *pc_tok = strtok(pc_buf, ",");
	uint32_t pc_count = 0;
	while (pc_tok && pc_count < num_p) {
		uint64_t cap = strtoull(pc_tok, NULL, 10);
		if (cap == 0) {
			fprintf(stderr,
				"dpa_plugin: SAPSQ_M disabled — invalid K_%u=%s\n",
				pc_count, pc_tok);
			return;
		}
		path_capacity[pc_count++] = cap;
		pc_tok = strtok(NULL, ",");
	}
	if (pc_count != num_p || pc_tok != NULL) {
		fprintf(stderr,
			"dpa_plugin: SAPSQ_M disabled — SAPSQ_PATH_CAP_IOPS "
			"requires exactly %u positive values\n",
			num_p);
		return;
	}

	/* epoch_period_us */
	const char *ep_env = getenv("SAPSQ_EPOCH_PERIOD_US");
	uint32_t epoch_period_us = (ep_env && ep_env[0])
				   ? (uint32_t)atoi(ep_env) : 1000u;
	if (epoch_period_us == 0)
		epoch_period_us = 1000u;

	/* Integer tenant weights. The experiment harness may choose a skewed
	 * distribution, but an unspecified deployment defaults to equal shares. */
	const char *wt_env = getenv("SAPSQ_WEIGHTS");
	uint32_t weights[SAPSQ_MAX_TENANTS] = {0};
	int n_w = 0;
	if (wt_env && wt_env[0]) {
		char buf[256];
		strncpy(buf, wt_env, sizeof(buf) - 1);
		buf[sizeof(buf) - 1] = '\0';
		char *tok = strtok(buf, ",");
		while (tok && n_w < (int)SAPSQ_MAX_TENANTS) {
			weights[n_w++] = (uint32_t)strtoul(tok, NULL, 10);
			tok = strtok(NULL, ",");
		}
	}
	if (n_w == 0) {
		for (uint32_t t = 0; t < num_t; t++)
			weights[t] = 1u;
		n_w = (int)num_t;
	}

	/* probe_rate_budget_q32 — 從 IOPS 換算 Q0.32 IO/tsc。
	 * 先讀 tsc_freq,再換算。
	 * probe_rate_iops → probe_rate_q32 = round(probe_iops / tsc_freq × 2^32) */
	const char *pf_env = getenv("SAPSQ_PROBE_RATE_IOPS");
	uint64_t probe_iops = (pf_env && pf_env[0])
			      ? strtoull(pf_env, NULL, 10) : 1000ULL;

	/* tsc_freq — 若 SAPSQ_HOST_TSC_FREQ 有設則用,否則讀 cntfrq_el0。
	 * DPA scheduler tick 讀 sapsq_host_tsc_freq 做 period 換算,必須一致。 */
	const char *tf_env = getenv("SAPSQ_HOST_TSC_FREQ");
	uint64_t tsc_freq;
	if (tf_env && tf_env[0]) {
		tsc_freq = strtoull(tf_env, NULL, 10);
		if (tsc_freq == 0)
			tsc_freq = 1000000000ULL;
	} else {
		__asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(tsc_freq));
		if (tsc_freq == 0)
			tsc_freq = 1000000000ULL;
	}

	/* Q0.32 換算:probe_q32 = round(probe_iops / tsc_freq × 2^32)。
	 * 與 M4 / 現有 SAPS_Q_PATH_BASE_IOPS_Q32 同公式。 */
	uint32_t probe_rate_q32 = (uint32_t)(
		(double)probe_iops / (double)tsc_freq * (double)(1ULL << 32));

	/* 寫入 M-series 欄位 */
	com->sapsq_my_tenant_id     = my_tid;
	/* E4: also set process-local tenant id so coordinator process itself uses
	 * the correct row in admission_check (same as tenant path). */
	g_sapsq_local_tenant_id = my_tid;
	com->sapsq_num_tenants      = num_t;
	com->sapsq_num_paths        = num_p;
	com->sapsq_link_cap_iops    = link_cap;
	com->sapsq_epoch_period_us  = epoch_period_us;
	com->sapsq_host_tsc_freq    = tsc_freq;
	com->sapsq_probe_rate_budget_q32 = probe_rate_q32;
	for (uint32_t p = 0; p < SAPSQ_MAX_PATHS; p++)
		com->sapsq_path_capacity_iops[p] =
			(p < num_p) ? path_capacity[p] : 0;

	/* TSC fallback for sapsq_run_epoch: 10ms window ensures the DPA epoch
	 * scheduler fires even when throttling prevents event accumulation.
	 * Without this, sapsq_epoch_interval_tsc_fallback stays 0 (only set by
	 * the old SAPS_Q_* init path), TSC fallback never triggers, submit_count
	 * stays 0, demand_ewma stays 0, scheduler_tick emits rate=0 → deadlock.
	 * tsc_freq / 100 = 10ms at 1 GHz (BF3 cntfrq_el0). */
	com->sapsq_epoch_interval_tsc_fallback = (uint32_t)(tsc_freq / 100u);

	for (uint32_t t = 0; t < SAPSQ_MAX_TENANTS; t++)
		com->sapsq_tenant_weight[t] = (t < (uint32_t)n_w) ? weights[t] : 0u;

	/* Lane OO fix: cross-init sapsq_tenant_weight_q16[] and
	 * sapsq_path_base_iops_q32[] so sapsq_run_epoch/sapsq_allocate (which
	 * uses the old SAPSQ_TENANT_MAX/SAPSQ_PATH_MAX fields) does not see
	 * all-zero inputs and output rates=[0,0,0,0].
	 *
	 * sapsq_run_epoch is always called alongside sapsq_scheduler_tick in
	 * the DPA outer loop; if sapsq_path_base_iops_q32 is 0 the allocator
	 * bails early (total_eligible_cap_q32==0) and the epoch log prints
	 * rates=[0,0,0,0], masking health transitions even when the M-series
	 * path (sapsq_scheduler_tick / sapsq_tenant_path_rate_budget_q32) is
	 * functioning correctly.
	 *
	 * The legacy diagnostic allocator retains an equal-share base because its
	 * Q32 budget plane is not used by the M-series enforcement path.
	 * weight_q16 = same integer as the M-series weight (scheduler reads
	 * them directly as unitless integers in both paths). */
	{
		uint32_t base_per_path = (num_p > 0)
					 ? (uint32_t)(link_cap / (uint64_t)num_p)
					 : (uint32_t)link_cap;
		for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++)
			com->sapsq_path_base_iops_q32[p] =
				(p < num_p) ? base_per_path : 0u;
		for (uint32_t t = 0; t < SAPSQ_TENANT_MAX; t++)
			com->sapsq_tenant_weight_q16[t] =
				(t < (uint32_t)n_w) ? weights[t] : 0u;
	}

	/* bypass_d_classifier — env gate for Lane I E1 baseline verification.
	 * SAPSQ_BYPASS_D_CLASSIFIER=1 forces all paths HEALTHY in sapsq_compute_health;
	 * allows slice 1-4 2D enforcement core to run without D classifier interference.
	 * Set to 0 (or unset) to re-enable D classifier for E3 path-degradation tests. */
	const char *bypd_env = getenv("SAPSQ_BYPASS_D_CLASSIFIER");
	com->sapsq_bypass_d_classifier = (bypd_env && bypd_env[0] == '1') ? 1u : 0u;

	/* bypass_saps_fsm — skip SAPS B7 classify + FSM + SHARED_FATE on DPA.
	 * SAPSQ_BYPASS_SAPS_FSM=1 prevents cold-start high-latency spikes from
	 * triggering path degradation during E1 baseline testing.
	 * Set to 0 (or unset) to re-enable SAPS B7 for E3/path-degradation tests. */
	const char *bypsm_env = getenv("SAPSQ_BYPASS_SAPS_FSM");
	com->sapsq_bypass_saps_fsm = (bypsm_env && bypsm_env[0] == '1') ? 1u : 0u;

	/* Controlled coupling comparison. Fixed mode retains health-informed
	 * path placement but does not reduce the namespace admission envelope.
	 * The legacy bypass variable maps to that mode. */
	const char *coupling_env = getenv("SAPSQ_HEALTH_COUPLING_MODE");
	const char *byphc_env = getenv("SAPSQ_BYPASS_HEALTH_COUPLING");
	if (coupling_env == NULL || coupling_env[0] == '\0' ||
	    strcmp(coupling_env, "continuous") == 0) {
		com->sapsq_bypass_health_coupling =
			SAPSQ_HEALTH_COUPLING_CONTINUOUS;
	} else if (strcmp(coupling_env, "fixed") == 0) {
		com->sapsq_bypass_health_coupling =
			SAPSQ_HEALTH_COUPLING_FIXED;
	} else if (strcmp(coupling_env, "binary") == 0) {
		com->sapsq_bypass_health_coupling =
			SAPSQ_HEALTH_COUPLING_BINARY;
	} else {
		fprintf(stderr,
			"dpa_plugin: unknown SAPSQ_HEALTH_COUPLING_MODE=%s; "
			"using continuous\n",
			coupling_env);
		com->sapsq_bypass_health_coupling =
			SAPSQ_HEALTH_COUPLING_CONTINUOUS;
	}
	if ((coupling_env == NULL || coupling_env[0] == '\0') &&
	    byphc_env && byphc_env[0] == '1') {
		com->sapsq_bypass_health_coupling =
			SAPSQ_HEALTH_COUPLING_FIXED;
	}

	/* Controlled signal-isolation mode. All modes retain the same HCAA
	 * allocator and committed-budget selector. REQUEST_RTT denotes the
	 * observed request completion time, not a separate network probe. */
	const char *health_source_env = getenv("SAPSQ_HEALTH_SOURCE");
	if (health_source_env == NULL ||
	    strcmp(health_source_env, "completion") == 0) {
		com->sapsq_health_source = SAPSQ_HEALTH_SOURCE_COMPLETION;
	} else if (strcmp(health_source_env, "queue_depth") == 0) {
		com->sapsq_health_source = SAPSQ_HEALTH_SOURCE_QUEUE_DEPTH;
	} else if (strcmp(health_source_env, "request_rtt") == 0) {
		com->sapsq_health_source = SAPSQ_HEALTH_SOURCE_REQUEST_RTT;
	} else {
		fprintf(stderr,
			"dpa_plugin: unknown SAPSQ_HEALTH_SOURCE=%s; "
			"using completion\n",
			health_source_env);
		com->sapsq_health_source = SAPSQ_HEALTH_SOURCE_COMPLETION;
	}

	/* Pre-seed probe token buckets so the first sapsq_refresh_and_consume_m
	 * call during cold-start (last_tsc==0 → dt_tsc=0 → refill=0) can still
	 * admit IO.  Without pre-seeding, every cold-start probe attempt rejects
	 * because tokens start at 0.  Seed = burst_cap_q16 = probe_rate_q32/10
	 * (same formula as sapsq_refresh_and_consume_m); clamped to 10 × Q16_ONE
	 * minimum so even a very-low probe rate allows the first ~10 IOs. */
	{
		uint32_t burst_seed = probe_rate_q32 / 10u;
		if (burst_seed < 10u * SAPSQ_Q16_ONE)
			burst_seed = 10u * SAPSQ_Q16_ONE;
		for (uint32_t t = 0; t < SAPSQ_MAX_TENANTS; t++)
			for (uint32_t p = 0; p < SAPSQ_MAX_PATHS; p++)
				com->sapsq_tenant_path_tokens_budget_q16[t][p] = burst_seed;
	}

	__atomic_thread_fence(__ATOMIC_RELEASE);
	com->sapsq_enabled = 1;

	/* Lane BBB fix (extended): flush ALL M-series config cache lines to DRAM
	 * so the DPA FlexIO window sees non-zero values for every field it reads.
	 *
	 * Previous flush range ended at sapsq_tenant_path_tokens_budget_q16
	 * (offset ~1342152), but sapsq_host_tsc_freq (offset ~1346400) and
	 * sapsq_host_submit_count (offset ~1346440) are 4 KB past that boundary
	 * and were never flushed.  DPA sapsq_update_demand_ewma() reads
	 * sapsq_host_tsc_freq first and returns early if it is 0 → demand stays
	 * 0 → rates=[0,0,0,0] permanently.
	 *
	 * Fix: extend flush to the end of the entire struct so every field DPA
	 * reads via the FlexIO window is guaranteed to be in DRAM.
	 * Stride 64 = one cache line.  DPA_RING_FLUSH_CACHELINE issues dc cvac;
	 * DPA_RING_FLUSH_BARRIER() issues the trailing dsb ish. */
	{
		const uint8_t *begin = (const uint8_t *)&com->sapsq_enabled;
		const uint8_t *end   = (const uint8_t *)com
				       + sizeof(*com);
		for (const uint8_t *p = begin; p < end; p += 64)
			DPA_RING_FLUSH_CACHELINE(p);
		DPA_RING_FLUSH_BARRIER();
	}

	fprintf(stderr,
		"dpa_plugin: SAPSQ_M enabled tid=%u num_t=%u num_p=%u "
		"link_cap=%lu epoch_us=%u tsc_freq=%lu probe_q32=%u "
		"bypass_d=%u bypass_fsm=%u health_coupling_mode=%u "
		"health_source=%u\n",
		my_tid, num_t, num_p,
		(unsigned long)link_cap, epoch_period_us,
		(unsigned long)tsc_freq, probe_rate_q32,
		com->sapsq_bypass_d_classifier,
		com->sapsq_bypass_saps_fsm,
		com->sapsq_bypass_health_coupling,
		com->sapsq_health_source);
	for (int i = 0; i < n_w; i++)
		fprintf(stderr, "dpa_plugin: SAPSQ_M tenant %d weight=%u\n",
			i, weights[i]);
	for (uint32_t p = 0; p < num_p; p++)
		fprintf(stderr, "dpa_plugin: SAPSQ_M path %u K_p=%lu IOPS\n",
			p, (unsigned long)path_capacity[p]);
}

int dpa_plugin_init(void)
{
	if (g_ctx.initialized)
		return 0;

	/* Initialize defaults for the new E-4 fields. */
	g_ctx.ring_memfd = -1;
	g_ctx.uds_listen_fd = -1;

	const char *disable_init = getenv("DPA_PLUGIN_DISABLE_INIT");
	if (disable_init && disable_init[0] == '1') {
		fprintf(stderr, "dpa_plugin: DPA_PLUGIN_DISABLE_INIT=1, skip init\n");
		g_ctx.initialized = 1;
		g_notify_enabled = 0;
		return 0;
	}

	/* E-4 role selection. */
	const char *role_env = getenv("DPA_PLUGIN_ROLE");
	if (role_env && !strcmp(role_env, "coordinator"))
		g_ctx.role = DPA_ROLE_COORDINATOR;
	else if (role_env && !strcmp(role_env, "tenant"))
		g_ctx.role = DPA_ROLE_TENANT;
	else
		g_ctx.role = DPA_ROLE_STANDALONE;

	const char *sock_env = getenv("DPA_PLUGIN_SOCK");
	const char *sock_path = (sock_env && sock_env[0]) ? sock_env
							   : DPA_PLUGIN_SOCK_DEFAULT;

	/* Parse sampling before the tenant attach fast path. Every producer shares
	 * the same completion ring, so coordinator and tenant processes must apply
	 * the same publication rate. */
	const char *sample_env = getenv("DPA_PLUGIN_SAMPLE_RATE");
	if (sample_env && sample_env[0]) {
		int sr = atoi(sample_env);
		g_sample_rate = (sr > 0) ? (uint32_t)sr : 1;
	} else {
		g_sample_rate = 1;
	}

	/* Tenant: connect to coordinator, receive memfd, attach. No FlexIO. */
	if (g_ctx.role == DPA_ROLE_TENANT) {
		uint64_t rsize = 0;
		int memfd = uds_client_get_memfd(sock_path, &rsize);
		if (memfd < 0) {
			fprintf(stderr, "dpa_plugin: tenant handshake failed\n");
			return -1;
		}
		if (dpa_plugin_attach(memfd) < 0) {
			close(memfd);
			return -1;
		}
		/* E4 coordinator/tenant: set process-local SAPS-Q tenant id.
		 * Tenant processes share the coordinator's ring; ring->sapsq_my_tenant_id
		 * is set by the coordinator (tid=0) so admission_check would use the wrong
		 * row for tenants 1..N-1 without this per-process local copy. */
		{
			const char *tid_env = getenv("SAPSQ_MY_TENANT_ID");
			if (tid_env && tid_env[0]) {
				uint32_t tid = (uint32_t)atoi(tid_env);
				if (tid < SAPSQ_MAX_TENANTS) {
					g_sapsq_local_tenant_id = tid;
					fprintf(stderr,
						"dpa_plugin: tenant local_tenant_id=%u (from SAPSQ_MY_TENANT_ID)\n",
						tid);
				}
			}
		}
		/* M3 v4 snapshot dumper:tenant proc 也要 dump 自己的 ledger,
		 * 給 host control daemon 做 closed-loop feedback。 */
		m3_snapshot_maybe_start(&g_ctx);
		fprintf(stderr,
			"dpa_plugin: tenant init ok (role=tenant sock=%s sample_rate=%u)\n",
			sock_path, g_sample_rate);
		return 0;
	}

	const char *devname = getenv("DPA_PLUGIN_DEV");
	if (!devname || !devname[0])
		devname = "mlx5_1";

	const char *notify_env = getenv("DPA_PLUGIN_NOTIFY");
	int want_notify = (!notify_env || notify_env[0] == '1');

	const char *n_conn_env = getenv("DPA_PLUGIN_N_CONN");
	g_n_conn = (n_conn_env && n_conn_env[0]) ? (uint32_t)atoi(n_conn_env) : 1;
	if (g_n_conn == 0) g_n_conn = 1;
	/* If N is power of two, cache the mask for & instead of %. */
	if ((g_n_conn & (g_n_conn - 1)) == 0)
		g_n_conn_mask = g_n_conn - 1;
	else
		g_n_conn_mask = 0;

	const char *sampler_path = getenv("DPA_PLUGIN_SAMPLER_CSV");

	/* E3 admission gate envs. */
	const char *adm_env = getenv("DPA_PLUGIN_ADMISSION");
	int want_adm = (adm_env && adm_env[0] == '1');
	const char *force_env = getenv("DPA_PLUGIN_FORCE_THROTTLE");
	if (force_env && !strcmp(force_env, "all"))
		g_force_mode = DPA_FORCE_ALL;
	else if (force_env && !strcmp(force_env, "half"))
		g_force_mode = DPA_FORCE_HALF;
	else if (force_env && !strcmp(force_env, "toggle"))
		g_force_mode = DPA_FORCE_TOGGLE;
	else
		g_force_mode = DPA_FORCE_NONE;

	/* Slice 1: independent retry-verdict overlay toggle. Default 0. */
	const char *overlay_env = getenv("DPA_PLUGIN_RETRY_OVERLAY");
	g_retry_overlay_enabled = (overlay_env && overlay_env[0] == '1') ? 1 : 0;

	fprintf(stderr,
		"dpa_plugin: init start (dev=%s notify=%d n_conn=%u adm=%d force=%s retry_overlay=%d sample_rate=%u)\n",
		devname, want_notify, g_n_conn, want_adm,
		g_force_mode == DPA_FORCE_ALL    ? "all" :
		g_force_mode == DPA_FORCE_HALF   ? "half" :
		g_force_mode == DPA_FORCE_TOGGLE ? "toggle" : "none",
		g_retry_overlay_enabled, g_sample_rate);

	if (open_ibv_dev(&g_ctx, devname))
		goto err;

	if (flexio_version_set(FLEXIO_VER_USED)) {
		fprintf(stderr, "dpa_plugin: flexio_version_set failed\n");
		goto err;
	}

	struct flexio_app_select_attr app_attr = {0};
	app_attr.app_name = DEV_APP_NAME_XSTR(DEV_APP_NAME);
	app_attr.hw_model_id = FLEXIO_HW_MODEL_DEF;
	app_attr.ibv_ctx = g_ctx.ibv_ctx;
	if (flexio_app_get(&app_attr, &g_ctx.app)) {
		fprintf(stderr, "dpa_plugin: flexio_app_get failed\n");
		goto err;
	}

	if (flexio_process_create(g_ctx.ibv_ctx, g_ctx.app, NULL, &g_ctx.process)) {
		fprintf(stderr, "dpa_plugin: flexio_process_create failed\n");
		goto err;
	}

	struct flexio_msg_stream_attr stream_attr = {0};
	stream_attr.data_bsize = MSG_HOST_BUFF_BSIZE;
	stream_attr.sync_mode = FLEXIO_MSG_DEV_SYNC_MODE_SYNC;
	stream_attr.level = FLEXIO_MSG_DEV_INFO;
	stream_attr.transport_mode = FLEXIO_MSG_TRANSPORT_QP_RC;
	if (flexio_msg_stream_create(g_ctx.process, &stream_attr, stderr, NULL,
				      &g_ctx.stream)) {
		fprintf(stderr, "dpa_plugin: flexio_msg_stream_create failed\n");
		goto err;
	}

	size_t ring_sz = sizeof(struct dpa_plugin_shared);
	size_t page_sz = (size_t)sysconf(_SC_PAGESIZE);
	size_t ring_alloc_size = (ring_sz + page_sz - 1) & ~(page_sz - 1);

	/* Ring backing is a memfd for both coordinator and standalone (tenant
	 * returns early above and never reaches here). The former standalone
	 * path used a posix_memalign heap ring; it now shares the coordinator's
	 * memfd backing so the out-of-band sapsq_dump helper can map the ring
	 * via /proc/<pid>/fd. Only the backing store changes (heap ->
	 * MAP_SHARED memfd); ring layout, size, and allocation semantics are
	 * unchanged, so the SAPS-Q algorithm is bit-identical to the P0.5 path. */
	if (alloc_ring_memfd(&g_ctx, ring_alloc_size))
		goto err;
	memset(g_ctx.ring, 0, g_ctx.ring_alloc_size);
	/* E1-fix1 cumulative-credit scheme:
	 *   per_qp_tokens = monotonic credit issued (DPA writer)
	 *   host_admitted = monotonic admits consumed (host writer)
	 *   gap = (uint32_t)(credit - admitted) > 0 means capacity to admit.
	 *
	 * Contract fix (dpa_plugin_com.h ~228-235): DPA is the SOLE writer of
	 * per_qp_tokens. The host must not write it. The gap check is wrap-safe
	 * (uint32 subtract), so to grant the same TOKENS_MAX_NORMAL initial
	 * headroom we instead seed host_admitted = credit - seed: then
	 *   gap = credit - (credit - seed) = seed   (wrap-safe for any credit).
	 *
	 * This call site is reached only on a FRESH ring — the memset(0) two
	 * lines above just zeroed the whole struct (coordinator/standalone init;
	 * the tenant attach path maps an existing ring and never seeds here), so
	 * credit reads 0 and host_admitted = (uint32_t)(0 - seed) yields gap =
	 * seed by 32-bit wrap. On a reused mapping the host would not re-run this
	 * init, so DPA's live counters are left untouched. */
	for (uint32_t i = 0; i < DPA_PLUGIN_ADM_SLOTS; i++) {
		uint32_t credit = g_ctx.ring->per_qp_tokens[i];  /* DPA owns; just read */
		g_ctx.ring->host_admitted[i] = (uint32_t)(credit - 1000000u);
	}
	/* v2 SAPS: seed retry_verdict table to UNSET (0xFF) so stock retry
	 * logic falls through until DPA has classified at least one
	 * completion. memset above zeroed it to DPA_SAPS_ACTION_TERMINAL
	 * which would incorrectly abort every retry during warmup. */
	memset((void *)g_ctx.ring->per_qp_path_retry_verdict, 0xFF,
	       sizeof(g_ctx.ring->per_qp_path_retry_verdict));

	/* M1 spike (2026-05-17): cross-stream NEWMA 模式開關。在 memset(g_ctx.ring)
	 * 清零之後寫入,確保不會被 init 覆蓋。DPA 端 process_event() 讀此 bit
	 * 決定要不要把 (qp_idx, path_idx) remap 到 (0, 0) 模擬跨 client 聚合。 */
	{
		const char *m1_env = getenv("SAPS_M1_SINGLE_STREAM");
		uint32_t m1_single = (m1_env && m1_env[0] == '1') ? 1u : 0u;
		g_ctx.ring->saps_m1_single_stream = m1_single;
		/* M1 reset (2026-05-19):每次 init 都 request DPA 清 g_path[0][0],
		 * 避免跨 testbed rep state 殘留;DPA 第一筆 event 處理後會 clear。 */
		g_ctx.ring->saps_m1_reset_request = m1_single;
		fprintf(stderr, "dpa_plugin: M1 single_stream=%u reset_request=%u\n",
			m1_single, m1_single);
	}
	{
		/* M2 spike (2026-05-19): per-client streaming PCA gate。
		 * 預設 0(off),避免影響既有 D-series benchmark。
		 * SAPS_M2_ENABLED=1 啟動 DPA 端 Oja's rule + cosine to baseline。
		 * 對應 specs/m2-dpa-implementation-plan-20260519.md。 */
		const char *m2_env = getenv("SAPS_M2_ENABLED");
		uint32_t m2_on = (m2_env && m2_env[0] == '1') ? 1u : 0u;
		g_ctx.ring->saps_m2_enabled = m2_on;
		fprintf(stderr, "dpa_plugin: M2 enabled=%u\n", m2_on);

		/* M2 v2 host classifier (2026-05-20):讀 DPA 寫的 m2_pca_conf_q16[]
		 * 算 entropy 三分類 → 寫 ring->per_client_joint_verdict[]。
		 * SAPS_M2_V2_CLASSIFIER=1 啟用,預設 0 保 backward compat。 */
		const char *m2v2_env = getenv("SAPS_M2_V2_CLASSIFIER");
		uint32_t m2v2_on = (m2v2_env && m2v2_env[0] == '1') ? 1u : 0u;
		g_ctx.ring->m2_v2_classifier = m2v2_on;
		fprintf(stderr, "dpa_plugin: M2 v2_classifier=%u\n", m2v2_on);
	}
	{
		/* M3 spike (2026-05-19): per-tenant WMM credit ledger gate。
		 * 對應 specs/m3-dpa-implementation-plan-20260519.md。預設 0(off)。
		 * SAPS_M3_LINK_IOPS 設 link 容量(預設 200_000 對齊 spike);
		 * 假設 TSC rate 1.5 GHz。cap_per_tsc_q32 = link_iops / tsc_rate × 2^32 */
		const char *m3_env = getenv("SAPS_M3_ENABLED");
		uint32_t m3_on = (m3_env && m3_env[0] == '1') ? 1u : 0u;
		const char *m3_iops_env = getenv("SAPS_M3_LINK_IOPS");
		uint64_t link_iops = 200000ULL;
		if (m3_iops_env && m3_iops_env[0]) {
			char *end;
			uint64_t v = strtoull(m3_iops_env, &end, 10);
			if (v > 0)
				link_iops = v;
		}
		uint64_t tsc_rate = 1500000000ULL;   /* 1.5 GHz BF3 hart */
		/* Σw normalize:plan §2.1 DPA refresh = w_q16 × cap_per_tsc_q32 >> 16,
		 * host 必須預先把 cap 除以 Σw_active(active tenant 數)。SAPS_M3_NUM_TENANTS
		 * 預設 4 對齊 spike A/B/C/D。 */
		const char *m3_nt_env = getenv("SAPS_M3_NUM_TENANTS");
		uint64_t num_tenants_active = 4ULL;
		if (m3_nt_env && m3_nt_env[0]) {
			char *end;
			uint64_t v = strtoull(m3_nt_env, &end, 10);
			if (v > 0 && v <= 16)
				num_tenants_active = v;
		}
		int64_t cap_q32 = (int64_t)((link_iops * ((__uint128_t)1 << 32))
					     / tsc_rate / num_tenants_active);
		g_ctx.ring->m3_enabled = m3_on;
		g_ctx.ring->m3_capacity_per_tsc_q32 = cap_q32;
		g_ctx.ring->m3_credit_per_io_q32 = (int64_t)1 << 32;  /* 1.0 in Q32.32 */
		/* M3 v2 (2026-05-20):SAPS_M3_V2_GATE=1 啟用 DPA-side admission gate。
		 * credit < 0 時 DPA 把 per_qp_tokens[] 凍結成 0,host fast-path 自然
		 * throttle 該 tenant。v1 為 observability only (gate=0)。 */
		const char *m3v2 = getenv("SAPS_M3_V2_GATE");
		g_ctx.ring->m3_v2_admission_gate = (m3v2 && m3v2[0] == '1') ? 1u : 0u;
		const char *m3v3 = getenv("SAPS_M3_V3_GATE");
		g_ctx.ring->m3_v3_freeze_gate = (m3v3 && m3v3[0] == '1') ? 1u : 0u;
		for (uint32_t t = 0; t < 16; t++)
			g_ctx.ring->m3_tenant_freeze[t] = 0;
		/* M3 per-proc tenant id override:每 bdevperf proc 拿 unique tenant_id,
		 * 避免 qp_id namespace 在 procs 間重複。0xFFFFFFFF = 不 override。 */
		const char *m3_tid = getenv("SAPS_M3_TENANT_ID");
		if (m3_tid && m3_tid[0]) {
			g_ctx.ring->m3_my_tenant_id = (uint32_t)atoi(m3_tid);
			fprintf(stderr, "dpa_plugin: M3 my_tenant_id=%u (per-proc)\n",
				g_ctx.ring->m3_my_tenant_id);
		} else {
			g_ctx.ring->m3_my_tenant_id = 0xFFFFFFFFu;
		}
		for (uint32_t t = 0; t < 16; t++)
			g_ctx.ring->m3_tenant_weights_q16[t] = 65536u;  /* default w=1.0 */
		/* SAPS_M3_WEIGHTS env override:格式 "w0,w1,w2,..." (float),由 host
		 * 端 normalize 並轉 Q16.16 寫進 shared struct。對應 spike scenario C
		 * weight=[3,1,1,1] 等場景。 */
		const char *w_env = getenv("SAPS_M3_WEIGHTS");
		if (w_env && w_env[0]) {
			double tw[16] = {0};
			int idx = 0;
			char buf[256];
			strncpy(buf, w_env, sizeof(buf) - 1);
			buf[sizeof(buf) - 1] = 0;
			char *tok = strtok(buf, ",");
			while (tok && idx < 16) {
				tw[idx++] = atof(tok);
				tok = strtok(NULL, ",");
			}
			for (uint32_t t = 0; t < 16; t++) {
				if (tw[t] > 0) {
					uint32_t q = (uint32_t)(tw[t] * 65536.0);
					g_ctx.ring->m3_tenant_weights_q16[t] = q;
				}
			}
			fprintf(stderr, "dpa_plugin: M3 weights override applied (%d tenants)\n", idx);
		}
		fprintf(stderr,
			"dpa_plugin: M3 enabled=%u v2_gate=%u link_iops=%lu num_tenants=%lu "
			"cap_per_tsc_q32=%ld\n",
			m3_on, g_ctx.ring->m3_v2_admission_gate,
			(unsigned long)link_iops,
			(unsigned long)num_tenants_active, (long)cap_q32);
	}

	{
		/* M3 v2 (M4 namespace, 2026-05-20):in-DPA per-tenant token bucket。
		 * 對應 specs/m3-v2-in-dpa-enforcement-20260520.md。
		 *
		 * Env vars:
		 *   SAPS_M4_ENABLED=1       啟用 (預設 0)
		 *   SAPS_M4_LINK_IOPS       link 容量 IOPS (預設 200000)
		 *   SAPS_M4_WEIGHTS         "w0,w1,w2,..." (float, 預設全 1)
		 *   SAPS_M4_TENANT_ID       本 proc 的 tenant id (per-proc 必設)
		 *   SAPS_M4_BURST_FACTOR    burst cap 多少 IO worth (預設 1000)
		 *
		 * Refresh rate: r_i = (w_i / Σw_active) × link_iops IOPS
		 *   per-tsc Q16.16 = r_i / tsc_freq × 2^16
		 * Burst cap Q16.16 = BURST_FACTOR × 2^16
		 *
		 * tsc_freq 對齊 M3:1.5 GHz BF3 hart。 */
		const char *m4_env = getenv("SAPS_M4_ENABLED");
		uint32_t m4_on = (m4_env && m4_env[0] == '1') ? 1u : 0u;

		uint64_t link_iops = 200000ULL;
		const char *m4_iops_env = getenv("SAPS_M4_LINK_IOPS");
		if (m4_iops_env && m4_iops_env[0]) {
			char *end;
			uint64_t v = strtoull(m4_iops_env, &end, 10);
			if (v > 0)
				link_iops = v;
		}

		/* aarch64 host: cntvct_el0 freq from cntfrq_el0 (BF3 SoC ~1 GHz).
		 * Used both for host fast-path refresh formulas AND for the
		 * Q32 rate calculation。 */
		uint64_t tsc_freq;
		__asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(tsc_freq));
		if (tsc_freq == 0)
			tsc_freq = 1000000000ULL;   /* defensive default */

		uint64_t burst_factor = 1000ULL;
		const char *bf_env = getenv("SAPS_M4_BURST_FACTOR");
		if (bf_env && bf_env[0]) {
			char *end;
			uint64_t v = strtoull(bf_env, &end, 10);
			if (v > 0)
				burst_factor = v;
		}

		double weights[16] = {0};
		int n_weights = 0;
		double sum_w = 0.0;
		const char *w_env = getenv("SAPS_M4_WEIGHTS");
		if (w_env && w_env[0]) {
			char buf[256];
			strncpy(buf, w_env, sizeof(buf) - 1);
			buf[sizeof(buf) - 1] = 0;
			char *tok = strtok(buf, ",");
			while (tok && n_weights < 16) {
				double wv = atof(tok);
				if (wv > 0)
					weights[n_weights] = wv;
				n_weights++;
				tok = strtok(NULL, ",");
			}
		}
		if (n_weights == 0) {
			weights[0] = weights[1] = weights[2] = weights[3] = 1.0;
			n_weights = 4;
		}
		for (int i = 0; i < n_weights; i++)
			sum_w += weights[i];
		if (sum_w <= 0)
			sum_w = 1.0;

		uint64_t cap_q16_val = burst_factor << 16;
		if (cap_q16_val > 0xFFFFFFFFULL)
			cap_q16_val = 0xFFFFFFFFULL;  /* clamp to uint32 max */

		for (uint32_t t = 0; t < 16; t++) {
			double w = (t < (uint32_t)n_weights) ? weights[t] : 0.0;
			double r_iops = (w / sum_w) * (double)link_iops;
			/* Q32 IO/tsc precision (Q16 was insufficient — 8% truncation
			 * at 100K IOPS / 1.5 GHz; see specs/m3-v2-in-dpa-enforcement
			 * §3 xverify learning). */
			double r_per_tsc_q32 = r_iops / (double)tsc_freq *
				(double)(1ULL << 32);
			uint64_t rate_q32 = (uint64_t)(r_per_tsc_q32 + 0.5);
			if (rate_q32 > 0xFFFFFFFFULL)
				rate_q32 = 0xFFFFFFFFULL;
			g_ctx.ring->m4_tenant_refresh_per_tsc_q32[t] = (uint32_t)rate_q32;
			g_ctx.ring->m4_tenant_burst_cap_q16[t] = (uint32_t)cap_q16_val;
			/* Cold-start with full burst so first IO can submit;
			 * subsequent refresh drives steady-state rate. */
			g_ctx.ring->m4_tenant_tokens_q16[t] = (uint32_t)cap_q16_val;
			g_ctx.ring->m4_tenant_reject_flag[t] = 0;
			g_ctx.ring->m4_tenant_last_refresh_tsc[t] = 0;
			g_ctx.ring->m4_tenant_admit_count[t] = 0;
			g_ctx.ring->m4_tenant_reject_count[t] = 0;
		}

		const char *m4_tid_env = getenv("SAPS_M4_TENANT_ID");
		if (m4_tid_env && m4_tid_env[0]) {
			g_ctx.ring->m4_my_tenant_id = (uint32_t)atoi(m4_tid_env);
		} else {
			g_ctx.ring->m4_my_tenant_id = 0xFFFFFFFFu;
		}

		g_ctx.ring->m4_v2_enabled = m4_on;

		if (m4_on) {
			fprintf(stderr,
				"dpa_plugin: M4 v2 enabled link_iops=%lu n_weights=%d sum_w=%.2f "
				"burst_factor=%lu my_tenant=%u\n",
				(unsigned long)link_iops, n_weights, sum_w,
				(unsigned long)burst_factor,
				g_ctx.ring->m4_my_tenant_id);
			for (int t = 0; t < n_weights; t++) {
				fprintf(stderr,
					"dpa_plugin: M4 tenant %d w=%.2f rate_q32=%u (%.2f IOPS) "
					"burst_q16=%u\n",
					t, weights[t],
					g_ctx.ring->m4_tenant_refresh_per_tsc_q32[t],
					(weights[t] / sum_w) * (double)link_iops,
					g_ctx.ring->m4_tenant_burst_cap_q16[t]);
			}
		}
	}

	{
		/* M5 v3 DPA DRR scheduler (2026-05-20):Alt B proactive work-conserving
		 * Deficit Round Robin。
		 *
		 * Env vars:
		 *   SAPS_M5_DRR_ENABLED=1          啟用 (預設 0)
		 *   SAPS_M5_LINK_IOPS              link 容量 IOPS (預設 200000)
		 *   SAPS_M5_WEIGHTS                "w0,w1,..." (float,預設 "1,1,1,1")
		 *   SAPS_M5_TENANT_ID              本 proc 的 tenant id (per-proc 必設)
		 *   SAPS_M5_BASE_QUANTUM_IOS       每 round 每 unit weight 的 quantum (預設 1000)
		 *   SAPS_M5_DRR_INTERVAL_US        DRR round 間隔 µs (預設 1000 = 1ms)
		 *
		 * quantum_q16[t] = round(w[t] / Σw × base_quantum_ios × (1<<16))
		 * drr_interval_tsc = interval_us × tsc_freq / 1e6
		 * cost_per_io_q16 = (1 << 16)  (1.0 IO per grant)
		 *
		 * tsc_freq: aarch64 cntfrq_el0 (BF3 SoC ~1 GHz) 同 M4。 */
		const char *m5_env = getenv("SAPS_M5_DRR_ENABLED");
		uint32_t m5_on = (m5_env && m5_env[0] == '1') ? 1u : 0u;

		uint64_t m5_link_iops = 200000ULL;
		const char *m5_iops_env = getenv("SAPS_M5_LINK_IOPS");
		if (m5_iops_env && m5_iops_env[0]) {
			char *end;
			uint64_t v = strtoull(m5_iops_env, &end, 10);
			if (v > 0)
				m5_link_iops = v;
		}
		(void)m5_link_iops;  /* used for quantum scaling below */

		uint64_t tsc_freq_m5;
		__asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(tsc_freq_m5));
		if (tsc_freq_m5 == 0)
			tsc_freq_m5 = 1000000000ULL;

		double m5_weights[16] = {0};
		int m5_n_weights = 0;
		double m5_sum_w = 0.0;
		const char *m5_w_env = getenv("SAPS_M5_WEIGHTS");
		if (m5_w_env && m5_w_env[0]) {
			char buf[256];
			strncpy(buf, m5_w_env, sizeof(buf) - 1);
			buf[sizeof(buf) - 1] = 0;
			char *tok = strtok(buf, ",");
			while (tok && m5_n_weights < 16) {
				double wv = atof(tok);
				if (wv > 0)
					m5_weights[m5_n_weights] = wv;
				m5_n_weights++;
				tok = strtok(NULL, ",");
			}
		}
		if (m5_n_weights == 0) {
			m5_weights[0] = m5_weights[1] = m5_weights[2] = m5_weights[3] = 1.0;
			m5_n_weights = 4;
		}
		for (int i = 0; i < m5_n_weights; i++)
			m5_sum_w += m5_weights[i];
		if (m5_sum_w <= 0)
			m5_sum_w = 1.0;

		/* burst_mult: per-tenant burst cap = rate_per_interval × burst_mult.
		 * Stored in m5_cost_per_io_q16 field (repurposed). DPA computes
		 * per-tenant burst_cap = (rate_q32 × interval_tsc >> 32) × burst_mult.
		 * Default 2 = allow up to 2 interval-worth of grants ahead.
		 * SAPS_M5_BASE_QUANTUM_IOS now controls this multiplier (default 2). */
		uint64_t burst_mult = 2ULL;
		const char *bq_env = getenv("SAPS_M5_BASE_QUANTUM_IOS");
		if (bq_env && bq_env[0]) {
			char *end;
			uint64_t v = strtoull(bq_env, &end, 10);
			if (v > 0)
				burst_mult = v;
		}

		uint64_t drr_interval_us = 1000ULL;  /* default 1ms */
		const char *di_env = getenv("SAPS_M5_DRR_INTERVAL_US");
		if (di_env && di_env[0]) {
			char *end;
			uint64_t v = strtoull(di_env, &end, 10);
			if (v > 0)
				drr_interval_us = v;
		}
		uint64_t drr_interval_tsc = drr_interval_us * tsc_freq_m5 / 1000000ULL;

		/* Init per-tenant quantum_q16 as Q32 rate (IO/tsc) — same precision
		 * as M4 refresh_per_tsc_q32. Formula:
		 *   rate_iops[t] = w[t]/Σw × link_iops
		 *   rate_q32 = round(rate_iops[t] / tsc_freq × 2^32)
		 * DPA m5_drr_round reads quantum_q16[t] as rate_q32 and computes:
		 *   new_grants = (rate_q32 × delta_tsc) >> 32 */
		for (uint32_t t = 0; t < 16; t++) {
			double w = (t < (uint32_t)m5_n_weights) ? m5_weights[t] : 0.0;
			double r_iops = (w / m5_sum_w) * (double)m5_link_iops;
			double r_q32 = r_iops / (double)tsc_freq_m5 * (double)(1ULL << 32);
			uint64_t rate_q32 = (uint64_t)(r_q32 + 0.5);
			if (rate_q32 > 0xFFFFFFFFULL) rate_q32 = 0xFFFFFFFFULL;
			g_ctx.ring->m5_tenant_quantum_q16[t] = (uint32_t)rate_q32;
			g_ctx.ring->m5_tenant_grant_count[t] = 0;
			g_ctx.ring->m5_tenant_consumed_count[t] = 0;
			g_ctx.ring->m5_tenant_deficit_q16[t] = 0;
			g_ctx.ring->m5_tenant_pending[t] = 0;
			g_ctx.ring->m5_tenant_admit_count[t] = 0;
			g_ctx.ring->m5_tenant_reject_count[t] = 0;
			g_ctx.ring->m5_tenant_idle_rounds[t] = 0;
			g_ctx.ring->m5_tenant_work_conserved[t] = 0;
		}
		/* m5_cost_per_io_q16 repurposed: stores burst_mult (plain uint32). */
		g_ctx.ring->m5_cost_per_io_q16 = (uint32_t)burst_mult;
		g_ctx.ring->m5_drr_interval_tsc = drr_interval_tsc;
		g_ctx.ring->m5_drr_last_tsc = 0;
		g_ctx.ring->m5_drr_round_count = 0;

		const char *m5_tid_env = getenv("SAPS_M5_TENANT_ID");
		if (m5_tid_env && m5_tid_env[0])
			g_ctx.ring->m5_my_tenant_id = (uint32_t)atoi(m5_tid_env);
		else
			g_ctx.ring->m5_my_tenant_id = 0xFFFFFFFFu;

		g_ctx.ring->m5_drr_enabled = m5_on;

		if (m5_on) {
			fprintf(stderr,
				"dpa_plugin: M5 DRR enabled link_iops=%lu n_weights=%d "
				"sum_w=%.2f burst_mult=%lu interval_us=%lu "
				"interval_tsc=%lu my_tenant=%u\n",
				(unsigned long)m5_link_iops, m5_n_weights, m5_sum_w,
				(unsigned long)burst_mult,
				(unsigned long)drr_interval_us,
				(unsigned long)drr_interval_tsc,
				g_ctx.ring->m5_my_tenant_id);
			for (int t = 0; t < m5_n_weights; t++) {
				fprintf(stderr,
					"dpa_plugin: M5 tenant %d w=%.2f rate_q32=%u "
					"(%.1f IOPS)\n",
					t, m5_weights[t],
					g_ctx.ring->m5_tenant_quantum_q16[t],
					(m5_weights[t] / m5_sum_w) * (double)m5_link_iops);
			}
		}
	}

	{
		/* SAPS-Q (2026-05-22) — DPA-advised, host-enforced per-(tenant,path)
		 * scheduler. Spec: specs/dm-research-redesign-20260522.md §4。
		 *
		 * DPA RP runs a weighted max-min allocator every epoch and publishes
		 * sapsq_tenant_path_rate_q32[t][p] (Q32 IO/tsc) together with
		 * sapsq_path_eligibility[p]. Host fast-path consumes the rates as a
		 * per-(tenant,path) token bucket, with M4 as stale-epoch fallback.
		 *
		 * Env vars:
		 *   SAPS_Q_ENABLED               master gate (default 0)
		 *   SAPS_Q_MY_TENANT_ID          this proc's tenant id (0..SAPSQ_TENANT_MAX-1)
		 *   SAPS_Q_EPOCH_INTERVAL_EVENTS DPA scheduler trigger (default 1024)
		 *   SAPS_Q_EPOCH_STALE_US        host fallback threshold µs (default 50000;
		 *                                Bug C 2026-05-24: was 1000, too tight vs ~14ms DPA cadence)
		 *   SAPS_Q_WEIGHTS               "w0,w1,w2,w3" Q16.16 (default "65536,21845,21845,21845")
		 *   SAPS_Q_DEMAND_Q32            "d0,d1,d2,d3" Q32 IO/tsc-equivalent (required when enabled)
		 *   SAPS_Q_PATH_BASE_IOPS_Q32    "b0,b1,b2,b3" Q32 IO/tsc per-path base capacity (required)
		 *   SAPS_Q_PROBE_RATE_Q32        "p0,p1,p2,p3" Q32 IO/tsc probe budget (default small)
		 *
		 * tsc_freq is aarch64 cntfrq_el0 — same as M4 / M5 above. */
		const char *q_env = getenv("SAPS_Q_ENABLED");
		uint32_t q_on = (q_env && q_env[0] == '1') ? 1u : 0u;

		/* defensive: zero every field first so a half-configured run can't
		 * inherit stale state from a previous mmap. */
		g_ctx.ring->sapsq_enabled = 0;
		g_ctx.ring->sapsq_my_tenant_id = 0;
		g_ctx.ring->sapsq_epoch_interval_events = 1024;
		g_ctx.ring->sapsq_epoch_stale_us = 1000;
		g_ctx.ring->sapsq_epoch_interval_tsc_fallback = 0;
		g_ctx.ring->sapsq_epoch = 0;
		g_ctx.ring->sapsq_epoch_commit = 0;
		g_ctx.ring->sapsq_last_epoch_tsc = 0;
		g_ctx.ring->sapsq_epoch_count_events = 0;
		for (uint32_t t = 0; t < SAPSQ_TENANT_MAX; t++) {
			g_ctx.ring->sapsq_tenant_weight_q16[t] = 0;
			g_ctx.ring->sapsq_tenant_demand_q32[t] = 0;
			for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
				g_ctx.ring->sapsq_tenant_path_rate_q32[t][p] = 0;
				g_ctx.ring->sapsq_tenant_path_tokens_q16[t][p] = 0;
				g_ctx.ring->sapsq_tenant_path_last_refresh_tsc[t][p] = 0;
				g_ctx.ring->sapsq_admit_count[t][p] = 0;
				g_ctx.ring->sapsq_reject_count[t][p] = 0;
				g_ctx.ring->sapsq_probe_count[t][p] = 0;
			}
		}
		for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
			g_ctx.ring->sapsq_path_base_iops_q32[p] = 0;
			g_ctx.ring->sapsq_probe_rate_q32[p] = 0;
			g_ctx.ring->sapsq_path_health_q16[p] = SAPSQ_HEALTH_HEALTHY_Q16;
			g_ctx.ring->sapsq_path_eligibility[p] = SAPSQ_ELIG_NORMAL;
		}
		g_ctx.ring->sapsq_stale_epoch_fallback = 0;
		g_ctx.ring->sapsq_total_epochs_consumed = 0;

		/* Zero-init the SAPS-Q §5.1 M-series 2D budget plane fields
		 * (SAPSQ_MAX_TENANTS × SAPSQ_MAX_PATHS arrays added in slice 1). */
		sapsq_init_shared(g_ctx.ring);

		if (q_on) {
			/* tenant id */
			const char *tid_env = getenv("SAPS_Q_MY_TENANT_ID");
			uint32_t my_tid = (tid_env && tid_env[0]) ? (uint32_t)atoi(tid_env) : 0u;
			if (my_tid >= SAPSQ_TENANT_MAX) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q disabled — MY_TENANT_ID=%u out of range\n",
					my_tid);
				goto sapsq_init_done;
			}

			/* epoch interval / staleness */
			const char *ei_env = getenv("SAPS_Q_EPOCH_INTERVAL_EVENTS");
			uint32_t epoch_interval = (ei_env && ei_env[0]) ? (uint32_t)atoi(ei_env) : 1024u;
			if (epoch_interval == 0) epoch_interval = 1024u;

			/* Bug C/D fix (2026-05-24): original default 1000µs was too tight
			 * vs DPA scheduler ~14ms cadence; raised to 50ms in Bug-C fix.
			 * Bug D: with N standalone DPA processes each owning a DPA app,
			 * the DPA core time-shares across all N apps, so effective epoch
			 * cadence per process ≈ N×14ms. At N=4 tenants that is ~56ms,
			 * meaning 50ms threshold still produced >90% stale-fallthrough
			 * for tenant 0. Raised to 500ms (covers N=32 with safety margin)
			 * while still catching genuine DPA stalls (>500ms gap). */
			const char *es_env = getenv("SAPS_Q_EPOCH_STALE_US");
			uint32_t stale_us = (es_env && es_env[0]) ? (uint32_t)atoi(es_env) : 500000u;
			if (stale_us == 0) stale_us = 500000u;

			/* Bug-A fix: TSC-based fallback interval for dormant event counter.
			 * Default: tsc_hz/100 = 10ms-equivalent at 1GHz AArch64 cntfrq_el0.
			 * Set 0 to disable (event-count trigger only). */
			uint64_t tsc_freq_early;
			__asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(tsc_freq_early));
			if (tsc_freq_early == 0) tsc_freq_early = 1000000000ULL;
			const char *ef_env = getenv("SAPS_Q_EPOCH_FALLBACK_TSC");
			uint32_t epoch_fallback_tsc;
			if (ef_env && ef_env[0]) {
				unsigned long v = strtoul(ef_env, NULL, 10);
				epoch_fallback_tsc = (v > 0xFFFFFFFFul) ? 0xFFFFFFFFu : (uint32_t)v;
			} else {
				/* default 10ms = tsc_hz / 100 */
				epoch_fallback_tsc = (uint32_t)(tsc_freq_early / 100u);
			}

			/* Weights (Q16.16, comma-separated). Default 3:1:1:1. */
			uint32_t weights_q16[SAPSQ_TENANT_MAX] = {0};
			int n_w = 0;
			const char *qw_env = getenv("SAPS_Q_WEIGHTS");
			if (qw_env && qw_env[0]) {
				char buf[512];
				strncpy(buf, qw_env, sizeof(buf) - 1);
				buf[sizeof(buf) - 1] = 0;
				char *tok = strtok(buf, ",");
				while (tok && n_w < (int)SAPSQ_TENANT_MAX) {
					unsigned long v = strtoul(tok, NULL, 10);
					if (v > 0xFFFFFFFFul) v = 0xFFFFFFFFul;
					weights_q16[n_w++] = (uint32_t)v;
					tok = strtok(NULL, ",");
				}
			}
			if (n_w == 0) {
				/* default 3:1:1:1 in Q16.16 */
				weights_q16[0] = 65536u;
				weights_q16[1] = 21845u;
				weights_q16[2] = 21845u;
				weights_q16[3] = 21845u;
				n_w = 4;
			}
			/* F-HI-3 + F-MD-1 fold-in (2026-05-22): clamp weights to Q16.16
			 * unit value (65536). Misconfig where operator wrote raw integer
			 * weights (e.g. 3, 1, 1, 1) is fine since 3 < 65536; but writing
			 * pre-multiplied values (e.g. 3*65536) inflates allocations.
			 * Cap each entry to 65536 (1.0 in Q16.16) and warn. */
			for (int i = 0; i < n_w; i++) {
				if (weights_q16[i] > 0x10000u) {
					uint32_t raw = weights_q16[i];
					weights_q16[i] = 0x10000u;
					fprintf(stderr,
						"dpa_plugin: SAPS_Q_WEIGHTS[%d]=%u exceeded clamp, "
						"capped to %u — likely misconfig\n",
						i, raw, 0x10000u);
				}
			}
			uint64_t sum_w_q16 = 0;
			for (int i = 0; i < n_w; i++) sum_w_q16 += weights_q16[i];
			if (sum_w_q16 == 0) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q disabled — SAPS_Q_WEIGHTS sums to 0\n");
				goto sapsq_init_done;
			}

			/* Demand (Q32 IO/tsc). REQUIRED when enabled.
			 *
			 * F-HI-3 fix (2026-05-22): clamp each parsed value to
			 * 0x40000000 (~250M IOPS-equiv at 1GHz tsc — leaves > 4×
			 * headroom for cross-product overflow in the allocator).
			 * Operator typos (e.g. pre-Q32 raw IOPS in the env var)
			 * would otherwise produce garbage allocations. */
			uint32_t demand_q32[SAPSQ_TENANT_MAX] = {0};
			int n_d = 0;
			const char *qd_env = getenv("SAPS_Q_DEMAND_Q32");
			if (qd_env && qd_env[0]) {
				char buf[512];
				strncpy(buf, qd_env, sizeof(buf) - 1);
				buf[sizeof(buf) - 1] = 0;
				char *tok = strtok(buf, ",");
				while (tok && n_d < (int)SAPSQ_TENANT_MAX) {
					unsigned long v = strtoul(tok, NULL, 10);
					if (v > 0xFFFFFFFFul) v = 0xFFFFFFFFul;
					uint32_t raw = (uint32_t)v;
					if (raw > 0x40000000u) {
						fprintf(stderr,
							"dpa_plugin: SAPS_Q_DEMAND_Q32[%d]=%u "
							"exceeded clamp, capped to %u — likely misconfig\n",
							n_d, raw, 0x40000000u);
						raw = 0x40000000u;
					}
					demand_q32[n_d++] = raw;
					tok = strtok(NULL, ",");
				}
			}
			if (n_d == 0) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q disabled — SAPS_Q_DEMAND_Q32 required\n");
				goto sapsq_init_done;
			}

			/* Path base IOPS (Q32 IO/tsc). REQUIRED.
			 * F-HI-3 fix: same clamp + warn semantics as demand. */
			uint32_t base_iops_q32[SAPSQ_PATH_MAX] = {0};
			int n_b = 0;
			const char *qb_env = getenv("SAPS_Q_PATH_BASE_IOPS_Q32");
			if (qb_env && qb_env[0]) {
				char buf[256];
				strncpy(buf, qb_env, sizeof(buf) - 1);
				buf[sizeof(buf) - 1] = 0;
				char *tok = strtok(buf, ",");
				while (tok && n_b < (int)SAPSQ_PATH_MAX) {
					unsigned long v = strtoul(tok, NULL, 10);
					if (v > 0xFFFFFFFFul) v = 0xFFFFFFFFul;
					uint32_t raw = (uint32_t)v;
					if (raw > 0x40000000u) {
						fprintf(stderr,
							"dpa_plugin: SAPS_Q_PATH_BASE_IOPS_Q32[%d]=%u "
							"exceeded clamp, capped to %u — likely misconfig\n",
							n_b, raw, 0x40000000u);
						raw = 0x40000000u;
					}
					base_iops_q32[n_b++] = raw;
					tok = strtok(NULL, ",");
				}
			}
			if (n_b == 0) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q disabled — SAPS_Q_PATH_BASE_IOPS_Q32 required\n");
				goto sapsq_init_done;
			}

			/* Probe rate (Q32 IO/tsc). Defaults to small per-path probe ≈ 100 IOPS-equiv
			 * (DPA reads sapsq_probe_rate_q32 for SAPSQ_ELIG_PROBE allocation; host
			 * uses sapsq_path_eligibility[] for probe-budget short-circuit). */
			uint32_t probe_q32[SAPSQ_PATH_MAX] = {0};
			int n_p = 0;
			const char *qp_env = getenv("SAPS_Q_PROBE_RATE_Q32");
			if (qp_env && qp_env[0]) {
				char buf[256];
				strncpy(buf, qp_env, sizeof(buf) - 1);
				buf[sizeof(buf) - 1] = 0;
				char *tok = strtok(buf, ",");
				while (tok && n_p < (int)SAPSQ_PATH_MAX) {
					unsigned long v = strtoul(tok, NULL, 10);
					if (v > 0xFFFFFFFFul) v = 0xFFFFFFFFul;
					uint32_t raw_p = (uint32_t)v;
					/* F-MD-* probe-rate consistency clamp: same 0x40000000
					 * cap + warn as demand_q32 / base_iops_q32 (F-HI-3). */
					if (raw_p > 0x40000000u) {
						fprintf(stderr,
							"dpa_plugin: SAPS_Q_PROBE_RATE_Q32[%d]=%u "
							"exceeded clamp, capped to %u — likely misconfig\n",
							n_p, raw_p, 0x40000000u);
						raw_p = 0x40000000u;
					}
					probe_q32[n_p++] = raw_p;
					tok = strtok(NULL, ",");
				}
			}
			/* tsc_freq for unit conversion log only (Q32 values already
			 * pre-converted by orchestrator) */
			uint64_t tsc_freq_q;
			__asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(tsc_freq_q));
			if (tsc_freq_q == 0) tsc_freq_q = 1000000000ULL;

			/* Publish to shared region */
			g_ctx.ring->sapsq_my_tenant_id = my_tid;
			g_ctx.ring->sapsq_epoch_interval_events = epoch_interval;
			g_ctx.ring->sapsq_epoch_stale_us = stale_us;
			g_ctx.ring->sapsq_epoch_interval_tsc_fallback = epoch_fallback_tsc;
			for (uint32_t t = 0; t < SAPSQ_TENANT_MAX; t++) {
				g_ctx.ring->sapsq_tenant_weight_q16[t] =
					(t < (uint32_t)n_w) ? weights_q16[t] : 0;
				g_ctx.ring->sapsq_tenant_demand_q32[t] =
					(t < (uint32_t)n_d) ? demand_q32[t] : 0;
			}
			for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
				g_ctx.ring->sapsq_path_base_iops_q32[p] =
					(p < (uint32_t)n_b) ? base_iops_q32[p] : 0;
				g_ctx.ring->sapsq_probe_rate_q32[p] =
					(p < (uint32_t)n_p) ? probe_q32[p] : 0;
			}

			__atomic_thread_fence(__ATOMIC_RELEASE);
			g_ctx.ring->sapsq_enabled = 1;

			/* F-HI-4 fix (2026-05-22): init race — DPA may observe
			 * sapsq_enabled=1 via window_read_inv BEFORE a subsequent
			 * window_read_inv covers the config writes above, leading
			 * to one or more epochs running with partial/zero config.
			 * Alternative chosen (simpler than seq counter): sleep 10ms
			 * here so the DPA has many window_read_inv cycles to see
			 * the config before the first orchestrated workload arrives.
			 * Testbed warmup already absorbs the first 10ms, so this is
			 * free. */
			usleep(10000);

			fprintf(stderr,
				"dpa_plugin: SAPS-Q enabled my_tid=%u epoch_interval=%u stale_us=%u "
				"fallback_tsc=%u tsc_freq=%lu n_w=%d n_d=%d n_b=%d n_p=%d\n",
				my_tid, epoch_interval, stale_us, epoch_fallback_tsc,
				(unsigned long)tsc_freq_q, n_w, n_d, n_b, n_p);
			for (int t = 0; t < n_w; t++) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q tenant %d w_q16=%u demand_q32=%u\n",
					t, weights_q16[t],
					(t < n_d) ? demand_q32[t] : 0u);
			}
			for (int p = 0; p < n_b; p++) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q path %d base_q32=%u probe_q32=%u\n",
					p, base_iops_q32[p],
					(p < n_p) ? probe_q32[p] : 0u);
			}
		}
sapsq_init_done:
		/* Slice 3: M-series init — reads SAPSQ_ENABLED and SAPSQ_* env vars.
		 * Must run after all goto sapsq_init_done jumps so SAPSQ_ENABLED=1 is
		 * honoured even when SAPS_Q_ENABLED=0 or the old init path bails early.
		 * sapsq_m_init() is a no-op when SAPSQ_ENABLED != "1". */
		sapsq_m_init(g_ctx.ring);
	}

	if (mlock(g_ctx.ring, g_ctx.ring_alloc_size))
		fprintf(stderr, "dpa_plugin: mlock warning: %s\n", strerror(errno));

	struct ibv_pd *pd = flexio_process_get_pd(g_ctx.process);
	int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
		     IBV_ACCESS_REMOTE_READ;
	g_ctx.mr = ibv_reg_mr(pd, g_ctx.ring, g_ctx.ring_alloc_size, access);
	if (!g_ctx.mr) {
		fprintf(stderr, "dpa_plugin: ibv_reg_mr failed: %s\n", strerror(errno));
		goto err;
	}

	if (flexio_window_create(g_ctx.process, pd, &g_ctx.window)) {
		fprintf(stderr, "dpa_plugin: flexio_window_create failed\n");
		goto err;
	}
	uint32_t win_id = flexio_window_get_id(g_ctx.window);

	struct dpa_plugin_transfer t = {
		.window_id = win_id,
		.mkey_id   = g_ctx.mr->lkey,
		.haddr     = (uint64_t)(uintptr_t)g_ctx.ring,
		.buf_bsize = (uint64_t)g_ctx.ring_alloc_size,
	};
	if (flexio_copy_from_host(g_ctx.process, &t, sizeof(t), &g_ctx.transfer_daddr)) {
		fprintf(stderr, "dpa_plugin: flexio_copy_from_host failed\n");
		goto err;
	}

	if (pthread_create(&g_ctx.rpc_thread, NULL, rpc_thread_fn, &g_ctx)) {
		fprintf(stderr, "dpa_plugin: pthread_create failed\n");
		goto err;
	}
	g_ctx.rpc_thread_started = 1;

	/* Pin rpc_thread off the SPDK reactor cores. The flexio_process_call loop
	 * is CPU-heavy and inherits affinity from the calling thread (bdevperf's
	 * app reactor on -m mask), so without this it competes with the JSON-RPC
	 * handler on the same core and causes bdev_nvme_attach_controller to hang
	 * indefinitely. Pin via DPA_PLUGIN_RPC_CPU env (default core 0). */
	{
		const char *rpc_cpu_env = getenv("DPA_PLUGIN_RPC_CPU");
		int cpu = (rpc_cpu_env && rpc_cpu_env[0]) ? atoi(rpc_cpu_env) : 0;
		cpu_set_t cs;
		CPU_ZERO(&cs);
		CPU_SET(cpu, &cs);
		int rc = pthread_setaffinity_np(g_ctx.rpc_thread, sizeof(cs), &cs);
		if (rc == 0) {
			fprintf(stderr, "dpa_plugin: rpc_thread pinned to CPU %d\n",
				cpu);
		} else {
			fprintf(stderr, "dpa_plugin: rpc_thread affinity CPU %d failed: %s\n",
				cpu, strerror(rc));
		}
	}

	/* M2 v2 classifier thread (optional). Only start if classifier enabled. */
	if (g_ctx.ring->m2_v2_classifier) {
		memset(g_host_m2_suspect_count, 0, sizeof(g_host_m2_suspect_count));
		memset(g_host_m2_prev_verdict,  DPA_M2_VERDICT_UNSET,
		       sizeof(g_host_m2_prev_verdict));
		g_ctx.m2_cls_stop = 0;
		if (pthread_create(&g_ctx.m2_cls_thread, NULL,
				    m2_classifier_fn, &g_ctx)) {
			fprintf(stderr, "dpa_plugin: M2 v2 classifier pthread_create failed\n");
		} else {
			g_ctx.m2_cls_started = 1;
			fprintf(stderr, "dpa_plugin: M2 v2 classifier started (tick=10ms)\n");
		}
	}

	/* E1 sampler (optional). Only start if DPA_PLUGIN_SAMPLER_CSV is set. */
	if (sampler_path && sampler_path[0]) {
		g_ctx.sampler_file = fopen(sampler_path, "w");
		if (!g_ctx.sampler_file) {
			fprintf(stderr, "dpa_plugin: sampler fopen(%s) failed: %s\n",
				sampler_path, strerror(errno));
		} else {
			g_ctx.sampler_stop = 0;
			if (pthread_create(&g_ctx.sampler_thread, NULL,
					    sampler_fn, &g_ctx)) {
				fprintf(stderr, "dpa_plugin: sampler pthread_create failed\n");
				fclose(g_ctx.sampler_file);
				g_ctx.sampler_file = NULL;
			} else {
				g_ctx.sampler_started = 1;
				fprintf(stderr, "dpa_plugin: sampler started -> %s\n",
					sampler_path);
			}
		}
	}

	g_ctx.initialized = 1;
	g_ctx.notify_enabled = want_notify;
	g_notify_enabled = want_notify;
	g_admission_enabled = want_adm;

	/* Coordinator: start UDS server so tenants can attach. */
	if (g_ctx.role == DPA_ROLE_COORDINATOR) {
		if (uds_server_start(&g_ctx, sock_path)) {
			fprintf(stderr, "dpa_plugin: UDS server start failed\n");
			goto err;
		}
	}

	/* M3 v4 snapshot dumper:coordinator/standalone proc 也 dump 自己的 ledger
	 * (standalone smoke 用此測試;coordinator 同時跑 IO 也視為一個 tenant)。 */
	m3_snapshot_maybe_start(&g_ctx);

	const char *role_str = (g_ctx.role == DPA_ROLE_COORDINATOR) ? "coordinator"
				: (g_ctx.role == DPA_ROLE_TENANT) ? "tenant"
				: "standalone";
	fprintf(stderr,
		"dpa_plugin: init ok (role=%s ring=%p lkey=0x%x win=%u n_conn=%u adm=%d)\n",
		role_str, (void *)g_ctx.ring, g_ctx.mr->lkey, win_id,
		g_n_conn, g_admission_enabled);
	return 0;

err:
	dpa_plugin_shutdown();
	return -1;
}

/* ----- Hot path ----- */

static __attribute__((noinline)) void dpa_plugin_flush_batch(void)
{
	uint32_t n = g_batch_count;
	if (n == 0)
		return;

	/* The shared index is a publication boundary. Advancing it before the
	 * entries are written lets the DPA consume holes when several processes
	 * flush concurrently. Serialize the small batch-copy critical section and
	 * publish the new head only after every entry cache line reaches memory. */
	while (__atomic_exchange_n(&g_ctx.ring->producer_lock, 1u,
				    __ATOMIC_ACQUIRE) != 0u) {
		__asm__ __volatile__("yield");
	}

	uint64_t base = __atomic_load_n(&g_ctx.ring->producer_idx,
					__ATOMIC_RELAXED);
	uint64_t end = base + n;
	uint64_t consumer = __atomic_load_n(
		&g_ctx.ring->consumer_idx, __ATOMIC_ACQUIRE);
	uint64_t lag = end >= consumer ? end - consumer : 0;
	uint64_t observed_max = __atomic_load_n(
		&g_ctx.ring->ring_max_lag, __ATOMIC_RELAXED);
	while (lag > observed_max &&
	       !__atomic_compare_exchange_n(
		       &g_ctx.ring->ring_max_lag, &observed_max, lag, false,
		       __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
		/* observed_max is refreshed by the failed compare-exchange. */
	}
	if (__builtin_expect(lag > DPA_PLUGIN_RING_SIZE, 0)) {
		__atomic_fetch_add(
			&g_ctx.ring->ring_overrun_count, 1, __ATOMIC_RELAXED);
	}
	uint32_t start = (uint32_t)(base & DPA_PLUGIN_RING_MASK);
	uint32_t tail_space = DPA_PLUGIN_RING_SIZE - start;
	if (n <= tail_space) {
		memcpy(&g_ctx.ring->entries[start], g_batch_buf,
		       (size_t)n * sizeof(struct dpa_plugin_notify_entry));
	} else {
		memcpy(&g_ctx.ring->entries[start], g_batch_buf,
		       (size_t)tail_space * sizeof(struct dpa_plugin_notify_entry));
		memcpy(&g_ctx.ring->entries[0], &g_batch_buf[tail_space],
		       (size_t)(n - tail_space) * sizeof(struct dpa_plugin_notify_entry));
	}

	/* BB fix: flush entry cache lines + producer_idx to DRAM so the DPA NIC
	 * can see them via the FlexIO PCIe window.  ARM64 __atomic_thread_fence
	 * only orders CPU-to-CPU stores; it does NOT write back cache lines to
	 * DRAM.  DPA window_read_inv re-fetches from DRAM — without dc cvac the
	 * DPA always reads stale zeros, events_seen stays 0, demand_ewma=0.
	 *
	 * Flush order: entries[] first (one dc cvac per 64B cacheline), then
	 * producer_idx (one dc cvac), then a single dsb ish barrier.  This
	 * ensures DPA sees complete entry data before it can observe the new
	 * producer_idx value that signals new entries are ready. */
	for (uint32_t _fi = 0; _fi < n; _fi++) {
		uint32_t _slot = (uint32_t)((base + _fi) & DPA_PLUGIN_RING_MASK);
		DPA_RING_FLUSH_CACHELINE(&g_ctx.ring->entries[_slot]);
	}
	DPA_RING_FLUSH_BARRIER();
	__atomic_store_n(&g_ctx.ring->producer_idx, end, __ATOMIC_RELEASE);
	DPA_RING_FLUSH_CACHELINE(&g_ctx.ring->producer_idx);
	DPA_RING_FLUSH_BARRIER();
	__atomic_store_n(&g_ctx.ring->producer_lock, 0u, __ATOMIC_RELEASE);

	g_batch_count = 0;
	__atomic_fetch_add(&g_batch_flushes, 1, __ATOMIC_RELAXED);
}

static inline void dpa_plugin_push(uint16_t cmd_id, uint8_t opcode,
				    uint16_t qp_id, uint16_t path_id,
				    uint16_t sct_sc, uint32_t nbytes,
				    uint32_t nsid, uint8_t retry_count,
				    uint64_t host_tsc, uint8_t kind)
{
	if (!g_notify_enabled)
		return;

	/* Slice 11a sub-sample gate. Select every Nth submission and remember the
	 * decision by path/CID so the matching completion follows it. Error
	 * completions bypass the gate to preserve sparse-error evidence. */
	if (__builtin_expect(g_sample_rate > 1, 0)) {
		uint32_t path_slot = (uint32_t)path_id & DPA_PLUGIN_PATH_MASK;
		uint32_t word = (uint32_t)cmd_id >> 6;
		uint64_t bit = 1ull << ((uint32_t)cmd_id & 63u);
		bool selected;

		if (kind == 0) {
			uint64_t seq = __atomic_fetch_add(&g_sample_submit_seq, 1,
							 __ATOMIC_RELAXED);
			selected = (seq % g_sample_rate) == 0;
			if (selected) {
				__atomic_fetch_or(&g_sampled_cmd[path_slot][word], bit,
						  __ATOMIC_RELAXED);
			} else {
				__atomic_fetch_and(&g_sampled_cmd[path_slot][word], ~bit,
						   __ATOMIC_RELAXED);
			}
		} else {
			selected = (__atomic_fetch_and(&g_sampled_cmd[path_slot][word],
						       ~bit, __ATOMIC_RELAXED) & bit) != 0;
		}

		if (sct_sc != 0) {
			g_sample_force_pub_errors++;
		} else if (!selected) {
			g_sample_skipped++;
			return;
		}
	}

	/* E1 multi-tenant simulation: rewrite qp_id into the N-tenant space.
	 * Uses a fast xorshift-ish hash of (qp_id, cmd_id, kind) so consecutive
	 * events land on pseudo-random tenants. */
	if (g_n_conn > 1) {
		uint32_t h = ((uint32_t)qp_id << 16) ^ (uint32_t)cmd_id;
		h ^= h >> 13;
		h *= 0x5bd1e995u;
		h ^= h >> 15;
		uint32_t idx = g_n_conn_mask ? (h & g_n_conn_mask)
					      : (h % g_n_conn);
		qp_id = (uint16_t)idx;
	}

	/* E4 coordinator/tenant: stamp qp_id with the process-local tenant ID so
	 * DPA can correctly attribute demand to the right SAPSQ tenant bucket
	 * (g_sapsq_submit_count[tid] where tid = qp & M3_TENANT_MASK).
	 * Without this, coordinator uses qpair->id (1/2/3) → lands on tenant
	 * slots 1/2/3 instead of slot 0; tenant processes land on wrong slots too.
	 * Only active in coordinator/tenant roles (not standalone). */
	if (g_sapsq_local_tenant_id < SAPSQ_MAX_TENANTS) {
		/* Preserve upper bits of qp_id (for g_conn CUSUM indexing) and
		 * replace only the low M3_TENANT_LOG2 bits with the tenant ID. */
		qp_id = (uint16_t)((qp_id & ~(uint16_t)M3_TENANT_MASK)
				   | (uint16_t)(g_sapsq_local_tenant_id & M3_TENANT_MASK));
	}
	if (kind == 0 && g_ctx.ring) {
		uint32_t tid = (uint32_t)qp_id & M3_TENANT_MASK;
		__atomic_fetch_add(
			&g_ctx.ring->host_submit_published[tid],
			1ull, __ATOMIC_RELAXED);
	}

	struct dpa_plugin_notify_entry *e = &g_batch_buf[g_batch_count];
	e->host_tsc    = host_tsc;
	e->cmd_id      = cmd_id;
	e->opcode      = opcode;
	e->kind        = kind;
	e->qp_id       = qp_id;
	e->path_id     = path_id;
	e->sct_sc      = sct_sc;
	e->nbytes      = nbytes;
	e->nsid        = nsid;
	e->retry_count = retry_count;
	g_batch_count++;
	__atomic_fetch_add(&g_batch_events_pushed, 1, __ATOMIC_RELAXED);
	if (__builtin_expect(g_batch_count >= DPA_PLUGIN_BATCH_SIZE, 0))
		dpa_plugin_flush_batch();
}

void dpa_plugin_on_io_submit(uint16_t cmd_id, uint8_t opcode, uint16_t qp_id,
			      uint16_t path_id, uint32_t nbytes, uint32_t nsid,
			      uint8_t retry_count, uint64_t host_tsc)
{
	dpa_plugin_push(cmd_id, opcode, qp_id, path_id, 0, nbytes, nsid,
			retry_count, host_tsc, 0);
}

/* ─── SAPS-Q qpair→path_id hashmap (2026-05-22) ─────────────────────────────
 *
 * Background: qpair->id is per-controller and starts at 1; in a 4-tenant ×
 * 3-path multipath deployment all 12 IO QPs share id=1, so the previous
 * `path_id = qp_id & DPA_PLUGIN_PATH_MASK` derivation collapses every
 * client→path connection into slot [tenant][1]. SAPS-Q per-path budget,
 * eligibility, probe and quarantine require a true per-path slot.
 *
 * Solution: SPDK calls dpa_plugin_register_qp() at qpair create with a
 * deterministic port-derived path_id. Lookups inside the admission gate
 * use this hashmap; misses (e.g. M3/M4/M5 or smoke-test callers) fall back
 * to the legacy mask.
 *
 * Layout: open-addressed linear probing, 64 slots (covers 12 active QPs
 * with > 5× load-factor headroom). Each slot stores qpair pointer as
 * uintptr_t (0 = empty) + uint16_t path_id. Inserts/removes serialised by
 * a single pthread spinlock; lookups are lock-free atomic loads.
 */
#define DPA_PLUGIN_QPMAP_SIZE 64u

/* F-HI-1 fix (2026-05-22): qpmap uses tombstones to preserve linear-probe
 * chains after unregister. Slot sentinels:
 *   qpair_key == 0                 → EMPTY (never used; terminates probe)
 *   qpair_key == DPA_QPMAP_TOMBSTONE → DELETED (probe continues; reusable on insert)
 *   else                           → live entry, key == (uintptr_t)qpair.
 * Lookup stops only on EMPTY. Insert reuses the first TOMBSTONE seen during
 * probe but must keep probing past it to detect an existing live key. */
#define DPA_QPMAP_TOMBSTONE ((uintptr_t)-1)

struct dpa_qp_path_entry {
	_Atomic(uintptr_t) qpair_key;  /* 0 = empty, -1 = tombstone, else live */
	uint16_t           path_id;
};
/* F-MD-* cacheline co-location assert: lock-free path_id_of() reader relies
 * on path_id sharing a cacheline with qpair_key so the RELEASE store on key
 * publishes path_id atomically from the reader's perspective. Struct is 10 B
 * today (8-byte atomic + 2-byte uint16_t, path_id at offset 8); this assert
 * fires if a future maintainer inserts padding that pushes path_id out of the
 * same 64-byte cacheline as qpair_key. */
_Static_assert(offsetof(struct dpa_qp_path_entry, path_id) < 64,
	       "path_id must share cacheline with qpair_key for lock-free read protocol");

static struct dpa_qp_path_entry g_dpa_qpmap[DPA_PLUGIN_QPMAP_SIZE];
static pthread_spinlock_t       g_dpa_qpmap_lock;
static int                      g_dpa_qpmap_lock_init;

static void dpa_qpmap_ensure_lock(void)
{
	if (__builtin_expect(g_dpa_qpmap_lock_init, 1))
		return;
	/* One-shot init. Called inside register/unregister/path_id_of when
	 * the lock hasn't been initialised yet (e.g. plugin loaded but
	 * dpa_plugin_init not yet run for the smoke-test caller). pthread
	 * spinlocks don't have a static initialiser, hence this guard. */
	static pthread_mutex_t bootstrap = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_lock(&bootstrap);
	if (!g_dpa_qpmap_lock_init) {
		pthread_spin_init(&g_dpa_qpmap_lock, PTHREAD_PROCESS_PRIVATE);
		g_dpa_qpmap_lock_init = 1;
	}
	pthread_mutex_unlock(&bootstrap);
}

/* Parse DPA_PLUGIN_PATH_MAP_PORTS once per process. Map kept in static
 * array indexed by raw uint16_t port; sentinel 0xFFFF = no override.
 * Lazily initialised on first call to dpa_plugin_path_id_from_port(). */
static uint16_t g_dpa_port_to_path[65536];
static int      g_dpa_port_map_initialised;

static void dpa_port_map_init_locked(void)
{
	for (uint32_t i = 0; i < 65536; i++)
		g_dpa_port_to_path[i] = 0xFFFFu;

	/* M2: DPA_PLUGIN_PORT_MAP="4500,4600,4700" — comma-separated port list,
	 * list position = path_id (4500→0, 4600→1, 4700→2). Simpler than the
	 * "port:pid,..." form below and matches the realapp 3-path layout where the
	 * default (port-4430)/10 formula would clamp all of 4500/4600/4700 to
	 * PATH_MAX-1 (=3). Parsed first; takes effect when DPA_PLUGIN_PATH_MAP_PORTS
	 * is not set. */
	const char *list_env = getenv("DPA_PLUGIN_PORT_MAP");
	if (list_env && list_env[0] && !getenv("DPA_PLUGIN_PATH_MAP_PORTS")) {
		const char *p = list_env;
		int n = 0;
		uint16_t pid = 0;
		while (*p && pid < DPA_PLUGIN_PATH_MAX) {
			while (*p == ' ' || *p == ',') p++;
			if (!*p) break;
			char *endp = NULL;
			long port = strtol(p, &endp, 10);
			if (endp == p) break;
			p = endp;
			if (port >= 0 && port < 65536) {
				g_dpa_port_to_path[port] = pid;
				n++;
			}
			pid++;
		}
		fprintf(stderr,
			"dpa_plugin: DPA_PLUGIN_PORT_MAP parsed %d entries from \"%s\"\n",
			n, list_env);
		g_dpa_port_map_initialised = 1;
		return;
	}

	const char *env = getenv("DPA_PLUGIN_PATH_MAP_PORTS");
	if (!env || !env[0]) {
		g_dpa_port_map_initialised = 1;
		return;
	}
	/* Parse "port:pid,port:pid,..." — robust to trailing commas. */
	const char *p = env;
	int n = 0;
	while (*p) {
		while (*p == ' ' || *p == ',') p++;
		if (!*p) break;
		char *endp = NULL;
		long port = strtol(p, &endp, 10);
		if (endp == p || *endp != ':') break;
		p = endp + 1;
		long pid = strtol(p, &endp, 10);
		if (endp == p) break;
		p = endp;
		if (port >= 0 && port < 65536 && pid >= 0 && pid < DPA_PLUGIN_PATH_MAX) {
			g_dpa_port_to_path[port] = (uint16_t)pid;
			n++;
		}
	}
	fprintf(stderr,
		"dpa_plugin: DPA_PLUGIN_PATH_MAP_PORTS parsed %d entries from \"%s\"\n",
		n, env);
	g_dpa_port_map_initialised = 1;
}

uint16_t dpa_plugin_path_id_from_port(uint16_t port)
{
	if (__builtin_expect(!g_dpa_port_map_initialised, 0)) {
		dpa_qpmap_ensure_lock();
		pthread_spin_lock(&g_dpa_qpmap_lock);
		if (!g_dpa_port_map_initialised)
			dpa_port_map_init_locked();
		pthread_spin_unlock(&g_dpa_qpmap_lock);
	}
	uint16_t mapped = g_dpa_port_to_path[port];
	if (mapped != 0xFFFFu)
		return mapped;
	/* Current testbed layouts when no explicit map is supplied:
	 *   single-NQN: 4430/4431/4432 -> A/B/C
	 *   multi-tenant: 45xx/46xx/47xx -> A/B/C (low two digits are tenant)
	 * Keep the older 4430/4440/4450 decade layout as a final fallback. */
	if (port >= 4500u && port < 4800u)
		return (uint16_t)((port - 4500u) / 100u);
	if (port >= 4430u && port < 4430u + DPA_PLUGIN_PATH_MAX)
		return (uint16_t)(port - 4430u);
	if (port < 4430)
		return 0;
	uint16_t pid = (uint16_t)((port - 4430u) / 10u);
	if (pid >= DPA_PLUGIN_PATH_MAX)
		pid = DPA_PLUGIN_PATH_MAX - 1u;
	return pid;
}

int dpa_plugin_register_qp(struct spdk_nvme_qpair *qpair, uint16_t path_id)
{
	if (!qpair)
		return -1;
	if (path_id >= DPA_PLUGIN_PATH_MAX)
		path_id = DPA_PLUGIN_PATH_MAX - 1u;

	dpa_qpmap_ensure_lock();
	uintptr_t key = (uintptr_t)qpair;
	/* F-HI-1 fix: linear probe with tombstone awareness. We track the first
	 * tombstone slot seen during the probe and reuse it if the key is not
	 * already present elsewhere in the chain. Probing must continue past
	 * tombstones (they are NOT chain terminators); only an EMPTY (== 0)
	 * slot ends the search. */
	uint32_t hash = (uint32_t)((key >> 4) ^ (key >> 12));
	pthread_spin_lock(&g_dpa_qpmap_lock);
	uint32_t insert_idx = DPA_PLUGIN_QPMAP_SIZE;  /* sentinel: not yet found */
	for (uint32_t step = 0; step < DPA_PLUGIN_QPMAP_SIZE; step++) {
		uint32_t idx = (hash + step) & (DPA_PLUGIN_QPMAP_SIZE - 1u);
		uintptr_t cur = __atomic_load_n(&g_dpa_qpmap[idx].qpair_key,
						__ATOMIC_ACQUIRE);
		if (cur == key) {
			/* Existing live entry — overwrite path_id in place. */
			g_dpa_qpmap[idx].path_id = path_id;
			__atomic_store_n(&g_dpa_qpmap[idx].qpair_key, key,
					 __ATOMIC_RELEASE);
			pthread_spin_unlock(&g_dpa_qpmap_lock);
			fprintf(stderr,
				"dpa_plugin: register_qp qpair=%p path_id=%u slot=%u (update)\n",
				(void *)qpair, (unsigned)path_id, idx);
			return 0;
		}
		if (cur == DPA_QPMAP_TOMBSTONE) {
			if (insert_idx == DPA_PLUGIN_QPMAP_SIZE)
				insert_idx = idx;
			continue;  /* keep probing — key may live further down chain */
		}
		if (cur == 0) {
			/* End of probe chain. Insert here (or at earlier tombstone). */
			if (insert_idx == DPA_PLUGIN_QPMAP_SIZE)
				insert_idx = idx;
			g_dpa_qpmap[insert_idx].path_id = path_id;
			__atomic_store_n(&g_dpa_qpmap[insert_idx].qpair_key, key,
					 __ATOMIC_RELEASE);
			pthread_spin_unlock(&g_dpa_qpmap_lock);
			fprintf(stderr,
				"dpa_plugin: register_qp qpair=%p path_id=%u slot=%u\n",
				(void *)qpair, (unsigned)path_id, insert_idx);
			return 0;
		}
	}
	/* Full scan completed without finding key or empty slot. If a tombstone
	 * was seen, reuse it. */
	if (insert_idx != DPA_PLUGIN_QPMAP_SIZE) {
		g_dpa_qpmap[insert_idx].path_id = path_id;
		__atomic_store_n(&g_dpa_qpmap[insert_idx].qpair_key, key,
				 __ATOMIC_RELEASE);
		pthread_spin_unlock(&g_dpa_qpmap_lock);
		fprintf(stderr,
			"dpa_plugin: register_qp qpair=%p path_id=%u slot=%u (tombstone-reuse)\n",
			(void *)qpair, (unsigned)path_id, insert_idx);
		return 0;
	}
	pthread_spin_unlock(&g_dpa_qpmap_lock);
	fprintf(stderr,
		"dpa_plugin: register_qp OVERFLOW qpair=%p path_id=%u "
		"(qpmap full, %u slots)\n",
		(void *)qpair, (unsigned)path_id, DPA_PLUGIN_QPMAP_SIZE);
	return -1;
}

void dpa_plugin_unregister_qp(struct spdk_nvme_qpair *qpair)
{
	if (!qpair || !g_dpa_qpmap_lock_init)
		return;
	uintptr_t key = (uintptr_t)qpair;
	/* F-HI-1 fix: write TOMBSTONE (not 0) so that subsequent lookups for
	 * other keys that probed through this slot do not terminate early.
	 * Probe continues past tombstones; only EMPTY (== 0) terminates. */
	uint32_t hash = (uint32_t)((key >> 4) ^ (key >> 12));
	pthread_spin_lock(&g_dpa_qpmap_lock);
	for (uint32_t step = 0; step < DPA_PLUGIN_QPMAP_SIZE; step++) {
		uint32_t idx = (hash + step) & (DPA_PLUGIN_QPMAP_SIZE - 1u);
		uintptr_t cur = __atomic_load_n(&g_dpa_qpmap[idx].qpair_key,
						__ATOMIC_ACQUIRE);
		if (cur == key) {
			/* F-MD-* tombstone hygiene: zero path_id before the
			 * RELEASE store so no reader can observe stale data.
			 * Protocol guarantees TOMBSTONE key → lookup skips,
			 * but defensive zeroing prevents future confusion. */
			g_dpa_qpmap[idx].path_id = 0;
			__atomic_store_n(&g_dpa_qpmap[idx].qpair_key,
					 DPA_QPMAP_TOMBSTONE,
					 __ATOMIC_RELEASE);
			break;
		}
		if (cur == 0)
			break;  /* empty slot in probe chain — entry not present */
		/* tombstone: keep probing */
	}
	pthread_spin_unlock(&g_dpa_qpmap_lock);
}

uint16_t dpa_plugin_path_id_of(struct spdk_nvme_qpair *qpair)
{
	if (!qpair || !g_dpa_qpmap_lock_init)
		return DPA_PLUGIN_PATH_MAX;  /* sentinel: caller falls back */
	uintptr_t key = (uintptr_t)qpair;
	uint32_t hash = (uint32_t)((key >> 4) ^ (key >> 12));
	/* Lock-free lookup. The slot key is atomic; a concurrent insert can
	 * race but writes the key with RELEASE *after* writing path_id, so
	 * the ACQUIRE load below sees a consistent (key, path_id) pair.
	 *
	 * F-HI-1 fix: continue probing past TOMBSTONE entries (they are not
	 * chain terminators). Only EMPTY (== 0) ends the search. */
	for (uint32_t step = 0; step < DPA_PLUGIN_QPMAP_SIZE; step++) {
		uint32_t idx = (hash + step) & (DPA_PLUGIN_QPMAP_SIZE - 1u);
		uintptr_t cur = __atomic_load_n(&g_dpa_qpmap[idx].qpair_key,
						__ATOMIC_ACQUIRE);
		if (cur == key)
			return g_dpa_qpmap[idx].path_id;
		if (cur == 0)
			break;
		/* tombstone: keep probing */
	}
	return DPA_PLUGIN_PATH_MAX;
}

/* E3 admission gate. Hot path — must be branchless-friendly.
 * Returns 0 (admit) or -EAGAIN (yield).
 *
 * nbytes: SQE transfer size in bytes (req->payload.size). SAPS-Q branch
 * scales cost proportional to nbytes/4096 so a 64K write consumes 16× the
 * tokens of a 4K read. Other branches ignore the parameter. */
int dpa_plugin_admission_check(struct spdk_nvme_qpair *qpair, uint16_t qp_id,
			       uint32_t nbytes)
{
	/* DIAG Lane BBB-entry: count total admission_check calls, every 100k print */
	{
		static __thread uint64_t __entry_cnt = 0;
		if ((++__entry_cnt % 100000ULL) == 0)
			fprintf(stderr, "DIAG/entry: cnt=%lu ring=%p sapsq_enabled=%u "
				"local_tid=%u\n",
				(unsigned long)__entry_cnt,
				(void *)g_ctx.ring,
				g_ctx.ring ? (unsigned)g_ctx.ring->sapsq_enabled : 99u,
				(unsigned)g_sapsq_local_tenant_id);
	}
	/* SAPS-Q (2026-05-22):DPA-advised per-(tenant,path) token bucket。
	 * Spec: specs/dm-research-redesign-20260522.md §4.4。
	 *
	 * If SAPS-Q is enabled AND the published epoch is fresh, this branch is
	 * the sole admission decision. If the epoch is stale (DPA scheduler did
	 * not commit within sapsq_epoch_stale_us), fall through to M4 static
	 * token bucket as substrate fallback.
	 *
	 * Path id derivation (2026-05-22 fix): use the host-side qpair→path_id
	 * hashmap populated at qpair create with the listener-port-derived
	 * slot. Fall back to (qp_id & DPA_PLUGIN_PATH_MASK) only when the
	 * caller didn't register a qpair pointer (e.g. legacy smoke tests),
	 * which keeps the prior behaviour for non-SAPS-Q callers but lets
	 * 4-tenant × 3-path multipath land each (tid, pid) bucket correctly.
	 *
	 * Anti-deadlock invariant (redesign §4.4):token refresh is host-TSC
	 * driven on every submit attempt, NEVER completion-driven。 */
	if (g_ctx.ring && g_ctx.ring->sapsq_enabled) {
		/* E4 coordinator/tenant: prefer process-local tenant id so each tenant
		 * process uses its own 2D bucket row.  Fall back to ring->sapsq_my_tenant_id
		 * for standalone (legacy) processes where g_sapsq_local_tenant_id is unset. */
		uint32_t my_tid = (g_sapsq_local_tenant_id < SAPSQ_MAX_TENANTS)
				  ? g_sapsq_local_tenant_id
				  : g_ctx.ring->sapsq_my_tenant_id;
		if (my_tid < SAPSQ_TENANT_MAX) {
			/* Slice 3 (2026-05-27): SAPS-Q M-series 2D enforcement。
			 * sapsq_epoch_seq/commit_seq 雙-seq stable-epoch read +
			 * selected-path budget enforcement。
			 *
			 * 若 sapsq_host_tsc_freq != 0(sapsq_m_init 已跑)且
			 * epoch_commit_seq > 0(DPA scheduler 已發佈至少一個 epoch),
			 * 走新 2D path。否則 fall through 到舊 sapsq_epoch_commit 路徑。
			 *
			 * path_id_hint: 同現有 path_id derivation 邏輯(qpair hashmap
			 * 優先,fallback qp_id & PATH_MASK)。 */
			if (g_ctx.ring->sapsq_host_tsc_freq != 0) {
				uint16_t path_hint = dpa_plugin_path_id_of(qpair);
				if (path_hint >= DPA_PLUGIN_PATH_MAX)
					path_hint = (uint16_t)(qp_id & DPA_PLUGIN_PATH_MASK);
				uint32_t num_paths = g_ctx.ring->sapsq_num_paths;
				if (num_paths == 0 || num_paths > SAPSQ_MAX_PATHS)
					num_paths = SAPSQ_MAX_PATHS;
				if (path_hint >= num_paths)
					path_hint = 0;

				int r2d = sapsq_admission_check_2d(
					g_ctx.ring,
					my_tid,
					(uint8_t)path_hint);

				if (r2d == 0) {
					/* ADMIT. The DPA event path records submitted work and
					 * updates the demand estimate used by the scheduler. */
					__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
					return 0;
				}
				if (r2d == SAPSQ_ADMIT_STALE_FALLBACK) {
					/* M-series cold-start: DPA scheduler_tick not yet
					 * committed (epoch_commit_seq==0 or rates all-zero).
					 * ADMIT freely so SPDK IO queue stays warm and DPA
					 * can observe demand via process_event.  Do NOT fall
					 * through to the old sapsq_epoch_commit token-bucket
					 * path — that path reads sapsq_tenant_path_rate_q32
					 * (old scheduler field, always 0 in M-series mode)
					 * and would reject every IO, deadlocking the system. */
					__atomic_fetch_add(
						&g_ctx.ring->sapsq_stale_epoch_fallback,
						1ull, __ATOMIC_RELAXED);
					__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
					return 0;  /* ADMIT during M-series cold-start */
				} else {
					/* -EAGAIN: the selected path has no token. */
					__atomic_fetch_add(
						&g_ctx.ring->sapsq_reject_count[my_tid][path_hint],
						1ull, __ATOMIC_RELAXED);
					__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
					__atomic_fetch_add(&g_ctx.ring->host_adm_throttles, 1, __ATOMIC_RELAXED);
					return -EAGAIN;
				}
			}

			uint16_t path_id = dpa_plugin_path_id_of(qpair);
			if (path_id >= DPA_PLUGIN_PATH_MAX)
				path_id = (uint16_t)(qp_id & DPA_PLUGIN_PATH_MASK);

			/* Bug C diagnosis (2026-05-24): per-thread sampled trace.
			 * Sample every 10000th call to stderr. The cumulative admit/
			 * reject/probe/stale counters already exist on the ring; we
			 * read them at exit via the tenant shutdown path. */
			static __thread uint64_t sapsq_diag_call_count = 0;
			uint64_t this_call = ++sapsq_diag_call_count;

			/* 1. Acquire-load committed epoch. */
			uint64_t epoch_commit = __atomic_load_n(
				&g_ctx.ring->sapsq_epoch_commit,
				__ATOMIC_ACQUIRE);

			/* 2. Host-TSC-anchored staleness check. We cannot compare DPA TSC
			 * to host TSC directly (different clocks), so we anchor on the
			 * host TSC at which THIS process last observed a new epoch. */
			static __thread uint64_t sapsq_last_seen_epoch = 0;
			static __thread uint64_t sapsq_last_seen_host_tsc = 0;
			static __thread uint64_t sapsq_tsc_freq_cached = 0;
			uint64_t now_tsc_q;
			__asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(now_tsc_q));
			if (sapsq_tsc_freq_cached == 0) {
				__asm__ __volatile__("mrs %0, cntfrq_el0"
						     : "=r"(sapsq_tsc_freq_cached));
				if (sapsq_tsc_freq_cached == 0)
					sapsq_tsc_freq_cached = 1000000000ULL;
			}

			if (epoch_commit != sapsq_last_seen_epoch) {
				sapsq_last_seen_epoch = epoch_commit;
				sapsq_last_seen_host_tsc = now_tsc_q;
				__atomic_fetch_add(
					&g_ctx.ring->sapsq_total_epochs_consumed,
					1ull, __ATOMIC_RELAXED);
			}

			/* Cold-start: if we have NEVER seen any epoch_commit (still 0),
			 * no rate data exists yet — fall through to M4 substrate.
			 *
			 * Bug D fix (2026-05-24): DO NOT fall through when rates were
			 * previously published (sapsq_last_seen_host_tsc != 0) even if
			 * the epoch has gone stale. In standalone multi-process mode the
			 * DPA time-shares across N apps; under N=4 contention a process
			 * may see no epoch advances for seconds at a time. The last
			 * published rate_q32 values remain valid — enforcing them is
			 * correct. Only true cold-start (epoch_commit never advanced)
			 * needs to fall through.
			 *
			 * The staleness counter is still incremented (observability) but
			 * does NOT cause a fallthrough anymore once we have seen data. */
			int stale;
			if (sapsq_last_seen_host_tsc == 0) {
				/* True cold-start: no data, fall through. */
				stale = 1;
			} else {
				uint64_t stale_tsc =
					(uint64_t)g_ctx.ring->sapsq_epoch_stale_us *
					sapsq_tsc_freq_cached / 1000000ULL;
				stale = (now_tsc_q - sapsq_last_seen_host_tsc) > stale_tsc;
			}

			if (stale) {
				__atomic_fetch_add(
					&g_ctx.ring->sapsq_stale_epoch_fallback,
					1ull, __ATOMIC_RELAXED);
				if ((this_call % 10000ull) == 0) {
					fprintf(stderr,
						"dpa_plugin: SAPS-Q admission_sample tid=%u pid=%u "
						"epoch_commit=%lu last_seen=%lu elapsed_tsc=%lu stale=1 "
						"decision=%s\n",
						my_tid, (unsigned)path_id,
						(unsigned long)epoch_commit,
						(unsigned long)sapsq_last_seen_epoch,
						(unsigned long)(now_tsc_q - sapsq_last_seen_host_tsc),
						(sapsq_last_seen_host_tsc == 0) ?
							"FALLTHROUGH(cold-start)" :
							"ENFORCE(stale-but-valid-rates)");
				}
				if (sapsq_last_seen_host_tsc == 0) {
					/* Cold-start only: fall through to M4 substrate. */
					goto sapsq_fallthrough;
				}
				/* Stale but have valid rates: fall through to token bucket
				 * below (do NOT goto sapsq_fallthrough). */
			}

			/* 3. Refresh per-(tenant,path) token bucket using host TSC delta.
			 *    refill_q16 = (rate_q32 × delta_tsc) >> 16
			 *    Q32 IO/tsc × tsc = Q32 IO → >> 16 = Q16.16 IO. */
			uint32_t rate_q32 = g_ctx.ring->sapsq_tenant_path_rate_q32[my_tid][path_id];
			uint64_t last_refresh = g_ctx.ring->sapsq_tenant_path_last_refresh_tsc[my_tid][path_id];
			uint64_t delta_tsc = (last_refresh && now_tsc_q > last_refresh)
					    ? (now_tsc_q - last_refresh) : 0ULL;
			if (delta_tsc > M3_DELTA_TSC_CAP)
				delta_tsc = M3_DELTA_TSC_CAP;
			uint64_t refill_q16 = ((uint64_t)rate_q32 * delta_tsc) >> 16;
			uint64_t current = g_ctx.ring->sapsq_tenant_path_tokens_q16[my_tid][path_id];
			uint64_t after = current + refill_q16;
			const uint64_t burst_cap_q16 = (uint64_t)1000ULL << 16; /* 1000 IOs */
			if (after > burst_cap_q16) after = burst_cap_q16;
			if (after > 0xFFFFFFFFULL) after = 0xFFFFFFFFULL;
			g_ctx.ring->sapsq_tenant_path_last_refresh_tsc[my_tid][path_id] = now_tsc_q;

			/* 4. Cost in Q16.16 IO units scaled by nbytes / 4096 (spec
			 * §4.4: cost = max(1, nbytes / 4096)). Examples:
			 *   nbytes=4096   → cost_ios=1   → cost_q16=0x00010000
			 *   nbytes=8192   → cost_ios=2   → cost_q16=0x00020000
			 *   nbytes=65536  → cost_ios=16  → cost_q16=0x00100000 (1048576)
			 * Cap at 256 IO so a single oversized SQE cannot drain the
			 * whole bucket in one shot (burst_cap_q16 = 1000 IOs). */
			uint32_t cost_ios = (nbytes < 4096u) ? 1u : (nbytes / 4096u);
			if (cost_ios > 256u) cost_ios = 256u;
			const uint32_t cost_q16 = cost_ios << 16;

			int decision_admit = (after >= cost_q16);
			if ((this_call % 10000ull) == 0) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q admission_sample tid=%u pid=%u "
					"epoch_commit=%lu rate_q32=%u delta_tsc=%lu refill_q16=%lu "
					"current_q16=%lu after_q16=%lu cost_q16=%u "
					"decision=%s nbytes=%u\n",
					my_tid, (unsigned)path_id,
					(unsigned long)epoch_commit,
					rate_q32,
					(unsigned long)delta_tsc,
					(unsigned long)refill_q16,
					(unsigned long)current,
					(unsigned long)after,
					cost_q16,
					decision_admit ? "ADMIT" : "REJECT",
					nbytes);
			}

			if (decision_admit) {
				g_ctx.ring->sapsq_tenant_path_tokens_q16[my_tid][path_id] =
					(uint32_t)(after - cost_q16);
				__atomic_fetch_add(
					&g_ctx.ring->sapsq_admit_count[my_tid][path_id],
					1ull, __ATOMIC_RELAXED);
				__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
				return 0;
			}

			/* Persist refreshed (insufficient) tokens for next attempt. */
			g_ctx.ring->sapsq_tenant_path_tokens_q16[my_tid][path_id] =
				(uint32_t)after;

			/* 5. PROBE eligibility — let 1 in 64 submits through to drive
			 * recovery on quarantined-but-probe paths. */
			if (g_ctx.ring->sapsq_path_eligibility[path_id] == SAPSQ_ELIG_PROBE) {
				uint64_t total = __atomic_fetch_add(
					&g_ctx.ring->sapsq_probe_count[my_tid][path_id],
					1ull, __ATOMIC_RELAXED);
				if ((total & 63ull) == 0) {
					__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
					return 0;
				}
			}

			__atomic_fetch_add(
				&g_ctx.ring->sapsq_reject_count[my_tid][path_id],
				1ull, __ATOMIC_RELAXED);
			__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
			__atomic_fetch_add(&g_ctx.ring->host_adm_throttles, 1, __ATOMIC_RELAXED);
			return -EAGAIN;
		}
sapsq_fallthrough:;
	}

	/* M3 v2 (M4 namespace, 2026-05-20):host fast-path token bucket。
	 *
	 * Architectural learning (testbed 2026-05-20):純 DPA-side enforcement 會
	 * deadlock — completion 停 → DPA process_event 停 → refresh 停 → reject
	 * 永遠 stuck。 解法:把 refresh+consume 放在 host admission_check (每次
	 * submit attempt 都會跑),DPA process_event 改成 pure observability
	 * (m4_tenant_admit_count / reject_count) 給 paper §6 evidence。
	 *
	 * State (atomic via __atomic_*):
	 *   m4_tenant_tokens_q16[tid]      — current tokens Q16.16
	 *   m4_tenant_last_refresh_tsc[tid] — last refresh wall TSC
	 *   m4_tenant_reject_flag[tid]     — observability (host writes)
	 *
	 * 必須在 g_admission_enabled / m3_v3_freeze 短路前,因為 M4 跟既有 E3 +
	 * M3 v3 完全獨立。 */
	if (g_ctx.ring && g_ctx.ring->m4_v2_enabled) {
		uint32_t m4_tid = g_ctx.ring->m4_my_tenant_id;
		if (m4_tid != 0xFFFFFFFFu) {
			m4_tid &= 15u;
			/* aarch64 virtual counter — plugin lib stays SPDK-API-free.
			 * cntvct_el0 frequency 對齊 cntfrq_el0(BF3 SoC: 1 GHz);
			 * 與 host init 用 1.5 GHz BF3 hart 假設不同,故 plugin init
			 * 取 cntfrq_el0 重新算 rate_q32 在 M4 init 時。 */
			uint64_t now_tsc;
			__asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(now_tsc));
			uint64_t last = __atomic_load_n(
				&g_ctx.ring->m4_tenant_last_refresh_tsc[m4_tid],
				__ATOMIC_ACQUIRE);
			uint32_t rate_q32 = g_ctx.ring->m4_tenant_refresh_per_tsc_q32[m4_tid];
			uint32_t cap_q16  = g_ctx.ring->m4_tenant_burst_cap_q16[m4_tid];
			uint32_t tokens = __atomic_load_n(
				&g_ctx.ring->m4_tenant_tokens_q16[m4_tid],
				__ATOMIC_ACQUIRE);

			if (last == 0) {
				/* cold-start: stamp last and use seed tokens. */
				__atomic_store_n(
					&g_ctx.ring->m4_tenant_last_refresh_tsc[m4_tid],
					now_tsc, __ATOMIC_RELEASE);
			} else if (now_tsc > last) {
				uint64_t delta_tsc = now_tsc - last;
				/* M3_DELTA_TSC_CAP from dev header — protect against
				 * pathological idle gap → uint overflow. */
				if (delta_tsc > 100000000ULL)
					delta_tsc = 100000000ULL;
				uint64_t refill_q16 =
					((uint64_t)rate_q32 * delta_tsc) >> 16;
				uint64_t new_tokens = (uint64_t)tokens + refill_q16;
				if (cap_q16 != 0 && new_tokens > cap_q16)
					new_tokens = cap_q16;
				tokens = (uint32_t)new_tokens;
				__atomic_store_n(
					&g_ctx.ring->m4_tenant_tokens_q16[m4_tid],
					tokens, __ATOMIC_RELEASE);
				__atomic_store_n(
					&g_ctx.ring->m4_tenant_last_refresh_tsc[m4_tid],
					now_tsc, __ATOMIC_RELEASE);
			}

			if (tokens < (1u << 16)) {
				g_ctx.ring->m4_tenant_reject_flag[m4_tid] = 1;
				__atomic_fetch_add(&g_ctx.ring->m4_tenant_reject_count[m4_tid], 1, __ATOMIC_RELAXED);
				__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
				__atomic_fetch_add(&g_ctx.ring->host_adm_throttles, 1, __ATOMIC_RELAXED);
				return -EAGAIN;
			}
			__atomic_fetch_sub(
				&g_ctx.ring->m4_tenant_tokens_q16[m4_tid],
				(1u << 16), __ATOMIC_ACQ_REL);
			g_ctx.ring->m4_tenant_reject_flag[m4_tid] = 0;
			__atomic_fetch_add(&g_ctx.ring->m4_tenant_admit_count[m4_tid], 1, __ATOMIC_RELAXED);
			/* fall through to E3 / M3 v3 path (independent gates) */
		}
	}

	/* M5 v3 DPA DRR scheduler gate (2026-05-20):Alt B proactive scheduler。
	 *
	 * DPA RP 每 ~1ms 跑一輪 Deficit Round Robin,把 per-tenant grant_count
	 * (monotonic uint64) 往上推。Host admission_check 比較:
	 *   grant_count[t] > consumed_count[t] → admit (atomic++ consumed)
	 *   grant_count[t] == consumed_count[t] → throttle (-EBUSY → SPDK queued_req)
	 *
	 * 同時 atomic++ m5_tenant_pending[t] 讓 DPA 知道 active tenant,
	 * 實現 work-conserving: idle tenant 的 deficit 累積並留給下輪,
	 * 活躍 tenant 可以借到更多 grant。
	 *
	 * 與 v2 差別 (paper §6 claim):
	 *   v2: 每 proc 獨立 token bucket,no cross-tenant interaction,
	 *       idle tenant share 浪費 (static rate cap)。
	 *   v3: DPA DRR work-conserving,idle tenant deficit overflow to active,
	 *       oversub 下 active tenant 拿到 >proportional share — 不浪費。
	 *
	 * 不需 winner ring / io_token / 改 hook signature (Alt B)。 */
	if (g_ctx.ring && g_ctx.ring->m5_drr_enabled) {
		/* Per-proc tenant id fix (2026-06-01): m5_my_tenant_id is a SINGLE
		 * shared-ring field, so under the coordinator/tenant model (all procs
		 * share one ring) every proc would gate against the same tenant's
		 * grant/consumed counters → broken multi-tenant enforcement. Prefer the
		 * process-local g_sapsq_local_tenant_id (set per-proc from
		 * SAPSQ_MY_TENANT_ID), mirroring the SAPS-Q 2D-bucket gate; fall back to
		 * the ring field only for legacy standalone procs. */
		uint32_t m5_tid = (g_sapsq_local_tenant_id < SAPSQ_MAX_TENANTS)
				  ? g_sapsq_local_tenant_id
				  : g_ctx.ring->m5_my_tenant_id;
		if (m5_tid != 0xFFFFFFFFu) {
			m5_tid &= (uint32_t)(M3_TENANT_MAX - 1);
			/* No pending signal needed: DPA uses grant_count - consumed_count
			 * gap to detect idle tenants (gap >= burst_cap → stop issuing). */
			uint64_t grant    = __atomic_load_n(
				&g_ctx.ring->m5_tenant_grant_count[m5_tid],
				__ATOMIC_ACQUIRE);
			uint64_t consumed = __atomic_load_n(
				&g_ctx.ring->m5_tenant_consumed_count[m5_tid],
				__ATOMIC_ACQUIRE);
			if (grant > consumed) {
				__atomic_fetch_add(
					&g_ctx.ring->m5_tenant_consumed_count[m5_tid],
					1ull, __ATOMIC_ACQ_REL);
				/* admit — fall through */
			} else {
				__atomic_fetch_add(&g_ctx.ring->m5_tenant_reject_count[m5_tid], 1, __ATOMIC_RELAXED);
				__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
				__atomic_fetch_add(&g_ctx.ring->host_adm_throttles, 1, __ATOMIC_RELAXED);
				return -EAGAIN;
			}
		}
	}

	/* M3 v3 freeze gate (2026-05-20):check per-tenant freeze flag set by DPA
	 * when credit < 0。比 cumulative-credit gap 直接(後者 credit=0 wrap 不
	 * throttle)。必須在 g_admission_enabled short-circuit 之前,因為 M3 v3
	 * 跟 E3 admission 獨立。 */
	if (g_ctx.ring && g_ctx.ring->m3_v3_freeze_gate) {
		uint32_t my_tenant = g_ctx.ring->m3_my_tenant_id;
		if (my_tenant != 0xFFFFFFFFu) {
			my_tenant &= 15u;   /* M3_TENANT_MASK */
			if (g_ctx.ring->m3_tenant_freeze[my_tenant]) {
				__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);
				return -EAGAIN;
			}
		}
	}

	/* Fast path: admission disabled → always admit, no memory read. */
	if (__builtin_expect(!g_admission_enabled, 1))
		return 0;

	__atomic_fetch_add(&g_ctx.ring->host_adm_checks, 1, __ATOMIC_RELAXED);

	int throttle = 0;
	switch (g_force_mode) {
	case DPA_FORCE_ALL:
		throttle = 1;
		break;
	case DPA_FORCE_HALF:
		throttle = (qp_id & 1u) ? 1 : 0;
		break;
	case DPA_FORCE_TOGGLE: {
		/* 50% duty cycle: flip gate state every DPA_FORCE_TOGGLE_PERIOD
		 * calls. Produces repeated admit/throttle bursts, exercising the
		 * queued_req → resubmit drain path across many transitions. */
		uint64_t n = g_ctx.ring->host_adm_checks;  /* already incremented */
		throttle = ((n / DPA_FORCE_TOGGLE_PERIOD) & 1ull) ? 1 : 0;
		break;
	}
	case DPA_FORCE_NONE:
	default: {
		/* E1-fix1: cumulative-credit rate limiter.
		 *   per_qp_tokens[slot] = monotonic credit issued by DPA
		 *   host_admitted[slot] = monotonic admits issued by host
		 *   admit iff credit > admitted; else -EAGAIN.
		 *
		 * DPA is sole writer of per_qp_tokens; host is sole writer of
		 * host_admitted. No MPMC race on either field. The monotonic
		 * property survives 32-bit wrap (uint32_t subtract is
		 * wrap-safe for gaps up to 2 G) so a long-running host can
		 * still compare correctly after credit/admitted wrap.
		 *
		 * Why host_admitted++ is safe even though we CAS instead of
		 * plain increment: multiple SPDK threads per QP is possible
		 * (not in spdk_nvme_perf -c 0x1 single-core, but in general);
		 * CAS-increment ensures each admission contributes exactly one
		 * slot regardless of thread count. */
		uint32_t slot = (uint32_t)qp_id & DPA_PLUGIN_ADM_MASK;
		uint32_t credit = __atomic_load_n(
			&g_ctx.ring->per_qp_tokens[slot], __ATOMIC_ACQUIRE);
		volatile uint32_t *adm_ptr = &g_ctx.ring->host_admitted[slot];
		uint32_t admitted = __atomic_load_n(adm_ptr, __ATOMIC_ACQUIRE);
		throttle = 1;
		/* gap is wrap-safe (uint32_t subtract). */
		while ((uint32_t)(credit - admitted) > 0) {
			if (__atomic_compare_exchange_n(adm_ptr, &admitted,
							admitted + 1,
							false,
							__ATOMIC_ACQ_REL,
							__ATOMIC_ACQUIRE)) {
				throttle = 0;
				break;
			}
			/* admitted refreshed on CAS fail — credit stays. */
		}
		break;
	}
	}

	if (throttle) {
		__atomic_fetch_add(&g_ctx.ring->host_adm_throttles, 1, __ATOMIC_RELAXED);
		return -EAGAIN;
	}
	return 0;
}

void dpa_plugin_on_io_complete(uint16_t cmd_id, uint8_t opcode, uint16_t qp_id,
				uint16_t path_id, uint16_t sct_sc,
				uint32_t nbytes, uint32_t nsid,
				uint8_t retry_count, uint64_t host_tsc)
{
	dpa_plugin_push(cmd_id, opcode, qp_id, path_id, sct_sc, nbytes, nsid,
			retry_count, host_tsc, 1);
}

/* Test accessor — see dpa_plugin.h comment. */
uint64_t dpa_plugin_read_producer_idx(void)
{
	if (!g_ctx.ring)
		return 0;
	return __atomic_load_n(&g_ctx.ring->producer_idx, __ATOMIC_ACQUIRE);
}

/* S3 SAPS-single path-score accessor — hot path (called per IO by
 * bdev_nvme_find_io_path). Must be branchless-friendly + noinline-free. */
/* S3 multipath ctrlr→slot map. See header comment on dpa_plugin_ctrlr_to_path_idx.
 * First-come first-served: first distinct ctrlr pointer gets slot 0, etc.
 * Lock-free via CAS on a small fixed array (DPA_PLUGIN_PATH_MAX=8 entries). */
static _Atomic(const void *) g_dpa_ctrlr_slots[DPA_PLUGIN_PATH_MAX];

/* Generation counter for the ctrlr→slot map. Bumped by
 * dpa_plugin_ctrlr_release() whenever a slot is tombstoned (ctrlr detach), so
 * every per-thread cache can detect that its entries may be stale and discard
 * them. Without this, a detached ctrlr's slot — and its accumulated health /
 * score / path_idx state — would be silently inherited by a reused ctrlr
 * pointer at the same address. */
static _Atomic uint32_t g_dpa_ctrlr_gen;

/* Slice 9.6 Fix 1 — per-thread direct-mapped cache for ctrlr→path_idx lookup.
 *
 * Slice 9.5 profile shows dpa_plugin_ctrlr_to_path_idx accounts for ~0.29 %
 * cycles + drives downstream icache pressure (linear-scan + atomic acquires
 * per IO submit/complete + 3× per select_io_path Pass 1). With ≤ 8 distinct
 * ctrlr pointers in the BF3 cluster, an 8-entry direct-mapped cache hits ~100 %
 * after warmup.
 *
 * Threading: SPDK reactors are single-threaded per core, so __thread storage
 * is naturally race-free. Atomic global slot table remains authoritative — the
 * cache is a fast-path filter only. On miss we fall through to the original
 * atomic scan, then update cache before returning.
 *
 * Invalidation: when a path is detached + re-attached, the SAME ctrlr pointer
 * can be reused. Because g_dpa_ctrlr_slots is the source of truth (not the
 * cache), and we update the cache only after consulting it, a stale cache
 * entry can return the wrong slot only if the ctrlr pointer was reassigned
 * to a *different* ctrlr struct in between. SPDK ctrlr structs are not pooled
 * back into pointer-aliasing reuse during a bdevperf run, so this is safe.
 *
 * Sentinel: cache[*].ctrlr == NULL means slot is empty (untouched on warmup
 * or after thread re-entry). Cache size is bounded by DPA_PLUGIN_PATH_MAX.
 */
struct dpa_ctrlr_cache_entry {
	const void *ctrlr;
	uint16_t    path_idx;
};
static __thread struct dpa_ctrlr_cache_entry g_dpa_ctrlr_cache[DPA_PLUGIN_PATH_MAX];
/* Generation this thread's cache was last validated against. On a mismatch
 * with the global g_dpa_ctrlr_gen the cache is flushed before use, so a
 * tombstoned/reused ctrlr pointer can never return a stale slot. */
static __thread uint32_t g_dpa_ctrlr_cache_gen;

/* Tombstone every slot owned by `ctrlr` and bump the generation so all
 * per-thread caches invalidate. Called from the SPDK ctrlr-destruct hook
 * (nvme_rdma_ctrlr_destruct) so a reused ctrlr pointer starts from a clean
 * slot instead of inheriting the dead path's health/score/path_idx state. */
void dpa_plugin_ctrlr_release(const void *ctrlr)
{
	if (!ctrlr)
		return;
	bool tombstoned = false;
	for (uint16_t i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		const void *cur = __atomic_load_n(&g_dpa_ctrlr_slots[i],
						  __ATOMIC_ACQUIRE);
		if (cur == ctrlr) {
			const void *expected = ctrlr;
			if (__atomic_compare_exchange_n(&g_dpa_ctrlr_slots[i],
							&expected, NULL, false,
							__ATOMIC_ACQ_REL,
							__ATOMIC_ACQUIRE))
				tombstoned = true;
		}
	}
	if (tombstoned)
		__atomic_fetch_add(&g_dpa_ctrlr_gen, 1u, __ATOMIC_ACQ_REL);
}

uint16_t dpa_plugin_ctrlr_to_path_idx(const void *ctrlr)
{
	if (!ctrlr) {
		return 0;
	}

	/* Generation check: if a release tombstoned any slot since this thread
	 * last looked, flush the whole per-thread cache so we re-resolve against
	 * the authoritative slot table below. */
	uint32_t gen = __atomic_load_n(&g_dpa_ctrlr_gen, __ATOMIC_ACQUIRE);
	if (__builtin_expect(g_dpa_ctrlr_cache_gen != gen, 0)) {
		for (uint16_t i = 0; i < DPA_PLUGIN_PATH_MAX; i++)
			g_dpa_ctrlr_cache[i].ctrlr = NULL;
		g_dpa_ctrlr_cache_gen = gen;
	}

	/* Fast path: per-thread cache scan, no atomics. */
	for (uint16_t i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		if (__builtin_expect(g_dpa_ctrlr_cache[i].ctrlr == ctrlr, 1)) {
			return g_dpa_ctrlr_cache[i].path_idx;
		}
	}

	/* Slow path: consult global atomic slot table. */
	uint16_t resolved_idx = DPA_PLUGIN_PATH_MAX - 1;
	for (uint16_t i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		const void *cur = __atomic_load_n(&g_dpa_ctrlr_slots[i], __ATOMIC_ACQUIRE);
		if (cur == ctrlr) {
			resolved_idx = i;
			goto cache_update;
		}
		if (cur == NULL) {
			const void *expected = NULL;
			if (__atomic_compare_exchange_n(&g_dpa_ctrlr_slots[i], &expected, ctrlr,
							false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
				resolved_idx = i;
				goto cache_update;
			}
			/* Lost the CAS — another thread claimed this slot. Re-check. */
			if (expected == ctrlr) {
				resolved_idx = i;
				goto cache_update;
			}
			/* Different ctrlr landed here; fall through to next slot. */
		}
	}
	/* Overflow — reuse last slot. resolved_idx already = MAX-1. */

cache_update:
	/* Insert into first empty cache slot, or replace slot 0. */
	for (uint16_t i = 0; i < DPA_PLUGIN_PATH_MAX; i++) {
		if (g_dpa_ctrlr_cache[i].ctrlr == NULL) {
			g_dpa_ctrlr_cache[i].ctrlr = ctrlr;
			g_dpa_ctrlr_cache[i].path_idx = resolved_idx;
			return resolved_idx;
		}
	}
	/* All cache slots full — overwrite slot 0. This is rare because the
	 * cache dimension matches the supported controller count. */
	g_dpa_ctrlr_cache[0].ctrlr = ctrlr;
	g_dpa_ctrlr_cache[0].path_idx = resolved_idx;
	return resolved_idx;
}

/* SAPS-Q rewrites notify-entry qp_id low bits to the process-local tenant ID
 * before the DPA consumes an event.  Path verdict readers must use that same
 * row.  SPDK qpair IDs are per-controller (normally 1 on every path), so
 * indexing these tables with the raw qpair ID reads another tenant's row and
 * makes a valid quarantine invisible to the selector. */
static inline uint32_t
dpa_plugin_path_qp_slot(uint16_t qp_id)
{
	if (g_ctx.ring && g_ctx.ring->sapsq_enabled &&
	    g_sapsq_local_tenant_id < SAPSQ_MAX_TENANTS)
		return g_sapsq_local_tenant_id & DPA_PLUGIN_CONN_MASK;
	return (uint32_t)qp_id & DPA_PLUGIN_CONN_MASK;
}

uint32_t dpa_plugin_read_path_score(uint16_t qp_id, uint16_t path_id)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return 0;
	uint32_t qp_slot = dpa_plugin_path_qp_slot(qp_id);
	uint32_t path_slot = (uint32_t)path_id & DPA_PLUGIN_PATH_MASK;
	return g_ctx.ring->per_qp_path_score[qp_slot][path_slot];
}

/* Slice 34 — per-opcode score accessor. opcode_class: 0=READ, 1=WRITE, 2=FLUSH. */
uint32_t dpa_plugin_read_path_score_opcode(uint16_t qp_id, uint16_t path_id,
					   uint8_t opcode_class)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return 0;
	if (opcode_class >= DPA_PLUGIN_OPC_CLASS_MAX)
		return 0;
	uint32_t qp_slot   = dpa_plugin_path_qp_slot(qp_id);
	uint32_t path_slot = (uint32_t)path_id  & DPA_PLUGIN_PATH_MASK;
	return g_ctx.ring->per_qp_path_score_opcode[qp_slot][path_slot][opcode_class];
}

/* Slice 34 — raw per-opcode P99 accessor. opcode_class: 0=READ, 1=WRITE, 2=FLUSH.
 * Returns per_qp_path_opc_p99[qp_slot][path_slot][opcode_class] from shared mem.
 * Returns 0 on warmup / not initialised. */
uint32_t dpa_plugin_read_path_opc_p99(uint16_t qp_id, uint16_t path_id,
				       uint8_t opcode_class)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return 0;
	if (opcode_class >= DPA_PLUGIN_OPC_CLASS_MAX)
		return 0;
	uint32_t qp_slot   = dpa_plugin_path_qp_slot(qp_id);
	uint32_t path_slot = (uint32_t)path_id  & DPA_PLUGIN_PATH_MASK;
	return g_ctx.ring->per_qp_path_opc_p99[qp_slot][path_slot][opcode_class];
}

int dpa_plugin_read_path_snapshot(uint16_t qp_id, uint16_t path_id,
				  uint8_t opcode_class, uint8_t use_opcode_score,
				  uint8_t need_capacity,
				  struct dpa_plugin_path_snapshot *out)
{
	if (__builtin_expect(out == NULL, 0))
		return 0;
	out->score = 0;
	out->opcode_score = 0;
	out->opc_p99 = 0;
	out->capacity = 0;
	out->fault_type = 0;
	out->state = DPA_SAPS_STATE_HEALTHY;
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return 0;

	uint32_t qp_slot = dpa_plugin_path_qp_slot(qp_id);
	uint32_t path_slot = (uint32_t)path_id & DPA_PLUGIN_PATH_MASK;

	if (use_opcode_score && opcode_class < DPA_PLUGIN_OPC_CLASS_MAX) {
		out->opcode_score =
			g_ctx.ring->per_qp_path_score_opcode[qp_slot][path_slot][opcode_class];
		out->opc_p99 =
			g_ctx.ring->per_qp_path_opc_p99[qp_slot][path_slot][opcode_class];
	}
	out->score = g_ctx.ring->per_qp_path_score[qp_slot][path_slot];
	out->fault_type = g_ctx.ring->per_qp_path_fault_type[qp_slot][path_slot];
	out->state = g_ctx.ring->per_qp_path_state[qp_slot][path_slot];
	if (need_capacity || out->fault_type == DPA_PLUGIN_FAULT_PROPORTIONAL_THROTTLE) {
		out->capacity = g_ctx.ring->per_qp_path_capacity[qp_slot][path_slot];
	}
	return 1;
}

/* Slice 9.6 Fix 2 — see header comment in dpa_plugin.h. */
void dpa_plugin_prefetch_path_score_row(uint16_t qp_id)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return;
	uint32_t qp_slot = dpa_plugin_path_qp_slot(qp_id);
	__builtin_prefetch((const void *)&g_ctx.ring->per_qp_path_score[qp_slot][0],
			   0 /* read */, 3 /* high temporal locality */);
	__builtin_prefetch((const void *)&g_ctx.ring->per_qp_path_score_opcode[qp_slot][0][0],
			   0 /* read */, 2 /* medium temporal locality */);
	__builtin_prefetch((const void *)&g_ctx.ring->per_qp_path_fault_type[qp_slot][0],
			   0 /* read */, 2 /* medium temporal locality */);
	__builtin_prefetch((const void *)&g_ctx.ring->per_qp_path_capacity[qp_slot][0],
			   0 /* read */, 1 /* low temporal locality */);
}

/* v2 SAPS retry verdict accessor — hot path, called from
 * bdev_nvme_check_retry_io() on each non-success completion. */
uint8_t dpa_plugin_read_retry_verdict(uint16_t qp_id, uint16_t path_id)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return DPA_SAPS_ACTION_UNSET;
	uint32_t qp_slot = dpa_plugin_path_qp_slot(qp_id);
	uint32_t path_slot = (uint32_t)path_id & DPA_PLUGIN_PATH_MASK;
	return g_ctx.ring->per_qp_path_retry_verdict[qp_slot][path_slot];
}

/* Slice 1 — accessor for the independent retry-verdict overlay toggle.
 * Returns 0 when plugin not initialised (g_retry_overlay_enabled is BSS-zero
 * before init runs). */
int dpa_plugin_retry_overlay_enabled(void)
{
	return g_retry_overlay_enabled;
}

/* v2 SAPS path state accessor — non-hot path, used for monitoring and smoke
 * tests (e.g. assert state==HEALTHY after a clean run). */
uint16_t dpa_plugin_read_path_state(uint16_t qp_id, uint16_t path_id)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return DPA_SAPS_STATE_HEALTHY;
	uint32_t qp_slot = dpa_plugin_path_qp_slot(qp_id);
	uint32_t path_slot = (uint32_t)path_id & DPA_PLUGIN_PATH_MASK;
	return g_ctx.ring->per_qp_path_state[qp_slot][path_slot];
}

/* Slice 19 — fault_type accessor. Host bdev_nvme.c selector uses this in
 * the SAPS_SCORE_MIN handler to pick idle-bypass weight: high-confidence
 * drain (PERPETUAL_SLOW / SPARSE_ERROR / BIMODAL_TAIL) gets a small
 * weight, while HEALTHY / FLAP get the full recovery weight. */
uint16_t dpa_plugin_read_path_fault_type(uint16_t qp_id, uint16_t path_id)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return 0;  /* HEALTHY = 0 by enum saps_fault_type */
	uint32_t qp_slot = dpa_plugin_path_qp_slot(qp_id);
	uint32_t path_slot = (uint32_t)path_id & DPA_PLUGIN_PATH_MASK;
	return g_ctx.ring->per_qp_path_fault_type[qp_slot][path_slot];
}

uint32_t dpa_plugin_read_path_capacity(uint16_t qp_id, uint16_t path_id)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return 0;
	uint32_t qp_slot = dpa_plugin_path_qp_slot(qp_id);
	uint32_t path_slot = (uint32_t)path_id & DPA_PLUGIN_PATH_MASK;
	return g_ctx.ring->per_qp_path_capacity[qp_slot][path_slot];
}

uint32_t dpa_plugin_read_qp_fault_mask(uint16_t qp_id)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return 0;
	uint32_t qp_slot = dpa_plugin_path_qp_slot(qp_id);
	return g_ctx.ring->per_qp_fault_mask[qp_slot][0];
}

void dpa_plugin_note_healthy_full_bypass(void)
{
	if (__builtin_expect(!g_ctx.ring || !g_notify_enabled, 0))
		return;
	__atomic_fetch_add(&g_ctx.ring->saps_healthy_full_bypass_count, 1, __ATOMIC_RELAXED);
}

/* SAPS-Q active query (Bug D path-routing fix, 2026-05-24):
 * When SAPS-Q is enabled, saps_try_healthy_bypass must not cache path 0 —
 * doing so prevents traffic distribution across all 3 paths per tenant and
 * limits total IOPS to ~1/3 of allocated rate. */
int dpa_plugin_sapsq_active(void)
{
	return g_ctx.ring && g_ctx.ring->sapsq_enabled;
}

/* ----- Teardown ----- */

void dpa_plugin_shutdown(void)
{
	/* g_batch_buf / g_batch_count are __thread (multi-reactor fix): this
	 * shutdown flush only drains THIS thread's residual buffer.  Other
	 * reactor threads' residue of <DPA_PLUGIN_BATCH_SIZE (64) events at
	 * shutdown is acceptable (lost only at teardown, never during steady
	 * state where every full batch is flushed). */
	if (g_notify_enabled && g_batch_count > 0 && g_ctx.ring)
		dpa_plugin_flush_batch();
	g_notify_enabled = 0;

	/* Tenant: only unmap + close memfd. Coordinator owns everything else. */
	if (g_ctx.role == DPA_ROLE_TENANT) {
		/* M3 snapshot dumper 先 stop;後面 munmap ring 會讓 thread 讀到 garbage */
		m3_snapshot_stop(&g_ctx);
		/* Bug C (2026-05-24): SAPS-Q exit stats — MUST be read BEFORE munmap. */
		if (g_ctx.ring && g_ctx.ring->sapsq_enabled) {
			uint32_t my_tid = g_ctx.ring->sapsq_my_tenant_id;
			fprintf(stderr,
				"dpa_plugin: SAPS-Q exit stats my_tid=%u "
				"stale_epoch_fallback=%lu total_epochs_consumed_at_exit=%lu "
				"last_epoch_commit=%lu\n",
				my_tid,
				(unsigned long)g_ctx.ring->sapsq_stale_epoch_fallback,
				(unsigned long)g_ctx.ring->sapsq_total_epochs_consumed,
				(unsigned long)g_ctx.ring->sapsq_epoch_commit);
			if (my_tid < SAPSQ_TENANT_MAX) {
				for (uint32_t p = 0; p < SAPSQ_PATH_MAX; p++) {
					fprintf(stderr,
						"dpa_plugin: SAPS-Q exit path[%u] admit=%lu reject=%lu probe=%lu "
						"rate_q32_last=%u tokens_q16_final=%u\n",
						p,
						(unsigned long)g_ctx.ring->sapsq_admit_count[my_tid][p],
						(unsigned long)g_ctx.ring->sapsq_reject_count[my_tid][p],
						(unsigned long)g_ctx.ring->sapsq_probe_count[my_tid][p],
						g_ctx.ring->sapsq_tenant_path_rate_q32[my_tid][p],
						g_ctx.ring->sapsq_tenant_path_tokens_q16[my_tid][p]);
				}
			}
		}
		if (g_ctx.ring) {
			munmap(g_ctx.ring, g_ctx.ring_alloc_size);
			g_ctx.ring = NULL;
		}
		if (g_ctx.ring_memfd >= 0) {
			close(g_ctx.ring_memfd);
			g_ctx.ring_memfd = -1;
		}
			fprintf(stderr,
				"dpa_plugin: tenant host stats events=%lu flushes=%lu avg_batch=%.2f\n",
				(unsigned long)g_batch_events_pushed,
				(unsigned long)g_batch_flushes,
				g_batch_flushes ? (double)g_batch_events_pushed /
						  (double)g_batch_flushes : 0.0);
			if (g_ctx.ring) {
				fprintf(stderr,
					"SAPS_SAMPLE rate=%u kept=%lu skipped=%lu force_pub_errors=%lu "
					"sample_events=%lu classify_state=%lu score_updates=%lu "
					"healthy_full_bypass=%lu\n",
					(unsigned)g_sample_rate,
					(unsigned long)g_batch_events_pushed,
					(unsigned long)g_sample_skipped,
					(unsigned long)g_sample_force_pub_errors,
					(unsigned long)g_ctx.ring->saps_sample_events,
					(unsigned long)g_ctx.ring->saps_classify_calls,
					(unsigned long)g_ctx.ring->saps_score_updates,
					(unsigned long)g_ctx.ring->saps_healthy_full_bypass_count);
			}
			g_ctx.initialized = 0;
			return;
		}

	/* Coordinator: stop UDS server first. */
	if (g_ctx.uds_thread_started) {
		g_ctx.uds_stop = 1;
		/* Closing the listen fd wakes accept() with EBADF. */
		if (g_ctx.uds_listen_fd >= 0) {
			shutdown(g_ctx.uds_listen_fd, SHUT_RDWR);
			close(g_ctx.uds_listen_fd);
			g_ctx.uds_listen_fd = -1;
		}
		pthread_join(g_ctx.uds_thread, NULL);
		g_ctx.uds_thread_started = 0;
		if (g_ctx.uds_path[0])
			unlink(g_ctx.uds_path);
	}

	if (g_ctx.sampler_started) {
		g_ctx.sampler_stop = 1;
		pthread_join(g_ctx.sampler_thread, NULL);
		g_ctx.sampler_started = 0;
		if (g_ctx.sampler_file) {
			fclose(g_ctx.sampler_file);
			g_ctx.sampler_file = NULL;
		}
	}

	/* M2 v2 classifier thread join (2026-05-20)。stop flag 設 1 後等 10ms tick
	 * 完成自然退出。 */
	if (g_ctx.m2_cls_started) {
		g_ctx.m2_cls_stop = 1;
		pthread_join(g_ctx.m2_cls_thread, NULL);
		g_ctx.m2_cls_started = 0;
	}

	/* M3 v4 snapshot dumper stop:在 ring 釋放前 join + unlink snapshot file。 */
	m3_snapshot_stop(&g_ctx);

	fprintf(stderr,
		"dpa_plugin: host stats events=%lu flushes=%lu avg_batch=%.2f\n",
		(unsigned long)g_batch_events_pushed,
		(unsigned long)g_batch_flushes,
		g_batch_flushes ? (double)g_batch_events_pushed /
				  (double)g_batch_flushes : 0.0);

	{
		uint64_t kept = g_batch_events_pushed;
		uint64_t skipped = g_sample_skipped;
		uint64_t errs = g_sample_force_pub_errors;
		uint64_t total = kept + skipped;
		fprintf(stderr,
			"dpa_plugin: sample stats rate=%u kept=%lu skipped=%lu force_pub_errors=%lu skip_pct=%.2f%%\n",
			(unsigned)g_sample_rate,
			(unsigned long)kept,
			(unsigned long)skipped,
			(unsigned long)errs,
			total ? 100.0 * (double)skipped / (double)total : 0.0);
	}

	if (g_ctx.ring) {
		uint64_t c = g_ctx.ring->host_adm_checks;
		uint64_t t = g_ctx.ring->host_adm_throttles;
		fprintf(stderr,
			"dpa_plugin: adm stats checks=%lu throttles=%lu throttle_pct=%.3f%%\n",
			(unsigned long)c, (unsigned long)t,
			c ? 100.0 * (double)t / (double)c : 0.0);
		/* SAPS-Q M-series coordinator exit stats.
		 * Orchestrator dump parser looks for:
		 *   epoch_commit_seq[=:](\d+)    (from sapsq_scheduler_tick)
		 *   sapsq_stale_epoch_fallback    (cold-start bypass counter)
		 *   sapsq_demand_iops[t]          (DPA EWMA demand per tenant) */
		if (g_ctx.ring->sapsq_enabled) {
			fprintf(stderr,
				"dpa_plugin: SAPS-Q coordinator exit "
				"epoch_commit_seq=%u sapsq_stale_epoch_fallback=%lu "
				"sapsq_epoch_commit=%lu\n",
				g_ctx.ring->sapsq_epoch_commit_seq,
				(unsigned long)g_ctx.ring->sapsq_stale_epoch_fallback,
				(unsigned long)g_ctx.ring->sapsq_epoch_commit);
			fprintf(stderr,
				"dpa_plugin: SAPS-Q ring producer=%lu consumer=%lu "
				"dpa_consumed=%lu overrun=%lu max_lag=%lu\n",
				(unsigned long)g_ctx.ring->producer_idx,
				(unsigned long)g_ctx.ring->consumer_idx,
				(unsigned long)g_ctx.ring->dpa_consumed,
				(unsigned long)g_ctx.ring->ring_overrun_count,
				(unsigned long)g_ctx.ring->ring_max_lag);
			for (uint32_t _t = 0; _t < g_ctx.ring->sapsq_num_tenants; _t++) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q attribution tenant=%u "
					"host_published=%lu dpa_consumed=%lu\n",
					_t,
					(unsigned long)g_ctx.ring->host_submit_published[_t],
					(unsigned long)g_ctx.ring->
						dpa_submit_consumed_by_tenant[_t]);
			}
			for (uint32_t _p = 0; _p < g_ctx.ring->sapsq_num_paths; _p++) {
				fprintf(stderr,
					"dpa_plugin: SAPS-Q path %u capacity_iops=%lu "
					"health_factor_q16=%u effective_capacity_iops=%lu\n",
					_p,
					(unsigned long)g_ctx.ring->sapsq_path_capacity_iops[_p],
					g_ctx.ring->sapsq_committed_path_health_factor_q16[_p],
					(unsigned long)g_ctx.ring->
						sapsq_committed_path_effective_capacity_iops[_p]);
			}
			for (uint32_t _t = 0; _t < SAPSQ_MAX_TENANTS; _t++) {
				if (g_ctx.ring->sapsq_tenant_weight[_t] == 0)
					continue;
				fprintf(stderr,
					"dpa_plugin: SAPS-Q tenant %u weight=%u "
					"demand_iops=%u\n",
					_t, g_ctx.ring->sapsq_tenant_weight[_t],
					g_ctx.ring->sapsq_demand_iops[_t]);
				for (uint32_t _p = 0; _p < g_ctx.ring->sapsq_num_paths; _p++) {
					fprintf(stderr,
						"dpa_plugin: SAPS-Q tenant %u path %u "
						"rate_budget_q32=%u admit=%lu reject=%lu\n",
						_t, _p,
						g_ctx.ring->sapsq_tenant_path_rate_budget_q32[_t][_p],
						(unsigned long)g_ctx.ring->sapsq_tenant_path_admit_count[_t][_p],
						(unsigned long)g_ctx.ring->sapsq_tenant_path_reject_count[_t][_p]);
				}
			}
		}
		/* M5 DRR diagnostics */
		if (g_ctx.ring->m5_drr_enabled) {
			fprintf(stderr,
				"dpa_plugin: M5 DRR rounds=%lu\n",
				(unsigned long)g_ctx.ring->m5_drr_round_count);
			for (uint32_t _t = 0; _t < 4; _t++) {
				fprintf(stderr,
					"dpa_plugin: M5 tenant %u grant=%lu consumed=%lu "
					"reject=%lu idle=%lu wc=%lu\n",
					_t,
					(unsigned long)g_ctx.ring->m5_tenant_grant_count[_t],
					(unsigned long)g_ctx.ring->m5_tenant_consumed_count[_t],
					(unsigned long)g_ctx.ring->m5_tenant_reject_count[_t],
					(unsigned long)g_ctx.ring->m5_tenant_idle_rounds[_t],
					(unsigned long)g_ctx.ring->m5_tenant_work_conserved[_t]);
			}
		}
	}

	if (g_ctx.rpc_thread_started) {
		if (g_ctx.ring) {
			g_ctx.ring->stop = 1;
			__atomic_thread_fence(__ATOMIC_SEQ_CST);
		}
		pthread_join(g_ctx.rpc_thread, NULL);
		g_ctx.rpc_thread_started = 0;
		fprintf(stderr, "dpa_plugin: DPA rpc final status=%d ret=0x%lx renewals=%d\n",
			g_ctx.rpc_status,
			(unsigned long)g_ctx.rpc_ret,
			g_ctx.rpc_renewals);
			if (g_ctx.ring) {
				__atomic_thread_fence(__ATOMIC_ACQUIRE);
				fprintf(stderr, "dpa_plugin: DPA consumed=%lu\n",
					(unsigned long)g_ctx.ring->dpa_consumed);
				fprintf(stderr,
					"SAPS_SAMPLE rate=%u kept=%lu skipped=%lu force_pub_errors=%lu "
					"sample_events=%lu classify_state=%lu score_updates=%lu "
					"healthy_full_bypass=%lu\n",
					(unsigned)g_sample_rate,
					(unsigned long)g_batch_events_pushed,
					(unsigned long)g_sample_skipped,
					(unsigned long)g_sample_force_pub_errors,
					(unsigned long)g_ctx.ring->saps_sample_events,
					(unsigned long)g_ctx.ring->saps_classify_calls,
					(unsigned long)g_ctx.ring->saps_score_updates,
					(unsigned long)g_ctx.ring->saps_healthy_full_bypass_count);
				/* D4 trace counter dump (2026-05-17). Per-path FSM dwell +
				 * fault_type histogram + transition matrix. Parse:
				 *   D4_TRACE path=<i> state_count=H:%lu D:%lu E:%lu R:%lu
				 *   D4_TRACE path=<i> fault_count=0:%lu 1:%lu ... 9:%lu
				 *   D4_TRACE path=<i> transitions from=<j> to=H:%lu D:%lu E:%lu R:%lu
				 * Where state buckets H=HEALTHY D=DEGRADING E=EXCLUDED R=RECOVERING. */
				for (unsigned pi = 0; pi < DPA_PLUGIN_PATH_MAX; pi++) {
					fprintf(stderr,
						"D4_TRACE path=%u state_count H=%lu D=%lu E=%lu R=%lu\n",
						pi,
						(unsigned long)g_ctx.ring->d4_state_count[pi][0],
						(unsigned long)g_ctx.ring->d4_state_count[pi][1],
						(unsigned long)g_ctx.ring->d4_state_count[pi][2],
						(unsigned long)g_ctx.ring->d4_state_count[pi][3]);
					fprintf(stderr,
						"D4_TRACE path=%u fault_count 0=%lu 1=%lu 2=%lu 3=%lu 4=%lu 5=%lu 6=%lu 7=%lu 8=%lu 9=%lu\n",
						pi,
						(unsigned long)g_ctx.ring->d4_fault_count[pi][0],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][1],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][2],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][3],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][4],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][5],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][6],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][7],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][8],
						(unsigned long)g_ctx.ring->d4_fault_count[pi][9]);
					fprintf(stderr,
						"D4_TRACE path=%u raw_fault 0=%lu 1=%lu 2=%lu 3=%lu 4=%lu 5=%lu 6=%lu 7=%lu 8=%lu 9=%lu\n",
						pi,
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][0],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][1],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][2],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][3],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][4],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][5],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][6],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][7],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][8],
						(unsigned long)g_ctx.ring->d4_raw_fault_count[pi][9]);
					for (unsigned fr = 0; fr < 4; fr++) {
						fprintf(stderr,
							"D4_TRACE path=%u transitions from=%u to H=%lu D=%lu E=%lu R=%lu\n",
							pi, fr,
							(unsigned long)g_ctx.ring->d4_state_transitions[pi][fr][0],
							(unsigned long)g_ctx.ring->d4_state_transitions[pi][fr][1],
							(unsigned long)g_ctx.ring->d4_state_transitions[pi][fr][2],
							(unsigned long)g_ctx.ring->d4_state_transitions[pi][fr][3]);
					}
					fprintf(stderr,
						"D4_TRACE path=%u classifier_state n=%lu max_log=%ld mean_log=%ld base_log=%ld "
						"tail_cnt=%u bimodal_consec=%u recent_samp=%u err_rate=%ld recent_err=%u\n",
						pi,
						(unsigned long)g_ctx.ring->d4_last_n[pi],
						(long)g_ctx.ring->d4_last_max_log_lat[pi],
						(long)g_ctx.ring->d4_last_mean_log_lat[pi],
						(long)g_ctx.ring->d4_last_baseline_log_lat[pi],
						g_ctx.ring->d4_last_tail_count[pi],
						g_ctx.ring->d4_last_bimodal_consec[pi],
						g_ctx.ring->d4_last_recent_sample[pi],
						(long)g_ctx.ring->d4_last_err_rate_ewma[pi],
						g_ctx.ring->d4_last_recent_err_count[pi]);
				}
			}
		}

	if (g_ctx.transfer_daddr) {
		flexio_buf_dev_free(g_ctx.process, g_ctx.transfer_daddr);
		g_ctx.transfer_daddr = 0;
	}
	if (g_ctx.window)  { flexio_window_destroy(g_ctx.window);    g_ctx.window = NULL; }
	if (g_ctx.mr)      { ibv_dereg_mr(g_ctx.mr);                 g_ctx.mr = NULL; }
	if (g_ctx.ring) {
		if (g_ctx.role == DPA_ROLE_COORDINATOR) {
			munmap(g_ctx.ring, g_ctx.ring_alloc_size);
		} else {
			free(g_ctx.ring);
		}
		g_ctx.ring = NULL;
	}
	if (g_ctx.ring_memfd >= 0) {
		close(g_ctx.ring_memfd);
		g_ctx.ring_memfd = -1;
	}
	if (g_ctx.stream)  { flexio_msg_stream_destroy(g_ctx.stream); g_ctx.stream = NULL; }
	if (g_ctx.process) { flexio_process_destroy(g_ctx.process);  g_ctx.process = NULL; }
	if (g_ctx.ibv_ctx) { ibv_close_device(g_ctx.ibv_ctx);        g_ctx.ibv_ctx = NULL; }
	if (g_ctx.dev_list){ ibv_free_device_list(g_ctx.dev_list);   g_ctx.dev_list = NULL; }

	g_ctx.initialized = 0;
}
