/*
 * sapsq_dump.c — read-only SAPS counter dumper
 *
 * Usage:   sudo sapsq_dump /proc/<pid>/fd/<n>
 * Output:  JSON to stdout — contains the active M-series budget plane,
 *          per-(tenant, path) enforcement counters, path health, and the
 *          older compatibility plane for diagnosis.
 *
 * Build:
 *   gcc -O2 -Wall -I../dpa-smart-initiator/flexio_build/samples/dpa_plugin \
 *       -o sapsq_dump sapsq_dump.c
 *
 * 為什麼用 /proc/<pid>/fd/<n>:
 *   dpa_plugin 用 memfd_create("dpa_plugin_ring", ...) 在 standalone proc 內
 *   建 ring,memfd 不是 file-backed,外部無法直接 open()。但 Linux procfs 把
 *   memfd 暴露成 /proc/<pid>/fd/<n>(target = "/memfd:dpa_plugin_ring (deleted)"),
 *   sudo 對它做 open() + mmap() PROT_READ + MAP_SHARED 可以讀同一塊 anonymous
 *   shared memory page。tenant bdevperf proc 用 PROT_READ|PROT_WRITE 寫,本
 *   helper 用 PROT_READ 讀,不會競爭。
 *
 * NOTE: 不要寫 ring(只讀)。bdevperf hot path 仍在跑時讀 counter 是 best-effort
 * snapshot,counter 是 uint64 atomic-relaxed counter,讀到 torn value 機率極低
 * (aarch64 8-byte align natural load is atomic)。
 */

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "dpa_plugin_com.h"

int main(int argc, char **argv)
{
	if (argc != 2) {
		fprintf(stderr, "usage: %s <memfd-procfs-path>\n", argv[0]);
		return 1;
	}

	int fd = open(argv[1], O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "open(%s): %s\n", argv[1], strerror(errno));
		return 2;
	}

	struct stat st;
	if (fstat(fd, &st) < 0) {
		fprintf(stderr, "fstat: %s\n", strerror(errno));
		close(fd);
		return 3;
	}
	size_t sz = (size_t)st.st_size;
	if (sz < sizeof(struct dpa_plugin_shared)) {
		fprintf(stderr, "ring size %zu < expected %zu (header mismatch?)\n",
			sz, sizeof(struct dpa_plugin_shared));
		close(fd);
		return 4;
	}

	void *addr = mmap(NULL, sz, PROT_READ, MAP_SHARED, fd, 0);
	if (addr == MAP_FAILED) {
		fprintf(stderr, "mmap: %s\n", strerror(errno));
		close(fd);
		return 5;
	}

	const struct dpa_plugin_shared *shared =
		(const struct dpa_plugin_shared *)addr;
	struct dpa_plugin_shared *snapshot = malloc(sizeof(*snapshot));
	if (snapshot == NULL) {
		fprintf(stderr, "malloc snapshot: %s\n", strerror(errno));
		munmap(addr, sz);
		close(fd);
		return 6;
	}
	memset(snapshot, 0, sizeof(*snapshot));

	/*
	 * The completion ring and per-QP classifier tables precede the SAPS-Q
	 * control state and account for almost all of this structure. Copying the
	 * entire mapping made a coherent snapshot unlikely while the scheduler
	 * committed epochs at high I/O rates. The dumper only reports fields from
	 * the SAPS-Q control region, plus a small set of monotonic ring counters.
	 */
	const size_t control_offset =
		offsetof(struct dpa_plugin_shared, sapsq_enabled);
	const size_t control_size = sizeof(*snapshot) - control_offset;
	int stable = 0;
	for (int retry = 0; retry < 100; retry++) {
		uint32_t commit_before = __atomic_load_n(
			&shared->sapsq_epoch_commit_seq, __ATOMIC_ACQUIRE);
		uint32_t epoch_before = __atomic_load_n(
			&shared->sapsq_epoch_seq, __ATOMIC_ACQUIRE);
		if (commit_before != epoch_before) {
			usleep(50);
			continue;
		}
		memcpy((char *)snapshot + control_offset,
		       (const char *)shared + control_offset,
		       control_size);
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		uint32_t commit_after = __atomic_load_n(
			&shared->sapsq_epoch_commit_seq, __ATOMIC_ACQUIRE);
		uint32_t epoch_after = __atomic_load_n(
			&shared->sapsq_epoch_seq, __ATOMIC_ACQUIRE);
		if (commit_before == commit_after &&
		    epoch_before == epoch_after &&
		    snapshot->sapsq_epoch_commit_seq ==
			    snapshot->sapsq_epoch_seq &&
		    snapshot->sapsq_epoch_commit_seq == commit_after) {
			stable = 1;
			break;
		}
		usleep(50);
	}
	if (!stable) {
		fprintf(stderr, "could not capture a stable committed epoch\n");
		free(snapshot);
		munmap(addr, sz);
		close(fd);
		return 7;
	}

	snapshot->producer_idx = __atomic_load_n(
		&shared->producer_idx, __ATOMIC_RELAXED);
	snapshot->consumer_idx = __atomic_load_n(
		&shared->consumer_idx, __ATOMIC_RELAXED);
	snapshot->ring_overrun_count = __atomic_load_n(
		&shared->ring_overrun_count, __ATOMIC_RELAXED);
	snapshot->ring_max_lag = __atomic_load_n(
		&shared->ring_max_lag, __ATOMIC_RELAXED);
	snapshot->dpa_consumed = __atomic_load_n(
		&shared->dpa_consumed, __ATOMIC_RELAXED);
	for (unsigned t = 0; t < SAPSQ_MAX_TENANTS; t++) {
		snapshot->host_submit_published[t] = __atomic_load_n(
			&shared->host_submit_published[t], __ATOMIC_RELAXED);
		snapshot->dpa_submit_consumed_by_tenant[t] = __atomic_load_n(
			&shared->dpa_submit_consumed_by_tenant[t],
			__ATOMIC_RELAXED);
	}

	const struct dpa_plugin_shared *ring = snapshot;

	/* Runtime dimensions for the SAPSQ_MAX_* budget plane (num_*, clamped to
	 * declared array bounds so a bogus ring value cannot over-read). */
	unsigned num_paths = (unsigned)ring->sapsq_num_paths;
	if (num_paths > SAPSQ_MAX_PATHS)
		num_paths = SAPSQ_MAX_PATHS;
	unsigned num_tenants = (unsigned)ring->sapsq_num_tenants;
	if (num_tenants > SAPSQ_MAX_TENANTS)
		num_tenants = SAPSQ_MAX_TENANTS;

	/* Emit JSON. Arrays sized to schema:
	 *   admit_count, reject_count, probe_count  [SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX]
	 *   path_health_q16, path_eligibility       [SAPSQ_PATH_MAX]
	 */
	printf("{\n");
	printf("  \"enabled\": %u,\n", (unsigned)ring->sapsq_enabled);
	printf("  \"bypass_health_coupling\": %u,\n",
		(unsigned)(ring->sapsq_bypass_health_coupling ==
			   SAPSQ_HEALTH_COUPLING_FIXED));
	printf("  \"health_coupling_mode\": %u,\n",
		(unsigned)ring->sapsq_bypass_health_coupling);
	printf("  \"health_source\": %u,\n",
		(unsigned)ring->sapsq_health_source);
	printf("  \"my_tenant_id\": %u,\n", (unsigned)ring->sapsq_my_tenant_id);
	printf("  \"num_tenants\": %u,\n", num_tenants);
	printf("  \"num_paths\": %u,\n", num_paths);
	printf("  \"link_cap_iops\": %lu,\n",
		(unsigned long)ring->sapsq_link_cap_iops);
	printf("  \"host_tsc_freq\": %lu,\n",
		(unsigned long)ring->sapsq_host_tsc_freq);
	printf("  \"probe_rate_budget_q32\": %u,\n",
		(unsigned)ring->sapsq_probe_rate_budget_q32);
	printf("  \"probe_rate_budget_iops\": %lu,\n",
		(unsigned long)(
			((uint64_t)ring->sapsq_probe_rate_budget_q32 *
			 ring->sapsq_host_tsc_freq) >> 32));
	printf("  \"path_capacity_iops\": [");
	for (unsigned p = 0; p < num_paths; p++) {
		printf("%s%lu", p ? "," : "",
			(unsigned long)ring->sapsq_path_capacity_iops[p]);
	}
	printf("],\n");
	printf("  \"path_effective_capacity_iops\": [");
	uint64_t live_effective_capacity_sum = 0;
	for (unsigned p = 0; p < num_paths; p++) {
		uint64_t effective =
			(ring->sapsq_path_capacity_iops[p] *
			 (uint64_t)ring->sapsq_path_health_factor_q16[p]) >> 16;
		live_effective_capacity_sum += effective;
		printf("%s%lu", p ? "," : "", (unsigned long)effective);
	}
	printf("],\n");
	printf("  \"committed_path_health_factor_q16\": [");
	for (unsigned p = 0; p < num_paths; p++) {
		printf("%s%u", p ? "," : "",
			(unsigned)ring->sapsq_committed_path_health_factor_q16[p]);
	}
	printf("],\n");
	uint64_t committed_effective_capacity_sum = 0;
	printf("  \"committed_path_effective_capacity_iops\": [");
	for (unsigned p = 0; p < num_paths; p++) {
		committed_effective_capacity_sum +=
			ring->sapsq_committed_path_effective_capacity_iops[p];
		printf("%s%lu", p ? "," : "",
			(unsigned long)
				ring->sapsq_committed_path_effective_capacity_iops[p]);
	}
	printf("],\n");
	uint64_t admitted_capacity = committed_effective_capacity_sum;
	if (ring->sapsq_bypass_health_coupling ==
	    SAPSQ_HEALTH_COUPLING_FIXED)
		admitted_capacity = ring->sapsq_link_cap_iops;
	else if (admitted_capacity > ring->sapsq_link_cap_iops)
		admitted_capacity = ring->sapsq_link_cap_iops;
	printf("  \"live_available_path_capacity_iops\": %lu,\n",
		(unsigned long)live_effective_capacity_sum);
	printf("  \"available_path_capacity_iops\": %lu,\n",
		(unsigned long)committed_effective_capacity_sum);
	printf("  \"admitted_capacity_iops\": %lu,\n",
		(unsigned long)admitted_capacity);
	printf("  \"epoch_seq\": %u,\n", (unsigned)ring->sapsq_epoch_seq);
	printf("  \"epoch_commit_seq\": %u,\n",
		(unsigned)ring->sapsq_epoch_commit_seq);
	printf("  \"epoch\": %lu,\n", (unsigned long)ring->sapsq_epoch);
	printf("  \"epoch_commit\": %lu,\n",
		(unsigned long)ring->sapsq_epoch_commit);
	printf("  \"last_epoch_tsc\": %lu,\n",
		(unsigned long)ring->sapsq_last_epoch_tsc);
	printf("  \"stale_epoch_fallback\": %lu,\n",
		(unsigned long)ring->sapsq_stale_epoch_fallback);
	printf("  \"total_epochs_consumed\": %lu,\n",
		(unsigned long)ring->sapsq_total_epochs_consumed);
	printf("  \"producer_idx\": %lu,\n",
		(unsigned long)ring->producer_idx);
	printf("  \"consumer_idx\": %lu,\n",
		(unsigned long)ring->consumer_idx);
	printf("  \"ring_overrun_count\": %lu,\n",
		(unsigned long)ring->ring_overrun_count);
	printf("  \"ring_max_lag\": %lu,\n",
		(unsigned long)ring->ring_max_lag);
	printf("  \"dpa_consumed\": %lu,\n",
		(unsigned long)ring->dpa_consumed);
	printf("  \"host_submit_published_by_tenant\": [");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("%s%lu", t ? "," : "",
			(unsigned long)ring->host_submit_published[t]);
	}
	printf("],\n");
	printf("  \"dpa_submit_consumed_by_tenant\": [");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("%s%lu", t ? "," : "",
			(unsigned long)ring->dpa_submit_consumed_by_tenant[t]);
	}
	printf("],\n");
	printf("  \"path_health_q16\": [");
	for (unsigned p = 0; p < SAPSQ_PATH_MAX; p++) {
		printf("%s%u", p ? "," : "",
			(unsigned)ring->sapsq_path_health_q16[p]);
	}
	printf("],\n");

	/* Continuous Q16.16 health factor (SAPSQ_MAX_PATHS plane) — smooth
	 * health-vs-magnitude signal. Indexed by runtime num_paths. */
	printf("  \"path_health_factor_q16\": [");
	for (unsigned p = 0; p < num_paths; p++) {
		printf("%s%u", p ? "," : "",
			(unsigned)ring->sapsq_path_health_factor_q16[p]);
	}
	printf("],\n");

	/* Discrete fault-class enum (SAPSQ_MAX_PATHS plane), uint8_t. */
	printf("  \"path_health_enum\": [");
	for (unsigned p = 0; p < num_paths; p++) {
		printf("%s%u", p ? "," : "",
			(unsigned)ring->sapsq_path_health[p]);
	}
	printf("],\n");

	printf("  \"path_eligibility\": [");
	for (unsigned p = 0; p < SAPSQ_PATH_MAX; p++) {
		printf("%s%u", p ? "," : "",
			(unsigned)ring->sapsq_path_eligibility[p]);
	}
	printf("],\n");

	printf("  \"tenant_weight_q16\": [");
	for (unsigned t = 0; t < SAPSQ_TENANT_MAX; t++) {
		printf("%s%u", t ? "," : "",
			(unsigned)ring->sapsq_tenant_weight_q16[t]);
	}
	printf("],\n");

	printf("  \"active_admit_count\": [\n");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("    [");
		for (unsigned p = 0; p < num_paths; p++) {
			printf("%s%lu", p ? "," : "",
				(unsigned long)ring->sapsq_tenant_path_admit_count[t][p]);
		}
		printf("]%s\n", (t + 1 == num_tenants) ? "" : ",");
	}
	printf("  ],\n");

	printf("  \"active_reject_count\": [\n");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("    [");
		for (unsigned p = 0; p < num_paths; p++) {
			printf("%s%lu", p ? "," : "",
				(unsigned long)ring->sapsq_tenant_path_reject_count[t][p]);
		}
		printf("]%s\n", (t + 1 == num_tenants) ? "" : ",");
	}
	printf("  ],\n");

	printf("  \"host_submit_count\": [");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("%s%lu", t ? "," : "",
			(unsigned long)ring->sapsq_host_submit_count[t]);
	}
	printf("],\n");

	printf("  \"demand_iops\": [");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("%s%u", t ? "," : "",
			(unsigned)ring->sapsq_demand_iops[t]);
	}
	printf("],\n");

	printf("  \"served_completions\": [\n");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("    [");
		for (unsigned p = 0; p < num_paths; p++) {
			printf("%s%lu", p ? "," : "",
				(unsigned long)ring->sapsq_served_iops[t][p]);
		}
		printf("]%s\n", (t + 1 == num_tenants) ? "" : ",");
	}
	printf("  ],\n");

	printf("  \"legacy_admit_count\": [\n");
	for (unsigned t = 0; t < SAPSQ_TENANT_MAX; t++) {
		printf("    [");
		for (unsigned p = 0; p < SAPSQ_PATH_MAX; p++) {
			printf("%s%lu", p ? "," : "",
				(unsigned long)ring->sapsq_admit_count[t][p]);
		}
		printf("]%s\n", (t + 1 == SAPSQ_TENANT_MAX) ? "" : ",");
	}
	printf("  ],\n");

	printf("  \"legacy_reject_count\": [\n");
	for (unsigned t = 0; t < SAPSQ_TENANT_MAX; t++) {
		printf("    [");
		for (unsigned p = 0; p < SAPSQ_PATH_MAX; p++) {
			printf("%s%lu", p ? "," : "",
				(unsigned long)ring->sapsq_reject_count[t][p]);
		}
		printf("]%s\n", (t + 1 == SAPSQ_TENANT_MAX) ? "" : ",");
	}
	printf("  ],\n");

	printf("  \"probe_count\": [\n");
	for (unsigned t = 0; t < SAPSQ_TENANT_MAX; t++) {
		printf("    [");
		for (unsigned p = 0; p < SAPSQ_PATH_MAX; p++) {
			printf("%s%lu", p ? "," : "",
				(unsigned long)ring->sapsq_probe_count[t][p]);
		}
		printf("]%s\n", (t + 1 == SAPSQ_TENANT_MAX) ? "" : ",");
	}
	printf("  ],\n");

	printf("  \"tenant_path_rate_q32\": [\n");
	for (unsigned t = 0; t < SAPSQ_TENANT_MAX; t++) {
		printf("    [");
		for (unsigned p = 0; p < SAPSQ_PATH_MAX; p++) {
			printf("%s%u", p ? "," : "",
				(unsigned)ring->sapsq_tenant_path_rate_q32[t][p]);
		}
		printf("]%s\n", (t + 1 == SAPSQ_TENANT_MAX) ? "" : ",");
	}
	printf("  ],\n");

	/* DPA-published per-(tenant, path) rate budget (SAPSQ_MAX_TENANTS ×
	 * SAPSQ_MAX_PATHS plane, Q0.32) — realized share for the allocation
	 * figure. Indexed by runtime num_tenants × num_paths. */
	printf("  \"tenant_path_rate_budget_q32\": [\n");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("    [");
		for (unsigned p = 0; p < num_paths; p++) {
			printf("%s%u", p ? "," : "",
				(unsigned)ring->sapsq_tenant_path_rate_budget_q32[t][p]);
		}
		printf("]%s\n", (t + 1 == num_tenants) ? "" : ",");
	}
	printf("  ],\n");

	/* Human-readable conversion of the active Q0.32 IO/tick budgets. */
	printf("  \"tenant_path_rate_budget_iops\": [\n");
	for (unsigned t = 0; t < num_tenants; t++) {
		printf("    [");
		for (unsigned p = 0; p < num_paths; p++) {
			uint64_t q32 =
				ring->sapsq_tenant_path_rate_budget_q32[t][p];
			uint64_t iops =
				(q32 * ring->sapsq_host_tsc_freq) >> 32;
			printf("%s%lu", p ? "," : "", (unsigned long)iops);
		}
		printf("]%s\n", (t + 1 == num_tenants) ? "" : ",");
	}
	printf("  ]\n");

	printf("}\n");

	free(snapshot);
	munmap(addr, sz);
	close(fd);
	return 0;
}
