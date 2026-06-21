/*
 * sapsq_dump.c — read-only SAPS-Q counter dumper
 *
 * Usage:   sudo sapsq_dump /proc/<pid>/fd/<n>
 * Output:  JSON to stdout — contains per-(tenant, path) admit/reject/probe
 *          counts, eligibility, health, plus global stale-epoch counters.
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

	const struct dpa_plugin_shared *ring =
		(const struct dpa_plugin_shared *)addr;

	/* Emit JSON. Arrays sized to schema:
	 *   admit_count, reject_count, probe_count  [SAPSQ_TENANT_MAX][SAPSQ_PATH_MAX]
	 *   path_health_q16, path_eligibility       [SAPSQ_PATH_MAX]
	 */
	printf("{\n");
	printf("  \"enabled\": %u,\n", (unsigned)ring->sapsq_enabled);
	printf("  \"my_tenant_id\": %u,\n", (unsigned)ring->sapsq_my_tenant_id);
	printf("  \"epoch\": %lu,\n", (unsigned long)ring->sapsq_epoch);
	printf("  \"epoch_commit\": %lu,\n",
		(unsigned long)ring->sapsq_epoch_commit);
	printf("  \"last_epoch_tsc\": %lu,\n",
		(unsigned long)ring->sapsq_last_epoch_tsc);
	printf("  \"stale_epoch_fallback\": %lu,\n",
		(unsigned long)ring->sapsq_stale_epoch_fallback);
	printf("  \"total_epochs_consumed\": %lu,\n",
		(unsigned long)ring->sapsq_total_epochs_consumed);

	printf("  \"path_health_q16\": [");
	for (unsigned p = 0; p < SAPSQ_PATH_MAX; p++) {
		printf("%s%u", p ? "," : "",
			(unsigned)ring->sapsq_path_health_q16[p]);
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

	printf("  \"admit_count\": [\n");
	for (unsigned t = 0; t < SAPSQ_TENANT_MAX; t++) {
		printf("    [");
		for (unsigned p = 0; p < SAPSQ_PATH_MAX; p++) {
			printf("%s%lu", p ? "," : "",
				(unsigned long)ring->sapsq_admit_count[t][p]);
		}
		printf("]%s\n", (t + 1 == SAPSQ_TENANT_MAX) ? "" : ",");
	}
	printf("  ],\n");

	printf("  \"reject_count\": [\n");
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
	printf("  ]\n");

	printf("}\n");

	munmap(addr, sz);
	close(fd);
	return 0;
}
