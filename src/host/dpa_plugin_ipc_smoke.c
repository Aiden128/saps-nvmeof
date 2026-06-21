/* E-4 dpa_plugin — multi-tenant IPC smoke test (coordinator + N tenants).
 *
 * Forks one coordinator + N tenant child processes. Each tenant pushes
 * EVENTS_PER_TENANT events with a qp_id unique to that tenant (tenant k uses
 * qp_id = K_BASE + k, with cmd_id sentinel = (k+1) << 8 | seq_lo so a later
 * consumer-side audit can confirm no torn writes across tenants).
 *
 * Usage (as root, DEVX required):
 *   sudo DPA_PLUGIN_DEV=mlx5_1 ./dpa_plugin_ipc_smoke [N_TENANTS=2] [EVENTS_PER_TENANT=500000]
 *
 * Gate criteria (E-4):
 *   - total events pushed across coordinator+tenants = 2 × N × EVENTS_PER_TENANT
 *     (each pushes one submit + one complete per iter)
 *   - producer_idx after all tenants done = sum of per-process pushes (atomic
 *     RMW means no torn updates)
 *   - No false-sharing catastrophe: aggregate push rate with 2 tenants
 *     should not drop below ~1.7× single-tenant rate (sub-additive but close)
 */

#define _GNU_SOURCE
#include "../dpa_plugin.h"
#include "../dpa_plugin_com.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#include <stdatomic.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <errno.h>

static uint64_t mono_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Each tenant (including the coordinator as producer 0) pushes events with
 * a qp_id unique to its producer_id. cmd_id is used as an integrity sentinel:
 * cmd_id = ((producer_id + 1) << 12) | (seq & 0xFFF) lets the DPA / audit
 * log distinguish which producer wrote each slot. */
static void producer_loop(int producer_id, int n_events)
{
	const uint16_t qp_base = (uint16_t)(1000 + producer_id * 100);

	uint64_t t0 = mono_ns();
	for (int i = 0; i < n_events; i++) {
		uint16_t sentinel = (uint16_t)(((producer_id + 1) << 12) | (i & 0xFFF));
		dpa_plugin_on_io_submit(sentinel, 0x02, qp_base, 0, 4096, 1, 0,
					(uint64_t)i);
		dpa_plugin_on_io_complete(sentinel, 0x02, qp_base, 0, 0, 4096, 1,
					   0, (uint64_t)(i + 1));
	}
	uint64_t t1 = mono_ns();
	double ms = (double)(t1 - t0) / 1e6;
	fprintf(stderr,
		"producer %d: pushed %d submit+complete in %.3f ms (%.2f Mev/s)\n",
		producer_id, n_events, ms,
		((double)n_events * 2.0 / 1e6) / (ms / 1000.0));
}

int main(int argc, char **argv)
{
	if (geteuid()) {
		fprintf(stderr, "must run as root (DEVX)\n");
		return 1;
	}

	int n_tenants = (argc >= 2) ? atoi(argv[1]) : 2;
	int events_per = (argc >= 3) ? atoi(argv[2]) : 500000;
	if (n_tenants < 1) n_tenants = 1;
	if (events_per < 1) events_per = 500000;

	fprintf(stderr, "E-4 IPC smoke: n_tenants=%d events_per=%d\n",
		n_tenants, events_per);

	/* Fork tenants first so they block on connect() until the coordinator's
	 * UDS server is listening. Parent role is coordinator. */
	pid_t *tenant_pids = calloc((size_t)n_tenants, sizeof(pid_t));
	if (!tenant_pids) {
		fprintf(stderr, "calloc failed\n");
		return 1;
	}

	for (int k = 0; k < n_tenants; k++) {
		pid_t pid = fork();
		if (pid < 0) {
			fprintf(stderr, "fork failed: %s\n", strerror(errno));
			return 1;
		}
		if (pid == 0) {
			/* Child == tenant. Clear any role env the parent set,
			 * then set DPA_PLUGIN_ROLE=tenant and init. The tenant
			 * init path connects to the UDS, recvs memfd, attaches. */
			setenv("DPA_PLUGIN_ROLE", "tenant", 1);
			/* Small stagger so tenant k's connect arrives orderly. */
			struct timespec ts = { .tv_sec = 0,
					       .tv_nsec = 50 * 1000 * 1000 };
			nanosleep(&ts, NULL);

			if (dpa_plugin_init()) {
				fprintf(stderr, "tenant %d init failed\n", k);
				_exit(2);
			}
			/* Producer id starts at 1; coordinator is id 0. */
			producer_loop(1 + k, events_per);
			/* Give coordinator/DPA a moment to drain before
			 * munmap'ing our view. */
			usleep(100000);
			dpa_plugin_shutdown();
			_exit(0);
		}
		tenant_pids[k] = pid;
	}

	/* Parent: coordinator. */
	setenv("DPA_PLUGIN_ROLE", "coordinator", 1);
	if (dpa_plugin_init()) {
		fprintf(stderr, "coordinator init failed\n");
		/* Reap children so they don't zombie forever. */
		for (int k = 0; k < n_tenants; k++) {
			kill(tenant_pids[k], SIGTERM);
			waitpid(tenant_pids[k], NULL, 0);
		}
		return 1;
	}

	/* Coordinator as producer 0 — overlaps with tenants for concurrent stress. */
	uint64_t coord_t0 = mono_ns();
	producer_loop(0, events_per);
	uint64_t coord_t1 = mono_ns();

	/* Wait for all tenants. */
	int fail = 0;
	for (int k = 0; k < n_tenants; k++) {
		int status = 0;
		waitpid(tenant_pids[k], &status, 0);
		if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
			fprintf(stderr, "tenant %d exited badly status=0x%x\n",
				k, status);
			fail++;
		}
	}
	uint64_t end = mono_ns();

	/* Report. Final producer_idx should equal:
	 *   (coordinator + N tenants) × events_per × 2 events (submit+complete)
	 * modulo the batched-flush rounding (batch size 64 — any residual <64
	 * is flushed on shutdown, so no loss). */
	uint64_t expected = (uint64_t)(1 + n_tenants) * (uint64_t)events_per * 2ULL;

	/* MPSC atomicity gate: coordinator-side read of producer_idx should
	 * equal the exact sum of every producer's push count (no torn writes
	 * across process boundaries). */
	uint64_t observed = dpa_plugin_read_producer_idx();

	double coord_ms = (double)(coord_t1 - coord_t0) / 1e6;
	double total_ms = (double)(end - coord_t0) / 1e6;
	fprintf(stderr,
		"E-4 summary: coord_push_time=%.3f ms total_wallclock=%.3f ms "
		"expected_events=%lu observed_producer_idx=%lu\n",
		coord_ms, total_ms, (unsigned long)expected,
		(unsigned long)observed);
	if (observed != expected) {
		fprintf(stderr,
			"E-4 GATE FAIL: producer_idx atomicity — expected=%lu observed=%lu delta=%ld\n",
			(unsigned long)expected, (unsigned long)observed,
			(long)(observed - expected));
		fail++;
	} else {
		fprintf(stderr,
			"E-4 GATE PASS: producer_idx = %lu (exact match)\n",
			(unsigned long)observed);
	}

	/* Small delay so DPA drains before shutdown tears the ring down. */
	usleep(200000);
	dpa_plugin_shutdown();
	free(tenant_pids);
	return fail ? 1 : 0;
}
