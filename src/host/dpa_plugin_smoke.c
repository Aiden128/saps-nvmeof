/* P0.3 dpa_plugin — standalone smoke test.
 *
 * Exercises init → N pushes → shutdown so we can confirm the library works
 * end-to-end before linking into SPDK. Also useful as a sanity check that the
 * DPA polling kernel really drains the ring.
 *
 * Usage: sudo DPA_PLUGIN_DEV=mlx5_1 ./dpa_plugin_smoke [iters=1000000]
 */

#include "../dpa_plugin.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>

static uint64_t rdtsc_like(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

int main(int argc, char **argv)
{
	int iters = (argc >= 2) ? atoi(argv[1]) : 1000000;
	if (geteuid()) {
		fprintf(stderr, "must run as root (DEVX)\n");
		return 1;
	}

	if (dpa_plugin_init()) {
		fprintf(stderr, "dpa_plugin_init failed\n");
		return 1;
	}

	uint64_t t0 = rdtsc_like();
	for (int i = 0; i < iters; i++) {
		dpa_plugin_on_io_submit((uint16_t)i, 0x02, 1, 0, 4096, 1, 0,
					(uint64_t)i);
		dpa_plugin_on_io_complete((uint16_t)i, 0x02, 1, 0, 0, 4096, 1,
					   0, (uint64_t)(i + 1));
	}
	uint64_t t1 = rdtsc_like();

	double total_ns = (double)(t1 - t0);
	double per_call_ns = total_ns / (double)(iters * 2);
	printf("pushed %d submit+complete pairs (%d calls) in %.3f ms\n",
	       iters, iters * 2, total_ns / 1e6);
	printf("per-call cost: %.2f ns\n", per_call_ns);

	/* Give DPA a moment to drain. */
	usleep(200000);

	/* SAPS_SMOKE_TAIL_SLEEP_S: optional extended drain window (seconds) for
	 * M3/M4 observability validation. Default 0 (preserve P0.3 overhead
	 * measurement behaviour). */
	const char *tail_env = getenv("SAPS_SMOKE_TAIL_SLEEP_S");
	if (tail_env && tail_env[0]) {
		int s = atoi(tail_env);
		if (s > 0) {
			fprintf(stderr, "dpa_plugin_smoke: tail sleep %ds\n", s);
			sleep((unsigned)s);
		}
	}

	dpa_plugin_shutdown();
	return 0;
}
