
#define _GNU_SOURCE
#include "../dpa_plugin.h"
#include "../dpa_plugin_com.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

static uint64_t now_us(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)(ts.tv_nsec / 1000ull);
}

static const char *verdict_name(uint8_t v)
{
	switch (v) {
	case DPA_M2_VERDICT_HEALTHY:    return "HEALTHY";
	case DPA_M2_VERDICT_INDIVIDUAL: return "INDIVIDUAL";
	case DPA_M2_VERDICT_JOINT:      return "JOINT";
	case DPA_M2_VERDICT_SUSPECT:    return "SUSPECT";
	case DPA_M2_VERDICT_UNSET:      return "UNSET";
	default:                        return "??";
	}
}

#define COS_FULL  65536
#define COS_HALF  32768

static int test_pattern(const char *name,
			 struct dpa_plugin_shared *ring,
			 const int32_t *conf,
			 const uint8_t *expected)
{
	int fail = 0;
	/* Seed conf_q16. Keep the published verdict from the preceding pattern,
	 * matching the persistent shared-memory state used in production. */
	for (int c = 0; c < 16; c++) {
		ring->m2_pca_conf_q16[c] = conf[c];
	}

	uint64_t t0 = now_us();
	host_m2_classifier_tick(ring);
	uint64_t t1 = now_us();

	fprintf(stderr, "\n[TEST %s] compute_latency_us=%lu\n", name, (unsigned long)(t1 - t0));
	for (int c = 0; c < 16; c++) {
		uint8_t got = ring->per_client_joint_verdict[c];
		uint8_t exp = expected[c];
		const char *tag = (got == exp) ? "OK" : "FAIL";
		if (got != exp) fail++;
		fprintf(stderr, "  client=%2d conf_q16=%6d got=%s expected=%s [%s]\n",
			c, conf[c], verdict_name(got), verdict_name(exp), tag);
	}
	if (fail) {
		fprintf(stderr, "[TEST %s] FAIL (%d clients wrong)\n", name, fail);
	} else {
		fprintf(stderr, "[TEST %s] PASS\n", name);
	}
	return fail;
}

int main(void)
{
	struct dpa_plugin_shared *ring = aligned_alloc(64, sizeof(*ring));
	if (!ring) {
		fprintf(stderr, "alloc failed\n");
		return 1;
	}
	memset(ring, 0, sizeof(*ring));
	ring->saps_m2_enabled  = 1;
	ring->m2_v2_classifier = 1;

	/* Production initialization seeds the classifier's previous-verdict state
	 * to UNSET. Prime the standalone test through the same warmup path before
	 * presenting the first completed confidence vector. */
	host_m2_classifier_tick(ring);

	int total_fail = 0;

	{
		int32_t conf[16];
		uint8_t exp[16];
		for (int c = 0; c < 16; c++) {
			conf[c] = COS_FULL;
			exp[c]  = DPA_M2_VERDICT_HEALTHY;
		}
		total_fail += test_pattern("P1 all-healthy", ring, conf, exp);
	}

	{
		int32_t conf[16];
		uint8_t exp[16];
		for (int c = 0; c < 16; c++) {
			conf[c] = COS_FULL;
			exp[c]  = DPA_M2_VERDICT_HEALTHY;
		}
		conf[7] = COS_HALF;
		exp[7]  = DPA_M2_VERDICT_INDIVIDUAL;
		total_fail += test_pattern("P2 individual c=7", ring, conf, exp);
	}

	{
		int32_t conf[16];
		uint8_t exp[16];
		for (int c = 0; c < 16; c++) {
			conf[c] = COS_HALF;
			exp[c]  = DPA_M2_VERDICT_JOINT;
		}
		total_fail += test_pattern("P3 joint all-16", ring, conf, exp);
	}

	{
		int32_t conf[16];
		uint8_t exp[16];
		for (int c = 0; c < 16; c++) {
			conf[c] = COS_FULL;
			exp[c]  = DPA_M2_VERDICT_HEALTHY;
		}
		for (int c = 0; c < 4; c++) {
			conf[c] = COS_HALF;
			exp[c]  = DPA_M2_VERDICT_SUSPECT;
		}
		total_fail += test_pattern("P4 subset 0..3 boundary", ring, conf, exp);
	}

	{
		int32_t conf[16];
		uint8_t exp[16];
		for (int c = 0; c < 16; c++) {
			conf[c] = 0;
			exp[c]  = DPA_M2_VERDICT_UNSET;
		}
		total_fail += test_pattern("P5 warmup all-zero", ring, conf, exp);
	}

	if (total_fail) {
		fprintf(stderr, "\n=== TEST SUMMARY: FAIL (%d total mismatches) ===\n",
			total_fail);
		free(ring);
		return 2;
	}
	fprintf(stderr, "\n=== TEST SUMMARY: PASS (all 5 patterns) ===\n");
	free(ring);
	return 0;
}
