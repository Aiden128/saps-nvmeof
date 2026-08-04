/* M2 v2 host classifier unit test (2026-05-20)。
 *
 * Standalone test program 驗 host_m2_classifier_tick() 對三個 fault pattern
 * 的 verdict 是否符合 spec:
 *   Pattern 1 — 全 HEALTHY (cosine ≈ 1.0 for all clients):verdict 全 HEALTHY
 *   Pattern 2 — Individual fault (1 client cosine 掉 → 0.5,其餘 ≈ 1.0):
 *               該 client verdict=INDIVIDUAL,其他 HEALTHY
 *   Pattern 3 — Joint/shared-fate (全部 16 client cosine 掉到 ≈ 0.5):
 *               全部 verdict=JOINT
 *
 * Detection latency 量法:在 wall-clock t0 寫 fault pattern conf_q16,然後
 * call host_m2_classifier_tick(),量 verdict 升級到正確類別的時間。Production
 * thread 跑 10ms tick,所以 worst-case latency = 10ms + classifier compute time。
 * Unit test 直接呼叫不睡眠 → 只量 compute 部分 (≪ 1ms)。
 *
 * Build: meson + ninja in samples/build。Run:
 *   sudo ./build/dpa_plugin/host/dpa_plugin_m2v2_test
 *
 * Exit code 0 = pass,非 0 = fail。 */

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

/* 1.0 in Q16.16 cosine。0.5 in Q16.16 = 32768。 */
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
	/* Allocate a local shared ring stub。Classifier 只讀 m2_pca_conf_q16 /
	 * saps_m2_enabled / m2_v2_classifier,寫 per_client_joint_verdict。 */
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

	/* Pattern 1:全 HEALTHY (cosine 全 1.0)。L_c 全 0 < noise floor →
	 * n_active=0 → 全 HEALTHY。 */
	{
		int32_t conf[16];
		uint8_t exp[16];
		for (int c = 0; c < 16; c++) {
			conf[c] = COS_FULL;
			exp[c]  = DPA_M2_VERDICT_HEALTHY;
		}
		total_fail += test_pattern("P1 all-healthy", ring, conf, exp);
	}

	/* Pattern 2:Individual fault — client 7 掉 cosine 到 0.5,其餘 1.0。
	 * L_c for client 7 = 0.5;其餘 0 → noise floor 過濾 → n_active=1 →
	 * entropy = -1.0 × log2(1.0) = 0 → bulk verdict=INDIVIDUAL → dominant_c=7。
	 * Client 7 → INDIVIDUAL,其餘 → HEALTHY。 */
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

	/* Pattern 3:Joint / shared-fate — 全 16 client cosine 都 0.5。
	 * L_c 全 0.5,sum=8.0,p_c=1/16 uniform → entropy = log2(16) = 4.0 ≥ 3.6
	 * → bulk verdict=JOINT → 全 16 client = JOINT。 */
	{
		int32_t conf[16];
		uint8_t exp[16];
		for (int c = 0; c < 16; c++) {
			conf[c] = COS_HALF;
			exp[c]  = DPA_M2_VERDICT_JOINT;
		}
		total_fail += test_pattern("P3 joint all-16", ring, conf, exp);
	}

	/* Pattern 4:Subset fault — client 0..3 掉 0.5,4..15 = 1.0。
	 * 4 active,p_c = 1/4 each → entropy = log2(4) = 2.0,正好在 SUSPECT
	 * 邊界 [2.0, 3.6)。第一 tick verdict 應為 SUSPECT (active client)。 */
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

	/* Pattern 5:Warmup — conf_q16 == 0 → verdict 應為 UNSET (test_pattern
	 * 在 seed 時已寫 UNSET,但 classifier 應主動維持 UNSET 不變)。 */
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
