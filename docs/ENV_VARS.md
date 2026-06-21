# SAPS-Q Environment Variables Reference

Source of truth: `dpa-smart-initiator/flexio_build/samples/dpa_plugin/host/dpa_plugin.c`
lines ~1518-1723 (SAPS-Q init block).

All Q-format conversions performed by `scripts/run_sapsq.py` (`weights_to_q16`,
`iops_to_q32`); host plugin reads CSV strings as-is and stores into the shared
ring.

## Variable Table

| Name | Format | Default | Example (4-tenant 3-path testbed) | Mode |
|------|--------|---------|-----------------------------------|------|
| `SAPS_Q_ENABLED` | `0` / `1` | `0` (gate off) | `1` | sapsq |
| `SAPS_Q_MY_TENANT_ID` | uint `[0, 16)` | `0` | `0`/`1`/`2`/`3` per-proc | sapsq |
| `SAPS_Q_EPOCH_INTERVAL_EVENTS` | uint events | `1024` | `1024` (~10us @100K IOPS/path) | sapsq |
| `SAPS_Q_EPOCH_STALE_US` | uint microseconds | `1000` | `1000` (host fallback if no fresh epoch in 1ms) | sapsq |
| `SAPS_Q_WEIGHTS` | CSV of uint Q16.16 | `"65536,21845,21845,21845"` (= 3:1:1:1) | `"32768,10923,10923,10923"` | sapsq |
| `SAPS_Q_DEMAND_Q32` | CSV of uint Q32 IO/tsc | (required) | `"429496,429496,429496,429496"` (= 100K IOPS @ 1GHz TSC) | sapsq |
| `SAPS_Q_PATH_BASE_IOPS_Q32` | CSV of uint Q32 IO/tsc | (required) | `"858993,858993,858993"` (= 200K IOPS @ 1GHz TSC, 3 paths) | sapsq |
| `SAPS_Q_PROBE_RATE_Q32` | CSV of uint Q32 IO/tsc | empty (no probe) | `"429,429,429"` (= 100 IOPS @ 1GHz TSC) | sapsq |
| `DPA_PLUGIN_PATH_MAP_PORTS` | CSV of `port:path_id` | unset (use formula) | `"4430:0,4431:0,4432:0,4433:0,4440:1,4441:1,4442:1,4443:1,4450:2,4451:2,4452:2,4453:2"` | sapsq |

## Q-Format Conversion Formulas

```
weight_q16 = round(w_i * 65536 / sum(w))     # Q16.16, sum == 65536
rate_q32   = round(iops_per_sec / tsc_hz * 2^32)
```

BF3 host aarch64 `cntfrq_el0 = 1_000_000_000` (1 GHz nominal). If running on a
host where `cat /sys/devices/system/clocksource/clocksource0/current_clocksource`
shows a different cntvct frequency, pass `--tsc-hz` to `run_sapsq.py`.

Example (orchestrator does this for you):

```python
weights = [3, 1, 1, 1]
weights_q16 = [int(round(w / 6 * 65536)) for w in weights]
# → [32768, 10923, 10923, 10923]

iops = 200_000
tsc_hz = 1_000_000_000
rate_q32 = int(round(iops / tsc_hz * (1 << 32)))
# → 858993
```

## Mode Matrix

| Mode | `SAPS_Q_ENABLED` | `SAPS_M4_ENABLED` | `SAPS_M5_DRR_ENABLED` | `DPA_PLUGIN_DISABLE_INIT` |
|------|------------------|---------------------|------------------------|----------------------------|
| `stock` | `0` | `0` | `0` | `1` (skip plugin entirely) |
| `m4_static` | `0` | `1` | `0` | `0` (need plugin for M4) |
| `sapsq` | `1` | `0` | `0` | `0` |
| `spdk_bdev_qos` | `0` | `0` | `0` | `1` (node1 enforces, host doesn't init plugin) |
| `sapsq` (single-tenant D-series, E5b) | `1` | `0` | `0` | `0` |

### E5b Single-Tenant D-Series Mode Notes

E5b 使用 D-series single-NQN topology（ports 4430/4431/4432 共用同一 NQN），tenant 數 = 1，`SAPS_Q_MY_TENANT_ID=0`。與 4-tenant multi-NQN mode 的差異：

- `SAPS_Q_WEIGHTS="65536"` — 單一租戶，全額 Q16.16 (65536 = 1.0)
- `DPA_PLUGIN_PATH_MAP_PORTS=4430:0,4431:1,4432:2` — **必須顯式設定**。D-series 三個 port 在同一 NQN 上，預設公式 `(port-4430)/10` 會把 4431→0、4432→0，三個 port 全 map 到 path_id=0，SAPS-Q per-path budget 失效。
- E5b 的 inline bdevperf 不走 `run_sapsq.py`，故這個 env var 由 driver script 直接注入（見 `scripts/sapsq_e5b_dseries_hero_with_sapsq.sh` 第 111 行）。

## How CSV Lengths Map to Tenant/Path Indices

- Weight / demand CSV: index `i` → tenant `i` (0..3 for 4-tenant testbed)
- Path-base / probe CSV: index `p` → path `p` (0..2 for 3-path testbed)

Plugin pads missing entries with `0` (which means "tenant inactive" or "path
unavailable"). The orchestrator validates the CSV lengths in `build_config()`
before launch.

## Init Failure Modes

`host/dpa_plugin.c` writes `dpa_plugin: SAPS-Q disabled — <reason>` to stderr and
falls through to M4 substrate when:

- `SAPS_Q_MY_TENANT_ID` >= `SAPSQ_TENANT_MAX` (16)
- `SAPS_Q_WEIGHTS` sums to 0
- `SAPS_Q_DEMAND_Q32` missing or empty
- `SAPS_Q_PATH_BASE_IOPS_Q32` missing or empty

The orchestrator does NOT detect these from stderr (greylist parsing brittle);
inspect `tenant_*/bdevperf.log` if `sapsq_total_epochs_consumed_delta` stays at
`0` across the run.

## Counter Observability

These are written by the host fast path (`dpa_plugin.c` line ~2057-2083) and
read by `scripts/sapsq_dump`:

| Counter | Field | Type | Reset |
|---------|-------|------|-------|
| Admits | `sapsq_admit_count[t][p]` | `uint64_t` (atomic relaxed add) | Init only |
| Rejects | `sapsq_reject_count[t][p]` | `uint64_t` | Init only |
| Probe submits | `sapsq_probe_count[t][p]` | `uint64_t` | Init only |
| Stale fallbacks | `sapsq_stale_epoch_fallback` | `uint64_t` (global) | Init only |
| Epochs consumed | `sapsq_total_epochs_consumed` | `uint64_t` (global) | Init only |
| Path health | `sapsq_path_health_q16[p]` | `uint32_t` (DPA writer) | DPA-driven |
| Path eligibility | `sapsq_path_eligibility[p]` | `uint8_t` (DPA writer) | DPA-driven |

Each tenant proc has its own ring (memfd in `standalone` role), so admit/reject
counters from `tenant_i`'s ring only reflect `tenant_i`'s submissions.
Cross-tenant aggregation in `aggregate_sapsq.py` sums across the per-proc
snapshots.
