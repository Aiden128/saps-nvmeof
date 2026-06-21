# SAPS-Q 評估測試台操作手冊 (Testbed Runbook)

> 版本日期：2026-05-22  
> 適用對象：新進操作員；單文件、可線性執行、無需查閱原始碼。  
> 環境變數完整參考：[`docs/sapsq_env_vars.md`](sapsq_env_vars.md)

---

## §1 概覽 (Overview)

**SAPS-Q** (Self-Adapting Path Scheduler with Q-format budgets) 是雙平面排程器 (two-plane scheduler)：DPA RP 計算每租戶 (per-tenant) 每路徑 (per-path) 的 IO 預算 (budget)；host 快速路徑 (fast path) 在提交 (submit) 時執行准入控制 (admission control)。排程目標是加權最大最小公平 (weighted max-min fairness)，同時具備閒置租戶工作守恆性 (work conservation)，並在路徑劣化 (path degradation) 時依路徑健康度 (path health) 動態調整預算。

**雙機架構：**

- **node1**（目標端 target）：運行 SPDK `nvmf_tgt`，提供 NVMe-oF 子系統 (subsystem)。網路位址固定為 `10.0.1.1`。
- **node2**（發起端 initiator）：運行 SPDK `bdevperf` + DPA RP plugin，執行多路徑 (multipath) IO 與 SAPS-Q 准入控制。

**本手冊涵蓋以下實驗 (experiment)：**

| 代號 | 名稱 | 描述 |
|------|------|------|
| E1 | 靜態公平性 (static fairness) | 4 個模式 × 5 次重複，驗證 weighted Jain ≥ 0.99 |
| E2 | 工作守恆 (work-conserving) | 租戶 1 閒置期間，確認其配額重分配給活躍租戶 |
| E3 | 路徑劣化 hero | 注入 path B 5ms 延遲，量 QoS 保護效果 |
| E4 | 權重重配 (reweight) | `[1,1,1,1]→[3,1,1,1]`，量收斂時間與過衝 (overshoot) |
| E5 | 開銷 (overhead) | D0/D1/D3 情境下 SAPS-Q on/off 的 IOPS 差異 |
| E6 | 恢復回滯 (recovery hysteresis) | 注入並移除 netem，量 `sapsq_path_health_q16` 時序 |
| E7 | N=8 擴展性 (scale) | 8 租戶 × 3 路徑，驗證 Jain 與總 IOPS 擴展效率 |
| E5b | D-series hero 保留驗證 (D-series hero with SAPS-Q) | D-series single-NQN topology，驗證 D1/D3 hero ratio 在 SAPS-Q 開啟後不回歸 |
| E8 | 混合 IO 大小公平性 (mixed workload fairness) | 4K latency-sensitive + 64K throughput-sensitive，驗證 nbytes cost scaling |
| E9 | 優先反轉工作守恆 (priority inversion work-conservation) | 高權重輕載租戶場景，SAPS-Q 應把空閒容量重分配給飽和租戶 |

---

## §2 前提條件確認清單 (Prerequisites Checklist)

在 node2 上逐項確認。每條命令都應回傳非錯誤結果，否則不可繼續。

### node1 上

- [ ] SPDK 安裝於 `/home/user/spdk`
  ```bash
  ssh node1 'ls /home/user/spdk/build/bin/nvmf_tgt'
  ```
- [ ] node1 可透過 SSH 免密碼登入（orchestrator 運行期間會 ssh node1 執行 tc-netem）
  ```bash
  ssh node1 'echo ok'
  ```

### node2 上

- [ ] patched SPDK + dpa-smart-initiator 已建置
  ```bash
  ls /home/user/spdk/build/lib/libspdk_nvme.a
  ls /home/user/DPA/nvme-of-controller/dpa-smart-initiator/flexio_build/samples/build/build.ninja
  ```
  若 `libspdk_nvme.a` 不存在或早於 2026-05-22（qp→path patch 日期），重新建置：
  ```bash
  cd /home/user/spdk && make -j$(nproc)
  ```

- [ ] DPA RP 韌體可用（plugin 載入時自動讀取；確認 `DPA_PLUGIN_PATH` 環境變數或預設路徑存在）
  ```bash
  ls /home/user/DPA/nvme-of-controller/dpa-smart-initiator/flexio_build/samples/build/
  ```

- [ ] RoCE 網路（3 IB 裝置 / 3 ports）正常
  ```bash
  ibv_devices
  # 應看到至少 3 個裝置
  ```

- [ ] chrony / NTP 誤差在 node1 ↔ node2 之間 ≤ 1ms
  ```bash
  chronyc tracking | grep 'System time'
  ssh node1 'chronyc tracking | grep "System time"'
  ```

- [ ] Hugepages：node2 需要至少 4096 × 2MB hugepages
  ```bash
  cat /proc/sys/vm/nr_hugepages
  # 若 < 4096：
  sudo sysctl vm.nr_hugepages=4096
  ```

- [ ] CPU 調速器 (governor) 設為 performance
  ```bash
  sudo cpupower frequency-set -g performance
  cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
  # 應輸出 performance
  ```

---

## §3 node2 一次性準備 (One-time Prep)

只需首次建置時執行。

### 3.1 建置 `sapsq_dump` 計數器讀取工具

```bash
cd /home/user/DPA/nvme-of-controller
gcc -O2 -Wall \
    -I dpa-smart-initiator/flexio_build/samples/dpa_plugin \
    -o scripts/sapsq_dump \
    scripts/sapsq_dump.c
```

驗證：
```bash
/home/user/DPA/nvme-of-controller/scripts/sapsq_dump 2>&1 | head -1
# 應輸出: usage: .../sapsq_dump <memfd-procfs-path>
```

若 `dpa_plugin_com.h` 找不到，確認路徑：
```bash
find /home/user/DPA/nvme-of-controller/dpa-smart-initiator -name dpa_plugin_com.h
```

### 3.2 確認 SPDK lib 版本包含 qp→path patch

```bash
ls -la /home/user/spdk/build/lib/libspdk_nvme.a
# mtime 應 ≥ 2026-05-22；若舊於此日期，重新 make
```

### 3.3 確認 `ARM1_PATH_B_IFACE` 常數

`scripts/run_sapsq.py` 第 100 行有：

```python
ARM1_PATH_B_IFACE = "enp1s0f1np1"
```

這是 node1 上 path B 對應的 RoCE 實體介面 (physical interface) 名稱，E3/E6 注入 `tc-netem` 時使用。在 node1 上確認：

```bash
ssh node1 'ip link show | grep "enp1s0f1np1"'
```

若名稱不符，編輯 `scripts/run_sapsq.py` 第 100 行改成正確介面名稱。

### 3.4 確認 `DPA_PLUGIN_PATH_MAP_PORTS` 公式

預設使用公式 `path_id = (trsvcid - 4430) / 10`，對應埠口 (port) 配置：

| 路徑 (path) | tenant 0 | tenant 1 | tenant 2 | tenant 3 |
|------------|---------|---------|---------|---------|
| path A | 4430 | 4431 | 4432 | 4433 |
| path B | 4440 | 4441 | 4442 | 4443 |
| path C | 4450 | 4451 | 4452 | 4453 |

若 node1 setup script 使用不同埠口，需設定：
```bash
export DPA_PLUGIN_PATH_MAP_PORTS="4430:0,4431:0,4432:0,4433:0,4440:1,4441:1,4442:1,4443:1,4450:2,4451:2,4452:2,4453:2"
```
否則不需要覆寫（orchestrator 使用內建公式）。

---

## §4 node1 Setup 決策矩陣 (Setup Decision Matrix)

所有 setup script 都在 **node1** 上執行。在 node2 上用 ssh 觸發。

| 實驗 | 使用哪個 setup script | 備註 |
|------|-----------------------|------|
| E1, E2, E4 | `setup_arm1_sapsq_4t3p.sh` | 4 租戶 × 3 路徑；orchestrator 預設會呼叫 |
| E3 | `setup_arm1_sapsq_4t3p.sh` | 同上；注入由 orchestrator 在跑實驗期間 ssh node1 執行 |
| E5 | `setup_arm1_tenants.sh`（D0 healthy baseline only） | D1/D3 scenarios 已移至 E5b；E5 只跑 D0 overhead |
| E5b | `setup_arm1.sh`（healthy restore）+ `setup_arm1_D1.sh` / `setup_arm1_D3.sh` | D-series single-NQN topology（`3path_targets/`）；E5b driver 直接管理，不走 run_sapsq.py |
| E6 | `setup_arm1_sapsq_4t3p.sh` | 同 E3；orchestrator 負責注入/移除 |
| E7 | `setup_arm1_sapsq_8t3p.sh` | 8 租戶 × 3 路徑 |
| E8, E9 | `setup_arm1_tenants.sh` | 4-NQN 3-path topology；每 rep 重跑一次（冪等） |

### 手動執行 node1 setup（如需提前驗證）

```bash
# 4t3p topology — E1/E2/E3/E4/E5/E6 適用
ssh node1 'bash /home/user/DPA/nvme-of-controller/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh'

# 驗證完成訊號
ssh node1 'bash /home/user/DPA/nvme-of-controller/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh' 2>&1 | grep "DONE"
# 應看到: [setup_sapsq_4t3p] DONE — 4 tenants × 3 paths ready
```

```bash
# 8t3p topology — E7 適用
ssh node1 'bash /home/user/DPA/nvme-of-controller/experiments/3path_targets/setup_arm1_sapsq_8t3p.sh'
```

### 清理 node1（在下一個實驗前或出錯後）

```bash
ssh node1 'bash /home/user/DPA/nvme-of-controller/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh --cleanup'
# 或 8t3p：
ssh node1 'bash /home/user/DPA/nvme-of-controller/experiments/3path_targets/setup_arm1_sapsq_8t3p.sh --cleanup'
```

> `--cleanup` 模式會 `pkill -9 nvmf_tgt` 並移除 socket 檔。冪等 (idempotent)，可重複執行。

---

## §5 實驗執行順序 (Experiment Run Sequence)

**建議順序**：E1 → E5 → E5b → E2 → E4 → E3 → E6 → E7 → E8 → E9

E1 最簡單，先驗基礎功能。E5 緊接著跑確保 SAPS-Q 沒有回歸 D-series。其餘按複雜度遞增。

所有命令均在 **node2** 上從 `/home/user/DPA/nvme-of-controller` 目錄執行。

---

### E1 — 靜態公平性 (Static Fairness)

**目的**：驗證 4 個模式（stock / m4_static / spdk_bdev_qos / sapsq）在飽和負載 (saturated load)、weights=`[3,1,1,1]` 下的加權公平性。

**預計時間**：約 25 分鐘（4 modes × 5 reps × 60s）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e1_static_fairness.sh
```

可選：加速跑（較少 rep，縮短驗證時間）：
```bash
REPS=3 DURATION=30 bash scripts/sapsq_e1_static_fairness.sh
```

**輸出位置**：`experiments/sapsq/e1_static_fairness/<YYYYMMDD_HHMMSS>/`

每個 mode 下有 `rep_0/` … `rep_4/` 目錄，各含：
- `aggregate.json` — per-rep 指標與 verdict
- `tenant_*/bdevperf.log` — 各租戶 bdevperf 原始輸出
- `sapsq_start.json` / `sapsq_end.json` / `sapsq_summary.json` — SAPS-Q 計數器快照 (sapsq mode 才有)

---

### E5 — 開銷守衛 (Overhead Guard)

**目的**：確保 SAPS-Q 開啟後 D0 healthy IOPS overhead ≤ 5%，且 D1/D3 的 hero number 不回歸。**先跑 E5 可及早發現 SAPS-Q 對既有 D-series 的影響。**

**預計時間**：約 50 分鐘（4 scenarios × 5 reps × 60s）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e5_overhead.sh
```

E5 driver 自動使用各別 node1 setup：

| Scenario | node1 setup 使用 | 模式 |
|----------|-----------------|------|
| `d0_off` | `setup_arm1_sapsq_4t3p.sh` | m4_static（SAPS-Q 關） |
| `d0_on` | `setup_arm1_sapsq_4t3p.sh` | sapsq（SAPS-Q 開） |
| `d1_on` | `setup_arm1_D1.sh` | sapsq（D1 fail-slow + SAPS-Q） |
| `d3_on` | `setup_arm1_D3.sh` | sapsq（D3 sct=3 + SAPS-Q） |

**輸出位置**：`experiments/sapsq/e5_overhead/<ts>/`

---

### E2 — 工作守恆 (Work-Conserving Idle Tenant)

**目的**：租戶 1 在第 20–40 秒閒置（SIGSTOP），SAPS-Q 應把其配額重分配給其他 3 個活躍租戶；m4_static 不應做到。

**預計時間**：約 10 分鐘（2 modes × 5 reps × 60s）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e2_idle_tenant.sh
```

E2 只跑 `m4_static` 和 `sapsq` 兩個模式（stock 無租戶概念）。

**輸出位置**：`experiments/sapsq/e2_idle_tenant/<ts>/`

---

### E4 — 權重重配 (Reweight Convergence)

**目的**：phase 1 weights=`[1,1,1,1]` 跑 15 秒，phase 2 weights=`[3,1,1,1]` 跑 45 秒，量 SAPS-Q 收斂到新配額的時間與過衝 (overshoot)。

**實作說明**：DPA plugin 環境變數只在 init 時讀取，無法 mid-run 更新。E4 採用 process relaunch 法：phase 1 結束後完全 kill，phase 2 立即用新 weights 重新啟動。aggregator 在 `e4` mode 下串接兩個 phase 的結果。

**預計時間**：約 10 分鐘（2 modes × 5 reps × (15+45)s + launch overhead）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e4_reweight.sh
```

E4 只跑 `m4_static` 和 `sapsq`（stock 無租戶概念；spdk_bdev_qos 需 mid-run 改 node1 設定，暫不支援）。

**輸出位置**：`experiments/sapsq/e4_reweight/<ts>/<mode>/rep_N/phase1/` + `phase2/`

---

### E3 — 路徑劣化 Hero (Path Degradation With QoS)

**目的**：論文主要 hero number。4 個模式 × 4 租戶 × 3 路徑，在 t=30s 對 path B 注入 5ms tc-netem 延遲，比較各模式的 per-tenant IOPS、weighted Jain、P99 tail latency、路徑健康度轉換時間。

> ⚠️ E3 需要真正的 3-path multipath topology。若 node1 未使用 `setup_arm1_sapsq_4t3p.sh`（只有 single-path），orchestrator 會 log warning 並繼續，但結果無效。

**預計時間**：約 25 分鐘（4 modes × 5 reps × 60s）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e3_path_degradation.sh
```

注入目標為 `ARM1_PATH_B_IFACE`（`enp1s0f1np1`，見 §3.3）。Orchestrator 在 t=30s 透過 ssh 執行 `tc qdisc add dev enp1s0f1np1 root netem delay 5ms`，實驗結束時清除。

**輸出位置**：`experiments/sapsq/e3_path_degradation/<ts>/`

---

### E6 — 恢復回滯 (Recovery Hysteresis)

**目的**：驗證 DPA scheduler 的 RECOVERING 狀態行為：注入 5ms netem 後 `sapsq_path_health_q16[1]` 應在 100ms 內下降；移除後應在 5s 內恢復至 ≥ 0.9 × HEALTHY；不應出現震盪 (oscillation)。

E6 只跑 `sapsq` mode，且**需要** `sapsq_dump` binary（periodic dump 每 100ms 取樣一次，強依賴）。

**預計時間**：約 5 分鐘（1 mode × 5 reps × 60s）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e6_recovery.sh
```

注入時間 t=10s、移除時間 t=30s、總時長 60s（預設值，可透過環境變數覆寫：`INJECT_AT`、`REMOVE_AT`、`DURATION`）。

**輸出位置**：`experiments/sapsq/e6_recovery/<ts>/sapsq/rep_N/`

週期性快照 (periodic dump) 位於 `sapsq_periodic/snap_<ms>.json`。

---

### E7 — N=8 擴展性 (Scale)

**目的**：reviewer 攻擊點：「SAPS-Q 在 4 租戶以外是否仍然有效？」。8 租戶 × 3 路徑，weights=`[3,2,1,1,1,1,1,1]`，驗證加權 Jain 與擴展效率 (scale efficiency)。

> ⚠️ E7 使用 `setup_arm1_sapsq_8t3p.sh`（非 4t3p），請確保 node1 先清理 4t3p topology。

**預計時間**：約 25 分鐘（4 modes × 5 reps × 60s）

```bash
cd /home/user/DPA/nvme-of-controller
# 可選：提供 E1 sapsq 的 N=4 baseline dir 以計算 scale efficiency
E7_BASELINE_N4_DIR="experiments/sapsq/e1_static_fairness/<ts>/sapsq/rep_0" \
bash scripts/sapsq_e7_scale.sh
```

若不提供 `E7_BASELINE_N4_DIR`，scale efficiency 項目為 informational（不影響 pass/fail 判斷其他指標）。

**輸出位置**：`experiments/sapsq/e7_scale/<ts>/`

---

### E5b — D-series Hero 保留驗證 (D-series Hero with SAPS-Q)

**目的**：確認 D1 (fail-slow) 和 D3 (sct=3 sparse errors) 的 hero ratio 在 SAPS-Q 開啟後仍在可接受範圍內。E5b 使用 D-series 的正確 single-NQN topology（3 個 `nvmf_tgt` 各 listen 4430/4431/4432，共用 `nqn.2024-01.io.spdk:mptest`），不走 `run_sapsq.py`（inline bdevperf）。

> ⚠️ E5b **不相容** E5 topology（E5 是 4-tenant per-NQN）。必須各自獨立跑，不可共用 node1 setup。

**預計時間**：約 40 分鐘（4 scenarios × 5 reps × 60s）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e5b_dseries_hero_with_sapsq.sh
```

**輸出位置**：`experiments/sapsq/e5b_dseries_hero/<ts>/`

四個 scenario：`d1_sapsqoff/`、`d1_sapsqon/`、`d3_sapsqoff/`、`d3_sapsqon/`，每個含 `rep_0..4/sapsq_summary.json`。

---

### E8 — 混合 IO 大小公平性 (Mixed Workload Fairness)

**目的**：驗證 SAPS-Q admission cost 按 `max(1, nbytes/4096)` 線性增長時，64K writer (tenant 1) 不會以 1 IO 的 token 偷跑整條 link 頻寬，latency-sensitive 4K reader (tenant 0) P99 仍 ≤ 1ms。

**預計時間**：約 55 分鐘（3 modes × 5 reps × 60s + 單獨 baseline 可選）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e8_mixed_workload.sh
```

**輸出位置**：`experiments/sapsq/e8_mixed_workload/<ts>/`

---

### E9 — 優先反轉工作守恆 (Priority Inversion Work-Conservation)

**目的**：高權重租戶 (tenant 0，weight=3，demand=20K) 輕載時，SAPS-Q 應把多餘容量重分配給飽和的低權重租戶 (tenant 1/2/3)；m4_static 靜態 token bucket 無法做到，作為對照。

**預計時間**：約 55 分鐘（3 modes × 5 reps × 60s）

```bash
cd /home/user/DPA/nvme-of-controller
bash scripts/sapsq_e9_priority_inversion.sh
```

**輸出位置**：`experiments/sapsq/e9_priority_inversion/<ts>/`

---

## §6 結果判讀 (Result Interpretation)

每個實驗跑完，`aggregate_sapsq.py` 自動輸出 `aggregate.json`，頂層欄位：

```json
{
  "verdict": { "pass": true/false, "reason": "..." },
  "metrics": { "weighted_jain": 0.994, "max_share_deviation": 0.031, ... }
}
```

### 接受標準表 (Acceptance Criteria)

| 實驗 | PASS 條件 |
|------|-----------|
| E1 | weighted Jain ≥ 0.99；每租戶實際配額偏差 (share deviation) ≤ 5% |
| E2 | 閒置窗口 (idle window) 期間活躍租戶聚合 IOPS 利用率 ≥ 90% 可用容量；sapsq beats m4_static 的 aggregate IOPS |
| E3 | 預算從劣化路徑 (degraded path) 移出的時間 < 100ms；latency-sensitive 租戶（tenant 0，weight=3）P99 < stock 且 < spdk_bdev_qos |
| E4 | 從 phase 1→phase 2 的配額轉換在 1s 內收斂；收斂過衝 < 20% |
| E5 | D0 SAPS-Q overhead ≤ 5%（healthy traffic only；D1/D3 hero 已移至 E5b） |
| E5b | d1_sapsqoff IOPS ≥ 700K；d1_sapsqon ≥ 0.85 × d1_sapsqoff；d3_sapsqoff max tail < 100ms；d3_sapsqon max tail < 4 × d3_sapsqoff |
| E6 | path health 在注入後 100ms 內下降；移除後 5s 內恢復至 ≥ 0.9 × HEALTHY；HEALTHY↔DEGRADED 轉換次數 ≤ 1 |
| E7 | weighted Jain ≥ 0.99（N=8）；per-tenant 偏差 ≤ 8%；總 IOPS ≥ 1.8× E1 N=4 baseline |
| E8 | latency tenant (t0, 4K) P99 ≤ 1ms；throughput tenant (t1, 64K) IOPS > 0（不被餓死）；SAPS-Q latency P99 ≤ stock 且 ≤ m4_static |
| E9 | t0 demand compliance ∈ [0.9, 1.1]；active Jain (t1/t2/t3) ≥ 0.99；全系統利用率 ≥ 95%；SAPS-Q t1/t2/t3 吞吐 ≥ m4_static +50% |

### 快速檢查 aggregator 結果

```bash
# 查看 E1 所有 mode 的 pass rate
for f in experiments/sapsq/e1_static_fairness/*/*/aggregate.json; do
    python3 -c "import json,sys; d=json.load(open('$f')); print('$f', d.get('verdict',d).get('pass'), d.get('metrics',{}).get('weighted_jain'))"
done
```

```bash
# 查看 E5 整體 verdict
python3 -c "import json; d=json.load(open('experiments/sapsq/e5_overhead/<ts>/aggregate.json')); print(d)"
```

---

## §7 故障排除 (Troubleshooting)

### `sapsq_stale_epoch_fallback` 計數持續增加

```
症狀：sapsq_summary.json 裡 sapsq_stale_epoch_fallback_delta 非零且大
原因：DPA RP 未在運行，或 epoch 刷新頻率 < host 端 stale 門檻
處理：
  1. 確認 DPA RP plugin 已載入：dmesg | grep -i dpa
  2. 確認 SAPS_Q_EPOCH_STALE_US 設定合理（預設 1000 µs）
  3. 確認 SAPS_Q_ENABLED=1 有傳入 bdevperf 環境
```

### 所有 path slot `[*][0]` 有值但 `[*][1..]` 全為 0

```
症狀：sapsq_summary.json 的 admit_count[t][1] 和 [t][2] 皆為 0
原因：qp→path mapping 未正確接線（Task #10 回歸）
處理：
  1. 確認 libspdk_nvme.a 的 mtime ≥ 2026-05-22
  2. 重新 make /home/user/spdk && 重跑 ninja
  3. 確認 run_sapsq.py attach_cmds() 有嘗試 3 個 port
```

### DPA 崩潰 / 無回應 (silence)

```
症狀：bdevperf 掛住或所有 tenant 立刻回傳 0 IOPS
處理：
  1. 查 node1 target log：ssh node1 'tail -50 /tmp/nvmf_sapsq_a.log'
  2. 查 node2 bdevperf log：cat experiments/sapsq/e*/…/tenant_0/bdevperf.log
  3. 確認 DPA plugin 初始化成功：grep -i "SAPS-Q disabled" tenant_*/bdevperf.log
     若有 "SAPS-Q disabled"，表示某個必要環境變數缺失或非法
```

### Hugepage 不足導致 bdevperf 啟動失敗

```
症狀：bdevperf stderr 出現 "Cannot get DMA memory"
處理：
  sudo sysctl vm.nr_hugepages=8192
  # 確認分配成功：cat /proc/sys/vm/nr_hugepages
```

### E3 / E6 注入後 node1 tc qdisc 殘留

```
症狀：下一次實驗一開始就有異常延遲；或 tc qdisc show dev enp1s0f1np1 有 netem rule
處理：
  ssh node1 'sudo tc qdisc del dev enp1s0f1np1 root 2>/dev/null || true'
  # 確認清乾淨：ssh node1 'tc qdisc show dev enp1s0f1np1'
  # 應輸出：qdisc noqueue 0: root ... (無 netem)
```

### RoCE 連線失敗（bdevperf attach timeout）

```
症狀：orchestrator log 出現 "Failed to connect to xxx:4430"
處理：
  1. 確認 node1 nvmf_tgt 正在運行：ssh node1 'pgrep nvmf_tgt'
  2. 確認 IB 介面 UP：ibv_devices && ibv_devinfo
  3. 確認埠口未被防火牆封鎖：
     ssh node1 'nc -l -p 4430 &' && nc 10.0.1.1 4430 && echo ok
  4. 確認 node1 setup script 有完整跑完（看 DONE 訊息）
```

### `sapsq_dump` 編譯失敗（找不到 header）

```
症狀：gcc error: dpa_plugin_com.h: No such file or directory
處理：
  find /home/user/DPA/nvme-of-controller/dpa-smart-initiator \
      -name dpa_plugin_com.h 2>/dev/null
  # 用實際路徑替換 -I 後的路徑重新編譯
```

---

## §8 清理 (Cleanup)

每次評估完成後、或實驗失敗需重置時執行。

### node1 清理

```bash
# E1/E2/E3/E4/E5/E6（4t3p topology）
ssh node1 'bash /home/user/DPA/nvme-of-controller/experiments/3path_targets/setup_arm1_sapsq_4t3p.sh --cleanup'

# E7（8t3p topology）
ssh node1 'bash /home/user/DPA/nvme-of-controller/experiments/3path_targets/setup_arm1_sapsq_8t3p.sh --cleanup'
```

### node1 tc qdisc 殘留清理（E3/E6 注入未清除時）

```bash
ssh node1 'sudo tc qdisc del dev enp1s0f1np1 root 2>/dev/null || true'
```

### node2 清理

node2 不需要額外清理步驟。Orchestrator 在正常退出時會 unload plugin 並 kill bdevperf processes。若 orchestrator 異常中斷：

```bash
# 手動清除殘留 bdevperf processes
sudo pkill -9 -f bdevperf || true

# 清除殘留 socket 檔
sudo rm -f /var/tmp/bdevperf_sapsq_proc*.sock
```

---

## 附錄：`run_sapsq.py` 主要旗標速查 (Key Flags)

| 旗標 (flag) | 預設值 | 說明 |
|------------|--------|------|
| `--tenants N` | 4 | 租戶數量（max 10，受埠口間距限制） |
| `--weights "3,1,1,1"` | `"3,1,1,1"` | 各租戶權重 (weight)，原始 ratio；orchestrator 自動轉 Q16.16 |
| `--duration S` | 60 | 每次 rep 跑幾秒 |
| `--qd N` | 32 | 每租戶 queue depth |
| `--mode M` | sapsq | `stock` / `m4_static` / `sapsq` / `spdk_bdev_qos` |
| `--demand-iops "x,x,x,x"` | `"100000,..."` | 每租戶目標 IOPS（orchestrator 轉 Q32） |
| `--path-base-iops "x,x,x"` | `"200000,..."` | 每路徑基礎容量 IOPS（orchestrator 轉 Q32） |
| `--probe-rate-iops "x,x,x"` | `"100,..."` | 每路徑 probe IOPS |
| `--n-paths N` | 3 | 每租戶路徑數 |
| `--inject-path-degradation P` | — | E3/E6：對 path P 注入 5ms netem（P=1 為 path B） |
| `--inject-time-s T` | 30 | 注入發生時間（秒） |
| `--inject-remove-time-s T` | 0 | E6：移除 netem 時間（0=持續到結束） |
| `--periodic-dump-interval-ms M` | 0 | E6：週期性 dump 間隔 ms（0=只做 start/end） |
| `--idle-tenant "1:20-40"` | — | E2：指定租戶閒置時間窗口 |
| `--no-setup-node1` | — | E4 phase 2 用：跳過 node1 重新 setup |
| `--dry-run` | — | 只印環境變數與命令，不實際執行 |
| `--tsc-hz HZ` | 1000000000 | 覆寫 TSC 頻率（BF3 arm64 預設 1GHz；非 BF3 硬體需要確認） |
| `--out-dir PATH` | 必填 | 輸出目錄（driver scripts 自動設定） |
