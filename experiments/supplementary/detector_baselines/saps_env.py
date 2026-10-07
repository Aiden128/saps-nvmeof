"""Controller environment profiles for the baseline harness."""

import os
from pathlib import Path


WEIGHTS = "3,1,1,1"
NUM_TENANTS = "4"
NUM_PATHS = "3"
EPOCH_US = "1000"
PROBE_RATE_IOPS = "1000"

# Static validation notes for the drive script/README: the current host_saps
# integration reads HOST_SAPS_* config but not DPA_PLUGIN_SAMPLE_RATE or
# per-path capacity envs. It divides HOST_SAPS_LINK_CAP_IOPS by path count.
HOST_SAPS_PARITY_GAPS = (
    "host_saps has no HOST_SAPS_PATH_CAP_IOPS support; it uses C/num_paths.",
    "host_saps has no sample-rate env; DPA_PLUGIN_SAMPLE_RATE is DPA-only.",
)
HOST_SAPS_EVIDENCE_REGEX = (
    r"host_saps active .* skipping dpa_plugin_init",
    r"host_saps active .* ticks_per_ms=",
    "host_saps control plane active",
    "host_saps CP epochs=",
)
HOST_SAPS_STARTUP_MARKERS = HOST_SAPS_EVIDENCE_REGEX


def _clean_env():
    return {
        key: value for key, value in os.environ.items()
        if not key.startswith(("DPA_PLUGIN_", "SAPSQ_", "SAPS_", "HOST_SAPS_"))
    }


def _csv(values):
    return ",".join(map(str, values))


def _base_env():
    env = _clean_env()
    env.update({
        "PYTHONDONTWRITEBYTECODE": "1",
        "HOST_SAPS_ENABLED": "0", "SAPS_TRACE": "0",
        "SAPS_SELECTOR_TRACE": "0", "SAPS_Q_ENABLED": "0",
        "SAPS_M4_ENABLED": "0", "DPA_PLUGIN_DEV": "mlx5_0",
        "DPA_PLUGIN_PATH_MAP_PORTS": "4430:0,4431:1,4432:2",
    })
    return env


def _disabled_arm_env(env):
    env.update({"DPA_PLUGIN_DISABLE_INIT": "1", "SAPSQ_ENABLED": "0",
                "SAPS_M2_ENABLED": "0", "SAPS_M5_DRR_ENABLED": "0"})
    return env


def _saps_q_env(tenant_id, runtime_dir, run_dir, path_caps, service_limit,
                counter_hz, sample_rate):
    env = _base_env()
    env.update({
        "DPA_PLUGIN_ROLE": "coordinator" if tenant_id == 0 else "tenant",
        "DPA_PLUGIN_SOCK": str(Path(runtime_dir) / "dpa.sock"),
        "DPA_PLUGIN_DISABLE_INIT": "0",
        "DPA_PLUGIN_SAMPLE_RATE": str(sample_rate),
        "SAPSQ_ENABLED": "1", "SAPSQ_MY_TENANT_ID": str(tenant_id),
        "SAPSQ_NUM_TENANTS": NUM_TENANTS,
        "SAPSQ_NUM_PATHS": NUM_PATHS,
        "SAPSQ_LINK_CAP_IOPS": str(service_limit),
        "SAPSQ_PATH_CAP_IOPS": _csv(path_caps),
        "SAPSQ_WEIGHTS": WEIGHTS, "SAPSQ_EPOCH_PERIOD_US": EPOCH_US,
        "SAPSQ_PROBE_RATE_IOPS": PROBE_RATE_IOPS,
        "SAPSQ_HEALTH_SOURCE": "completion",
        "SAPSQ_BYPASS_D_CLASSIFIER": "0", "SAPSQ_BYPASS_SAPS_FSM": "0",
        "SAPSQ_BYPASS_HEALTH_COUPLING": "0",
        "SAPSQ_BYPASS_BUDGET_SELECTION": "0",
        "SAPSQ_HEALTH_COUPLING_MODE": "continuous", "SAPS_ACTIVE_PROBE": "1",
        "SAPS_M2_ENABLED": "1", "SAPS_M2_V2_CLASSIFIER": "0",
        "SAPS_M5_DRR_ENABLED": "1", "SAPS_M5_WEIGHTS": WEIGHTS,
        "SAPS_M5_LINK_IOPS": str(service_limit),
        "SAPS_M5_TENANT_ID": str(tenant_id),
    })
    if tenant_id == 0:
        env["DPA_PLUGIN_SAMPLER_CSV"] = str(Path(run_dir) / "controller_sampler.csv")
    if counter_hz is not None:
        env["SAPSQ_HOST_TSC_FREQ"] = str(counter_hz)
    return env


def _host_saps_env(tenant_id, run_dir, path_caps, service_limit):
    env = _base_env()
    env.update({
        "DPA_PLUGIN_ROLE": "standalone",
        "DPA_PLUGIN_DISABLE_INIT": "1",
        "HOST_SAPS_ENABLED": "1",
        "SAPSQ_ENABLED": "0",
        "SAPS_M2_ENABLED": "0",
        "SAPS_M5_DRR_ENABLED": "0",
        "HOST_SAPS_NUM_TENANTS": NUM_TENANTS,
        "HOST_SAPS_NUM_PATHS": str(len(path_caps)),
        "HOST_SAPS_MY_TENANT_ID": str(tenant_id),
        "HOST_SAPS_LINK_CAP_IOPS": str(service_limit),
        "HOST_SAPS_EPOCH_PERIOD_US": EPOCH_US,
        "HOST_SAPS_PROBE_RATE_IOPS": PROBE_RATE_IOPS,
        "HOST_SAPS_WEIGHTS": WEIGHTS,
        "HOST_SAPS_M3_ENABLED": "1",
        "HOST_SAPS_M4_ENABLED": "1",
        "HOST_SAPS_M5_DRR_ENABLED": "1",
        "HOST_SAPS_BYPASS_D_CLASSIFIER": "0",
        "HOST_SAPS_CP_EVIDENCE_FILE": str(
            Path(run_dir) / f"host_saps_cp_t{tenant_id}.log"
        ),
    })
    return env


def process_env(arm, tenant_id, runtime_dir, run_dir, path_caps, service_limit,
                counter_hz=None, sample_rate=32):
    # Prevent inherited ablation, tracing, and tenant settings from changing a cell.
    if arm == "host_saps":
        return _host_saps_env(tenant_id, run_dir, path_caps, service_limit)

    if arm not in ("saps_q", "saps_binary", "health_only"):
        return _disabled_arm_env(_base_env())

    env = _saps_q_env(tenant_id, runtime_dir, run_dir, path_caps, service_limit,
                      counter_hz, sample_rate)
    if arm == "saps_binary":
        env["SAPSQ_HEALTH_COUPLING_MODE"] = "binary"
    elif arm == "health_only":
        # Summit-v2 decoupled arm: health-aware placement, weighted admission at C,
        # no health-coupled budget shrink.
        env.update({
            "SAPS_M2_ENABLED": "1",
            "SAPS_M2_V2_CLASSIFIER": "0",
            "SAPS_M5_DRR_ENABLED": "1",
            "SAPSQ_BYPASS_SAPS_FSM": "0",
            "SAPSQ_BYPASS_HEALTH_COUPLING": "1",
            "SAPSQ_BYPASS_BUDGET_SELECTION": "1",
            "SAPSQ_HEALTH_COUPLING_MODE": "fixed",
        })
    # Otherwise the plugin reads cntfrq_el0; never infer it from CPU MHz.
    return env
