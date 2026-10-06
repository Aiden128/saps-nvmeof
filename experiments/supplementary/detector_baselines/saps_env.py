"""Explicit full-SAPS profile adapted from the original E2/competitor builders."""

import os
from pathlib import Path


def process_env(arm, tenant_id, runtime_dir, run_dir, path_caps, service_limit,
                counter_hz=None):
    # Prevent inherited ablation, tracing, and tenant settings from changing a cell.
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("DPA_PLUGIN_", "SAPSQ_", "SAPS_", "HOST_SAPS_"))}
    env.update({
        "PYTHONDONTWRITEBYTECODE": "1",
        "HOST_SAPS_ENABLED": "0", "SAPS_TRACE": "0",
        "SAPS_SELECTOR_TRACE": "0", "SAPS_Q_ENABLED": "0",
        "SAPS_M4_ENABLED": "0", "DPA_PLUGIN_DEV": "mlx5_0",
        "DPA_PLUGIN_PATH_MAP_PORTS": "4430:0,4431:1,4432:2",
    })
    if arm != "saps_q":
        env.update({"DPA_PLUGIN_DISABLE_INIT": "1", "SAPSQ_ENABLED": "0",
                    "SAPS_M2_ENABLED": "0", "SAPS_M5_DRR_ENABLED": "0"})
        return env
    env.update({
        "DPA_PLUGIN_ROLE": "coordinator" if tenant_id == 0 else "tenant",
        "DPA_PLUGIN_SOCK": str(Path(runtime_dir) / "dpa.sock"),
        "DPA_PLUGIN_DISABLE_INIT": "0", "DPA_PLUGIN_SAMPLE_RATE": "32",
        "SAPSQ_ENABLED": "1", "SAPSQ_MY_TENANT_ID": str(tenant_id),
        "SAPSQ_NUM_TENANTS": "4", "SAPSQ_NUM_PATHS": "3",
        "SAPSQ_LINK_CAP_IOPS": str(service_limit),
        "SAPSQ_PATH_CAP_IOPS": ",".join(map(str, path_caps)),
        "SAPSQ_WEIGHTS": "3,1,1,1", "SAPSQ_EPOCH_PERIOD_US": "1000",
        "SAPSQ_PROBE_RATE_IOPS": "1000", "SAPSQ_HEALTH_SOURCE": "completion",
        "SAPSQ_BYPASS_D_CLASSIFIER": "0", "SAPSQ_BYPASS_SAPS_FSM": "0",
        "SAPSQ_BYPASS_HEALTH_COUPLING": "0",
        "SAPSQ_BYPASS_BUDGET_SELECTION": "0",
        "SAPSQ_HEALTH_COUPLING_MODE": "continuous", "SAPS_ACTIVE_PROBE": "1",
        "SAPS_M2_ENABLED": "1", "SAPS_M2_V2_CLASSIFIER": "0",
        "SAPS_M5_DRR_ENABLED": "1", "SAPS_M5_WEIGHTS": "3,1,1,1",
        "SAPS_M5_LINK_IOPS": str(service_limit),
        "SAPS_M5_TENANT_ID": str(tenant_id),
    })
    if tenant_id == 0:
        env["DPA_PLUGIN_SAMPLER_CSV"] = str(Path(run_dir) / "controller_sampler.csv")
    if counter_hz is not None:
        env["SAPSQ_HOST_TSC_FREQ"] = str(counter_hz)
    # Otherwise the plugin reads cntfrq_el0; never infer it from CPU MHz.
    return env
