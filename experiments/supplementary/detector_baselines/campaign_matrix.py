"""Bounded cell plans for the four baseline_v2 campaigns (no hardware I/O)."""


def campaign_cells(experiment, smoke=False):
    if experiment not in ("c1", "c2", "c3", "c4"):
        raise ValueError(f"unknown experiment: {experiment}")
    arms = {
        "c1": ("health_only", "saps_binary"),
        "c2": ("saps_q", "saps_binary", "health_only"),
        "c3": ("stock_round_robin", "host_saps", "saps_q"),
        "c4": ("saps_q",),
    }[experiment]
    delays = {"c1": (5000,), "c2": (500, 1000, 2000, 5000),
              "c3": (0,), "c4": (1000, 5000)}[experiment]
    if smoke:
        delays = delays[:1]
    rates = (16, 32, 64) if experiment == "c4" else (32,)
    repeats = 1 if smoke else (3 if experiment in ("c1", "c3") else 2)
    duration = 20 if smoke else 60
    window = (8, 17) if smoke else (23, 49)
    cells = []
    for repeat in range(1, repeats + 1):
        for delay in delays:
            for arm in arms:
                for rate in rates:
                    run_id = f"r{repeat}_{arm}_s{rate}_d{delay}us"
                    cells.append({
                        "run_id": run_id, "arm": arm, "duration": duration,
                        "fault_at": (5 if smoke else 20) if delay else None,
                        "restore_at": (18 if smoke else 50) if delay else None,
                        "window": window, "fault_delay_us": delay,
                        "sample_rate": rate, "repeat": repeat,
                    })
    return cells
