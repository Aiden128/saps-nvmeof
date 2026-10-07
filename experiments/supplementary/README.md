# Detector-baseline, delay-sweep, placement, and RocksDB experiments

These scripts configure three SPDK NVMe-oF targets behind the SAPS initiator
and run the experiments of Sections 4.5, 4.7, 4.8, and 4.9. Host paths and
addresses in the scripts match the evaluation testbed; adjust `D`, `SPDK`,
`ROOT`, and the target address for another setup.

## SPDK initiator with the DPA plugin

The RocksDB test drives the SAPS controller from `spdk_tgt`, built with the
plugin initialization enabled (see `integration/spdk/`):

    make -C app/spdk_tgt DPA_PLUGIN=1 \
        DPA_PLUGIN_DIR=<plugin source dir> DPA_PLUGIN_BUILD_DIR=<plugin build dir>
    cp build/bin/spdk_tgt build/bin/spdk_tgt.saps

The SAPS log must contain
`SAPSQ_M enabled ... bypass_d=0 bypass_fsm=0 health_coupling_mode=0 health_source=0`.

## RocksDB (Section 4.9)

`rocksdb/` runs `db_bench readrandom` (8 threads, 60 s, 2,000,000 keys,
1 KB values, direct I/O, 8 MB block cache) on a read-only ext4 file system over
SPDK's network block device with three paths. Twenty seconds into each run,
path B receives a 5 ms delay (`D1`) or the NVMe path-related status Internal
Path Error for reads (`P3`).

    bash drive_stock.sh   # stock round robin: healthy, D1, P3 x 3 runs
    bash drive_saps.sh    # SAPS controller profile, same matrix
    python3 validate_rocksdb.py <stock_out>/full <saps_out>/full

## Detector baselines, delay sweep, placement, sensitivity (Sections 4.5, 4.7, 4.8)

`detector_baselines/` runs four tenants with 3:1:1:1 weights, 4 KB random
reads at QD32, and three paths. See `detector_baselines/README.md` for the
campaigns: the original detector comparison and c1 (Table 6), c2 (binary
health mode sweep), c3 (controller placement), and c4 (sampling rate).

    sudo bash drive_campaign.sh --campaign c2 --smoke --out <dir>
    sudo bash drive_campaign.sh --campaign c2 --full  --out <dir>
