# experiments (Sections 4.8 and 4.9)

These experiments configure three SPDK NVMe-oF targets behind the SAPS
initiator; the targets listen on 10.0.0.2 (`mlx5_0`). Targets use cores 40-47; the initiator uses core 4
(RocksDB) or cores 4-11 (bdevperf). Every SPDK process uses
`--single-file-segments` so that hugepage backing stays a few files per
process.

Paths in the scripts match the evaluation host (`/home/aiden/spdk`,
`/mnt/nvme0n1p1/aiden/DPA/nvme-of-controller`); adjust `D`, `SPDK`, and `ROOT`
for another machine.

## SPDK initiator with the DPA plugin

The RocksDB test drives the SAPS controller from `spdk_tgt`, which must be
built with the plugin initialization enabled (see
`integration/spdk/0001-saps-spdk-integration.patch`, `app/spdk_tgt`):

    make -C app/spdk_tgt DPA_PLUGIN=1 \
        DPA_PLUGIN_DIR=<repo>/src  DPA_PLUGIN_BUILD_DIR=<plugin build dir>
    cp build/bin/spdk_tgt build/bin/spdk_tgt.saps

`drive_saps.sh` uses `spdk_tgt.saps`; the log must contain
`SAPSQ_M enabled ... bypass_d=0 bypass_fsm=0 health_coupling_mode=0 health_source=0`.

## RocksDB (Section 4.9)

`rocksdb/` runs RocksDB 9.7.4 `db_bench readrandom` (8 threads, 60 s,
2,000,000 keys, 1 KB values, direct I/O, 8 MB block cache) on a read-only ext4
file system over SPDK's network block device. Three paths reach three DRAM
namespaces preloaded with the same 12 GiB image. Twenty seconds into each run,
path B gets a 5 ms delay (`D1`) or NVMe media-error status for reads (`D3`).

    bash drive_stock.sh   # stock round robin: healthy, D1, D3 x 3 runs
    bash drive_saps.sh    # SAPS (evaluated controller profile), same matrix
    python3 validate_rocksdb.py <stock_out>/full <saps_out>/full

Each driver reserves hugepages, starts the targets, builds or reuses the
database image, runs a smoke gate, and restores the hugepage count on exit.

## Detector baselines (Section 4.8)

`detector_baselines/` compares SAPS, fixed-threshold failover (500 us),
adaptive EWMA/CUSUM failover, and stock round robin with four 3:1:1:1 tenants,
4 KB random reads at QD32, three 300 KIOPS paths, and a 900 KIOPS service
limit. Path B's delay is set to 5 ms at 20 s of each 60 s run; the measurement
window is 23-49 s. See `detector_baselines/README.md`.

    sudo bash drive_campaign.sh --smoke --out <dir>
    sudo bash drive_campaign.sh --full  --out <dir>
