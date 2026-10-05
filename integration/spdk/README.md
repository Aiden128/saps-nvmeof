# SPDK integration

`0001-saps-spdk-integration.patch` contains every SPDK change used in the
evaluation reported in the paper. It applies to upstream SPDK commit
`a83e52f1da18807e21b552a0fe35057f8e9ea586` (14 April 2026) with
`git apply`.

The patch covers:

- `lib/nvme/host_saps.c`, `lib/nvme/nvme_rdma.c`: submit and completion hooks
  that feed the DPA controller and enforce the committed tenant-path budgets.
- `module/bdev/nvme/bdev_nvme.c`: the multipath selector, which chooses among
  paths in proportion to the committed budget row and limits probe-only paths
  to one in-flight request.
- `lib/nvmf/*`, `module/bdev/delay`, `module/bdev/error`, and the new
  `module/bdev/bimodal_delay`: target-side fault injection (service caps,
  injected delay, bimodal tail delay, and injected completion status) with
  their RPCs.
- `app/*`, `examples/bdev/bdevperf`, `mk/*`: build integration for the DPA
  plugin, gated by `DPA_PLUGIN=1`.

## Apply and build

```bash
git clone https://github.com/spdk/spdk.git && cd spdk
git checkout a83e52f1da18807e21b552a0fe35057f8e9ea586
git submodule update --init
git apply /path/to/saps-nvmeof/integration/spdk/0001-saps-spdk-integration.patch
./configure --with-rdma
make -j"$(nproc)" DPA_PLUGIN=1 \
     SAPS_FLEXIO_SAMPLES=/path/to/doca/flexio_build/samples
```

`SAPS_FLEXIO_SAMPLES` must point to a DOCA FlexIO samples tree in which
`dpa_plugin/` is a copy of this repository's `src/` and `build/` holds its
Meson output (`build/dpa_plugin/host/libdpa_plugin.a`,
`build/dpa_plugin/dev/dpa_plugin_app.a`) together with the samples'
`build/common/host/libcommon_host.a`. Builds without `DPA_PLUGIN=1` do not
need this variable.

The patch was checked with `git apply --check` against the base commit above.
It replaces the excerpts previously kept in `integration/spdk-patches/`, which
summarized the hook points but could not be applied.
