# Runtime configuration

The current controller is enabled with the `SAPSQ_*` variables below.
Identifiers keep the historical `sapsq` prefix for compatibility with
experiment manifests.

## Required controller settings

| Variable | Meaning |
| --- | --- |
| `SAPSQ_ENABLED` | Set to `1` to enable the current SAPS scheduler. |
| `SAPSQ_MY_TENANT_ID` | Zero-based tenant row used by the current process. |
| `SAPSQ_NUM_TENANTS` | Number of active tenant rows. The implementation supports up to 16. |
| `SAPSQ_NUM_PATHS` | Number of logical paths. The implementation supports up to 8. |
| `SAPSQ_LINK_CAP_IOPS` | Namespace service envelope `C`, in IOPS. |
| `SAPSQ_PATH_CAP_IOPS` | Comma-separated provisioned path capacities `K_p`, in IOPS. Exactly one positive value is required per path. |
| `SAPSQ_WEIGHTS` | Comma-separated positive tenant weights. Unspecified weights default to one. |

A controller configuration without `SAPSQ_PATH_CAP_IOPS` is rejected. The
implementation does not infer a missing path capacity from the namespace
envelope.

## Timing and sampling

| Variable | Default | Meaning |
| --- | ---: | --- |
| `SAPSQ_EPOCH_PERIOD_US` | `1000` | Allocation epoch in microseconds. |
| `SAPSQ_PROBE_RATE_IOPS` | `1000` | Per-tenant recovery-probe allowance before it is divided among probe paths. |
| `SAPSQ_HOST_TSC_FREQ` | architectural counter | Host counter frequency used for rate conversion. |
| `DPA_PLUGIN_SAMPLE_RATE` | `1` | Observe one completion in every N completions. |

The shared-ring size is a compile-time setting controlled by
`DPA_PLUGIN_RING_LOG2` in `src/dpa_plugin_com.h`.

## Path identity and process coordination

| Variable | Meaning |
| --- | --- |
| `DPA_PLUGIN_ROLE` | `coordinator` creates the shared ring. `tenant` attaches to it. |
| `DPA_PLUGIN_SOCK` | Unix-domain socket used to exchange the shared-memory descriptor. |
| `DPA_PLUGIN_PATH_MAP_PORTS` | Explicit transport-port to logical-path mapping, such as `4430:0,4431:1,4432:2`. |
| `DPA_PLUGIN_NOTIFY` | Enables submission and completion publication. |
| `DPA_PLUGIN_ADMISSION` | Enables host-side consumption of committed rate budgets. |

## Controlled experiment modes

These variables define comparison arms. They are not separate production
controllers.

| Variable | Values | Meaning |
| --- | --- | --- |
| `SAPSQ_HEALTH_COUPLING_MODE` | `continuous`, `fixed`, `binary` | Selects graded HCAA, nominal-envelope allocation, or binary exclusion. |
| `SAPSQ_HEALTH_SOURCE` | `completion`, `queue_depth`, `request_rtt` | Selects the observation consumed by the same allocator. |
| `SAPSQ_BYPASS_D_CLASSIFIER` | `0` or `1` | Forces all paths healthy when set to one. |
| `SAPSQ_BYPASS_SAPS_FSM` | `0` or `1` | Disables the completion-driven state machine when set to one. |
| `SAPSQ_BYPASS_HEALTH_COUPLING` | `0` or `1` | Legacy alias for fixed-envelope comparison when no explicit mode is set. |
| `SAPSQ_BYPASS_BUDGET_SELECTION` | `0` or `1` | Bypasses the committed-budget path selector for an ablation. |

The paper campaigns build complete environment profiles in their driver code.
Use those profiles when reproducing a figure instead of setting individual
switches by hand.

## Legacy variables

The source retains older `SAPS_Q_*` and `SAPS_M*` variables because earlier
prototype modes share the same library. They are not used by the current paper
campaigns unless a comparison driver sets them explicitly.
