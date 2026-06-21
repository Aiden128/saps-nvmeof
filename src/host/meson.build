static_library('dpa_plugin',
	['dpa_plugin.c'],
	c_args: ['-DDEV_APP_NAME='+dev_app_name, '-fPIC'],
	native: true,
	install: false,
	dependencies: [ibverbs_dep, mlx5_dep, flexio_dep, common_host_dep],
	link_depends: sample_dev_target
)

executable('dpa_plugin_smoke',
	['dpa_plugin_smoke.c', 'dpa_plugin.c'],
	c_args: '-DDEV_APP_NAME='+dev_app_name,
	native: true,
	dependencies: [ibverbs_dep, mlx5_dep, flexio_dep, common_host_dep, sample_device_dep],
	link_depends: sample_dev_target
)

# E-4 multi-tenant IPC smoke (coordinator + N tenant processes)
executable('dpa_plugin_ipc_smoke',
	['dpa_plugin_ipc_smoke.c', 'dpa_plugin.c'],
	c_args: '-DDEV_APP_NAME='+dev_app_name,
	native: true,
	dependencies: [ibverbs_dep, mlx5_dep, flexio_dep, common_host_dep, sample_device_dep],
	link_depends: sample_dev_target
)

# M2 v2 host-side joint-anomaly classifier unit test (2026-05-20)
executable('dpa_plugin_m2v2_test',
	['dpa_plugin_m2v2_test.c', 'dpa_plugin.c'],
	c_args: '-DDEV_APP_NAME='+dev_app_name,
	native: true,
	dependencies: [ibverbs_dep, mlx5_dep, flexio_dep, common_host_dep, sample_device_dep],
	link_depends: sample_dev_target
)
