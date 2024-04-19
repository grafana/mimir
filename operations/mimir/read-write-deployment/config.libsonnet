{
  _config+:: {
    mimir_write_replicas: 3,
    mimir_write_max_unavailable: 25,
    mimir_write_data_disk_size: '100Gi',
    mimir_write_data_disk_class: 'fast',
    mimir_write_allow_multiple_replicas_on_same_node: false,
    mimir_read_replicas: 2,
    mimir_read_topology_spread_max_skew: 1,
    mimir_backend_replicas: 3,
    mimir_backend_max_unavailable: 10,
    mimir_backend_data_disk_size: '250Gi',
    mimir_backend_data_disk_class: 'fast-dont-retain',
    mimir_backend_allow_multiple_replicas_on_same_node: false,

    // Query-scheduler ring-based service discovery is always enabled in the Mimir read-write deployment mode.
    query_scheduler_service_discovery_mode: if $._config.is_read_write_deployment_mode then 'ring' else super.query_scheduler_service_discovery_mode,

    // Overrides-exporter is part of the backend component in the Mimir read-write deployment mode.
    overrides_exporter_enabled: if $._config.is_read_write_deployment_mode then false else super.overrides_exporter_enabled,
    overrides_exporter_ring_enabled: if $._config.is_read_write_deployment_mode then true else super.overrides_exporter_enabled,
  },

  // Mimir read-write deployment mode makes some strong assumptions about what must enabled enabled and disabled.
  assert !$._config.is_read_write_deployment_mode || $._config.compactor_max_concurrency == 1
         : 'please set compactor_max_concurrency to 1 when using Mimir read-write deployment mode',

  assert !$._config.is_read_write_deployment_mode || $._config.multi_zone_ingester_enabled
         : 'please set multi_zone_ingester_enabled to true when using Mimir read-write deployment mode',

  assert !$._config.is_read_write_deployment_mode || $._config.multi_zone_store_gateway_enabled
         : 'please set multi_zone_store_gateway_enabled to true when using Mimir read-write deployment mode',

  assert $._config.is_microservices_deployment_mode || !$._config.autoscaling_querier_enabled
         : 'please set autoscaling_querier_enabled to false when using Mimir read-write deployment mode',

  assert $._config.is_microservices_deployment_mode || !$._config.ruler_remote_evaluation_enabled
         : 'please set ruler_remote_evaluation_enabled to false when using Mimir read-write deployment mode',

  assert $._config.is_microservices_deployment_mode || !$._config.overrides_exporter_enabled
         : 'please set overrides_exporter_enabled to false when using Mimir read-write deployment mode',

  assert !$._config.is_read_write_deployment_mode || $._config.memberlist_ring_enabled
         : 'please set memberlist_ring_enabled to true when using Mimir read-write deployment mode',
}
