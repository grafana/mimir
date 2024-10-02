// This is the initial state of Mimir namespace, before getting migrated to ingest storage.
(import 'test-base.jsonnet') {
  _config+:: {
    cluster: 'test-cluster',

    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    // Configure features required by ingest storage migration.
    ruler_remote_evaluation_enabled: true,

    // Copied from grafana-mimir.libsonnet.
    // Enable v2 ingester automated downscale unless ingest storage or read-write mode are enabled (they are incompatible).
    ingester_automated_downscale_v2_enabled: !$._config.ingest_storage_enabled && !$._config.is_read_write_deployment_mode,
    // Deploy an HPA in cells with ingester_automated_downscale_v2_enabled enabled
    new_ingester_hpa_enabled: $._config.ingester_automated_downscale_v2_enabled,
    // Disable v1 ingester automated downscale if v2 is enabled, or ingest storage autoscaling is enabled.
    ingester_automated_downscale_enabled: !$._config.ingester_automated_downscale_v2_enabled && !$._config.ingest_storage_ingester_autoscaling_ingester_annotations_enabled,

    _config+:: {
      config_api_enabled: true,
    },
    config_api_enabled: true,

    limits_operator+: {
      deploy: true,
      mount_configmap_in_mimir: true,

      dry_run: false,  // dry-run will not write the configmap
      observe_config_api: true,

      overrides_configmap_name: 'limits-operator-overrides',
      overrides_configmap_filename: 'overrides.json',
      overrides_mountpoint: '/etc/%s' % $._config.limits_operator.overrides_configmap_name,
      overrides_path: '%s/%s' % [$._config.limits_operator.overrides_mountpoint, $._config.limits_operator.overrides_configmap_filename],

      trigger_enabled: true,
    },

    shuffle_sharding+:: {
      ingester_write_path_enabled: true,
      ingester_read_path_enabled: true,
      querier_enabled: true,
      ruler_enabled: true,
      store_gateway_enabled: true,
    },
  },
}

{
  _config+:: {
    // Add limits-operator metric for Sigyn ingester ScaledObject.
    // On cleanup please move this next to new_ingester_hpa_triggers in limits-operator.libsonnet.
    ingest_storage_ingester_autoscaling_triggers+: if $._config.limits_operator.deploy && $._config.limits_operator.trigger_enabled then [$._config.limits_operator_scaling_trigger] else [],

    // Reduce stabilization window for scale ups from 30m to 30m, so that limits-operator doesn't need to
    // wait for too long before it gets its ingesters.
    // On cleanup please move this into ingest-storage-custom.libsonnet, or upstream to Mimir.
    ingest_storage_ingester_autoscaling_scale_up_stabilization_window_seconds: $.util.parseDuration('10m'),
  },
}
