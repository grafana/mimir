local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    ruler_enabled: true,
    ruler_remote_evaluation_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',

    alertmanager_enabled: true,
    alertmanager_storage_bucket_name: 'alerts-bucket',

    autoscaling_querier_enabled: true,
    autoscaling_querier_min_replicas: 3,
    autoscaling_querier_max_replicas: 30,

    autoscaling_ruler_querier_enabled: true,
    autoscaling_ruler_querier_min_replicas: 3,
    autoscaling_ruler_querier_max_replicas: 30,

    autoscaling_distributor_enabled: true,
    autoscaling_distributor_min_replicas: 3,
    autoscaling_distributor_max_replicas: 30,
  },

  local k = import 'ksonnet-util/kausal.libsonnet',
  distributor_container+::
    // Test a non-integer memory request, to verify that this gets converted into an integer for
    // the KEDA threshold
    k.util.resourcesRequests(2, '3.2Gi') +
    k.util.resourcesLimits(null, '6Gi'),
  ruler_querier_container+::
    // Test a <1 non-integer CPU request, to verify that this gets converted into an integer for
    // the KEDA threshold
    // Also specify CPU request as a string to make sure it works
    k.util.resourcesRequests('0.2', '1Gi'),
}
