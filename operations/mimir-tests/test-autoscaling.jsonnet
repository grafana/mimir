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

    autoscaling_alertmanager_enabled: true,
    autoscaling_alertmanager_min_replicas: 3,
    autoscaling_alertmanager_max_replicas: 30,

    autoscaling_querier_enabled: true,
    autoscaling_querier_min_replicas: 3,
    autoscaling_querier_max_replicas: 30,
    autoscaling_querier_predictive_scaling_enabled: true,

    autoscaling_ruler_querier_enabled: true,
    autoscaling_ruler_querier_min_replicas: 3,
    autoscaling_ruler_querier_max_replicas: 30,

    autoscaling_distributor_enabled: true,
    autoscaling_distributor_min_replicas: 3,
    autoscaling_distributor_max_replicas: 30,

    autoscaling_query_frontend_enabled: true,
    autoscaling_query_frontend_min_replicas: 3,
    autoscaling_query_frontend_max_replicas: 30,

    autoscaling_ruler_query_frontend_enabled: true,
    autoscaling_ruler_query_frontend_min_replicas: 3,
    autoscaling_ruler_query_frontend_max_replicas: 30,

    autoscaling_ruler_enabled: true,
    autoscaling_ruler_min_replicas: 2,
    autoscaling_ruler_max_replicas: 10,
  },

  local k = import 'ksonnet-util/kausal.libsonnet',
  distributor_container+::
    // Test a non-integer memory request, to verify that this gets converted into an integer for
    // the KEDA threshold
    k.util.resourcesRequests(2, '3.2Gi') +
    k.util.resourcesLimits(null, '6Gi'),
  query_frontend_container+::
    // Test a CPU request set in milli-CPUs to verify this gets converted into an integer for
    // the KEDA threshold.
    k.util.resourcesRequests('2500m', '600Mi'),
  ruler_querier_container+::
    // Test a <1 non-integer CPU request, to verify that this gets converted into an integer for
    // the KEDA threshold
    // Also specify CPU request as a string to make sure it works
    k.util.resourcesRequests('0.2', '1Gi'),

  // Test ScaledObject with metric_type.
  test_scaled_object: $.newScaledObject('test', $._config.namespace, {
    min_replica_count: 20,
    max_replica_count: 30,

    triggers: [
      {
        metric_name: 'cortex_test_hpa_%s' % $._config.namespace,
        metric_type: 'Value',  // This is what we're testing.
        query: 'some_query_goes_here',
        threshold: '123',
      },
      {
        metric_name: 'cortex_test_hpa_%s_ignore_null_values_false' % $._config.namespace,
        query: 'query',
        threshold: '123',
        ignore_null_values: false,  // Boolean is supported, and converted to string.
      },
      {
        metric_name: 'cortex_test_hpa_%s_ignore_null_values_true' % $._config.namespace,
        query: 'query',
        threshold: '123',
        ignore_null_values: 'true',  // String is supported, and left as-is.
      },
    ],
  }),
}
