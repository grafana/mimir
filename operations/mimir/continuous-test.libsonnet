local k = import 'ksonnet-util/kausal.libsonnet';

{
  _config+: {
    continuous_test_enabled: false,
    continuous_test_tenant_id: error 'you must configure the tenant ID to use for continuous testing',
    continuous_test_write_endpoint: error 'you must configure the write endpoint for continuous testing',
    continuous_test_read_endpoint: error 'you must configure the read endpoint for continuous testing',
  },

  local container = k.core.v1.container,
  local containerPort = k.core.v1.containerPort,
  local deployment = k.apps.v1.deployment,

  continuous_test_args:: {
    'tests.write-endpoint': $._config.continuous_test_write_endpoint,
    'tests.read-endpoint': $._config.continuous_test_read_endpoint,
    'tests.tenant-id': $._config.continuous_test_tenant_id,
    'tests.write-read-series-test.num-series': 1000,
    'tests.write-read-series-test.max-query-age': '48h',
  },

  continuous_test_node_affinity_matchers:: [],

  continuous_test_container::
    container.new('continuous-test', $._images.continuous_test) +
    container.withArgsMixin(k.util.mapToFlags($.continuous_test_args)) +
    container.withPorts([
      k.core.v1.containerPort.new('http-metrics', 8080),
    ]) +
    k.util.resourcesRequests('1', '512Mi') +
    k.util.resourcesLimits(null, '1Gi') +
    $.jaeger_mixin,

  continuous_test_deployment: if !$._config.continuous_test_enabled then null else
    deployment.new('continuous-test', 1, [$.continuous_test_container]) +
    $.newMimirNodeAffinityMatchers($.continuous_test_node_affinity_matchers) +
    // It doesn't make sense to run multiple continuous-test instances at the same time.
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(0) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1),

  continuous_test_pdb: if !$._config.continuous_test_enabled then null else
    $.newMimirPdb('continuous-test'),
}
