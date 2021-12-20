{
  local container = $.core.v1.container,

  table_manager_args::
    $._config.storageConfig
    {
      target: 'table-manager',

      // Rate limit Bigtable Admin calls.  Google seem to limit to ~100QPS,
      // and given 2yrs worth of tables (~100) a sync will table 20s.  This
      // allows you to run upto 20 independant Cortex clusters on the same
      // Google project before running into issues.
      'bigtable.grpc-client-rate-limit': 5.0,
      'bigtable.grpc-client-rate-limit-burst': 5,
      'bigtable.backoff-on-ratelimits': true,
      'bigtable.table-cache.enabled': true,
      'table-manager.poll-interval': '10m',
      'table-manager.periodic-table.grace-period': '3h',
    },

  table_manager_container::
    if $._config.table_manager_enabled then
      container.new('table-manager', $._images.tableManager) +
      container.withPorts($.util.defaultPorts) +
      container.withArgsMixin($.util.mapToFlags($.table_manager_args)) +
      $.util.resourcesRequests('100m', '100Mi') +
      $.util.resourcesLimits('200m', '200Mi') +
      $.util.readinessProbe +
      $.jaeger_mixin
    else {},

  local deployment = $.apps.v1.deployment,

  table_manager_deployment:
    if $._config.table_manager_enabled then
      deployment.new('table-manager', 1, [$.table_manager_container]) +
      $.storage_config_mixin
    else {},

  table_manager_service:
    if $._config.table_manager_enabled then
      $.util.serviceFor($.table_manager_deployment)
    else {},
}
