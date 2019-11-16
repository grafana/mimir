{
  local container = $.core.v1.container,

  table_manager_args::
    $._config.storageConfig
    {
      target: 'table-manager',

      // Cassandra / BigTable doesn't use these fields, so set them to zero
      'dynamodb.chunk-table.inactive-read-throughput': 0,
      'dynamodb.chunk-table.inactive-write-throughput': 0,
      'dynamodb.chunk-table.read-throughput': 0,
      'dynamodb.chunk-table.write-throughput': 0,
      'dynamodb.periodic-table.inactive-read-throughput': 0,
      'dynamodb.periodic-table.inactive-write-throughput': 0,
      'dynamodb.periodic-table.read-throughput': 0,
      'dynamodb.periodic-table.write-throughput': 0,

      // Rate limit Bigtable Admin calls.  Google seem to limit to ~100QPS,
      // and given 2yrs worth of tables (~100) a sync will table 20s.  This
      // allows you to run upto 20 independant Cortex clusters on the same
      // Google project before running into issues.
      'dynamodb.poll-interval': '10m',
      'dynamodb.periodic-table.grace-period': '3h',
      'bigtable.grpc-client-rate-limit': 5.0,
      'bigtable.grpc-client-rate-limit-burst': 5,
      'bigtable.backoff-on-ratelimits': true,
      'bigtable.table-cache.enabled': true,
    },

  table_manager_container::
    if $._config.table_manager_enabled then
      container.new('table-manager', $._images.tableManager) +
      container.withPorts($.util.defaultPorts) +
      container.withArgsMixin($.util.mapToFlags($.table_manager_args)) +
      $.util.resourcesRequests('100m', '100Mi') +
      $.util.resourcesLimits('200m', '200Mi') +
      $.jaeger_mixin
    else {},

  local deployment = $.apps.v1beta1.deployment,

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
