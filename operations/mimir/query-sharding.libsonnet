{
  _config+: {
    query_sharding_enabled: false,

    // Raise the msg size for the GRPC messages query-frontend <-> querier by this factor when query sharding is enabled
    query_sharding_msg_size_factor: 4,

    overrides+: if $._config.query_sharding_enabled then {
      // Target 6M active series.
      big_user+:: {
        query_sharding_total_shards: 8,
      },

      // Target 12M active series.
      super_user+:: {
        query_sharding_total_shards: 8,
      },

      // Target 16M active series.
      mega_user+:: {
        query_sharding_total_shards: 8,
      },
    }
    else {},
  },

  // When sharding is enabled, scale the query-frontend to have at least 20% of the replicas of queriers.
  local ensure_replica_ratio_to_queriers = {
    // We check if replicas is set because it may be missing if queriers are autoscaling.
    local min_replicas = if std.objectHas($.querier_deployment.spec, 'replicas') && $.querier_deployment.spec.replicas != null then
      std.max(std.floor(0.2 * $.querier_deployment.spec.replicas), 2)
    else
      2,

    spec+: {
      replicas: std.max(super.replicas, min_replicas),
    },
  },
  query_frontend_deployment+: if $._config.query_sharding_enabled then ensure_replica_ratio_to_queriers else {},

  query_frontend_args+:: if !$._config.query_sharding_enabled then {} else
    // When sharding is enabled, query-frontend runs PromQL engine internally.
    $._config.queryEngineConfig {
      'query-frontend.parallelize-shardable-queries': true,
      // disable query sharding by default for all tenants
      'query-frontend.query-sharding-total-shards': 0,
      // Adjust max query parallelism to 16x sharding, without sharding we can run a full 15d queries in parallel.
      // 15d * 16 shards = 240 subqueries.
      'querier.max-query-parallelism': 240,

      'query-frontend.query-sharding-max-sharded-queries': 128,

      'server.grpc-max-recv-msg-size-bytes': super['server.grpc-max-recv-msg-size-bytes'] * $._config.query_sharding_msg_size_factor,
    },

  query_scheduler_args+:: if !$._config.query_sharding_enabled then {} else {
    // Query sharding generates a higher order of magnitude of requests.
    'query-scheduler.max-outstanding-requests-per-tenant': 800,
  },

  querier_args+:: if !$._config.query_sharding_enabled then {} else {
    // The expectation is that if sharding is enabled, we would run more but smaller
    // queries on the queriers. However this can't be extended too far because several
    // queries (including instant queries) can't be sharded. Therefore, we must strike a balance
    // which allows us to process more sharded queries in parallel when requested, but not overload
    // queriers during normal queries.
    'querier.max-concurrent': 16,

    // Raise the msg size for the GRPC messages query-frontend <-> querier by a factor when query sharding is enabled
    'querier.frontend-client.grpc-max-send-msg-size': super['querier.frontend-client.grpc-max-send-msg-size'] * $._config.query_sharding_msg_size_factor,
  },
}
