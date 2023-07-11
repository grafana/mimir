{
  _config+:: {
    shuffle_sharding:: {
      // Shuffle sharding can be selectively enabled/disabled for each service.
      ingester_write_path_enabled: false,
      ingester_read_path_enabled: false,
      querier_enabled: false,
      ruler_enabled: false,
      store_gateway_enabled: false,

      // Default shard sizes. We want the shard size to be divisible by the number of zones.
      // We typically run 3 zones
      ingester_shard_size: 3,
      querier_shard_size: 10,
      store_gateway_shard_size: 3,
      ruler_shard_size: 2,

      // Target amount of series we want each shard to have per ingester.
      target_series_per_ingester: 100e3,
      // Target utilization of max_global_series_per_user per user.
      // For example, if we're setting 6M limit for users using 3M, this should be 50%.
      // Setting this lower than the actual usage will result in using more than target_series_per_ingester, and vice-versa.
      // This value should be changed gradually, to avoid a sudden increase in user's shard size (which would lower the local ingester limit).
      target_utilization_percentage: 50,
    },

    // Check if shuffle-sharding has been enabled.
    local shuffle_sharding_enabled =
      $._config.shuffle_sharding.ingester_write_path_enabled ||
      $._config.shuffle_sharding.ingester_read_path_enabled ||
      $._config.shuffle_sharding.querier_enabled ||
      $._config.shuffle_sharding.store_gateway_enabled ||
      $._config.shuffle_sharding.ruler_enabled,


    local roundUpToMultipleOfThree(n) = std.ceil(n / 3) * 3,

    // The ingesters shard size has been computed this way:
    // - Target for XX% utilisation of the user class (see $._config.target_utilization_percentage_for_overrides)
    // - Target each tenant to YYYK series / ingester (after replication) (see $._config.target_series_per_ingesters_for_overrides)
    // - Round up and ensure it's a multiple of 3 (so that it's multi-zone ready)
    //
    // Eg. small class = 1M series = ceil(((1M * 3) * 50%) / 100K) = 15
    //
    // We're setting ingestion_tenant_shard_size based on a fixed series number for that user class rather than depending on max_global_series_per_user
    // because we don't want the shards to change during temporary limits increases.
    local ingesterTenantShardSize(series) = std.max(
      $._config.shuffle_sharding.ingester_shard_size,
      roundUpToMultipleOfThree(series * 3 * $._config.shuffle_sharding.target_utilization_percentage / 100 / $._config.shuffle_sharding.target_series_per_ingester)
    ),

    overrides+:: if !shuffle_sharding_enabled then {} else {
      // Use defaults for this user class.
      extra_small_user+:: {},

      // Target 300K active series.
      medium_small_user+:: {
        ingestion_tenant_shard_size: ingesterTenantShardSize(300e3),
        store_gateway_tenant_shard_size: std.max(3, $._config.shuffle_sharding.store_gateway_shard_size),
        ruler_tenant_shard_size: std.max(2, $._config.shuffle_sharding.ruler_shard_size),
      },

      // Target 1M active series.
      small_user+:: {
        ingestion_tenant_shard_size: ingesterTenantShardSize(1e6),
        store_gateway_tenant_shard_size: std.max(6, $._config.shuffle_sharding.store_gateway_shard_size),
        ruler_tenant_shard_size: std.max(2, $._config.shuffle_sharding.ruler_shard_size),
      },

      // Target 3M active series.
      medium_user+:: {
        ingestion_tenant_shard_size: ingesterTenantShardSize(3e6),
        store_gateway_tenant_shard_size: std.max(9, $._config.shuffle_sharding.store_gateway_shard_size),
        ruler_tenant_shard_size: std.max(2, $._config.shuffle_sharding.ruler_shard_size),
      },

      // Target 6M active series.
      big_user+:: {
        ingestion_tenant_shard_size: ingesterTenantShardSize(6e6),
        store_gateway_tenant_shard_size: std.max(12, $._config.shuffle_sharding.store_gateway_shard_size),
        ruler_tenant_shard_size: std.max(3, $._config.shuffle_sharding.ruler_shard_size),
      },

      // Target 12M active series.
      super_user+:: {
        ingestion_tenant_shard_size: ingesterTenantShardSize(12e6),
        store_gateway_tenant_shard_size: std.max(18, $._config.shuffle_sharding.store_gateway_shard_size),
        ruler_tenant_shard_size: std.max(6, $._config.shuffle_sharding.ruler_shard_size),
      },

      // Target 16M active series.
      mega_user+:: {
        ingestion_tenant_shard_size: ingesterTenantShardSize(16e6),
        store_gateway_tenant_shard_size: std.max(24, $._config.shuffle_sharding.store_gateway_shard_size),
        ruler_tenant_shard_size: std.max(8, $._config.shuffle_sharding.ruler_shard_size),
      },

      // Target 24M active series.
      user_24M+:: {
        ingestion_tenant_shard_size: ingesterTenantShardSize(24e6),
        store_gateway_tenant_shard_size: std.max(30, $._config.shuffle_sharding.store_gateway_shard_size),
        ruler_tenant_shard_size: std.max(8, $._config.shuffle_sharding.ruler_shard_size),
      },

      // Target 32M active series.
      user_32M+:: {
        ingestion_tenant_shard_size: ingesterTenantShardSize(32e6),
        store_gateway_tenant_shard_size: std.max(42, $._config.shuffle_sharding.store_gateway_shard_size),
        ruler_tenant_shard_size: std.max(12, $._config.shuffle_sharding.ruler_shard_size),
      },
    },
  },

  distributor_args+:: if !$._config.shuffle_sharding.ingester_write_path_enabled then {} else {
    'distributor.ingestion-tenant-shard-size': $._config.shuffle_sharding.ingester_shard_size,
  },

  ingester_args+:: if !$._config.shuffle_sharding.ingester_write_path_enabled then {} else {
    // The shuffle sharding configuration is required on ingesters too because of global limits.
    'distributor.ingestion-tenant-shard-size': $._config.shuffle_sharding.ingester_shard_size,
  },

  query_frontend_args+:: if !$._config.shuffle_sharding.querier_enabled then {} else {
    'query-frontend.max-queriers-per-tenant': $._config.shuffle_sharding.querier_shard_size,
  },

  query_scheduler_args+:: if !$._config.shuffle_sharding.querier_enabled then {} else {
    'query-frontend.max-queriers-per-tenant': $._config.shuffle_sharding.querier_shard_size,
  },

  querier_args+:: (
    if !$._config.shuffle_sharding.store_gateway_enabled then {} else {
      'store-gateway.tenant-shard-size': $._config.shuffle_sharding.store_gateway_shard_size,
    }
  ) + (
    if !($._config.shuffle_sharding.ingester_write_path_enabled && !$._config.shuffle_sharding.ingester_read_path_enabled) then {} else {
      // If shuffle sharding is enabled for the write path but isn't enabled for the read path, Mimir will query all ingesters
      'querier.shuffle-sharding-ingesters-enabled': 'false',
    }
  ) + (
    if !$._config.shuffle_sharding.ingester_read_path_enabled then {} else {
      'distributor.ingestion-tenant-shard-size': $._config.shuffle_sharding.ingester_shard_size,
    }
  ),

  store_gateway_args+:: if !$._config.shuffle_sharding.store_gateway_enabled then {} else {
    'store-gateway.tenant-shard-size': $._config.shuffle_sharding.store_gateway_shard_size,
  },

  ruler_args+:: (
    if !$._config.shuffle_sharding.ruler_enabled then {} else {
      'ruler.tenant-shard-size': $._config.shuffle_sharding.ruler_shard_size,
    }
  ) + (
    if !$._config.shuffle_sharding.ingester_write_path_enabled then {} else {
      // Required because the ruler directly writes to ingesters.
      'distributor.ingestion-tenant-shard-size': $._config.shuffle_sharding.ingester_shard_size,
    }
  ) + (
    if !$._config.shuffle_sharding.ingester_read_path_enabled then {} else {
      'distributor.ingestion-tenant-shard-size': $._config.shuffle_sharding.ingester_shard_size,
    }
  ) + (
    if !($._config.shuffle_sharding.ingester_write_path_enabled && !$._config.shuffle_sharding.ingester_read_path_enabled) then {} else {
      // If shuffle sharding is enabled for the write path but isn't enabled for the read path, Mimir will query all ingesters
      'querier.shuffle-sharding-ingesters-enabled': 'false',
    }
  ) + (
    if !$._config.shuffle_sharding.store_gateway_enabled then {} else {
      'store-gateway.tenant-shard-size': $._config.shuffle_sharding.store_gateway_shard_size,
    }
  ),
}
