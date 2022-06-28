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
    },

    // Check if shuffle-sharding has been enabled.
    local shuffle_sharding_enabled =
      $._config.shuffle_sharding.ingester_write_path_enabled ||
      $._config.shuffle_sharding.ingester_read_path_enabled ||
      $._config.shuffle_sharding.querier_enabled ||
      $._config.shuffle_sharding.store_gateway_enabled ||
      $._config.shuffle_sharding.ruler_enabled,

    // The ingesters shard size has been computed this way:
    // - Target for 50% utilisation of the user class
    // - Target each tenant to 100K series / ingester (after replication)
    // - Round up and ensure it's a multiple of 3 (so that it's multi-zone ready)
    //
    // Eg. small class = 1M series = ceil(((1M * 3) * 50%) / 100K) = 15
    //
    overrides+:: if !shuffle_sharding_enabled then {} else {
      // Use defaults for this user class.
      extra_small_user+:: {},

      // Target 300K active series.
      medium_small_user+:: {
        ingestion_tenant_shard_size: 6,
        store_gateway_tenant_shard_size: 3,
        ruler_tenant_shard_size: 2,
      },

      // Target 1M active series.
      small_user+:: {
        ingestion_tenant_shard_size: 15,
        store_gateway_tenant_shard_size: 6,
        ruler_tenant_shard_size: 2,
      },

      // Target 3M active series.
      medium_user+:: {
        ingestion_tenant_shard_size: 45,
        store_gateway_tenant_shard_size: 9,
        ruler_tenant_shard_size: 2,
      },

      // Target 6M active series.
      big_user+:: {
        ingestion_tenant_shard_size: 90,
        store_gateway_tenant_shard_size: 12,
        ruler_tenant_shard_size: 3,
      },

      // Target 12M active series.
      super_user+:: {
        ingestion_tenant_shard_size: 180,
        store_gateway_tenant_shard_size: 18,
        ruler_tenant_shard_size: 6,
      },

      // Target 16M active series.
      mega_user+:: {
        ingestion_tenant_shard_size: 240,
        store_gateway_tenant_shard_size: 24,
        ruler_tenant_shard_size: 8,
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
