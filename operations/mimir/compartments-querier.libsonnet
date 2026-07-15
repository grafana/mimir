{
  // Make queriers compartment-aware.
  local perCompartmentQuerierArgs = if !$._config.compartments_ingester_enabled then {} else $.mimirCompartmentsCommonArgs {
    // Queriers inherit this otherwise unused setting through commonConfig. Keep it consistent with
    // compartments so shared config validation can verify the rendered workload.
    'ingest-storage.kafka.address': $._config.compartments_ingest_storage_kafka_address,

    // The querier reads each read compartment's dedicated blocks bucket, resolving the
    // '<read-compartment-id>' placeholder per compartment. This overrides the single bucket name set by
    // blocksStorageConfig, and is required: with compartments enabled the querier fails to start unless
    // the blocks bucket name contains the placeholder.
    [$.mimirBlocksStorageBucketNameFlag]: $._config.compartments_blocks_storage_bucket_name,
  },

  querier_args+:: perCompartmentQuerierArgs,
  querier_zone_a_args+:: perCompartmentQuerierArgs,
  querier_zone_b_args+:: perCompartmentQuerierArgs,
  querier_zone_c_args+:: perCompartmentQuerierArgs,

  // Config validation.
  local querierCompartmentConfigError = if !$._config.compartments_ingester_enabled then null else $.validateMimirCompartmentsConfig([
    'querier_deployment',
    'querier_zone_a_deployment',
    'querier_zone_b_deployment',
    'querier_zone_c_deployment',
    'ruler_querier_deployment',
    'ruler_querier_zone_a_deployment',
    'ruler_querier_zone_b_deployment',
    'ruler_querier_zone_c_deployment',
  ]),
  assert querierCompartmentConfigError == null : querierCompartmentConfigError,
}
