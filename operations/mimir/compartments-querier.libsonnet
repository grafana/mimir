{
  // Make queriers compartment-aware.
  local perCompartmentQuerierArgs = if !$._config.compartments_ingester_enabled then {} else $.mimirCompartmentsCommonArgs {
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
}
