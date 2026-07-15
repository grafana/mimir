{
  // Make query-frontends compartment-aware.
  local perCompartmentQueryFrontendArgs = if !$._config.compartments_ingester_enabled then {} else $.mimirCompartmentsCommonArgs {
    // The query-frontend monitors the last produced offset of every read-compartment topic in every write
    // compartment's Kafka cluster (to enforce strong read consistency), so its address must target every
    // cluster via the '<write-compartment-id>' placeholder.
    'ingest-storage.kafka.address': $._config.compartments_ingest_storage_kafka_address,
  },

  query_frontend_args+:: perCompartmentQueryFrontendArgs,

  // Config validation.
  local queryFrontendCompartmentConfigError = if !$._config.compartments_ingester_enabled then null else $.validateMimirCompartmentsConfig([
    'query_frontend_deployment',
    'query_frontend_zone_a_deployment',
    'query_frontend_zone_b_deployment',
    'query_frontend_zone_c_deployment',
    'ruler_query_frontend_deployment',
    'ruler_query_frontend_zone_a_deployment',
    'ruler_query_frontend_zone_b_deployment',
    'ruler_query_frontend_zone_c_deployment',
  ]),
  assert queryFrontendCompartmentConfigError == null : queryFrontendCompartmentConfigError,
}
