{
  // Make queriers compartment-aware.
  local perCompartmentQuerierArgs = if !$._config.compartments_ingester_enabled then {} else $.mimirCompartmentsCommonArgs,

  querier_args+:: perCompartmentQuerierArgs,
  querier_zone_a_args+:: perCompartmentQuerierArgs,
  querier_zone_b_args+:: perCompartmentQuerierArgs,
  querier_zone_c_args+:: perCompartmentQuerierArgs,
}
