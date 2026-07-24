{
  local isEnabled = $._config.compartments_enabled,

  // Ruler and zonal query-scheduler args are derived from this field and pick up the mixin via late binding.
  query_scheduler_args+:: if !isEnabled then {} else $.mimirCompartmentsCommonArgs,

  // Config validation.
  local querySchedulerCompartmentConfigError = if !isEnabled then null else $.validateMimirCompartmentsConfig([
    'query_scheduler_deployment',
    'query_scheduler_zone_a_deployment',
    'query_scheduler_zone_b_deployment',
    'query_scheduler_zone_c_deployment',
    'ruler_query_scheduler_deployment',
    'ruler_query_scheduler_zone_a_deployment',
    'ruler_query_scheduler_zone_b_deployment',
    'ruler_query_scheduler_zone_c_deployment',
  ]),
  assert querySchedulerCompartmentConfigError == null : querySchedulerCompartmentConfigError,
}
