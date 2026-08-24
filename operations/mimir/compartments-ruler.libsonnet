{
  _config+:: {
    compartments_ruler_enabled: $._config.compartments_enabled,
  },

  assert !$._config.compartments_ruler_enabled || $._config.compartments_distributor_enabled
         : 'compartments_ruler_enabled requires compartments_distributor_enabled',

  local isEnabled = $._config.compartments_ruler_enabled,

  rulerDistributorZoneAddress(zone)::
    'dns:///distributor-zone-%s.%s.svc.%s:9095' % [zone, $._config.namespace, $._config.cluster_domain],

  local rulerCompartmentArgs(zone) =
    $.mimirCompartmentsCommonArgs {
      // Rulers are global, so keep the placeholder needed to address every read compartment.
      [$.mimirBlocksStorageBucketNameFlag]: $._config.compartments_blocks_storage_bucket_name,
      'ingest-storage.kafka.address': $._config.compartments_ingest_storage_kafka_address,
      'ruler.distributor.address': $.rulerDistributorZoneAddress(zone),
      'ruler.distributor.remote-timeout': '10s',
    },

  ruler_args+:: if !isEnabled then {} else rulerCompartmentArgs('a'),
  ruler_zone_a_args+:: if !isEnabled then {} else rulerCompartmentArgs('a'),
  ruler_zone_b_args+:: if !isEnabled then {} else rulerCompartmentArgs('b'),
  ruler_zone_c_args+:: if !isEnabled then {} else rulerCompartmentArgs('c'),

  // Config validation.
  local rulerCompartmentConfigError = if !isEnabled then null else $.validateMimirCompartmentsConfig([
    'ruler_deployment',
    'ruler_zone_a_deployment',
    'ruler_zone_b_deployment',
    'ruler_zone_c_deployment',
  ]),
  assert rulerCompartmentConfigError == null : rulerCompartmentConfigError,
}
