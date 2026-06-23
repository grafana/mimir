{
  _config+:: {
    // Whether the compartments architecture is enabled.
    compartments_enabled: false,

    // Number of read and write compartments.
    compartments_read_count: 1,
    compartments_write_count: 1,

    // Kafka topic produced/consumed per read compartment. Must contain the '<read-compartment-id>'
    // placeholder, which Mimir replaces with the read compartment index at runtime.
    compartments_ingest_storage_kafka_topic: 'ingest-rc-<read-compartment-id>',
  },

  assert $._config.compartments_read_count >= 1 : 'compartments_read_count must be >= 1',
  assert $._config.compartments_write_count >= 1 : 'compartments_write_count must be >= 1',

  assert !$._config.compartments_enabled || $._config.ruler_remote_evaluation_enabled
         : 'compartments require ruler remote rule evaluation (ruler_remote_evaluation_enabled)',

  // Common CLI args shared by every compartment-aware Mimir component.
  mimirCompartmentsCommonArgs:: {
    'compartments.enabled': true,
    'compartments.read.num-compartments': $._config.compartments_read_count,
    'compartments.write.num-compartments': $._config.compartments_write_count,
    'ingest-storage.kafka.topic': $._config.compartments_ingest_storage_kafka_topic,
  },

  // Apply a mixin to every compartment entry in a compartments resource map.
  // Pass super.fieldName as compartmentMap to match the established super-in-for-clause convention.
  // mixin can be either a static object applied to all compartments, or a function(compartmentIdx)
  // that returns a per-compartment mixin.
  // Example: foo_zone_a_deployments+: $.mimirCompartmentsOverrides(super.foo_zone_a_deployments, someMixin),
  // Example: foo_zone_a_compartments_args+: $.mimirCompartmentsOverrides(super.foo_zone_a_compartments_args, function(compartmentIdx) { 'compartment-id': compartmentIdx }),
  mimirCompartmentsOverrides(compartmentMap, mixin):: {
    [compartmentKey]+: if std.isFunction(mixin) then mixin(std.parseInt(std.split(compartmentKey, '_')[1])) else mixin
    for compartmentKey in std.objectFields(compartmentMap)
  },

  // Creates a { compartment_0: …, compartment_1: … } resource map when enabled, otherwise returns {}.
  // fn is called with the compartment index (integer) for each compartment in [0, numCompartments).
  // Usage: foo_zone_a_deployments: $.mimirCompartmentsCreateIf(isEnabled && isZoneAEnabled, numCompartments,
  //          function(compartment) $.newFooCompartmentDeployment('a', compartment, …)),
  mimirCompartmentsCreateIf(enabled, numCompartments, fn):: if !enabled then {} else {
    ['compartment_%d' % compartment]: fn(compartment)
    for compartment in std.range(0, numCompartments - 1)
  },
}
