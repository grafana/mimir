local jsonpath = import 'github.com/jsonnet-libs/xtd/jsonpath.libsonnet';

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

  // Kafka cluster address template for compartment components. Carries the '<write-compartment-id>'
  // placeholder, which Mimir replaces with the write compartment index at runtime (distributors target
  // their own cluster; ingesters consume from every write compartment's cluster). Mimir's jsonnet does
  // not deploy Kafka, so this is a fictitious address mirroring the non-compartment default.
  compartments_ingest_storage_kafka_address:: 'kafka-wc-<write-compartment-id>.%(namespace)s.svc.%(cluster_domain)s:9092' % $._config,

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

  // Validates that per-compartment Deployments/StatefulSets configuration options are correctly
  // parameterised for the compartment they belong to.
  validateMimirCompartmentsConfig(resourceNames)::
    local root = $;
    local addressFlag = '-ingest-storage.kafka.address';
    local topicFlag = '-ingest-storage.kafka.topic';
    local writePlaceholder = '<write-compartment-id>';
    local readPlaceholder = '<read-compartment-id>';

    local isDigit(c) = std.length(std.findSubstr(c, '0123456789')) > 0;

    // Whether s contains token not immediately followed by a digit, so "-wc-1" doesn't match "-wc-10".
    local containsIdToken(s, token) =
      std.foldl(
        function(found, idx)
          found || (local after = idx + std.length(token); after >= std.length(s) || !isDigit(s[after])),
        std.findSubstr(token, s),
        false
      );

    // {kind: 'write'|'read', id: int} extracted from a resource name, or null.
    local extractCompartment(name) =
      if std.length(std.findSubstr('-wc-', name)) > 0 then
        local parts = std.split(name, '-wc-');
        { kind: 'write', id: std.parseInt(parts[std.length(parts) - 1]) }
      else if std.length(std.findSubstr('-rc-', name)) > 0 then
        local parts = std.split(name, '-rc-');
        { kind: 'read', id: std.parseInt(parts[std.length(parts) - 1]) }
      else
        null;

    // Value of a "-flag=value" CLI arg in a container, or null if absent.
    local flagValue(container, flag) =
      if std.objectHas(container, 'args') then
        local prefix = flag + '=';
        local matching = std.filter(function(arg) std.isString(arg) && std.startsWith(arg, prefix), container.args);
        if std.length(matching) == 0 then null else std.substr(matching[0], std.length(prefix), std.length(matching[0]))
      else
        null;

    local validateAddress(name, compartment, container) =
      local value = flagValue(container, addressFlag);
      if value == null then
        null
      else if compartment.kind == 'write' then
        if std.length(std.findSubstr(writePlaceholder, value)) > 0 then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but write compartment %d must target its own Kafka cluster (the address must contain "-wc-%d", not the "%s" placeholder).' % [name, addressFlag, value, compartment.id, compartment.id, writePlaceholder]
        else if !containsIdToken(value, '-wc-%d' % compartment.id) then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but write compartment %d must target its own Kafka cluster (the address must contain "-wc-%d").' % [name, addressFlag, value, compartment.id, compartment.id]
        else
          null
      else if std.length(std.findSubstr(writePlaceholder, value)) == 0 then
        'The Deployment or StatefulSet "%s" sets "%s=%s", but a read compartment consumes from every write compartment\'s Kafka cluster, so the address must contain the "%s" placeholder.' % [name, addressFlag, value, writePlaceholder]
      else
        null;

    local validateTopic(name, compartment, container) =
      local value = flagValue(container, topicFlag);
      if value == null then
        null
      else if compartment.kind == 'read' then
        if std.length(std.findSubstr(readPlaceholder, value)) > 0 then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but read compartment %d consumes a single topic (the topic must contain "-rc-%d", not the "%s" placeholder).' % [name, topicFlag, value, compartment.id, compartment.id, readPlaceholder]
        else if !containsIdToken(value, '-rc-%d' % compartment.id) then
          'The Deployment or StatefulSet "%s" sets "%s=%s", but read compartment %d consumes a single topic (the topic must contain "-rc-%d").' % [name, topicFlag, value, compartment.id, compartment.id]
        else
          null
      else if std.length(std.findSubstr(readPlaceholder, value)) == 0 then
        'The Deployment or StatefulSet "%s" sets "%s=%s", but a write compartment produces to every read compartment\'s topic, so the topic must contain the "%s" placeholder.' % [name, topicFlag, value, readPlaceholder]
      else
        null;

    local validateContainer(name, compartment, container) =
      std.foldl(
        function(firstError, validator) if firstError != null then firstError else validator(name, compartment, container),
        [validateAddress, validateTopic],
        null
      );

    local validateResource(resource) =
      if resource == null then
        null
      else
        local name = resource.metadata.name;
        local compartment = extractCompartment(name);
        if compartment == null then
          'Unable to extract the compartment id from the Deployment or StatefulSet name "%s": expected the name to contain "-wc-<id>" or "-rc-<id>".' % name
        else
          local containers = jsonpath.getJSONPath(resource, 'spec.template.spec.containers', []);
          std.foldl(
            function(firstError, container) if firstError != null then firstError else validateContainer(name, compartment, container),
            containers,
            null
          );

    local getResourceName(value) =
      if std.isObject(value) && std.objectHas(value, 'metadata') && std.objectHas(value.metadata, 'name') then value.metadata.name else null;

    local validateResourceSet(resourceOrSet) =
      if getResourceName(resourceOrSet) != null then
        validateResource(resourceOrSet)
      else if std.isObject(resourceOrSet) then
        std.foldl(
          function(firstError, key) if firstError != null then firstError else validateResource(resourceOrSet[key]),
          std.objectFields(resourceOrSet),
          null
        )
      else
        null;

    std.foldl(
      function(firstError, resourceName) if firstError != null then firstError else validateResourceSet(root[resourceName]),
      resourceNames,
      null
    ),
}
