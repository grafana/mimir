local jsonpath = import 'github.com/jsonnet-libs/xtd/jsonpath.libsonnet';

{
  _config+: {
    // Use a zone aware pod disruption budget for ingester and/or store-gateways
    multi_zone_zpdb_enabled: $._config.multi_zone_ingester_enabled || $._config.multi_zone_store_gateway_enabled,

    // Ordered list of availability zones where multi-zone components should be deployed to.
    // Mimir zone-a deployments are scheduled to the first AZ in the list, zone-b deployment to the second AZ,
    // and zone-c deployments to the third AZ. Maximum 3 AZs are supported.
    multi_zone_availability_zones: [],

    // We can update the queryBlocksStorageConfig only once the migration is over. During the migration
    // we don't want to apply these changes to single-zone store-gateways too.
    queryBlocksStorageConfig+:: if !$._config.multi_zone_store_gateway_enabled || !$._config.multi_zone_store_gateway_read_path_enabled || $._config.multi_zone_store_gateway_migration_enabled then {} else {
      'store-gateway.sharding-ring.zone-awareness-enabled': 'true',
      'store-gateway.sharding-ring.prefix': 'multi-zone/',
    },

    // Toleration to add to all Mimir components when multi-zone deployment is enabled.
    multi_zone_schedule_toleration: 'secondary-az',
  },

  assert !$._config.multi_zone_zpdb_enabled || $._config.rollout_operator_webhooks_enabled : 'zpdb configuration requires rollout_operator_webhooks_enabled=true',
  assert std.length($._config.multi_zone_availability_zones) <= 3 : 'Mimir jsonnet supports a maximum of 3 availability zones',

  //
  // Utilities.
  //

  newMimirMultiZoneToleration()::
    if $._config.multi_zone_schedule_toleration == '' then [] else [
      $.core.v1.toleration.withKey('topology') +
      $.core.v1.toleration.withOperator('Equal') +
      $.core.v1.toleration.withValue($._config.multi_zone_schedule_toleration) +
      $.core.v1.toleration.withEffect('NoSchedule'),
    ],

  // Validates multi-zone configuration for Mimir deployments:
  // - CLI arguments and environment variables containing local addresses must be zonal endpoints.
  // - If memberlist zone-aware routing is enabled, it validates that the configured zone is the correct one.
  //
  // Returns null on success, or an error message string on failure.
  //
  // Params:
  // - deploymentNames: list of jsonnet field names, defined at root level, that Deployments or StatefulSets
  //                    to check.
  //
  // Usage example:
  //   local schedulerError = $.validateMimirMultiZoneConfig([
  //     'ruler_query_scheduler_zone_a_deployment',
  //     'ruler_query_scheduler_zone_b_deployment',
  //     'ruler_query_scheduler_zone_c_deployment',
  //   ]),
  //   assert schedulerError == null: schedulerError,
  validateMimirMultiZoneConfig(deploymentNames)::
    local root = $;
    local excludedArgs = [
      // Memberlist is currently cross-AZ.
      '-memberlist.join',
      // Alertmanager is not deployed per-zone.
      '-ruler.alertmanager-url',
    ];
    local excludedEnvVars = [
      // Alloy installation is currently centralised.
      'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT',
      'OTEL_TRACES_SAMPLER_ARG',
    ];

    local isContainerArgExcluded(arg) =
      std.foldl(
        function(acc, excludedArg) acc || std.startsWith(arg, excludedArg + '='),
        excludedArgs,
        false
      );

    local isContainerEnvVarExcluded(envVarName) =
      std.foldl(
        function(acc, excludedEnvVar) acc || (envVarName == excludedEnvVar),
        excludedEnvVars,
        false
      );

    // Extracts the zone letter (a, b, or c) from a deployment name.
    // Expected format: "*_zone_[abc]_*"
    // Returns: the zone letter or null if not found
    local extractZoneLetter(deploymentName) =
      if std.length(std.findSubstr('_zone_a_', deploymentName)) > 0 then
        'a'
      else if std.length(std.findSubstr('_zone_b_', deploymentName)) > 0 then
        'b'
      else if std.length(std.findSubstr('_zone_c_', deploymentName)) > 0 then
        'c'
      else
        null;

    local validateContainerMemberlistConfig(deploymentName, expectedZone, container) =
      if root._config.memberlist_zone_aware_routing_enabled then
        local flagName = '-memberlist.zone-aware-routing.instance-availability-zone';

        if std.objectHas(container, 'args') then
          local flagPrefix = flagName + '=';
          local expectedValue = 'zone-' + expectedZone;

          // Find the memberlist flag in the args
          local memberlistArgs = std.filter(
            function(arg) std.isString(arg) && std.startsWith(arg, flagPrefix),
            container.args
          );

          if std.length(memberlistArgs) == 0 then
            'The Deployment or StatefulSet "%s" is missing the required CLI flag "%s" (expected value: "%s").' % [deploymentName, flagName, expectedValue]
          else
            local actualValue = std.substr(memberlistArgs[0], std.length(flagPrefix), std.length(memberlistArgs[0]));
            if actualValue == expectedValue then
              null
            else
              'The Deployment or StatefulSet "%s" contains the CLI flag "%s" with value "%s" but expected "%s" (based on deployment name).' % [deploymentName, flagName, actualValue, expectedValue]
        else
          'The Deployment or StatefulSet "%s" is missing the required CLI flag "%s" (expected value: "%s").' % [deploymentName, flagName, 'zone-' + expectedZone]
      else
        null;

    local validateContainerArg(deploymentName, expectedZone, arg) =
      if std.isString(arg) && std.length(std.findSubstr('cluster.local', arg)) > 0 && !isContainerArgExcluded(arg) then
        local expectedZoneNotation = '-zone-' + expectedZone;
        if std.length(std.findSubstr(expectedZoneNotation, arg)) > 0 || std.length(std.findSubstr('-multi-zone', arg)) > 0 then
          null
        else
          'The Deployment or StatefulSet "%s" contains the CLI flag "%s" with a non-matching zone. Use an address in the "%s" zone (based on deployment name) or a multi-zone address, or add this CLI flag to the exclusion list.' % [deploymentName, arg, expectedZoneNotation]
      else
        null;

    local validateContainerArgs(deploymentName, expectedZone, container) =
      if std.objectHas(container, 'args') then
        std.foldl(
          function(firstError, arg)
            if firstError != null then firstError
            else validateContainerArg(deploymentName, expectedZone, arg),
          container.args,
          null
        )
      else
        null;

    local validateContainerEnvVar(deploymentName, expectedZone, env) =
      if std.objectHas(env, 'name') && std.objectHas(env, 'value') && !isContainerEnvVarExcluded(env.name) then
        local value = env.value;
        if std.isString(value) && std.length(std.findSubstr('cluster.local', value)) > 0 then
          local expectedZoneNotation = '-zone-' + expectedZone;
          if std.length(std.findSubstr(expectedZoneNotation, value)) > 0 then
            null
          else
            'The Deployment or StatefulSet "%s" contains the environment variable "%s" with value "%s" with a non-matching zone. Use an address in the "%s" zone (based on deployment name), or add this environment variable to the exclusion list.' % [deploymentName, env.name, value, expectedZoneNotation]
        else
          null
      else
        null;

    local validateContainerEnvVars(deploymentName, expectedZone, container) =
      if std.objectHas(container, 'env') then
        std.foldl(
          function(firstError, env)
            if firstError != null then firstError
            else validateContainerEnvVar(deploymentName, expectedZone, env),
          container.env,
          null
        )
      else
        null;

    local validateContainer(deploymentName, expectedZone, container) =
      local validators = [
        validateContainerMemberlistConfig,
        validateContainerArgs,
        validateContainerEnvVars,
      ];
      std.foldl(
        function(firstError, validator)
          if firstError != null then firstError
          else validator(deploymentName, expectedZone, container),
        validators,
        null
      );

    local validateDeployment(deploymentName, deployment) =
      local expectedZone = extractZoneLetter(deploymentName);
      if expectedZone == null then
        'Unable to extract zone letter from deployment name "%s". Expected format: "*_zone_[abc]_*"' % deploymentName
      else if deployment == null then
        null
      else
        local containers = jsonpath.getJSONPath(deployment, 'spec.template.spec.containers', []);
        std.foldl(
          function(firstError, container)
            if firstError != null then firstError
            else validateContainer(deploymentName, expectedZone, container),
          containers,
          null
        );

    // Validate all input deployments and return the first error found, or null if all valid.
    std.foldl(
      function(firstError, deploymentName)
        if firstError != null then firstError
        else validateDeployment(deploymentName, root[deploymentName]),
      deploymentNames,
      null
    ),
}
