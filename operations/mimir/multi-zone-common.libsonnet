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
}
