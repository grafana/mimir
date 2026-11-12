{
  local replicaTemplateWatchRule = {
    apiGroups: ['rollout-operator.grafana.com'],
    resources: ['replicatemplates'],
    verbs: ['get', 'list', 'watch'],
  },

  _config+: {
    // Configure the rollout operator to accept webhook requests made as part of scaling
    // statefulsets up or down. This allows the rollout operator to ensure that stateful
    // components (ingesters, store-gateways) are scaled up or down safely.
    rollout_operator_webhooks_enabled: $._config.multi_zone_ingester_enabled ||
                                       $._config.multi_zone_store_gateway_enabled,

    rollout_operator_enabled: $._config.multi_zone_ingester_enabled ||
                              $._config.multi_zone_store_gateway_enabled ||
                              $._config.cortex_compactor_concurrent_rollout_enabled ||
                              $._config.ingest_storage_ingester_autoscaling_enabled ||
                              $._config.rollout_operator_webhooks_enabled,

    // Ignore these labels used for controlling webhook behavior when creating services.
    service_ignored_labels+:: ['grafana.com/no-downscale', 'grafana.com/prepare-downscale'],

    rollout_operator_replica_template_access_enabled: $._config.ingest_storage_ingester_autoscaling_enabled || $._config.ingester_automated_downscale_v2_enabled,
    replica_template_custom_resource_definition_enabled: $._config.ingest_storage_ingester_autoscaling_enabled || $._config.ingester_automated_downscale_v2_enabled,
  },

  // Consumers can pin rollout-operator Jsonnet independently of Mimir's dependency.
  rollout_operator_args+:: {
    [if $._config.rollout_operator_replica_template_access_enabled then 'replica-templates.watch-enabled']: true,
  },

  rollout_operator_role:
    local role = if 'rollout_operator_role' in super then super.rollout_operator_role else null;
    if role == null || !$._config.rollout_operator_replica_template_access_enabled then role else role {
      rules: if std.member(super.rules, replicaTemplateWatchRule) then super.rules else super.rules + [replicaTemplateWatchRule],
    },

  rollout_operator_container+:
    if std.get($, 'tracing_env_mixin') != null then $.tracing_env_mixin else {},

  rollout_operator_pdb: if !$._config.rollout_operator_enabled then null else
    $.newMimirPdb('rollout-operator'),
}
