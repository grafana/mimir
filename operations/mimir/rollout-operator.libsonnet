{
  local clusterRole = $.rbac.v1.clusterRole,
  local clusterRoleBinding = $.rbac.v1.clusterRoleBinding,
  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local mutatingWebhook = $.admissionregistration.v1.mutatingWebhook,
  local mutatingWebhookConfiguration = $.admissionregistration.v1.mutatingWebhookConfiguration,
  local policyRule = $.rbac.v1.policyRule,
  local role = $.rbac.v1.role,
  local roleBinding = $.rbac.v1.roleBinding,
  local service = $.core.v1.service,
  local serviceAccount = $.core.v1.serviceAccount,
  local servicePort = $.core.v1.servicePort,
  local validatingWebhook = $.admissionregistration.v1.validatingWebhook,
  local validatingWebhookConfiguration = $.admissionregistration.v1.validatingWebhookConfiguration,

  _config+:: {
    // Configure the rollout operator to accept webhook requests made as part of scaling
    // statefulsets up or down. This allows the rollout operator to ensure that stateful
    // components (ingesters, store-gateways) are scaled up or down safely.
    enable_rollout_operator_webhook: false,

    // ignore_rollout_operator_*_webhook_failures will set the rollout-operator to ignore
    // webhook failures. Useful during a rollout to a new cell, where rollout-operator service
    // is still not created, as the webhook might be created before the service, and that could
    // block other operations that would block the service creation.
    ignore_rollout_operator_no_downscale_webhook_failures: false,
    ignore_rollout_operator_prepare_downscale_webhook_failures: false,

    // Ignore these labels used for controlling webhook behavior when creating services.
    service_ignored_labels+:: ['grafana.com/no-downscale', 'grafana.com/prepare-downscale'],
  },

  local rollout_operator_enabled =
    $._config.multi_zone_ingester_enabled ||
    $._config.multi_zone_store_gateway_enabled ||
    $._config.cortex_compactor_concurrent_rollout_enabled ||
    $._config.ingest_storage_ingester_autoscaling_enabled ||
    $._config.enable_rollout_operator_webhook,

  rollout_operator_args:: {
    'kubernetes.namespace': $._config.namespace,
  } + if $._config.enable_rollout_operator_webhook then {
    '-server-tls.enabled': 'true',
  } else {},

  rollout_operator_node_affinity_matchers:: [],

  rollout_operator_container::
    container.new('rollout-operator', $._images.rollout_operator) +
    container.withArgsMixin($.util.mapToFlags($.rollout_operator_args)) +
    container.withPorts([
      $.core.v1.containerPort.new('http-metrics', 8001),
    ]) +
    $.util.resourcesRequests('100m', '100Mi') +
    $.util.resourcesLimits(null, '200Mi') +
    container.mixin.readinessProbe.httpGet.withPath('/ready') +
    container.mixin.readinessProbe.httpGet.withPort(8001) +
    container.mixin.readinessProbe.withInitialDelaySeconds(5) +
    container.mixin.readinessProbe.withTimeoutSeconds(1) +
    $.jaeger_mixin,

  rollout_operator_deployment: if !rollout_operator_enabled then null else
    deployment.new('rollout-operator', 1, [$.rollout_operator_container]) +
    deployment.mixin.metadata.withName('rollout-operator') +
    deployment.mixin.spec.template.spec.withServiceAccountName('rollout-operator') +
    // Ensure Kubernetes doesn't run 2 operators at the same time.
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(0) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
    $.newMimirNodeAffinityMatchers($.rollout_operator_node_affinity_matchers),

  rollout_operator_service: if !rollout_operator_enabled || !$._config.enable_rollout_operator_webhook then null else
    service.new(
      'rollout-operator',
      { name: 'rollout-operator' },
      servicePort.newNamed('https', 443, 8443) +
      servicePort.withProtocol('TCP'),
    ),

  rollout_operator_role: if !rollout_operator_enabled then null else
    role.new('rollout-operator-role') +
    role.mixin.metadata.withNamespace($._config.namespace) +
    role.withRulesMixin([
      policyRule.withApiGroups('') +
      policyRule.withResources(['pods']) +
      policyRule.withVerbs(['list', 'get', 'watch', 'delete']),
      policyRule.withApiGroups('apps') +
      policyRule.withResources(['statefulsets']) +
      policyRule.withVerbs(['list', 'get', 'watch', 'patch']),
      policyRule.withApiGroups('apps') +
      policyRule.withResources(['statefulsets/status']) +
      policyRule.withVerbs(['update']),
    ]),

  rollout_operator_rolebinding: if !rollout_operator_enabled then null else
    roleBinding.new('rollout-operator-rolebinding') +
    roleBinding.mixin.metadata.withNamespace($._config.namespace) +
    roleBinding.mixin.roleRef.withApiGroup('rbac.authorization.k8s.io') +
    roleBinding.mixin.roleRef.withKind('Role') +
    roleBinding.mixin.roleRef.withName('rollout-operator-role') +
    roleBinding.withSubjectsMixin({
      kind: 'ServiceAccount',
      name: 'rollout-operator',
      namespace: $._config.namespace,
    }),

  rollout_operator_webhook_cert_secret_role: if !rollout_operator_enabled || !$._config.enable_rollout_operator_webhook then null else
    role.new('rollout-operator-webhook-cert-secret-role') +
    role.mixin.metadata.withNamespace($._config.namespace) +
    role.withRulesMixin([
      policyRule.withApiGroups('')
      + policyRule.withResources(['secrets'])
      + policyRule.withVerbs(['create']),
      policyRule.withApiGroups('')
      + policyRule.withResources(['secrets'])
      + policyRule.withVerbs(['update', 'get'])
      + policyRule.withResourceNames(['rollout-operator-self-signed-certificate']),
    ]),

  rollout_operator_webhook_cert_secret_rolebinding: if !rollout_operator_enabled || !$._config.enable_rollout_operator_webhook then null else
    roleBinding.new('rollout-operator-webhook-cert-secret-rolebinding') +
    roleBinding.mixin.metadata.withNamespace($._config.namespace) +
    roleBinding.mixin.roleRef.withApiGroup('rbac.authorization.k8s.io') +
    roleBinding.mixin.roleRef.withKind('Role') +
    roleBinding.mixin.roleRef.withName('rollout-operator-webhook-cert-secret-role') +
    roleBinding.withSubjectsMixin({
      kind: 'ServiceAccount',
      name: 'rollout-operator',
      namespace: $._config.namespace,
    }),

  rollout_operator_webhook_cert_update_clusterrole: if !rollout_operator_enabled || !$._config.enable_rollout_operator_webhook then null else
    clusterRole.new('rollout-operator-%s-webhook-cert-update-role' % $._config.namespace) +
    clusterRole.withRulesMixin([
      policyRule.withApiGroups('admissionregistration.k8s.io')
      + policyRule.withResources(['validatingwebhookconfigurations', 'mutatingwebhookconfigurations'])
      + policyRule.withVerbs(['list', 'patch']),
    ]),

  rollout_operator_webhook_cert_update_clusterrolebinding: if !rollout_operator_enabled || !$._config.enable_rollout_operator_webhook then null else
    clusterRoleBinding.new('rollout-operator-%s-webhook-cert-secret-rolebinding' % $._config.namespace) +
    clusterRoleBinding.mixin.roleRef.withApiGroup('rbac.authorization.k8s.io') +
    clusterRoleBinding.mixin.roleRef.withKind('ClusterRole') +
    clusterRoleBinding.mixin.roleRef.withName('rollout-operator-%s-webhook-cert-update-role' % $._config.namespace) +
    clusterRoleBinding.withSubjectsMixin({
      kind: 'ServiceAccount',
      name: 'rollout-operator',
      namespace: $._config.namespace,
    }),

  no_downscale_webhook: if !rollout_operator_enabled || !$._config.enable_rollout_operator_webhook then null else
    validatingWebhookConfiguration.new('no-downscale-%s' % $._config.namespace) +
    validatingWebhookConfiguration.mixin.metadata.withLabels({
      'grafana.com/namespace': $._config.namespace,
      'grafana.com/inject-rollout-operator-ca': 'true',
    }) +
    validatingWebhookConfiguration.withWebhooksMixin([
      validatingWebhook.withName('no-downscale-%s.grafana.com' % $._config.namespace)
      + validatingWebhook.withAdmissionReviewVersions(['v1'])
      + validatingWebhook.withFailurePolicy(if $._config.ignore_rollout_operator_no_downscale_webhook_failures then 'Ignore' else 'Fail')
      + validatingWebhook.withMatchPolicy('Equivalent')
      + validatingWebhook.withSideEffects('None')
      + validatingWebhook.withTimeoutSeconds(10)
      + validatingWebhook.withRulesMixin([
        {
          apiGroups: ['apps'],
          apiVersions: ['v1'],
          operations: ['UPDATE'],
          resources: ['statefulsets', 'statefulsets/scale'],
          scope: 'Namespaced',
        },
      ])
      + {
        namespaceSelector: {
          matchLabels: {
            'kubernetes.io/metadata.name': $._config.namespace,
          },
        },
        clientConfig: {
          service: {
            name: 'rollout-operator',
            namespace: $._config.namespace,
            path: '/admission/no-downscale',
            port: 443,
          },
        },
      },
    ]),

  prepare_downscale_webhook: if !rollout_operator_enabled || !$._config.enable_rollout_operator_webhook then null else
    mutatingWebhookConfiguration.new('prepare-downscale-%s' % $._config.namespace) +
    mutatingWebhookConfiguration.mixin.metadata.withLabels({
      'grafana.com/namespace': $._config.namespace,
      'grafana.com/inject-rollout-operator-ca': 'true',
    }) +
    mutatingWebhookConfiguration.withWebhooksMixin([
      mutatingWebhook.withName('prepare-downscale-%s.grafana.com' % $._config.namespace)
      + mutatingWebhook.withAdmissionReviewVersions(['v1'])
      + mutatingWebhook.withFailurePolicy(if $._config.ignore_rollout_operator_prepare_downscale_webhook_failures then 'Ignore' else 'Fail')
      + mutatingWebhook.withMatchPolicy('Equivalent')
      + mutatingWebhook.withSideEffects('NoneOnDryRun')
      + mutatingWebhook.withTimeoutSeconds(10)
      + mutatingWebhook.withRulesMixin([
        {
          apiGroups: ['apps'],
          apiVersions: ['v1'],
          operations: ['UPDATE'],
          resources: ['statefulsets', 'statefulsets/scale'],
          scope: 'Namespaced',
        },
      ])
      + {
        namespaceSelector: {
          matchLabels: {
            'kubernetes.io/metadata.name': $._config.namespace,
          },
        },
        clientConfig: {
          service: {
            name: 'rollout-operator',
            namespace: $._config.namespace,
            path: '/admission/prepare-downscale',
            port: 443,
          },
        },
      },
    ]),

  rollout_operator_service_account: if !rollout_operator_enabled then null else
    serviceAccount.new('rollout-operator'),

  rollout_operator_pdb: if !rollout_operator_enabled then null else
    $.newMimirPdb('rollout-operator'),
}
