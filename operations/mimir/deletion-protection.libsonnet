{
  _config+:: {
    // When enabled, prevents accidental deletion of specific Mimir components. To delete these StatefulSets you need to
    // disable the deletion protection first, and then delete the StatefulSets.
    ingester_deletion_protection_enabled: false,
    store_gateway_deletion_protection_enabled: false,
  },

  local policy = $.admissionregistration.v1.validatingAdmissionPolicy,
  local binding = $.admissionregistration.v1.validatingAdmissionPolicyBinding,

  // Helper function to create a deletion protection policy for StatefulSets.
  // The policy blocks deletion of StatefulSets whose name starts with the given name.
  newStatefulSetDeletionProtectionPolicy(name):: {
    local policyName = '%s-%s-deletion-protection' % [$._config.namespace, name],

    policy:
      policy.new(policyName) +
      policy.spec.withFailurePolicy('Fail') +
      policy.spec.matchConstraints.withResourceRules([{
        apiGroups: ['apps'],
        apiVersions: ['v1'],
        operations: ['DELETE'],
        resources: ['statefulsets'],
      }]) +
      policy.spec.withValidations([{
        expression: '!oldObject.metadata.name.startsWith("%s")' % name,
        messageExpression: '"Deleting " + oldObject.metadata.name + " StatefulSet is forbidden because it is highly destructive. If you must proceed, consult the \'Mimir components deletion protection\' runbook to understand the consequences and how to unblock the deletion."',
        reason: 'Forbidden',
      }]),

    binding:
      binding.new(policyName) +
      binding.spec.withPolicyName(policyName) +
      binding.spec.withValidationActions(['Deny']) +
      binding.spec.matchResources.namespaceSelector.withMatchLabels({
        'kubernetes.io/metadata.name': $._config.namespace,
      }),
  },

  // Ingester deletion protection
  local ingesterProtection = $.newStatefulSetDeletionProtectionPolicy('ingester'),
  ingester_deletion_protection_policy: if !$._config.ingester_deletion_protection_enabled then null else ingesterProtection.policy,
  ingester_deletion_protection_policy_binding: if !$._config.ingester_deletion_protection_enabled then null else ingesterProtection.binding,

  // Store-gateway deletion protection
  local storeGatewayProtection = $.newStatefulSetDeletionProtectionPolicy('store-gateway'),
  store_gateway_deletion_protection_policy: if !$._config.store_gateway_deletion_protection_enabled then null else storeGatewayProtection.policy,
  store_gateway_deletion_protection_policy_binding: if !$._config.store_gateway_deletion_protection_enabled then null else storeGatewayProtection.binding,
}
