local rolloutOperator = import 'mimir/rollout-operator.libsonnet';

local podRule = { apiGroups: [''], resources: ['pods'], verbs: ['get', 'list', 'watch', 'delete'] };
local watchRule = {
  apiGroups: ['rollout-operator.grafana.com'],
  resources: ['replicatemplates'],
  verbs: ['get', 'list', 'watch'],
};

// Model a consumer that pins its rollout-operator Jsonnet independently of Mimir.
local base(access=true, role={ rules: [podRule] }, withRole=true) = {
  _config:: {
    multi_zone_ingester_enabled: true,
    multi_zone_store_gateway_enabled: false,
    cortex_compactor_concurrent_rollout_enabled: false,
    ingest_storage_ingester_autoscaling_enabled: access,
    ingester_automated_downscale_v2_enabled: false,
    service_ignored_labels: [],
  },
  rollout_operator_args:: { 'kubernetes.namespace': 'default' },
  [if withRole then 'rollout_operator_role']: role,
};

local enabled = base() + rolloutOperator;
assert enabled.rollout_operator_args['replica-templates.watch-enabled'] == true :
       'ReplicaTemplate access must enable watching';
assert enabled.rollout_operator_args['kubernetes.namespace'] == 'default' :
       'Existing operator arguments must be preserved';
assert enabled.rollout_operator_role.rules == [podRule, watchRule] :
       'Watching requires get/list/watch without dropping existing permissions';

local alreadyGranted = base(role={ rules: [podRule, watchRule] }) + rolloutOperator;
assert alreadyGranted.rollout_operator_role.rules == [podRule, watchRule] :
       'Existing ReplicaTemplate watch permissions must not be duplicated';

local disabled = base(access=false) + rolloutOperator;
assert !std.objectHas(disabled.rollout_operator_args, 'replica-templates.watch-enabled') :
       'Watching must stay disabled without ReplicaTemplate access';
assert disabled.rollout_operator_role.rules == [podRule] :
       'Watch permissions must stay disabled without ReplicaTemplate access';

local nullRole = base(role=null) + rolloutOperator;
assert nullRole.rollout_operator_role == null : 'A disabled operator must keep its null Role';

local absentRole = base(withRole=false) + rolloutOperator;
assert absentRole.rollout_operator_role == null : 'Consumers without an operator Role must remain valid';

{}
