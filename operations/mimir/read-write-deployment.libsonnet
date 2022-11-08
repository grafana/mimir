{
  _config+:: {
    read_write_deployment_enabled: false,

    mimir_write_replicas: 3,
    mimir_write_max_unavailable: 25,
    mimir_write_data_disk_size: '100Gi',
    mimir_write_data_disk_class: 'fast',
    mimir_write_allow_multiple_replicas_on_same_node: false,
    mimir_read_replicas: 2,
    mimir_read_topology_spread_max_skew: 1,
    mimir_backend_replicas: 3,
    mimir_backend_max_unavailable: 10,
    mimir_backend_data_disk_size: '100Gi',
    mimir_backend_data_disk_class: 'fast-dont-retain',
    mimir_backend_allow_multiple_replicas_on_same_node: false,

    // Query-scheduler ring-based service discovery is always enabled in the Mimir read-write deployment mode.
    query_scheduler_service_discovery_mode: if $._config.read_write_deployment_enabled then 'ring' else super.query_scheduler_service_discovery_mode,

    // Overrides-exporter is part of the backend component in the Mimir read-write deployment mode.
    overrides_exporter_enabled: if $._config.read_write_deployment_enabled then false else super.overrides_exporter_enabled,
  },

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local envVar = $.core.v1.envVar,
  local service = $.core.v1.service,
  local podAntiAffinity = deployment.mixin.spec.template.spec.affinity.podAntiAffinity,
  local podDisruptionBudget = $.policy.v1beta1.podDisruptionBudget,
  local pvc = $.core.v1.persistentVolumeClaim,
  local statefulSet = $.apps.v1.statefulSet,
  local volumeMount = $.core.v1.volumeMount,

  // Mimir read-write deployment mode makes some strong assumptions about what must enabled enabled and disabled.
  check_compactor_max_concurrency: if !$._config.read_write_deployment_enabled || $._config.compactor_max_concurrency == 1 then null else
    error 'please set compactor_max_concurrency to 1 when using Mimir read-write deployment mode',

  check_ingester_multi_zone: if !$._config.read_write_deployment_enabled || $._config.multi_zone_ingester_enabled then null else
    error 'please set multi_zone_ingester_enabled to true when using Mimir read-write deployment mode',

  check_store_gateway_multi_zone: if !$._config.read_write_deployment_enabled || $._config.multi_zone_store_gateway_enabled then null else
    error 'please set multi_zone_store_gateway_enabled to true when using Mimir read-write deployment mode',

  check_querier_autoscaling: if !$._config.read_write_deployment_enabled || !$._config.autoscaling_querier_enabled then null else
    error 'please set autoscaling_querier_enabled to false when using Mimir read-write deployment mode',

  check_ruler_remote_evaluation_enabled: if !$._config.read_write_deployment_enabled || !$._config.ruler_remote_evaluation_enabled then null else
    error 'please set ruler_remote_evaluation_enabled to false when using Mimir read-write deployment mode',

  check_overrides_exporter_enabled: if !$._config.read_write_deployment_enabled || !$._config.overrides_exporter_enabled then null else
    error 'please set overrides_exporter_enabled to false when using Mimir read-write deployment mode',

  check_memberlist_ring: if !$._config.read_write_deployment_enabled || $._config.memberlist_ring_enabled then null else
    error 'please set memberlist_ring_enabled to true when using Mimir read-write deployment mode',

  // Uninstall resources used by microservices deployment mode.
  local uninstall(name) =
    if ($._config.read_write_deployment_enabled) then null
    else (if name in super then super[name] else null),

  alertmanager_statefulset: uninstall('alertmanager_statefulset'),
  compactor_statefulset: uninstall('compactor_statefulset'),
  distributor_deployment: uninstall('distributor_deployment'),
  ingester_statefulset: uninstall('ingester_statefulset'),
  ingester_zone_a_statefulset: uninstall('ingester_zone_a_statefulset'),
  ingester_zone_b_statefulset: uninstall('ingester_zone_b_statefulset'),
  ingester_zone_c_statefulset: uninstall('ingester_zone_c_statefulset'),
  querier_deployment: uninstall('querier_deployment'),
  query_frontend_deployment: uninstall('query_frontend_deployment'),
  query_scheduler_deployment: uninstall('query_scheduler_deployment'),
  ruler_deployment: uninstall('ruler_deployment'),
  store_gateway_statefulset: uninstall('store_gateway_statefulset'),
  store_gateway_zone_a_statefulset: uninstall('store_gateway_zone_a_statefulset'),
  store_gateway_zone_b_statefulset: uninstall('store_gateway_zone_b_statefulset'),
  store_gateway_zone_c_statefulset: uninstall('store_gateway_zone_c_statefulset'),

  alertmanager_service: uninstall('alertmanager_service'),
  compactor_service: uninstall('compactor_service'),
  distributor_service: uninstall('distributor_service'),
  ingester_service: uninstall('ingester_service'),
  ingester_zone_a_service: uninstall('ingester_zone_a_service'),
  ingester_zone_b_service: uninstall('ingester_zone_b_service'),
  ingester_zone_c_service: uninstall('ingester_zone_c_service'),
  querier_service: uninstall('querier_service'),
  query_frontend_service: uninstall('query_frontend_service'),
  query_frontend_discovery_service: uninstall('query_frontend_discovery_service'),
  query_scheduler_service: uninstall('query_scheduler_service'),
  query_scheduler_discovery_service: uninstall('query_scheduler_discovery_service'),
  ruler_service: uninstall('ruler_service'),
  store_gateway_service: uninstall('store_gateway_service'),
  store_gateway_multi_zone_service: uninstall('store_gateway_multi_zone_service'),
  store_gateway_zone_a_service: uninstall('store_gateway_zone_a_service'),
  store_gateway_zone_b_service: uninstall('store_gateway_zone_b_service'),
  store_gateway_zone_c_service: uninstall('store_gateway_zone_c_service'),

  alertmanager_pdb: uninstall('alertmanager_pdb'),
  ingester_pdb: uninstall('ingester_pdb'),
  ingester_rollout_pdb: uninstall('ingester_rollout_pdb'),
  store_gateway_pdb: uninstall('store_gateway_pdb'),
  store_gateway_rollout_pdb: uninstall('store_gateway_rollout_pdb'),

  // Utils.
  local gossipLabel = $.apps.v1.statefulSet.spec.template.metadata.withLabelsMixin({ [$._config.gossip_member_label]: 'true' }),
  local byContainerPort = function(x) x.containerPort,

  newMimirRolloutGroupPDB(rolloutGroup, maxUnavailable)::
    podDisruptionBudget.new() +
    podDisruptionBudget.mixin.metadata.withName('%s-rollout-pdb' % rolloutGroup) +
    podDisruptionBudget.mixin.metadata.withLabels({ name: '%s-rollout-pdb' % rolloutGroup }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ 'rollout-group': rolloutGroup }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(maxUnavailable),

  // Creates a service resolving to all replicas in a rollout group.
  newMimirRolloutGroupService(rolloutGroup, deployments, ignored_labels=[])::
    local name = rolloutGroup;
    local selector = { 'rollout-group': rolloutGroup };
    local container = $.core.v1.container;
    local service = $.core.v1.service;
    local servicePort = $.core.v1.servicePort;

    // Find all (unique) ports exposed by all containers in the input deployments.
    // The std.set() function filters out all duplicates.
    local exposedPorts = std.set([
      {
        containerName: c.name,
        containerPort: p.containerPort,
        portName: p.name,
        portProtocol: if std.objectHas(p, 'protocol') then p.protocol else null,
      }
      for d in deployments
      for c in d.spec.template.spec.containers
      for p in (c + container.withPortsMixin([])).ports
    ], function(entry) entry.containerPort);

    // Generate the definition for the ports to expose from the service.
    local ports = [
      servicePort.newNamed(
        name=('%(container)s-%(port)s' % { container: port.containerName, port: port.portName }),
        port=port.containerPort,
        targetPort=port.containerPort
      ) +
      (if port.portProtocol != null then servicePort.withProtocol(port.portProtocol) else {})
      for port in exposedPorts
    ];

    service.new(name, selector, ports) +
    service.mixin.metadata.withLabels({ name: name }) +
    // Override the selector because the service.new() automatically inject the name label matcher too.
    { spec+: { selector: selector } },

  //
  // Write component.
  //

  mimir_write_args::
    $.distributor_args +
    $.ingester_args + {
      target: 'write',

      // No ballast required.
      'mem-ballast-size-bytes': null,
    },

  mimir_write_zone_a_args:: {},
  mimir_write_zone_b_args:: {},
  mimir_write_zone_c_args:: {},

  mimir_write_ports::
    std.uniq(
      std.sort(
        $.distributor_ports +
        $.ingester_ports,
        byContainerPort
      ), byContainerPort
    ),

  local mimir_write_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.mimir_write_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.mimir_write_data_disk_class) +
    pvc.mixin.metadata.withName('mimir-write-data'),

  newMimirWriteZoneContainer(zone, zone_args)::
    container.new('mimir-write', $._images.mimir_write) +
    container.withPorts($.mimir_write_ports) +
    container.withArgsMixin($.util.mapToFlags(
      $.mimir_write_args + zone_args + {
        'ingester.ring.zone-awareness-enabled': 'true',
        'ingester.ring.instance-availability-zone': 'zone-%s' % zone,
      },
    )) +
    $.util.resourcesRequests('4', '15Gi') +
    $.util.resourcesLimits(null, '25Gi') +
    $.util.readinessProbe +
    $.core.v1.container.withVolumeMountsMixin([
      volumeMount.new('mimir-write-data', '/data'),
    ]) +
    $.jaeger_mixin,

  newMimirWriteZoneStatefulset(zone, container)::
    local name = 'mimir-write-zone-%s' % zone;
    local replicas = std.ceil($._config.mimir_write_replicas / 3);

    $.newMimirStatefulSet(name, replicas, container, mimir_write_data_pvc) +
    statefulSet.mixin.metadata.withLabels({ 'rollout-group': 'mimir-write' }) +
    statefulSet.mixin.metadata.withAnnotations({ 'rollout-max-unavailable': std.toString($._config.mimir_write_max_unavailable) }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name, 'rollout-group': 'mimir-write' }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': 'mimir-write' }) +
    statefulSet.mixin.spec.updateStrategy.withType('OnDelete') +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(1200) +
    $.mimirVolumeMounts +
    $.util.podPriority('high') +
    (if $._config.memberlist_ring_enabled then gossipLabel else {}) +
    (if $._config.mimir_write_allow_multiple_replicas_on_same_node then {} else {
       spec+:
         // Allow to schedule 2+ mimir-writes in the same zone on the same node, but do not schedule 2+ mimir-writes in
         // different zones on the same node. In case of 1 node failure in the Kubernetes cluster, only mimir-writes
         // in 1 zone will be affected.
         podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecution([
           podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
           podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
             { key: 'rollout-group', operator: 'In', values: ['mimir-write'] },
             { key: 'name', operator: 'NotIn', values: [name] },
           ]) +
           podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
         ]).spec,
     }),

  // Creates a headless service for the per-zone mimir-write StatefulSet. We don't use it
  // but we need to create it anyway because it's responsible for the network identity of
  // the StatefulSet pods. For more information, see:
  // https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#statefulset-v1-apps
  newMimirWriteZoneService(sts)::
    $.util.serviceFor(sts, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),  // Headless.

  mimir_write_zone_a_container:: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneContainer('a', $.mimir_write_zone_a_args),

  mimir_write_zone_b_container:: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneContainer('b', $.mimir_write_zone_b_args),

  mimir_write_zone_c_container:: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneContainer('c', $.mimir_write_zone_c_args),

  mimir_write_zone_a_statefulset: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneStatefulset('a', $.mimir_write_zone_a_container),

  mimir_write_zone_b_statefulset: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneStatefulset('b', $.mimir_write_zone_b_container),

  mimir_write_zone_c_statefulset: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneStatefulset('c', $.mimir_write_zone_c_container),

  mimir_write_zone_a_service: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneService($.mimir_write_zone_a_statefulset),

  mimir_write_zone_b_service: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneService($.mimir_write_zone_b_statefulset),

  mimir_write_zone_c_service: if !$._config.read_write_deployment_enabled then null else
    $.newMimirWriteZoneService($.mimir_write_zone_c_statefulset),

  // This service is used as ingress on the write path, and to access the admin UI.
  mimir_write_service: if !$._config.read_write_deployment_enabled then null else
    $.newMimirRolloutGroupService('mimir-write', [$.mimir_write_zone_a_statefulset, $.mimir_write_zone_b_statefulset, $.mimir_write_zone_c_statefulset], $._config.service_ignored_labels),

  mimir_write_rollout_pdb: if !$._config.read_write_deployment_enabled then null else
    $.newMimirRolloutGroupPDB('mimir-write', 1),

  //
  // Read component.
  //

  mimir_read_args::
    $.query_frontend_args +
    $.querier_args + {
      target: 'read',
      // Restrict number of active query-schedulers.
      'query-scheduler.max-used-instances': 2,
    },

  mimir_read_ports::
    std.uniq(
      std.sort(
        $.querier_ports +
        $.ruler_ports,
        byContainerPort
      ), byContainerPort
    ),

  mimir_read_env_map:: $.querier_env_map,

  mimir_read_container:: if !$._config.read_write_deployment_enabled then null else
    container.new('mimir-read', $._images.mimir_read) +
    container.withPorts($.mimir_read_ports) +
    container.withArgsMixin($.util.mapToFlags($.mimir_read_args)) +
    $.jaeger_mixin +
    $.util.readinessProbe +
    container.withEnvMap($.mimir_read_env_map) +
    $.util.resourcesRequests('1', '12Gi') +
    $.util.resourcesLimits(null, '24Gi'),

  mimir_read_deployment: if !$._config.read_write_deployment_enabled then null else
    deployment.new('mimir-read', $._config.mimir_read_replicas, [$.mimir_read_container]) +
    $.mimirVolumeMounts +
    $.newMimirSpreadTopology('mimir-read', $._config.mimir_read_topology_spread_max_skew) +
    (if !std.isObject($._config.node_selector) then {} else deployment.mixin.spec.template.spec.withNodeSelectorMixin($._config.node_selector)) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxSurge(5) +
    deployment.mixin.spec.strategy.rollingUpdate.withMaxUnavailable(1) +
    (if $._config.memberlist_ring_enabled then gossipLabel else {}),

  mimir_read_service: if !$._config.read_write_deployment_enabled then null else
    $.util.serviceFor($.mimir_read_deployment, $._config.service_ignored_labels),

  //
  // Backend component.
  //

  mimir_backend_args::
    $.store_gateway_args +
    $.compactor_args +
    $.query_scheduler_args +
    $.ruler_args + {
      target: 'backend',

      // Do not conflict with /data/tsdb and /data/tokens used by store-gateway.
      'compactor.data-dir': '/data/compactor',

      // Run the Alertmanager with the local filesystem, so we don't really use it as bundled with Alertmanager at Grafana Labs.
      'alertmanager-storage.backend': 'filesystem',

      // Use ruler's remote evaluation mode.
      'querier.frontend-address': null,
      'ruler.query-frontend.address': 'dns:///mimir-read.%s.svc.cluster.local:9095' % $._config.namespace,

      // Restrict number of active query-schedulers.
      'query-scheduler.max-used-instances': 2,
    },

  mimir_backend_zone_a_args:: {},
  mimir_backend_zone_b_args:: {},
  mimir_backend_zone_c_args:: {},

  local mimir_backend_data_pvc =
    pvc.new() +
    pvc.mixin.spec.resources.withRequests({ storage: $._config.mimir_backend_data_disk_size }) +
    pvc.mixin.spec.withAccessModes(['ReadWriteOnce']) +
    pvc.mixin.spec.withStorageClassName($._config.mimir_backend_data_disk_class) +
    pvc.mixin.metadata.withName('mimir-backend-data'),

  mimir_backend_ports::
    std.uniq(
      std.sort(
        $.store_gateway_ports +
        $.compactor_ports,
        byContainerPort
      ), byContainerPort
    ),

  newMimirBackendZoneContainer(zone, zone_args)::
    container.new('mimir-backend', $._images.mimir_backend) +
    container.withPorts($.mimir_backend_ports) +
    container.withArgsMixin($.util.mapToFlags($.mimir_backend_args + zone_args + {
      'store-gateway.sharding-ring.instance-availability-zone': 'zone-%s' % zone,
      'store-gateway.sharding-ring.zone-awareness-enabled': true,

      // Use a different prefix so that both single-zone and multi-zone store-gateway rings can co-exists.
      'store-gateway.sharding-ring.prefix': 'multi-zone/',

      // Do not unregister from ring at shutdown, so that no blocks re-shuffling occurs during rollouts.
      'store-gateway.sharding-ring.unregister-on-shutdown': false,
    })) +
    container.withVolumeMountsMixin([volumeMount.new('mimir-backend-data', '/data')]) +
    $.util.resourcesRequests(1, '12Gi') +
    $.util.resourcesLimits(null, '18Gi') +
    $.util.readinessProbe +
    $.jaeger_mixin,

  newMimirBackendZoneStatefulset(zone, container)::
    local name = 'mimir-backend-zone-%s' % zone;

    $.newMimirStatefulSet(name, 3, container, mimir_backend_data_pvc) +
    statefulSet.mixin.metadata.withLabels({ 'rollout-group': 'mimir-backend' }) +
    statefulSet.mixin.metadata.withAnnotations({ 'rollout-max-unavailable': std.toString($._config.mimir_backend_max_unavailable) }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name, 'rollout-group': 'mimir-backend' }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': 'mimir-backend' }) +
    statefulSet.mixin.spec.updateStrategy.withType('OnDelete') +
    statefulSet.mixin.spec.withReplicas(std.ceil($._config.mimir_backend_replicas / 3)) +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds(900) +
    $.mimirVolumeMounts +
    (if $._config.memberlist_ring_enabled then gossipLabel else {}) +
    (if $._config.mimir_backend_allow_multiple_replicas_on_same_node then {} else {
       spec+:
         // Allow to schedule 2+ mimir-backends in the same zone on the same node, but do not schedule 2+ mimir-backends in
         // different zones on the same node. In case of 1 node failure in the Kubernetes cluster, only mimir-backends
         // in 1 zone will be affected.
         podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecution([
           podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
           podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
             { key: 'rollout-group', operator: 'In', values: ['mimir-backend'] },
             { key: 'name', operator: 'NotIn', values: [name] },
           ]) +
           podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
         ]).spec,
     }),

  // Creates a headless service for the per-zone mimir-backends StatefulSet. We don't use it
  // but we need to create it anyway because it's responsible for the network identity of
  // the StatefulSet pods. For more information, see:
  // https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#statefulset-v1-apps
  newMimirBackendZoneService(sts)::
    $.util.serviceFor(sts, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),  // Headless.

  mimir_backend_zone_a_container:: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneContainer('a', $.mimir_backend_zone_a_args),

  mimir_backend_zone_b_container:: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneContainer('b', $.mimir_backend_zone_b_args),

  mimir_backend_zone_c_container:: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneContainer('c', $.mimir_backend_zone_c_args),

  mimir_backend_zone_a_statefulset: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneStatefulset('a', $.mimir_backend_zone_a_container),

  mimir_backend_zone_b_statefulset: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneStatefulset('b', $.mimir_backend_zone_b_container),

  mimir_backend_zone_c_statefulset: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneStatefulset('c', $.mimir_backend_zone_c_container),

  mimir_backend_zone_a_service: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneService($.mimir_backend_zone_a_statefulset),

  mimir_backend_zone_b_service: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneService($.mimir_backend_zone_b_statefulset),

  mimir_backend_zone_c_service: if !$._config.read_write_deployment_enabled then null else
    $.newMimirBackendZoneService($.mimir_backend_zone_c_statefulset),

  // Create a service backed by all mimir-backend replicas (in all zone).
  // This service is used to access the mimir-backend admin UI.
  mimir_backend_service: if !$._config.read_write_deployment_enabled then null else
    $.newMimirRolloutGroupService('mimir-backend', [$.mimir_backend_zone_a_statefulset, $.mimir_backend_zone_b_statefulset, $.mimir_backend_zone_c_statefulset], $._config.service_ignored_labels),

  mimir_backend_rollout_pdb: if !$._config.read_write_deployment_enabled then null else
    $.newMimirRolloutGroupPDB('mimir-backend', 1),
}
