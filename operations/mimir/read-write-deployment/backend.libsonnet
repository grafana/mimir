{
  local container = $.core.v1.container,
  local service = $.core.v1.service,
  local podAntiAffinity = $.apps.v1.deployment.mixin.spec.template.spec.affinity.podAntiAffinity,
  local pvc = $.core.v1.persistentVolumeClaim,
  local statefulSet = $.apps.v1.statefulSet,
  local volumeMount = $.core.v1.volumeMount,

  // Utils.
  local gossipLabel = $.apps.v1.statefulSet.spec.template.metadata.withLabelsMixin({ [$._config.gossip_member_label]: 'true' }),
  local byContainerPort = function(x) x.containerPort,

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

  mimir_backend_zone_a_container:: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneContainer('a', $.mimir_backend_zone_a_args),

  mimir_backend_zone_b_container:: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneContainer('b', $.mimir_backend_zone_b_args),

  mimir_backend_zone_c_container:: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneContainer('c', $.mimir_backend_zone_c_args),

  mimir_backend_zone_a_statefulset: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneStatefulset('a', $.mimir_backend_zone_a_container),

  mimir_backend_zone_b_statefulset: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneStatefulset('b', $.mimir_backend_zone_b_container),

  mimir_backend_zone_c_statefulset: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneStatefulset('c', $.mimir_backend_zone_c_container),

  mimir_backend_zone_a_service: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneService($.mimir_backend_zone_a_statefulset),

  mimir_backend_zone_b_service: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneService($.mimir_backend_zone_b_statefulset),

  mimir_backend_zone_c_service: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirBackendZoneService($.mimir_backend_zone_c_statefulset),

  // Create a service backed by all mimir-backend replicas (in all zone).
  // This service is used to access the mimir-backend admin UI.
  mimir_backend_service: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirRolloutGroupService('mimir-backend', [$.mimir_backend_zone_a_statefulset, $.mimir_backend_zone_b_statefulset, $.mimir_backend_zone_c_statefulset], $._config.service_ignored_labels),

  mimir_backend_rollout_pdb: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirRolloutGroupPDB('mimir-backend', 1),
}
