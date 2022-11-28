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

  mimir_write_zone_a_container:: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneContainer('a', $.mimir_write_zone_a_args),

  mimir_write_zone_b_container:: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneContainer('b', $.mimir_write_zone_b_args),

  mimir_write_zone_c_container:: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneContainer('c', $.mimir_write_zone_c_args),

  mimir_write_zone_a_statefulset: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneStatefulset('a', $.mimir_write_zone_a_container),

  mimir_write_zone_b_statefulset: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneStatefulset('b', $.mimir_write_zone_b_container),

  mimir_write_zone_c_statefulset: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneStatefulset('c', $.mimir_write_zone_c_container),

  mimir_write_zone_a_service: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneService($.mimir_write_zone_a_statefulset),

  mimir_write_zone_b_service: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneService($.mimir_write_zone_b_statefulset),

  mimir_write_zone_c_service: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirWriteZoneService($.mimir_write_zone_c_statefulset),

  // This service is used as ingress on the write path, and to access the admin UI.
  mimir_write_service: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirRolloutGroupService('mimir-write', [$.mimir_write_zone_a_statefulset, $.mimir_write_zone_b_statefulset, $.mimir_write_zone_c_statefulset], $._config.service_ignored_labels),

  mimir_write_rollout_pdb: if !$._config.is_read_write_deployment_mode then null else
    $.newMimirRolloutGroupPDB('mimir-write', 1),
}
