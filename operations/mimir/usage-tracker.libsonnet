// The usage-tracker is an experimental component that tracks the active series of each tenant, and lets the
// distributors reject the series that exceed the active series limit. It requires ingest storage (Kafka).
{
  _config+:: {
    usage_tracker_enabled: false,
    usage_tracker_replicas_per_zone: 2,

    usage_tracker_distributor_enabled: $._config.usage_tracker_enabled,
    usage_tracker_distributor_zone_a_enabled: self.usage_tracker_distributor_enabled,
    usage_tracker_distributor_zone_b_enabled: self.usage_tracker_distributor_enabled,
    usage_tracker_ruler_enabled: $._config.usage_tracker_enabled,

    multi_zone_usage_tracker_max_unavailable: 50,

    // Configs for testing & rollout.
    usage_tracker_use_global_series_limits: true,
    usage_tracker_distributor_ignore_rejected_series: false,
    usage_tracker_distributor_ignore_errors: false,

    // Usage-tracker config that should set both on server and clients.
    usageTrackerConfig:: {
      'usage-tracker.enabled': true,
      'usage-tracker.partitions': $._config.usage_tracker_partitions,
      'usage-tracker.idle-timeout': '20m',
      'usage-tracker.partition-ring.prefix': '',
      'usage-tracker.instance-ring.prefix': '',
      'usage-tracker.instance-ring.heartbeat-period': '30s',
      'usage-tracker.instance-ring.heartbeat-timeout': '2m',
      // Even though this flag says `distributor.` it's enforced in usage-tracker.
      // It's also set on the distributors through `distributorLimitsConfig.
      [if std.get($._config.limits, 'max_active_series_per_user') != null then 'distributor.max-active-series-per-user']: $._config.limits.max_active_series_per_user,
    },

    distributorUsageTrackerConfig:: {
      [if $._config.usage_tracker_distributor_ignore_rejected_series then 'distributor.usage-tracker-client.ignore-rejected-series']: 'true',
      [if $._config.usage_tracker_distributor_ignore_errors then 'distributor.usage-tracker-client.ignore-errors']: 'true',
      'distributor.usage-tracker-client.sync-batch-delay': '50ms',
      'distributor.usage-tracker-client.batch-delay': '50ms',
      'distributor.usage-tracker-client.requests-hedging-delay': '500ms',
    },

    usage_tracker_snapshots_bucket_name: error 'must specify the usage-tracker snapshots bucket name',

    usage_tracker_partitions: 64,

    // These are the knobs to tweak if running into OOMs, increase interval (to prometheus scrape interval?)
    // and reduce max partitions to create per reconcile in order to tame memory spikes.
    usage_tracker_partition_reconcile_interval_seconds: 1,
    // Limited to 4 concurrent partition creations (down from 16) as we've seen some OOMs after restarts.
    usage_tracker_max_partitions_to_create_per_reconcile: 4,
  },

  //
  // Config validation.
  //

  // The usage-tracker is designed to run a power-of-2 number of replicas per zone. With any other
  // number, some replicas get up to 2x the load of the others.
  assert std.pow(2, std.floor(std.log($._config.usage_tracker_replicas_per_zone) / std.log(2))) == $._config.usage_tracker_replicas_per_zone :
         'usage_tracker_replicas_per_zone must be a power of 2, got %d' % $._config.usage_tracker_replicas_per_zone,

  assert std.pow(2, std.floor(std.log($._config.usage_tracker_partitions) / std.log(2))) == $._config.usage_tracker_partitions :
         'usage_tracker_partitions must be a power of 2, got %d' % $._config.usage_tracker_partitions,

  assert $._config.usage_tracker_replicas_per_zone <= $._config.usage_tracker_partitions :
         'usage_tracker_replicas_per_zone (%d) must not exceed usage_tracker_partitions (%d)' % [$._config.usage_tracker_replicas_per_zone, $._config.usage_tracker_partitions],

  //
  // Usage-tracker.
  //

  local container = $.core.v1.container,
  local deployment = $.apps.v1.deployment,
  local podAntiAffinity = deployment.mixin.spec.template.spec.affinity.podAntiAffinity,
  local podDisruptionBudget = $.policy.v1.podDisruptionBudget,
  local statefulSet = $.apps.v1.statefulSet,
  local service = $.core.v1.service,

  newUsageTrackerContainer(args, envVarMap={})::
    container.new('usage-tracker', $._images.usage_tracker) +
    container.withPorts($.usage_tracker_ports) +
    container.withArgsMixin($.util.mapToFlags(args)) +
    container.withEnvMap(std.prune(envVarMap {
      GOMAXPROCS: std.toString(
        std.max(
          std.ceil(
            $.util.parseCPU($.usage_tracker_container.resources.requests.cpu) * 1.25
          ),
          2
        )
      ),
    })) +
    $.util.resourcesRequestsMixin('2', '4Gi') +
    $.util.resourcesLimits(null, '6Gi') +
    $.util.readinessProbe +
    $.tracing_env_mixin,

  usage_tracker_termination_grace_period_seconds:: 120,

  // Add key-value pairs as annotations specifically to the usage-tracker.
  usage_tracker_annotations:: {},

  newUsageTrackerStatefulSet(name, container, nodeAffinityMatchers=[])::
    $.newMimirStatefulSet(name, null, container, []) +
    $.newMimirNodeAffinityMatchers(nodeAffinityMatchers) +
    $.newMimirStatefulsetAnnotationsMixin($.mimirComponentAnnotations + $.usage_tracker_annotations) +
    statefulSet.mixin.spec.template.spec.withTerminationGracePeriodSeconds($.usage_tracker_termination_grace_period_seconds) +
    $.mimirVolumeMounts,

  newUsageTrackerZoneStatefulSet(zone, container, nodeAffinityMatchers=[])::
    local name = 'usage-tracker-zone-%s' % zone;

    $.newUsageTrackerStatefulSet(name, container, nodeAffinityMatchers) +

    // Coordinate rollout using rollout-operator.
    statefulSet.mixin.metadata.withLabels({ 'rollout-group': 'usage-tracker' }) +
    statefulSet.mixin.metadata.withAnnotations({ 'rollout-max-unavailable': std.toString($._config.multi_zone_usage_tracker_max_unavailable) }) +
    statefulSet.mixin.spec.template.metadata.withLabels({ name: name, 'rollout-group': 'usage-tracker' }) +
    statefulSet.mixin.spec.selector.withMatchLabels({ name: name, 'rollout-group': 'usage-tracker' }) +
    statefulSet.mixin.spec.updateStrategy.withType('OnDelete') +

    statefulSet.mixin.metadata.withAnnotationsMixin({
      // Zones are scaled down at the same time, with no delay between the zones.
      'grafana.com/min-time-between-zones-downscale': '0',
    }) +

    $.removeReplicasFromSpec +
    statefulSet.mixin.metadata.withAnnotationsMixin({
      // Usage tracker statefulset will follow number of replicas from ReplicaTemplate/usage-tracker-zone-x,
      // each zone following its own ReplicaTemplate.
      'grafana.com/rollout-mirror-replicas-from-resource-api-version': 'rollout-operator.grafana.com/v1',
      'grafana.com/rollout-mirror-replicas-from-resource-kind': 'ReplicaTemplate',
      'grafana.com/rollout-mirror-replicas-from-resource-name': name,
      'grafana.com/rollout-delayed-downscale': '10m',
      'grafana.com/rollout-prepare-delayed-downscale-url': 'http://pod/usage-tracker/prepare-instance-ring-downscale',
    }) +

    // Allow to schedule 2+ usage-trackers in the same zone on the same node, but do not schedule 2+ usage-trackers in
    // different zones on the same node. In case of 1 node failure in the Kubernetes cluster, only usage-trackers
    // in 1 zone will be affected.
    {
      spec+:
        podAntiAffinity.withRequiredDuringSchedulingIgnoredDuringExecution([
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.new() +
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.mixin.labelSelector.withMatchExpressions([
            { key: 'rollout-group', operator: 'In', values: ['usage-tracker'] },
            { key: 'name', operator: 'NotIn', values: [name] },
          ]) +
          podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecutionType.withTopologyKey('kubernetes.io/hostname'),
        ]).spec,
    },

  usage_tracker_ports:: $.util.defaultPorts,

  usage_tracker_kafka_producer_client_id_settings:: $.ingest_storage_kafka_producer_client_id_settings,
  usage_tracker_kafka_consumer_client_id_settings:: $.ingest_storage_kafka_consumer_client_id_settings,

  usage_tracker_args::
    $._config.commonConfig +
    $._config.storageConfig +
    $._config.usageStatsConfig +
    $._config.grpcConfig +
    $._config.usageTrackerConfig +
    $.mimirRuntimeConfigFile +
    {
      target: 'usage-tracker',

      'server.http-listen-port': $._config.server_http_port,

      'enable-go-runtime-metrics': true,

      'usage-tracker.partition-reconcile-interval': '%ds' % $._config.usage_tracker_partition_reconcile_interval_seconds,
      'usage-tracker.max-partitions-to-create-per-reconcile': $._config.usage_tracker_max_partitions_to_create_per_reconcile,

      'usage-tracker.events-storage.writer.address': $.ingest_storage_kafka_producer_address,
      'usage-tracker.events-storage.writer.topic': 'usage-tracker-events',
      'usage-tracker.events-storage.writer.client-id': $.mimirKafkaClientID($.usage_tracker_kafka_producer_client_id_settings),
      'usage-tracker.events-storage.writer.auto-create-topic-default-partitions': $._config.usage_tracker_partitions,

      'usage-tracker.events-storage.reader.address': $.ingest_storage_kafka_consumer_address,
      'usage-tracker.events-storage.reader.topic': 'usage-tracker-events',
      'usage-tracker.events-storage.reader.client-id': $.mimirKafkaClientID($.usage_tracker_kafka_consumer_client_id_settings),

      'usage-tracker.snapshots-metadata.writer.address': $.ingest_storage_kafka_producer_address,
      'usage-tracker.snapshots-metadata.writer.topic': 'usage-tracker-snapshots-metadata',
      'usage-tracker.snapshots-metadata.writer.client-id': $.mimirKafkaClientID($.usage_tracker_kafka_producer_client_id_settings),
      'usage-tracker.snapshots-metadata.writer.auto-create-topic-default-partitions': $._config.usage_tracker_partitions,

      'usage-tracker.snapshots-metadata.reader.address': $.ingest_storage_kafka_consumer_address,
      'usage-tracker.snapshots-metadata.reader.topic': 'usage-tracker-snapshots-metadata',
      'usage-tracker.snapshots-metadata.reader.client-id': $.mimirKafkaClientID($.usage_tracker_kafka_consumer_client_id_settings),

      [
      if $._config.storage_backend == 'gcs' then 'usage-tracker.snapshots-storage.gcs.bucket-name'
      else if $._config.storage_backend == 's3' then 'usage-tracker.snapshots-storage.s3.bucket-name'
      else if $._config.storage_backend == 'azure' then 'usage-tracker.snapshots-storage.azure.container-name'
      ]: $._config.usage_tracker_snapshots_bucket_name,

      // Configure snapsahots load backoff to be less aggressive.
      'usage-tracker.snapshots-load-backoff.backoff-retries': 3,
      'usage-tracker.snapshots-load-backoff.backoff-min-period': '1s',

      [if $._config.usage_tracker_use_global_series_limits then 'usage-tracker.use-global-series-limits']: 'true',
      [if $._config.usage_tracker_use_global_series_limits then 'ingester.max-global-series-per-user']: $._config.limits.max_global_series_per_user,
    },

  usage_tracker_zone_a_args:: {},
  usage_tracker_zone_b_args:: {},

  usage_tracker_zone_a_node_affinity_matchers:: [],
  usage_tracker_zone_b_node_affinity_matchers:: [],

  usage_tracker_env_map:: {},

  usage_tracker_container:: $.newUsageTrackerContainer($.usage_tracker_args, $.usage_tracker_env_map),

  // Each StatefulSet must register in its own zone, even when both run in the same AZ: an instance owns
  // partitions based on the number of instances in its zone, so with a single zone half of them have no owner.
  usage_tracker_zone_a_container::
    $.usage_tracker_container +
    $.core.v1.container.withArgs($.util.mapToFlags($.usage_tracker_args + $.usage_tracker_zone_a_args + {
      'usage-tracker.instance-ring.instance-availability-zone': 'zone-a',
    })),

  usage_tracker_zone_b_container::
    $.usage_tracker_container +
    $.core.v1.container.withArgs($.util.mapToFlags($.usage_tracker_args + $.usage_tracker_zone_b_args + {
      'usage-tracker.instance-ring.instance-availability-zone': 'zone-b',
    })),

  usage_tracker_zone_a_statefulset: if !$._config.usage_tracker_enabled then null else
    $.newUsageTrackerZoneStatefulSet('a', $.usage_tracker_zone_a_container, $.usage_tracker_zone_a_node_affinity_matchers),

  usage_tracker_zone_b_statefulset: if !$._config.usage_tracker_enabled then null else
    $.newUsageTrackerZoneStatefulSet('b', $.usage_tracker_zone_b_container, $.usage_tracker_zone_b_node_affinity_matchers),

  usage_tracker_zone_a_replica_template: if !$._config.usage_tracker_enabled then null else
    $.replicaTemplate('usage-tracker-zone-a', $._config.usage_tracker_replicas_per_zone, 'name=usage-tracker-zone-a'),
  usage_tracker_zone_b_replica_template: if !$._config.usage_tracker_enabled then null else
    $.replicaTemplate('usage-tracker-zone-b', $._config.usage_tracker_replicas_per_zone, 'name=usage-tracker-zone-b'),

  usage_tracker_zone_a_service: if !$._config.usage_tracker_enabled then null else
    $.util.serviceFor($.usage_tracker_zone_a_statefulset, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  usage_tracker_zone_b_service: if !$._config.usage_tracker_enabled then null else
    $.util.serviceFor($.usage_tracker_zone_b_statefulset, $._config.service_ignored_labels) +
    service.mixin.spec.withClusterIp('None'),

  usage_tracker_rollout_pdb: if !$._config.usage_tracker_enabled then null else
    podDisruptionBudget.new('usage-tracker-rollout') +
    podDisruptionBudget.mixin.metadata.withLabels({ name: 'usage-tracker-rollout' }) +
    podDisruptionBudget.mixin.spec.selector.withMatchLabels({ 'rollout-group': 'usage-tracker' }) +
    podDisruptionBudget.mixin.spec.withMaxUnavailable(1),

  //
  // Distributor.
  //

  distributor_args+:: if !$._config.usage_tracker_distributor_enabled then {} else
    $._config.usageTrackerConfig + $._config.distributorUsageTrackerConfig + {
      'distributor.usage-tracker-client.prefer-availability-zone': 'zone-a',
    },

  distributor_zone_a_args+:: if !$._config.usage_tracker_distributor_zone_a_enabled then {} else
    $._config.usageTrackerConfig + $._config.distributorUsageTrackerConfig + {
      'distributor.usage-tracker-client.prefer-availability-zone': 'zone-a',
    },

  distributor_zone_b_args+:: if !$._config.usage_tracker_distributor_zone_b_enabled then {} else
    $._config.usageTrackerConfig + $._config.distributorUsageTrackerConfig + {
      'distributor.usage-tracker-client.prefer-availability-zone': 'zone-b',
    },

  // Ruler.

  ruler_args+:: if !$._config.usage_tracker_ruler_enabled then {} else
    $._config.usageTrackerConfig + $._config.distributorUsageTrackerConfig + {
      'distributor.usage-tracker-client.prefer-availability-zone': 'zone-a',
    },

  ruler_zone_a_args+:: if !$._config.usage_tracker_ruler_enabled then {} else {
    'distributor.usage-tracker-client.prefer-availability-zone': 'zone-a',
  },

  ruler_zone_b_args+:: if !$._config.usage_tracker_ruler_enabled then {} else {
    'distributor.usage-tracker-client.prefer-availability-zone': 'zone-b',
  },
}
