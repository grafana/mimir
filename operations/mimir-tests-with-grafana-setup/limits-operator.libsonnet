local k = import 'ksonnet-util/kausal.libsonnet',
      ksonnetUtil = import 'ksonnet-util/util.libsonnet',
      externalSecrets = import 'external-secrets/main.libsonnet',
      kafka = import 'kafka-cloud/kafka.libsonnet',
      configMap = k.core.v1.configMap,
      deployment = k.apps.v1.deployment,
      container = k.core.v1.container,
      policyRule = k.rbac.v1.policyRule,
      service = k.core.v1.service,
      serviceAccount = k.core.v1.serviceAccount,
      servicePort = k.core.v1.servicePort,
      statefulSet = k.apps.v1.statefulSet,
      volume = k.core.v1.volume,
      volumeMount = k.core.v1.volumeMount;

// removeVolume returns a mixin that removes volume and volumeMounts for given configmap name.
local removeVolume(name) =
  { spec+: { template+: { spec+: { volumes: [v for v in super.volumes if v.name != name] } } } } +
  statefulSet.mapContainers(function(c) c { volumeMounts: [vm for vm in c.volumeMounts if vm.name != name] });

{
  _images+:: {
    limits_operator: 'us-docker.pkg.dev/grafanalabs-dev/docker-mimir-limits-operator-dev/mimir-limits-operator:main-20240923-160047-9f09426b13bd5f9d70bff4eab8304bed9946f90b',
    redpanda_console: 'docker.redpanda.com/redpandadata/console:v2.7.0@sha256:6ee7ed1203a7d4fd4181765929b28cb2450296402d82cd4d1c491c2d5191f45d',
  },

  _config+:: {
    limits_operator+: {
      deploy: false,
      mount_configmap_in_mimir: false,

      dry_run: false,  // dry-run will not write the configmap
      observe_config_api: false,

      overrides_configmap_name: 'limits-operator-overrides',
      overrides_configmap_filename: 'overrides.json',
      overrides_mountpoint: '/etc/%s' % $._config.limits_operator.overrides_configmap_name,
      overrides_path: '%s/%s' % [$._config.limits_operator.overrides_mountpoint, $._config.limits_operator.overrides_configmap_filename],

      trigger_enabled: false,
      use_partitions: $._config.ingest_storage_enabled,
    },

    configmaps+:
      if $._config.limits_operator.mount_configmap_in_mimir
      then { [$._config.limits_operator.overrides_configmap_name]: $._config.limits_operator.overrides_mountpoint }
      else {},

    runtime_config_files+:
      if $._config.limits_operator.mount_configmap_in_mimir
      then [$._config.limits_operator.overrides_path]
      else [],

    assert !$._config.limits_operator.observe_config_api || $._config.config_api_enabled : 'config-api should be enabled in order to observe it.',
    assert !$._config.limits_operator.mount_configmap_in_mimir || ($._config.limits_operator.deploy && !$._config.limits_operator.dry_run) : 'mount_configmap_in_mimir requires limits-operator to be deployed and not dry_run.',

    limits_operator_scaling_trigger:: {
      query: 'mimir_limits_operator_desired_min_ingesters{namespace="%s"}' % [$._config.namespace],
      threshold: '1',
      metric_type: 'AverageValue',  // HPA will compute desired replicas as "<query result> / <threshold>".
    },

    new_ingester_hpa_triggers+: if $._config.limits_operator.deploy && $._config.limits_operator.trigger_enabled then [$._config.limits_operator_scaling_trigger] else [],
  },

  limits_operator+: if !$._config.limits_operator.deploy then {} else {
    // Skipped, not interesting for ingest-storage migration.
    //    secret: externalSecrets.mapExternalSecret('limits-operator', 'common/mimir-limits-operator'),

    local metrics = {
      address: 'https://prometheus-dev-01-dev-us-central-0.grafana-dev.net/api/prom/',
      username: '9960',
    },

    args:: {
      'common.kafka.brokers': 'limitskafka.%s.svc.cluster.local.:9092' % $._config.namespace,
      'common.kafka.topic-prefix': 'mlo-%s-' % $._config.namespace,

      'frontend.namespace': $._config.namespace,
      'frontend.base-path': '/directory-cortex/%s/limits-operator' % $._config.namespace,
      // Root directory has subfilter: https://github.com/grafana/deployment_tools/blob/c230a01660733a53ebfba6f7ab49e0a79b9fde53/ksonnet/lib/cluster-admin-pages/nginx-directory.libsonnet#L120-L120
      'frontend.remove-subfilter-base-path-prefix': '/directory-cortex',

      'runtime-config-observer.files': std.manifestJsonMinified({
        'merged-overrides': {
          kind: { type: 'merged' },
          paths: [
            file
            for file in $._config.runtime_config_files
            // Don't mount our own overrides as part of calculated overrides.
            if file != $._config.limits_operator.overrides_path
          ],
        },
      }),
      [if $._config.limits_operator.dry_run then 'configmap-updater.enabled']: 'false',
      [if !$._config.limits_operator.dry_run then 'configmap-updater.kubernetes-namespace']: $._config.namespace,
      [if !$._config.limits_operator.dry_run then 'configmap-updater.configmap-name']: $._config.limits_operator.overrides_configmap_name,
      [if !$._config.limits_operator.dry_run then 'configmap-updater.configmap-filename']: $._config.limits_operator.overrides_configmap_filename,

      'server.host': '0.0.0.0',
      'server.port': '8001',

      [if !$._config.limits_operator.observe_config_api then 'config-api-observer.enabled']: 'false',
      'config-api.address': 'http://config-api.%(namespace)s.svc.%(cluster_domain)s/prometheus' % $._config,

      'metrics.address': metrics.address,
      'metrics.username': metrics.username,
      'metrics.password-file': '/etc/limits-operator-secret/metrics-read-token',

      'cell-observer.namespace': $._config.namespace,

      'operator.business.default-limits.max-global-series-per-user': $._config.limits.max_global_series_per_user,
      'operator.business.default-limits.ingestion-tenant-shard-size': $._config.shuffle_sharding.ingester_shard_size,
      'operator.business.default-limits.ingestion-partitions-tenant-shard-size': $._config.shuffle_sharding.ingester_partitions_shard_size,
      'operator.business.default-limits.ingestion-rate': $._config.limits.ingestion_rate,
      'operator.business.default-limits.ingestion-burst-size': $._config.limits.ingestion_burst_size,
      'operator.business.default-limits.store-gateway-tenant-shard-size': $._config.shuffle_sharding.store_gateway_shard_size,
      // -operator.default-limits.ingestion-burst-factor: we don't seem to be it anywhere, so we skip it here.
      'operator.business.limits-increase-duration-after-relabeling-change': '3h',

      // sigyn cells have a different series target.
      'cell-observer.default-target-series': '%d' % (if $._config.ingest_storage_enabled then 1.5e6 else 2e6),
      [if $._config.limits_operator.use_partitions then 'cell-observer.use-partitions']: true,
    },

    container::
      container.new('limits-operator', $._images.limits_operator)
      + container.withArgsMixin($.util.mapToFlags($.limits_operator.args))
      + container.withPorts([$.core.v1.containerPort.new('http-metrics', 8001)])
      + $.util.resourcesRequests('100m', '128Mi')
      + $.util.resourcesLimits(null, '256Mi')
      + container.mixin.readinessProbe.httpGet.withPath('/ready')
      + container.mixin.readinessProbe.httpGet.withPort(8001)
      + container.mixin.readinessProbe.withInitialDelaySeconds(5)
      + container.mixin.readinessProbe.withTimeoutSeconds(1)
      + $.jaeger_mixin,

    statefulset:
      statefulSet.new('limits-operator', 1, [$.limits_operator.container])
      + statefulSet.mixin.spec.withServiceName('limits-operator')
      //      + statefulSet.mixin.spec.template.spec.withImagePullSecrets({ name: $._config.gar_pull_secret })
      + (if $._config.limits_operator.dry_run then {} else statefulSet.mixin.spec.template.spec.withServiceAccountName('limits-operator'))
      + k.util.secretVolumeMount('limits-operator', '/etc/limits-operator-secret')
      // Mount mimir volumes, but don't mount our own overrides, as we are who are creating them.
      + $.mimirVolumeMounts + removeVolume($._config.limits_operator.overrides_configmap_name),

    service:
      ksonnetUtil.serviceFor($.limits_operator.statefulset)
      + service.mixin.spec.withPorts([servicePort.newNamed('http-metrics', 80, 'http-metrics')]),

    rbac: if $._config.limits_operator.dry_run then {} else
      ksonnetUtil.namespacedRBAC('limits-operator', [
        // Create any name.
        policyRule.withApiGroups(['']) +
        policyRule.withResources(['configmaps']) +
        policyRule.withVerbs(['create']),
        // Update only specific one.
        policyRule.withApiGroups(['']) +
        policyRule.withResources(['configmaps']) +
        policyRule.withResourceNames([$._config.limits_operator.overrides_configmap_name]) +
        policyRule.withVerbs(['get', 'update']),
      ], $._config.namespace),
  },
}

// Skip Kafka deployment, it's not interesting for ingest-storage migration.

// limitskafka kafka helm chart instance, with 3 replicas + redpanda console for monitoring.
//{
//  limits_operator+: if !$._config.limits_operator.deploy then {} else {
//    local jmx_configmap_name = 'limits-operator-kafka-jmx-configuration',
//    limits_operator_kafka+: kafka {
//      _config+:: {
//        namespace: $._config.namespace,
//        pull_secret: 'gcr',
//
//        kafka+: {
//          cluster_id: kafka.generate_cluster_id('%(cluster)s.%(namespace)s.mimir-limits-operator' % $._config),
//          chartVersion: '22.1.6',
//          helmValues+: {
//            image: kafka.process_image_name($._images.adaptive_metrics_kafka),
//
//            replicaCount: 3,
//            defaultReplicationFactor: 3,
//            numPartitions: 2,
//            deleteTopicEnable: true,
//
//            zookeeper+: { enabled: false },
//            provisioning+: { enabled: false },
//
//            kraft+: {
//              clusterId: $._config.kafka.cluster_id,
//              processRoles: 'broker,controller',
//            },
//
//            local megabyte = 1e6,
//            local gigabyte = 1e9,
//            logRetentionHours: 32 * 24,
//            logRetentionBytes: '%d' % (1 * gigabyte),
//            logSegmentBytes: '%d' % (100 * megabyte),
//
//            service+: {
//              headless+: {
//                // needed for cluster bootstrapping when all pods are non-ready
//                publishNotReadyAddresses: true,
//              },
//            },
//
//            readinessProbe: { initialDelaySeconds: 180 },
//
//            // Kafka resources
//            resources: {
//              requests: {
//                cpu: '250m',
//                memory: '1Gi',
//              },
//              limits: {
//                memory: '1Gi',  // See heap_opts.
//              },
//            },
//            heapOpts: '-Xms512m -Xmx512m',
//            persistence: { size: '10Gi', storageClass: 'fast' },
//
//            // Resources for metrics exporter.
//            metrics+: {
//              jmx+: {
//                // enable jmx_exporter for the subset of metrics configured in the given configmap.
//                enabled: true,
//                resources: {
//                  requests: { cpu: '0.1', memory: '100Mi' },
//                  limits: { cpu: '1', memory: '1Gi' },
//                },
//                existingConfigmap: jmx_configmap_name,
//              },
//            },
//
//            pdb+: { create: true, maxUnavailable: 1 },
//            // use host names that can be resolved across clusters.
//            clusterDomain: $._config.cluster + '.local',
//          },
//        },
//      },
//
//      kafka_jmx_metrics_config:
//        $.core.v1.configMap.new(jmx_configmap_name) +
//        $.core.v1.configMap.withData({ 'jmx-kafka-prometheus.yml': $.util.manifestYaml(import 'mimir-aggregations/kafka/kafka_metrics.libsonnet') }),
//
//      local this = self,
//      // override kafkaChart because we're using a custom name.
//      kafkaChart: kafka.instantiateKafkaChart('limitskafka', {
//        chartVersion: this._config.kafka.chartVersion,
//        // Following fields are used by Helm chart libsonnet.
//        namespace: $._config.namespace,
//        values: this._config.kafka.helmValues,
//        changeControllerName: false,
//      }),
//    },
//
//    redpanda_console_container+::
//      container.new('redpanda-console', $._images.redpanda_console)
//      + container.withPorts([$.core.v1.containerPort.new('http', 8080)])
//      + container.withEnvMixin(
//        [
//          { name: 'KAFKA_BROKERS', value: 'limitskafka.%s.svc.cluster.local.:9092' % $._config.namespace },
//          { name: 'SERVER_BASEPATH', value: '/directory-cortex/%s/limits-operator-kafka-redpanda-console/' % $._config.namespace },
//        ]
//      )
//      + $.util.resourcesRequests('100m', '128Mi')
//      + $.util.resourcesLimits(null, '256Mi'),
//
//    redpanda_console_deployment:
//      deployment.new('limits-operator-redpanda-console', 1, [$.limits_operator.redpanda_console_container]),
//
//    redpanda_console_service:
//      k.util.serviceFor($.limits_operator.redpanda_console_deployment),
//  },
//}
