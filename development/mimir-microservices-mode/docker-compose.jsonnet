local docker = import 'libs/docker.libsonnet';
local envoy = import 'libs/envoy.libsonnet';
local envs = import 'libs/envs.libsonnet';
local grafana = import 'libs/grafana.libsonnet';
local jaeger = import 'libs/jaeger.libsonnet';
local mimir = import 'libs/mimir.libsonnet';
local otel = import 'libs/otel.libsonnet';
local prometheus = import 'libs/prometheus.libsonnet';
local services = import 'libs/services.libsonnet';
local utils = import 'libs/utils.libsonnet';

{
  _config:: {
    // If true, Mimir services are run under Delve debugger, that can be attached to via remote-debugging session.
    // Note that Delve doesn't forward signals to the Mimir process, so Mimir components don't shutdown cleanly.
    debug: false,

    // How long should Mimir docker containers sleep before Mimir is started.
    sleep_seconds: 3,

    // Whether ruler should use the query-frontend and queriers to execute queries, rather than executing them in-process
    ruler_use_remote_execution: false,

    // Three options are supported for ring in this jsonnet:
    // - consul
    // - memberlist (consul is not started at all)
    // - multi (uses consul as primary and memberlist as secondary, but this can be switched in runtime via runtime.yaml)
    ring: 'memberlist',

    enable_continuous_test: true,

    // If true, a load generator is started.
    enable_load_generator: false,

    // If true, start and enable scraping by these components.
    // Note that if more than one component is enabled, the dashboards shown in Grafana may contain duplicate series or aggregates may be doubled or tripled.
    enable_grafana_agent: false,
    // If true, start a base prometheus that scrapes the Mimir component metrics and remote writes to distributor-1.
    // Two additional Prometheus instances are started that scrape the same memcached-exporter and load-generator
    // targets and remote write to distributor-2.
    prometheus: {
      enabled: true,  // If Prometheus is disabled, recording rules will not be evaluated and so dashboards in Grafana that depend on these recorded series will display no data.
      port: 9090,
    },

    otel_collector: {
      enabled: false,
      metrics_port: 8083,
    },

    // If true, a query-tee instance with a single backend is started.
    enable_query_tee: false,

    envoy: {
      traffic_port: 8080,
      metrics_port: 9901,
    },

    jaeger: {
      enabled: true,
      port: 16686,
    },

    // These are all the Mimir services, you can disable any of them by scaling to 0
    distributors: {
      replicas: 2,
      first_port: 8000,
    },

    ingesters: {
      replicas: 3,
      first_port: 8002,
    },

    queriers: {
      replicas: 1,
      first_port: 8005,
    },

    query_frontends: {
      replicas: 1,
      first_port: 8007,
    },

    query_schedulers: {
      replicas: 1,
      first_port: 8008,
    },

    compactors: {
      replicas: 1,
      first_port: 8006,
    },

    store_gateways: {
      replicas: 3,
      first_port: 8011,
    },

    rulers: {
      replicas: 2,
      first_port: 8022,
    },

    alertmanagers: {
      replicas: 2,
      first_port: 18040,
    },

    // enter any config overrides here and they'll be rendered into the runtime config
    runtime_config: {},
  },

  'docker-compose.yml': utils.manifestFormattedYaml(docker.newComposeFile({
    services:
      // External Components
      envoy.newEnvoyService({ traffic_port: $._config.envoy.traffic_port, metrics_port: $._config.envoy.metrics_port }) +
      grafana.newGrafanaService +
      (if $._config.enable_grafana_agent then grafana.newGrafanaAgentService else {}) +
      (
        if $._config.prometheus.enabled then
          prometheus.newPrometheusService({
            name: 'prometheus',
            port: $._config.prometheus.port,
            extraVolumes: [
              '../../operations/mimir-mixin-compiled/alerts.yaml:/etc/mixin/mimir-alerts.yaml',
              '../../operations/mimir-mixin-compiled/rules.yaml:/etc/mixin/mimir-rules.yaml',
            ],
          }) +
          prometheus.newPrometheusService({ name: 'prometheus-ha-1', port: $._config.prometheus.port + 1 }) +
          prometheus.newPrometheusService({ name: 'prometheus-ha-2', port: $._config.prometheus.port + 2 })
      ) +
      // External Dependencies
      services.minio +
      services.memcached +
      services.memcached_exporter +
      jaeger.newJaegerService +
      (if $._config.ring == 'consul' || $._config.ring == 'multi' then services.consul else {}) +
      (if $._config.otel_collector.enabled then otel.newOtelCollectorService({ config: $._config }) else {}) +
      mimir.newMimirServices($._config).services +
      {},
  })),

  'generated/runtime.yaml': utils.manifestFormattedYaml($._config.runtime_config),

  'generated/prometheus.yaml': utils.manifestFormattedYaml(
    prometheus.newConfig({
      scrape_interval: '5s',
      write_target: 'http://%(host)s:%(port)s/api/v1/push' % { host: 'envoy', port: $._config.envoy.traffic_port },
      targets: $.primary_scrape_targets,
      name: 'mimir-microservices',
      rule_files: [
        '/etc/mixin/mimir-alerts.yaml',
        '/etc/mixin/mimir-rules.yaml',
      ],
    })
  ),

  'generated/prometheus-ha-1.yaml': utils.manifestFormattedYaml(
    prometheus.newConfig({
      scrape_interval: '5s',
      write_target: 'http://%(host)s:%(port)s/api/v1/push' % { host: 'envoy', port: $._config.envoy.traffic_port },
      targets: $.secondary_scrape_targets,
      name: 'prometheus-ha-1',
    })
  ),

  'generated/prometheus-ha-2.yaml': utils.manifestFormattedYaml(
    prometheus.newConfig({
      scrape_interval: '5s',
      write_target: 'http://%(host)s:%(port)s/api/v1/push' % { host: 'envoy', port: $._config.envoy.traffic_port },
      targets: $.secondary_scrape_targets,
      name: 'prometheus-ha-2',
    })
  ),

  'generated/datasources.yaml': utils.manifestFormattedYaml(grafana.newDatasourcesForConfig($._config)),
  'generated/dashboards-mimir.yaml': utils.manifestFormattedYaml(grafana.mimirDashboards),
  'generated/grafana-agent.yaml': utils.manifestFormattedYaml(grafana.newAgentConfig({ config: $._config, targets: $.primary_scrape_targets })),
  'generated/otel-collector.yaml': utils.manifestFormattedYaml(otel.newOtelCollectorConfig({ config: $._config, targets: $.primary_scrape_targets })),

  'generated/envoy.yaml': utils.manifestFormattedYaml(envoy.newMimirEnvoyConfig({ config: $._config })),

  // These configuration files are imported verbatim as jsonnet'ifying them wouldn't provide any benefit
  'generated/alertmanager.yaml': importstr 'config/alertmanager.yaml',
  'generated/mimir.yaml': importstr 'config/mimir.yaml',

  primary_scrape_targets::
    (if $._config.distributors.replicas > 0 then ['distributor-%(id)d:%(port)d' % { id: id, port: $._config.distributors.first_port + id - 1 } for id in std.range(1, $._config.distributors.replicas)] else []) +
    (if $._config.ingesters.replicas > 0 then ['ingester-%(id)d:%(port)d' % { id: id, port: $._config.ingesters.first_port + id - 1 } for id in std.range(1, $._config.ingesters.replicas)] else []) +
    (if $._config.queriers.replicas > 0 then ['querier-%(id)d:%(port)d' % { id: id, port: $._config.queriers.first_port + id - 1 } for id in std.range(1, $._config.queriers.replicas)] else []) +
    (if $._config.rulers.replicas > 0 then ['ruler-%(id)d:%(port)d' % { id: id, port: $._config.rulers.first_port + id - 1 } for id in std.range(1, $._config.rulers.replicas)] else []) +
    (if $._config.compactors.replicas > 0 then ['compactor-%(id)d:%(port)d' % { id: id, port: $._config.compactors.first_port + id - 1 } for id in std.range(1, $._config.compactors.replicas)] else []) +
    (if $._config.query_frontends.replicas > 0 then ['query_frontend-%(id)d:%(port)d' % { id: id, port: $._config.query_frontends.first_port + id - 1 } for id in std.range(1, $._config.query_frontends.replicas)] else []) +
    (if $._config.query_schedulers.replicas > 0 then ['query_scheduler-%(id)d:%(port)d' % { id: id, port: $._config.query_schedulers.first_port + id - 1 } for id in std.range(1, $._config.query_schedulers.replicas)] else []) +
    (if $._config.store_gateways.replicas > 0 then ['store-gateway-%(id)d:%(port)d' % { id: id, port: $._config.store_gateways.first_port + id - 1 } for id in std.range(1, $._config.store_gateways.replicas)] else []) +
    $.secondary_scrape_targets,

  secondary_scrape_targets::
    [
      'envoy:%(port)d' % $._config.envoy.traffic_port,
      'memcached-exporter:9150',
      'continuous-test:8090',
      'load-generator:9900',
    ],
}
