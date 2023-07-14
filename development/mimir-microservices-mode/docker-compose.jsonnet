std.manifestYamlDoc({
  _config:: {
    // Cache backend to use for results, chunks, index, and metadata caches. Options are 'memcached' or 'redis'.
    cache_backend: 'memcached',

    // If true, Mimir services are run under Delve debugger, that can be attached to via remote-debugging session.
    // Note that Delve doesn't forward signals to the Mimir process, so Mimir components don't shutdown cleanly.
    debug: false,

    // How long should Mimir docker containers sleep before Mimir is started.
    sleep_seconds: 3,

    // Whether query-frontend and querier should use query-scheduler. If set to true, query-scheduler is started as well.
    use_query_scheduler: true,

    // Three options are supported for ring in this jsonnet:
    // - consul
    // - memberlist (consul is not started at all)
    // - multi (uses consul as primary and memberlist as secondary, but this can be switched in runtime via runtime.yaml)
    ring: 'memberlist',

    // If true, a load generator is started.
    enable_load_generator: true,

    // If true, start and enable scraping by these components.
    // Note that if more than one component is enabled, the dashboards shown in Grafana may contain duplicate series or aggregates may be doubled or tripled.
    enable_grafana_agent: false,
    enable_prometheus: true,  // If Prometheus is disabled, recording rules will not be evaluated and so dashboards in Grafana that depend on these recorded series will display no data.
    enable_otel_collector: false,
  },

  // We explicitely list all important services here, so that it's easy to disable them by commenting out.
  services:
    self.gateway +
    self.distributors(2) +
    self.ingesters +
    self.read_components +  // Querier, Frontend and query-scheduler, if enabled.
    self.store_gateways +
    self.compactor +
    //    self.rulers(2) +
    //    self.alertmanagers(3) +
    self.nginx +
    self.minio +
    (if $._config.enable_prometheus then self.prometheus else {}) +
    self.grafana +
    //    (if $._config.enable_grafana_agent then self.grafana_agent else {}) +
    //    (if $._config.enable_otel_collector then self.otel_collector else {}) +
    //    self.jaeger +
    //    (if $._config.ring == 'consul' || $._config.ring == 'multi' then self.consul else {}) +
    (if $._config.cache_backend == 'redis' then self.redis else self.memcached + self.memcached_exporter) +
    //    (if $._config.enable_load_generator then self.load_generator else {}) +
    {},

  gateway:: {
    'gateway-1': {
      image: 'us.gcr.io/kubernetes-dev/cloud-backend-gateway:master-a08103ce-WIP',
      command: ['/usr/bin/cloud-backend-gateway', '-server.http-listen-port=7999', '-auth.disabled', '-cortex.oauth.enabled=false', '-auditlogging.enabled=false', '-cortex.distributor.endpoint=dns:///distributor:9000', '-instrumentation.server.port=7998'],
      entrypoint: '',
      ports: ['7998:7998', '7999:7999'],
    },
  },

  distributors(replicas):: {
    distributor: mimirService({
      name: 'distributor',
      target: 'distributor',
      httpPort: 8000,

      // Do not publish ports otherwise they will conflict with 2+ replicas.
      httpPortPublished: false,
      memberlistBindPortPublished: false,
      debugPortPublished: false,
    }) + {
      deploy: {
        replicas: replicas,
      },
    },
  },

  ingesters:: {
    'ingester-1': mimirService({
      name: 'ingester-1',
      target: 'ingester',
      httpPort: 8002,
      jaegerApp: 'ingester-1',
      extraVolumes: ['.data-ingester-1:/tmp/mimir-tsdb-ingester:delegated'],
    }),

    'ingester-2': mimirService({
      name: 'ingester-2',
      target: 'ingester',
      httpPort: 8003,
      jaegerApp: 'ingester-2',
      extraVolumes: ['.data-ingester-2:/tmp/mimir-tsdb-ingester:delegated'],
    }),
  },

  read_components::
    {
      querier: mimirService({
        name: 'querier',
        target: 'querier',
        httpPort: 8004,
        extraArguments:
          // Use of scheduler is activated by `-querier.scheduler-address` option and setting -querier.frontend-address option to nothing.
          if $._config.use_query_scheduler then '-querier.scheduler-address=query-scheduler:9011 -querier.frontend-address=' else '',
      }),

      'query-frontend': mimirService({
        name: 'query-frontend',
        target: 'query-frontend',
        httpPort: 8007,
        jaegerApp: 'query-frontend',
        extraArguments:
          '-query-frontend.max-total-query-length=8760h' +
          // Use of scheduler is activated by `-query-frontend.scheduler-address` option.
          (if $._config.use_query_scheduler then ' -query-frontend.scheduler-address=query-scheduler:9011' else ''),
      }),
    } + (
      if $._config.use_query_scheduler then {
        'query-scheduler': mimirService({
          name: 'query-scheduler',
          target: 'query-scheduler',
          httpPort: 8011,
          extraArguments: '-query-frontend.max-total-query-length=8760h',
        }),
      } else {}
    ),

  compactor:: {
    compactor: mimirService({
      name: 'compactor',
      target: 'compactor',
      httpPort: 8006,
    }),
  },

  rulers(count):: if count <= 0 then {} else {
    ['ruler-%d' % id]: mimirService({
      name: 'ruler-' + id,
      target: 'ruler',
      httpPort: 8020 + id,
      jaegerApp: 'ruler-%d' % id,
    })
    for id in std.range(1, count)
  },

  alertmanagers(count):: if count <= 0 then {} else {
    ['alertmanager-%d' % id]: mimirService({
      name: 'alertmanager-' + id,
      target: 'alertmanager',
      httpPort: 8030 + id,
      extraArguments: '-alertmanager.web.external-url=http://localhost:%d/alertmanager' % (8030 + id),
      jaegerApp: 'alertmanager-%d' % id,
    })
    for id in std.range(1, count)
  },

  store_gateways:: {
    'store-gateway-1': mimirService({
      name: 'store-gateway-1',
      target: 'store-gateway',
      httpPort: 8008,
      jaegerApp: 'store-gateway-1',
    }),

    'store-gateway-2': mimirService({
      name: 'store-gateway-2',
      target: 'store-gateway',
      httpPort: 8009,
      jaegerApp: 'store-gateway-2',
    }),
  },

  local all_caches = ['-blocks-storage.bucket-store.index-cache', '-blocks-storage.bucket-store.chunks-cache', '-blocks-storage.bucket-store.metadata-cache', '-query-frontend.results-cache', '-ruler-storage.cache'],

  local all_rings = ['-ingester.ring', '-distributor.ring', '-compactor.ring', '-store-gateway.sharding-ring', '-ruler.ring', '-alertmanager.sharding-ring'],

  // This function builds docker-compose declaration for Mimir service.
  // Default grpcPort is (httpPort + 1000), and default debug port is (httpPort + 10000)
  local mimirService(serviceOptions) = {
    local defaultOptions = {
      local s = self,
      name: error 'missing name',
      target: error 'missing target',
      jaegerApp: self.target,
      httpPort: error 'missing httpPort',
      httpPortPublished: true,
      grpcPort: self.httpPort + 1000,
      debugPort: self.httpPort + 10000,
      debugPortPublished: true,
      // Extra arguments passed to Mimir command line.
      extraArguments: '',
      dependsOn: ['minio'] + (if $._config.ring == 'consul' || $._config.ring == 'multi' then ['consul'] else if s.target != 'distributor' then ['distributor'] else []),
      env: {
        JAEGER_AGENT_HOST: 'jaeger',
        JAEGER_AGENT_PORT: 6831,
        JAEGER_SAMPLER_TYPE: 'const',
        JAEGER_SAMPLER_PARAM: 1,
        JAEGER_TAGS: 'app=%s' % s.jaegerApp,
      },
      extraVolumes: [],
      memberlistNodeName: self.jaegerApp,
      memberlistBindPort: self.httpPort + 2000,
      memberlistBindPortPublished: true,
    },

    local options = defaultOptions + serviceOptions,

    build: {
      context: '.',
      dockerfile: 'dev.dockerfile',
    },
    image: 'mimir',
    command: [
      'sh',
      '-c',
      std.join(' ', [
        // some of the following expressions use "... else null", which std.join seem to ignore.
        (if $._config.sleep_seconds > 0 then 'sleep %d &&' % [$._config.sleep_seconds] else null),
        (if $._config.debug then 'exec ./dlv exec ./mimir --listen=:%(debugPort)d --headless=true --api-version=2 --accept-multiclient --continue -- ' % options else 'exec ./mimir'),
        ('-config.file=./config/mimir.yaml -target=%(target)s -server.http-listen-port=%(httpPort)d -server.grpc-listen-port=%(grpcPort)d -activity-tracker.filepath=/activity/%(target)s-%(httpPort)d %(extraArguments)s' % options),
        (if $._config.ring == 'memberlist' || $._config.ring == 'multi' then '-memberlist.nodename=%(memberlistNodeName)s -memberlist.bind-port=%(memberlistBindPort)d' % options else null),
        (if $._config.ring == 'memberlist' then std.join(' ', [x + '.store=memberlist' for x in all_rings]) else null),
        (if $._config.ring == 'multi' then std.join(' ', [x + '.store=multi' for x in all_rings] + [x + '.multi.primary=consul' for x in all_rings] + [x + '.multi.secondary=memberlist' for x in all_rings]) else null),
        std.join(' ', if $._config.cache_backend == 'redis' then [x + '.backend=redis' for x in all_caches] + [x + '.redis.endpoint=redis:6379' for x in all_caches] else [x + '.backend=memcached' for x in all_caches] + [x + '.memcached.addresses=dns+memcached:11211' for x in all_caches]),
      ]),
    ],
    environment: [
      '%s=%s' % [key, options.env[key]]
      for key in std.objectFields(options.env)
      if options.env[key] != null
    ],
    hostname: options.name,
    // Only publish HTTP and debug port, but not gRPC one.
    ports:
      (if options.httpPortPublished then ['%d:%d' % [options.httpPort, options.httpPort]] else []) +
      (if options.memberlistBindPortPublished then ['%d:%d' % [options.memberlistBindPort, options.memberlistBindPort]] else []) +
      (if $._config.debug && options.debugPortPublished then ['%d:%d' % [options.debugPort, options.debugPort]] else []),
    depends_on: options.dependsOn,
    volumes: ['./config:/mimir/config', './activity:/activity'] + options.extraVolumes,
  },

  // Other services used by Mimir.
  consul:: {
    consul: {
      image: 'consul',
      command: ['agent', '-dev', '-client=0.0.0.0', '-log-level=info'],
      ports: ['8500:8500'],
    },
  },

  nginx:: {
    nginx: {
      hostname: 'nginx',
      image: 'nginxinc/nginx-unprivileged:1.22-alpine',
      environment: [
        'NGINX_ENVSUBST_OUTPUT_DIR=/etc/nginx',
        'DISTRIBUTOR_HOST=gateway:7999',
        'ALERT_MANAGER_HOST=alertmanager-1:8031',
        'RULER_HOST=ruler-1:8021',
        'QUERY_FRONTEND_HOST=query-frontend:8007',
        'COMPACTOR_HOST=compactor:8007',
      ],
      ports: ['8080:8080'],
      volumes: ['../common/config:/etc/nginx/templates'],
    },
  },

  minio:: {
    minio: {
      image: 'minio/minio',
      command: ['server', '--console-address', ':9001', '/data'],
      environment: ['MINIO_ROOT_USER=mimir', 'MINIO_ROOT_PASSWORD=supersecret'],
      ports: [
        '9000:9000',
        '9001:9001',
      ],
      volumes: ['.data-minio:/data:delegated'],
    },
  },

  memcached:: {
    memcached: {
      image: 'memcached:1.6.19-alpine',
      ports: [
        '11211:11211',
      ],
    },
  },

  memcached_exporter:: {
    'memcached-exporter': {
      image: 'prom/memcached-exporter:v0.6.0',
      command: ['--memcached.address=memcached:11211', '--web.listen-address=0.0.0.0:9150'],
    },
  },

  redis:: {
    redis: {
      image: 'redis:7.0.7',
      command: [
        'redis-server',
        '--maxmemory 64mb',
        '--maxmemory-policy allkeys-lru',
        "--save ''",
        '--appendonly no',
      ],
    },
  },

  prometheus:: {
    prometheus: {
      image: 'prom/prometheus:v2.45.0',
      command: [
        '--config.file=/etc/prometheus/prometheus.yaml',
        '--enable-feature=exemplar-storage',
        '--enable-feature=native-histograms',
      ],
      volumes: [
        './config:/etc/prometheus',
        '../../operations/mimir-mixin-compiled/alerts.yaml:/etc/mixin/mimir-alerts.yaml',
        '../../operations/mimir-mixin-compiled/rules.yaml:/etc/mixin/mimir-rules.yaml',
      ],
      ports: ['9090:9090'],
    },
  },

  grafana:: {
    grafana: {
      image: 'grafana/grafana:9.4.3',
      environment: [
        'GF_AUTH_ANONYMOUS_ENABLED=true',
        'GF_AUTH_ANONYMOUS_ORG_ROLE=Admin',
      ],
      volumes: [
        './config/datasource-mimir.yaml:/etc/grafana/provisioning/datasources/mimir.yaml',
        './config/dashboards-mimir.yaml:/etc/grafana/provisioning/dashboards/mimir.yaml',
        '../../operations/mimir-mixin-compiled/dashboards:/var/lib/grafana/dashboards/Mimir',
      ],
      ports: ['3000:3000'],
    },
  },

  grafana_agent:: {
    // Scrape the metrics also with the Grafana agent (useful to test metadata ingestion
    // until metadata remote write is not supported by Prometheus).
    'grafana-agent': {
      image: 'grafana/agent:v0.21.2',
      command: ['-config.file=/etc/agent-config/grafana-agent.yaml', '-prometheus.wal-directory=/tmp'],
      volumes: ['./config:/etc/agent-config'],
      ports: ['9091:9091'],
    },
  },

  jaeger:: {
    jaeger: {
      image: 'jaegertracing/all-in-one',
      ports: ['16686:16686', '14268'],
    },
  },

  otel_collector:: {
    otel_collector: {
      image: 'otel/opentelemetry-collector-contrib:0.54.0',
      command: ['--config=/etc/otel-collector/otel-collector.yaml'],
      volumes: ['./config:/etc/otel-collector'],
      ports: ['8083:8083'],
    },
  },

  load_generator:: {
    'load-generator': {
      image: 'pracucci/cortex-load-generator:add-query-support-8633d4e',
      command: [
        '--remote-url=http://gateway:7999/api/v1/push',
        '--remote-write-concurrency=5',
        '--remote-write-interval=10s',
        '--series-count=1000',
        '--tenants-count=1',
        '--query-enabled=true',
        '--query-interval=1s',
        '--query-url=http://querier:8004/prometheus',
        '--server-metrics-port=9900',
      ],
      ports: ['9900:9900'],
    },
  },

  // docker-compose YAML output version.
  version: '3.4',

  // "true" option for std.manifestYamlDoc indents arrays in objects.
}, true)
