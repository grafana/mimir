std.manifestYamlDoc({
  _config:: {
    // If true, Mimir services are run under Delve debugger, that can be attached to via remote-debugging session.
    // Note that Delve doesn't forward signals to the Mimir process, so Mimir components don't shutdown cleanly.
    debug: false,

    // How long should Mimir docker containers sleep before Mimir is started.
    sleep_seconds: 3,

    // Whether ruler should use the query-frontend and queriers to execute queries, rather than executing them in-process
    ruler_use_remote_execution: false,

    enable_continuous_test: true,

    // If true, a load generator is started.
    enable_load_generator: false,

    // If true, start and enable scraping by these components.
    // Note that if more than one component is enabled, the dashboards shown in Grafana may contain duplicate series or aggregates may be doubled or tripled.
    enable_grafana_agent: false,
    // If true, start a base prometheus that scrapes the Mimir component metrics and remote writes to distributor-1.
    // Two additional Prometheus instances are started that scrape the same memcached-exporter and load-generator
    // targets and remote write to distributor-2.
    enable_prometheus: true,  // If Prometheus is disabled, recording rules will not be evaluated and so dashboards in Grafana that depend on these recorded series will display no data.
    // Whether to use Prometheus Remote-Write 2.0 or not for the main Prometheus. This also turns on NHCB conversion.
    enable_prometheus_rw2: false,
    enable_otel_collector: false,

    // If true, a query-tee instance with a single backend is started.
    enable_query_tee: false,

    // If true, a secondary query path is started.
    enable_secondary_query_path: false,
    secondary_query_path_extra_args: [],
  },

  // We explicitely list all important services here, so that it's easy to disable them by commenting out.
  services:
    self.distributor +
    self.ingesters +
    self.read_components +  // querier, query-frontend, and query-scheduler.
    (if $._config.enable_secondary_query_path then self.secondary_read_components else {}) +
    self.store_gateways(3) +
    self.compactor +
    self.rulers(2) +
    self.alertmanagers(3) +
    self.nginx +
    self.minio +
    (if $._config.enable_continuous_test then self.continuous_test else {}) +
    (if $._config.enable_prometheus then self.prometheus + self.prompair1 + self.prompair2 else {}) +
    self.grafana +
    (if $._config.enable_grafana_agent then self.grafana_agent else {}) +
    (if $._config.enable_otel_collector then self.otel_collector else {}) +
    self.jaeger +
    self.consul +
    self.memcached +
    (if $._config.enable_load_generator then self.load_generator else {}) +
    (if $._config.enable_query_tee then self.query_tee else {}) +
    {},

  distributor:: {
    'distributor-1': mimirService({
      name: 'distributor-1',
      target: 'distributor',
      httpPort: 8000,
      extraArguments: ['-distributor.ha-tracker.consul.hostname=consul:8500'],
    }),

    'distributor-2': mimirService({
      name: 'distributor-2',
      target: 'distributor',
      httpPort: 8001,
      extraArguments: ['-distributor.ha-tracker.consul.hostname=consul:8500'],
    }),
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

    'ingester-3': mimirService({
      name: 'ingester-3',
      target: 'ingester',
      httpPort: 8004,
      jaegerApp: 'ingester-3',
      extraVolumes: ['.data-ingester-3:/tmp/mimir-tsdb-ingester:delegated'],
    }),
  },

  read_path(prefix='', portOffset=0, extraArgs=[]):: {
    [prefix + 'querier']: mimirService({
      name: prefix + 'querier',
      target: 'querier',
      httpPort: 8005 + portOffset,
      jaegerApp: prefix + 'querier',
      extraArguments: extraArgs,
    }),

    [prefix + 'query-frontend']: mimirService({
      name: prefix + 'query-frontend',
      target: 'query-frontend',
      httpPort: 8007 + portOffset,
      jaegerApp: prefix + 'query-frontend',
      extraArguments: extraArgs,
    }),

    [prefix + 'query-scheduler']: mimirService({
      name: prefix + 'query-scheduler',
      target: 'query-scheduler',
      httpPort: 8008 + portOffset,
      jaegerApp: prefix + 'query-scheduler',
      extraArguments: extraArgs,
    }),
  },

  read_components:: self.read_path(),

  secondary_read_components:: self.read_path(
    prefix='secondary-',
    portOffset=100,
    extraArgs=['-querier.ring.prefix=secondary-querier/', '-query-scheduler.ring.prefix=secondary-scheduler/'] + $._config.secondary_query_path_extra_args,
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
      httpPort: 8021 + id,
      jaegerApp: 'ruler-%d' % id,
      extraArguments: if $._config.ruler_use_remote_execution then ['-ruler.query-frontend.address=dns:///query-frontend:9007'] else [],
    })
    for id in std.range(1, count)
  },

  alertmanagers(count):: if count <= 0 then {} else {
    ['alertmanager-%d' % id]: mimirService({
      name: 'alertmanager-' + id,
      target: 'alertmanager',
      httpPort: 8030 + id,
      jaegerApp: 'alertmanager-%d' % id,
      extraArguments: ['-alertmanager.web.external-url=http://localhost:%d/alertmanager' % (8030 + id)],
    })
    for id in std.range(1, count)
  },

  store_gateways(count):: {
    ['store-gateway-%d' % id]: mimirService({
      name: 'store-gateway-' + id,
      target: 'store-gateway',
      httpPort: 8010 + id,
      jaegerApp: 'store-gateway-%d' % id,
    })
    for id in std.range(1, count)
  },

  continuous_test:: {
    'continuous-test': mimirService({
      name: 'continuous-test',
      target: 'continuous-test',
      httpPort: 8090,
      extraArguments: [
        '-tests.run-interval=2m',
        '-tests.read-endpoint=http://query-frontend:8007/prometheus',
        '-tests.tenant-id=mimir-continuous-test',
        '-tests.write-endpoint=http://distributor-1:8000',
        '-tests.write-read-series-test.max-query-age=1h',
        '-tests.write-read-series-test.num-series=100',
      ],
    }),
  },

  local all_caches = ['-blocks-storage.bucket-store.index-cache', '-blocks-storage.bucket-store.chunks-cache', '-blocks-storage.bucket-store.metadata-cache', '-query-frontend.results-cache', '-ruler-storage.cache'],

  local jaegerEnv(appName) = {
    JAEGER_AGENT_HOST: 'jaeger',
    JAEGER_AGENT_PORT: 6831,
    JAEGER_SAMPLER_TYPE: 'const',
    JAEGER_SAMPLER_PARAM: 1,
    JAEGER_TAGS: 'app=%s' % appName,
  },

  local formatEnv(env) = [
    '%s=%s' % [key, env[key]]
    for key in std.objectFields(env)
    if env[key] != null
  ],

  // This function builds docker-compose declaration for Mimir service.
  // Default grpcPort is (httpPort + 1000), and default debug port is (httpPort + 10000)
  local mimirService(serviceOptions) = std.prune({
    local defaultOptions = {
      local s = self,
      name: error 'missing name',
      target: error 'missing target',
      image: 'mimir',
      jaegerApp: self.target,
      httpPort: error 'missing httpPort',
      grpcPort: self.httpPort + 1000,
      debugPort: self.httpPort + 10000,
      // Extra arguments passed to Mimir command line.
      extraArguments: [],
      dependsOn: ['minio'] + (if s.target != 'distributor' then ['distributor-1'] else []),
      env: jaegerEnv(s.jaegerApp),
      extraVolumes: [],
      memberlistNodeName: self.jaegerApp,
      memberlistBindPort: self.httpPort + 2000,
    },

    local options = defaultOptions + serviceOptions,

    // If the image we're using is "mimir", we're building from local source. If it's anything else,
    // assume we're using a public Mimir image from dockerhub. This means that the image is distroless
    // and has `/bin/mimir` set as an entrypoint. That requires passing CLI flags in 'command' instead
    // of a string that will be passed to a shell.
    local isLocalImage = options.image == 'mimir',
    local flags = [
      '-config.file=/etc/mimir/mimir.yaml',
      '-target=%(target)s' % options,
      '-server.http-listen-port=%(httpPort)d' % options,
      '-server.grpc-listen-port=%(grpcPort)d' % options,
      '-activity-tracker.filepath=/tmp/activity/%(target)s-%(httpPort)d' % options,
      '-memberlist.nodename=%(memberlistNodeName)s' % options,
      '-memberlist.bind-port=%(memberlistBindPort)d' % options,
    ] + options.extraArguments,

    // If we're using a local image, assemble a string of the binary to execute and CLI flags to pass to
    // a shell. If this is a public Mimir image from dockerhub, pass an array of CLI flags as the command
    // since the image is distroless (no shell) and the entrypoint is `/bin/mimir`.
    local command = if isLocalImage then ['/bin/sh', '-c', std.join(' ', [
      // some of the following expressions use "... else null", which std.join seem to ignore.
      (if $._config.sleep_seconds > 0 then 'sleep %d &&' % [$._config.sleep_seconds] else null),
      (if $._config.debug then 'exec /bin/dlv exec /bin/mimir --listen=:%(debugPort)d --headless=true --api-version=2 --accept-multiclient --continue -- ' % options else 'exec /bin/mimir'),
    ] + flags)] else flags,

    build: if isLocalImage then {
      context: '.',
      dockerfile: 'dev.dockerfile',
    } else null,
    image: options.image,
    command: command,
    environment: formatEnv(options.env),
    hostname: options.name,
    // Only publish HTTP and debug port, but not gRPC one.
    ports: ['%d:%d' % [options.httpPort, options.httpPort]] +
           ['%d:%d' % [options.memberlistBindPort, options.memberlistBindPort]] +
           if $._config.debug then [
             '%d:%d' % [options.debugPort, options.debugPort],
           ] else [],
    depends_on: options.dependsOn,
    volumes: ['./config:/etc/mimir', './activity:/tmp/activity'] + options.extraVolumes,
  }),

  // Other services used by Mimir.
  consul:: {
    consul: {
      image: 'consul:1.15',
      command: ['agent', '-dev', '-client=0.0.0.0', '-log-level=debug'],
      hostname: 'consul',
      ports: ['8500:8500'],
    },
  },

  nginx:: {
    nginx: {
      hostname: 'nginx',
      image: 'nginxinc/nginx-unprivileged:1.22-alpine',
      depends_on: [
        'distributor-1',
        'alertmanager-1',
        'ruler-1',
        'query-frontend',
        'compactor',
        'grafana',
      ],
      environment: [
        'NGINX_ENVSUBST_OUTPUT_DIR=/etc/nginx',
        'DISTRIBUTOR_HOST=distributor-1:8000',
        'ALERT_MANAGER_HOST=alertmanager-1:8031',
        'RULER_HOST=ruler-1:8022',
        'QUERY_FRONTEND_HOST=query-frontend:8007',
        'COMPACTOR_HOST=compactor:8006',
      ],
      ports: ['8080:8080'],
      volumes: ['../common/config:/etc/nginx/templates'],
    },
  },

  minio:: {
    minio: {
      image: 'minio/minio:RELEASE.2025-05-24T17-08-30Z',
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
      image: 'memcached:1.6.34-alpine',
      ports: [
        '11211:11211',
      ],
    },
  },

  memcached_exporter:: {
    'memcached-exporter': {
      image: 'prom/memcached-exporter:v0.15.3',
      command: ['--memcached.address=memcached:11211', '--web.listen-address=0.0.0.0:9150'],
    },
  },

  prometheus:: {
    prometheus: {
      image: 'prom/prometheus:v3.9.1',
      command: [
        if $._config.enable_prometheus_rw2 then '--config.file=/etc/prometheus/prometheusRW2.yaml' else '--config.file=/etc/prometheus/prometheus.yaml',
        '--enable-feature=exemplar-storage',
      ],
      volumes: [
        './config:/etc/prometheus',
        '../../operations/mimir-mixin-compiled/alerts.yaml:/etc/mixin/mimir-alerts.yaml',
        '../../operations/mimir-mixin-compiled/rules.yaml:/etc/mixin/mimir-rules.yaml',
      ],
      ports: ['9090:9090'],
    },
  },

  prompair1:: {
    prompair1: {
      image: 'prom/prometheus:v3.9.1',
      hostname: 'prom-ha-pair-1',
      command: [
        '--config.file=/etc/prometheus/prom-ha-pair-1.yaml',
        '--enable-feature=exemplar-storage',
      ],
      volumes: [
        './config:/etc/prometheus',
      ],
      ports: ['9092:9090'],
    },
  },

  prompair2:: {
    prompair2: {
      image: 'prom/prometheus:v3.9.1',
      hostname: 'prom-ha-pair-2',
      command: [
        '--config.file=/etc/prometheus/prom-ha-pair-2.yaml',
        '--enable-feature=exemplar-storage',
      ],
      volumes: [
        './config:/etc/prometheus',
      ],
      ports: ['9093:9090'],
    },
  },


  grafana:: {
    grafana: {
      image: 'grafana/grafana:12.1.1',
      environment: [
        'GF_AUTH_ANONYMOUS_ENABLED=true',
        'GF_AUTH_ANONYMOUS_ORG_ROLE=Admin',
      ],
      volumes: [
        './config/datasources.yaml:/etc/grafana/provisioning/datasources/mimir.yaml',
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
      image: 'grafana/agent:v0.37.3',
      command: ['-config.file=/etc/agent-config/grafana-agent.yaml', '-metrics.wal-directory=/tmp', '-server.http.address=127.0.0.1:9091'],
      volumes: ['./config:/etc/agent-config'],
      ports: ['9091:9091'],
    },
  },

  jaeger:: {
    jaeger: {
      // Use 1.62 specifically since 1.63 removes the agent which we depend on for now.
      image: 'jaegertracing/all-in-one:1.62.0',
      ports: ['16686:16686', '14268'],
    },
  },

  otel_collector:: {
    otel_collector: {
      image: 'otel/opentelemetry-collector-contrib:0.88.0',
      command: ['--config=/etc/otel-collector/otel-collector.yaml'],
      volumes: ['./config:/etc/otel-collector'],
      ports: ['8083:8083'],
    },
  },

  load_generator:: {
    'load-generator': {
      image: 'pracucci/cortex-load-generator:add-query-support-8633d4e',
      command: [
        '--remote-url=http://distributor-2:8001/api/v1/push',
        '--remote-write-concurrency=5',
        '--remote-write-interval=10s',
        '--series-count=1000',
        '--tenants-count=1',
        '--query-enabled=true',
        '--query-interval=1s',
        '--query-url=http://querier:8005/prometheus',
        '--server-metrics-port=9900',
      ],
      ports: ['9900:9900'],
    },
  },

  query_tee:: {
    'query-tee': {
      local env = jaegerEnv('query-tee'),

      image: 'query-tee',
      build: {
        context: '../../cmd/query-tee',
      },
      command: '-backend.endpoints=http://nginx:8080 -backend.preferred=nginx -proxy.passthrough-non-registered-routes=true -server.path-prefix=/prometheus',
      environment: formatEnv(env),
      hostname: 'query-tee',
      ports: ['9999:80'],
    },
  },

  // "true" option for std.manifestYamlDoc indents arrays in objects.
}, true)
