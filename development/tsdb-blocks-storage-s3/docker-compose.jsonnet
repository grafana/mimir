std.manifestYamlDoc({
  version: '3.4',  // docker-compose YAML output version.

  _config:: {
    // If true, Mimir services are run under Delve debugger, that can be attached to via remote-debugging session.
    // Note that Delve doesn't forward signals to the Mimir process, so Mimir components don't shutdown cleanly.
    debug: false,
  },

  services: self.mimirServices + self.commonServices,

  mimirServices:: {
    distributor: mimirService({
      target: 'distributor',
      httpPort: 8001,
    }),

    'ingester-1': mimirService({
      target: 'ingester',
      httpPort: 8002,
      jaegerApp: 'ingester-1',
      extraVolumes: ['.data-ingester-1:/tmp/mimir-tsdb-ingester:delegated'],
    }),

    'ingester-2': mimirService({
      target: 'ingester',
      httpPort: 8003,
      jaegerApp: 'ingester-2',
      extraVolumes: ['.data-ingester-2:/tmp/mimir-tsdb-ingester:delegated'],
    }),

    querier: mimirService({
      target: 'querier',
      httpPort: 8004,
    }),

    compactor: mimirService({
      target: 'compactor',
      httpPort: 8006,
    }),

    'query-frontend': mimirService({
      target: 'query-frontend',
      httpPort: 8007,
      extraArguments: '-store.max-query-length=8760h',
    }),

    'query-scheduler': mimirService({
      target: 'query-scheduler',
      httpPort: 8011,
      extraArguments: '-store.max-query-length=8760h',
    }),

    // This frontend uses query-scheduler, activated by `-frontend.scheduler-address` option.
    'query-frontend-with-scheduler': mimirService({
      target: 'query-frontend',
      httpPort: 8012,
      jaegerApp: 'query-frontend-with-scheduler',
      extraArguments: '-store.max-query-length=8760h -frontend.scheduler-address=query-scheduler:9011',
    }),

    // This querier is connecting to query-scheduler, instead of query-frontend. This is achieved by setting -querier.scheduler-address="..."
    'querier-with-scheduler': mimirService({
      target: 'querier',
      httpPort: 8013,
      jaegerApp: 'querier-with-scheduler',
      extraArguments: '-querier.scheduler-address=query-scheduler:9011 -querier.frontend-address=',
    }),

    purger: mimirService({
      target: 'purger',
      httpPort: 8014,
    }),

    'store-gateway-1': mimirService({
      target: 'store-gateway',
      httpPort: 8008,
      jaegerApp: 'store-gateway-1',
    }),

    'store-gateway-2': mimirService({
      target: 'store-gateway',
      httpPort: 8009,
      jaegerApp: 'store-gateway-2',
    }),

    'alertmanager-1': mimirService({
      target: 'alertmanager',
      httpPort: 8031,
      extraArguments: '-alertmanager.web.external-url=http://localhost:8031/alertmanager',
      jaegerApp: 'alertmanager-1',
    }),
    'alertmanager-2': mimirService({
      target: 'alertmanager',
      httpPort: 8032,
      extraArguments: '-alertmanager.web.external-url=http://localhost:8032/alertmanager',
      jaegerApp: 'alertmanager-2',
    }),
    'alertmanager-3': mimirService({
      target: 'alertmanager',
      httpPort: 8033,
      extraArguments: '-alertmanager.web.external-url=http://localhost:8033/alertmanager',
      jaegerApp: 'alertmanager-3',
    }),

    'ruler-1': mimirService({
      target: 'ruler',
      httpPort: 8021,
      jaegerApp: 'ruler-1',
    }),

    'ruler-2': mimirService({
      target: 'ruler',
      httpPort: 8022,
      jaegerApp: 'ruler-2',
    }),
  },

  // This function builds docker-compose declaration for Mimir service.
  // Default grpcPort is (httpPort + 1000), and default debug port is (httpPort + 10000)
  local mimirService(serviceOptions) = {
    local defaultOptions = {
      local s = self,
      target: error 'missing target',
      jaegerApp: self.target,
      httpPort: error 'missing httpPort',
      grpcPort: self.httpPort + 1000,
      debugPort: self.httpPort + 10000,
      // Extra arguments passed to Mimir command line.
      extraArguments: '',
      dependsOn: ['consul', 'minio'],
      env: {
        JAEGER_AGENT_HOST: 'jaeger',
        JAEGER_AGENT_PORT: 6831,
        JAEGER_SAMPLER_TYPE: 'const',
        JAEGER_SAMPLER_PARAM: 1,
        JAEGER_TAGS: 'app=%s' % s.jaegerApp,
      },
      extraVolumes: [],
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
      if $._config.debug then
        'sleep 3 && exec ./dlv exec ./mimir --listen=:%(debugPort)d --headless=true --api-version=2 --accept-multiclient --continue -- -config.file=./config/mimir.yaml -target=%(target)s -server.http-listen-port=%(httpPort)d -server.grpc-listen-port=%(grpcPort)d %(extraArguments)s' % options
      else
        'sleep 3 && exec ./mimir -config.file=./config/mimir.yaml -target=%(target)s -server.http-listen-port=%(httpPort)d -server.grpc-listen-port=%(grpcPort)d %(extraArguments)s' % options,
    ],
    environment: [
      '%s=%s' % [key, options.env[key]]
      for key in std.objectFields(options.env)
      if options.env[key] != null
    ],
    // Only publish HTTP and debug port, but not gRPC one.
    ports: ['%d:%d' % [options.httpPort, options.httpPort]] +
           if $._config.debug then [
             '%d:%d' % [options.debugPort, options.debugPort],
           ] else [],
    depends_on: options.dependsOn,
    volumes: ['./config:/mimir/config'] + options.extraVolumes,
  },

  commonServices:: {
    consul: {
      image: 'consul',
      command: ['agent', '-dev', '-client=0.0.0.0', '-log-level=info'],
      ports: ['8500:8500'],
    },

    minio: {
      image: 'minio/minio',
      command: ['server', '/data'],
      environment: ['MINIO_ACCESS_KEY=mimir', 'MINIO_SECRET_KEY=supersecret'],
      ports: ['9000:9000'],
      volumes: ['.data-minio:/data:delegated'],
    },

    memcached: {
      image: 'memcached:1.6',
    },

    prometheus: {
      image: 'prom/prometheus:v2.27.1',
      command: [
        '--config.file=/etc/prometheus/prometheus.yaml',
        '--enable-feature=exemplar-storage',
      ],
      volumes: ['./config:/etc/prometheus'],
      ports: ['9090:9090'],
    },

    // Scrape the metrics also with the Grafana agent (useful to test metadata ingestion
    // until metadata remote write is not supported by Prometheus).
    'grafana-agent': {
      image: 'grafana/agent:v0.2.0',
      command: ['-config.file=/etc/agent-config/grafana-agent.yaml', '-prometheus.wal-directory=/tmp'],
      volumes: ['./config:/etc/agent-config'],
      ports: ['9091:9091'],
    },

    jaeger: {
      image: 'jaegertracing/all-in-one',
      ports: ['16686:16686', '14268'],
    },
  },

  // "true" option for std.manifestYamlDoc indents arrays in objects.
}, true)
