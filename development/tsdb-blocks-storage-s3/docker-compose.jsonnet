std.manifestYamlDoc({
  _config:: {
    // If true, Mimir services are run under Delve debugger, that can be attached to via remote-debugging session.
    // Note that Delve doesn't forward signals to the Mimir process, so Mimir components don't shutdown cleanly.
    debug: false,

    // How long should Mimir docker containers sleep before Mimir is started.
    sleep_seconds: 3,

    // Whether query-frontend and querier should use query-scheduler. If set to true, query-scheduler is started as well.
    use_query_scheduler: false,
  },

  // We explicitely list all important services here, so that it's easy to disable them by commenting out.
  services:
    self.distributor +
    self.ingesters +
    self.read_components +  // Querier, Frontend and query-scheduler, if enabled.
    self.store_gateways +
    self.compactor +
    self.rulers(2) +
    self.alertmanagers(3) +
    self.purger +
    self.consul +
    self.minio +
    self.prometheus +
    self.grafana_agent +
    self.memcached +
    self.jaeger +
    {},

  distributor:: {
    distributor: mimirService({
      target: 'distributor',
      httpPort: 8001,
    }),
  },

  ingesters:: {
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
  },

  read_components::
    {
      querier: mimirService({
        target: 'querier',
        httpPort: 8004,
        extraArguments:
          // Use of scheduler is activated by `-querier.scheduler-address` option and setting -querier.frontend-address option to nothing.
          if $._config.use_query_scheduler then '-querier.scheduler-address=query-scheduler:9011 -querier.frontend-address=' else '',
      }),

      'query-frontend': mimirService({
        target: 'query-frontend',
        httpPort: 8007,
        jaegerApp: 'query-frontend',
        extraArguments:
          '-store.max-query-length=8760h' +
          // Use of scheduler is activated by `-frontend.scheduler-address` option.
          (if $._config.use_query_scheduler then ' -frontend.scheduler-address=query-scheduler:9011' else ''),
      }),
    } + (
      if $._config.use_query_scheduler then {
        'query-scheduler': mimirService({
          target: 'query-scheduler',
          httpPort: 8011,
          extraArguments: '-store.max-query-length=8760h',
        }),
      } else {}
    ),

  compactor:: {
    compactor: mimirService({
      target: 'compactor',
      httpPort: 8006,
    }),
  },

  rulers(count):: if count <= 0 then {} else {
    ['ruler-%d' % id]: mimirService({
      target: 'ruler',
      httpPort: 8020 + id,
      jaegerApp: 'ruler-%d' % id,
    })
    for id in std.range(1, count)
  },

  alertmanagers(count):: if count <= 0 then {} else {
    ['alertmanager-%d' % id]: mimirService({
      target: 'alertmanager',
      httpPort: 8030 + id,
      extraArguments: '-alertmanager.web.external-url=http://localhost:%d/alertmanager' % (8030 + id),
      jaegerApp: 'alertmanager-%d' % id,
    })
    for id in std.range(1, count)
  },

  store_gateways:: {
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
  },

  purger:: {
    purger: mimirService({
      target: 'purger',
      httpPort: 8014,
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
      (if $._config.sleep_seconds > 0 then 'sleep %d && ' % [$._config.sleep_seconds] else '') +
      (if $._config.debug then 'exec ./dlv exec ./mimir --listen=:%(debugPort)d --headless=true --api-version=2 --accept-multiclient --continue -- ' % options
       else 'exec ./mimir ') +
      '-config.file=./config/mimir.yaml -target=%(target)s -server.http-listen-port=%(httpPort)d -server.grpc-listen-port=%(grpcPort)d %(extraArguments)s' % options,
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

  // Other services used by Mimir.
  consul:: {
    consul: {
      image: 'consul',
      command: ['agent', '-dev', '-client=0.0.0.0', '-log-level=info'],
      ports: ['8500:8500'],
    },
  },

  minio:: {
    minio: {
      image: 'minio/minio',
      command: ['server', '/data'],
      environment: ['MINIO_ACCESS_KEY=mimir', 'MINIO_SECRET_KEY=supersecret'],
      ports: ['9000:9000'],
      volumes: ['.data-minio:/data:delegated'],
    },
  },

  memcached:: {
    memcached: {
      image: 'memcached:1.6',
    },
  },

  prometheus:: {
    prometheus: {
      image: 'prom/prometheus:v2.27.1',
      command: [
        '--config.file=/etc/prometheus/prometheus.yaml',
        '--enable-feature=exemplar-storage',
      ],
      volumes: ['./config:/etc/prometheus'],
      ports: ['9090:9090'],
    },
  },

  grafana_agent:: {
    // Scrape the metrics also with the Grafana agent (useful to test metadata ingestion
    // until metadata remote write is not supported by Prometheus).
    'grafana-agent': {
      image: 'grafana/agent:v0.2.0',
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

  // docker-compose YAML output version.
  version: '3.4',

  // "true" option for std.manifestYamlDoc indents arrays in objects.
}, true)
