std.manifestYamlDoc({
  // We explicitely list all important services here, so that it's easy to disable them by commenting out.
  services:
    self.write +
    self.read +
    self.backend +
    self.minio +
    self.grafana_agent +
    self.memcached +
    {},

  write:: {
    'mimir-write-1': mimirService({
      name: 'mimir-write-1',
      target: 'write',
      httpPort: 8001,
      extraVolumes: ['.data-mimir-write-1:/data:delegated'],
    }),
    'mimir-write-2': mimirService({
      name: 'mimir-write-2',
      target: 'write',
      httpPort: 8002,
      extraVolumes: ['.data-mimir-write-2:/data:delegated'],
    }),
    'mimir-write-3': mimirService({
      name: 'mimir-write-3',
      target: 'write',
      httpPort: 8003,
      extraVolumes: ['.data-mimir-write-3:/data:delegated'],
    }),
  },

  read:: {
    'mimir-read-1': mimirService({
      name: 'mimir-read-1',
      target: 'read',
      httpPort: 8004,
    }),
    'mimir-read-2': mimirService({
      name: 'mimir-read-2',
      target: 'read',
      httpPort: 8005,
    }),
  },

  backend:: {
    'mimir-backend-1': mimirService({
      name: 'mimir-backend-1',
      target: 'backend',
      httpPort: 8006,
    }),
    'mimir-backend-2': mimirService({
      name: 'mimir-backend-2',
      target: 'backend',
      httpPort: 8007,
    }),
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

  // This function builds docker-compose declaration for Mimir service.
  // Default grpcPort is (httpPort + 1000).
  local mimirService(serviceOptions) = {
    local defaultOptions = {
      local s = self,
      name: error 'missing name',
      target: error 'missing target',
      httpPort: error 'missing httpPort',
      grpcPort: self.httpPort + 1000,
      dependsOn: ['minio'],
      env: {},
      extraVolumes: [],
      memberlistBindPort: self.httpPort + 2000,
    },

    local options = defaultOptions + serviceOptions,

    build: {
      context: '.',
      dockerfile: 'dev.dockerfile',
    },
    image: 'mimir',
    command: [
      './mimir',
      '-config.file=./config/mimir.yaml' % options,
      '-target=%(target)s' % options,
      '-server.http-listen-port=%(httpPort)d' % options,
      '-server.grpc-listen-port=%(grpcPort)d' % options,
      '-activity-tracker.filepath=/activity/%(target)s-%(httpPort)d' % options,
    ],
    environment: [
      '%s=%s' % [key, options.env[key]]
      for key in std.objectFields(options.env)
      if options.env[key] != null
    ],
    hostname: options.name,
    // Only publish HTTP port, but not gRPC one.
    ports: ['%d:%d' % [options.httpPort, options.httpPort]],
    depends_on: options.dependsOn,
    volumes: ['./config:/mimir/config', './activity:/activity'] + options.extraVolumes,
  },

  // docker-compose YAML output version.
  version: '3.4',

  // "true" option for std.manifestYamlDoc indents arrays in objects.
}, true)
