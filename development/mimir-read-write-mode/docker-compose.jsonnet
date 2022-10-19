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
      publishedHttpPort: 8001,
      extraVolumes: ['.data-mimir-write-1:/data:delegated'],
    }),
    'mimir-write-2': mimirService({
      name: 'mimir-write-2',
      target: 'write',
      publishedHttpPort: 8002,
      extraVolumes: ['.data-mimir-write-2:/data:delegated'],
    }),
    'mimir-write-3': mimirService({
      name: 'mimir-write-3',
      target: 'write',
      publishedHttpPort: 8003,
      extraVolumes: ['.data-mimir-write-3:/data:delegated'],
    }),
  },

  read:: {
    'mimir-read-1': mimirService({
      name: 'mimir-read-1',
      target: 'read',
      publishedHttpPort: 8004,
    }),
    'mimir-read-2': mimirService({
      name: 'mimir-read-2',
      target: 'read',
      publishedHttpPort: 8005,
    }),
  },

  backend:: {
    'mimir-backend-1': mimirService({
      name: 'mimir-backend-1',
      target: 'backend',
      publishedHttpPort: 8006,
    }),
    'mimir-backend-2': mimirService({
      name: 'mimir-backend-2',
      target: 'backend',
      publishedHttpPort: 8007,
    }),
  },

  minio:: {
    minio: {
      image: 'minio/minio',
      command: ['server', '/data'],
      environment: ['MINIO_ROOT_USER=mimir', 'MINIO_ROOT_PASSWORD=supersecret'],
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
  local mimirService(serviceOptions) = {
    local defaultOptions = {
      local s = self,
      name: error 'missing name',
      target: error 'missing target',
      publishedHttpPort: error 'missing publishedHttpPort',
      dependsOn: ['minio'],
      env: {},
      extraVolumes: [],
      memberlistBindPort: self.publishedHttpPort + 2000,
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
      '-activity-tracker.filepath=/activity/%(name)s' % options,
    ],
    environment: [
      '%s=%s' % [key, options.env[key]]
      for key in std.objectFields(options.env)
      if options.env[key] != null
    ],
    hostname: options.name,
    // Only publish HTTP port, but not gRPC one.
    ports: ['%d:8080' % options.publishedHttpPort],
    depends_on: options.dependsOn,
    volumes: ['./config:/mimir/config', './activity:/activity'] + options.extraVolumes,
  },

  // docker-compose YAML output version.
  version: '3.4',

  // "true" option for std.manifestYamlDoc indents arrays in objects.
}, true)
