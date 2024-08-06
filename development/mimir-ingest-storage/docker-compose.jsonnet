std.manifestYamlDoc({
  // We explicitely list all important services here, so that it's easy to disable them by commenting out.
  services:
    self.write +
    self.read +
    self.backend +
    self.nginx +
    self.minio +
    self.grafana +
    self.grafana_agent +
    self.memcached +
    self.kafka +
    {},

  write:: {
    // Zone-a.
    'mimir-write-zone-a-1': mimirService({
      name: 'mimir-write-zone-a-1',
      target: 'write',
      publishedHttpPort: 8001,
      extraArguments: ['-ingester.ring.instance-availability-zone=zone-a'],
      extraVolumes: ['.data-mimir-write-zone-a-1:/data:delegated'],
    }),
    'mimir-write-zone-a-2': mimirService({
      name: 'mimir-write-zone-a-2',
      target: 'write',
      publishedHttpPort: 8002,
      extraArguments: ['-ingester.ring.instance-availability-zone=zone-a'],
      extraVolumes: ['.data-mimir-write-zone-a-2:/data:delegated'],
    }),
    'mimir-write-zone-a-3': mimirService({
      name: 'mimir-write-zone-a-3',
      target: 'write',
      publishedHttpPort: 8003,
      extraArguments: ['-ingester.ring.instance-availability-zone=zone-a'],
      extraVolumes: ['.data-mimir-write-zone-a-3:/data:delegated'],
    }),

    // Zone-b.
    'mimir-write-zone-b-1': mimirService({
      name: 'mimir-write-zone-b-1',
      target: 'write',
      publishedHttpPort: 8011,
      extraArguments: ['-ingester.ring.instance-availability-zone=zone-b'],
      extraVolumes: ['.data-mimir-write-zone-b-1:/data:delegated'],
    }),
    'mimir-write-zone-b-2': mimirService({
      name: 'mimir-write-zone-b-2',
      target: 'write',
      publishedHttpPort: 8012,
      extraArguments: ['-ingester.ring.instance-availability-zone=zone-b'],
      extraVolumes: ['.data-mimir-write-zone-b-2:/data:delegated'],
    }),
    'mimir-write-zone-b-3': mimirService({
      name: 'mimir-write-zone-b-3',
      target: 'write',
      publishedHttpPort: 8013,
      extraArguments: ['-ingester.ring.instance-availability-zone=zone-b'],
      extraVolumes: ['.data-mimir-write-zone-b-3:/data:delegated'],
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

  nginx:: {
    nginx: {
      hostname: 'nginx',
      image: 'nginxinc/nginx-unprivileged:1.22-alpine',
      environment: [
        'NGINX_ENVSUBST_OUTPUT_DIR=/etc/nginx',
        'DISTRIBUTOR_HOST=mimir-write-1:8080',
        'ALERT_MANAGER_HOST=mimir-backend-1:8080',
        'RULER_HOST=mimir-backend-1:8080',
        'QUERY_FRONTEND_HOST=mimir-read-1:8080',
        'COMPACTOR_HOST=mimir-backend-1:8080',
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

  kafka:: {
    kafka: {
      image: 'confluentinc/cp-kafka:latest',
      environment: [
        'CLUSTER_ID=zH1GDqcNTzGMDCXm5VZQdg',  // Cluster ID is required in KRaft mode; the value is random UUID.
        'KAFKA_BROKER_ID=1',
        'KAFKA_NUM_PARTITIONS=100',  // Default number of partitions for auto-created topics.
        'KAFKA_PROCESS_ROLES=broker,controller',
        'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,PLAINTEXT_HOST://:29092',
        'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
        'KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT',
        'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
        'KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
        'KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=10000',

        // Decomment the following config to keep a short retention of records in Kafka.
        // This is useful to test the behaviour when Kafka records are deleted.
        // 'KAFKA_LOG_RETENTION_MINUTES=1',
        // 'KAFKA_LOG_SEGMENT_BYTES=1000000',
      ],
      ports: [
        '29092:29092',
      ],
      healthcheck: {
        test: 'nc -z localhost 9092 || exit -1',
        start_period: '1s',
        interval: '1s',
        timeout: '1s',
        retries: '30',
      },
    },
  },

  memcached:: {
    memcached: {
      image: 'memcached:1.6.19-alpine',
    },
  },

  grafana:: {
    grafana: {
      image: 'grafana/grafana:10.4.3',
      environment: [
        'GF_AUTH_ANONYMOUS_ENABLED=true',
        'GF_AUTH_ANONYMOUS_ORG_ROLE=Admin',
      ],
      volumes: [
        './config/datasource-mimir.yaml:/etc/grafana/provisioning/datasources/mimir.yaml',
        './config/grafana-provisioning.yaml:/etc/grafana/provisioning/dashboards/local.yml',
        '../../operations/mimir-mixin-compiled/dashboards:/var/lib/grafana/dashboards',
      ],
      ports: ['3000:3000'],
    },
  },

  grafana_agent:: {
    // Scrape the metrics also with the Grafana agent (useful to test metadata ingestion
    // until metadata remote write is not supported by Prometheus).
    'grafana-agent': {
      image: 'grafana/agent:v0.40.0',
      command: ['run', '--storage.path=/tmp', '--server.http.listen-addr=127.0.0.1:9091', '/etc/agent-config/grafana-agent.flow'],
      volumes: ['./config:/etc/agent-config'],
      ports: ['9091:9091'],
      environment: {
        AGENT_MODE: 'flow',
      },
    },
  },

  // This function builds docker-compose declaration for Mimir service.
  local mimirService(serviceOptions) = {
    local defaultOptions = {
      local s = self,
      name: error 'missing name',
      target: error 'missing target',
      publishedHttpPort: error 'missing publishedHttpPort',
      dependsOn: {
        minio: { condition: 'service_started' },
        kafka: { condition: 'service_healthy' },
      },
      env: {},
      extraArguments: [],
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
    ] + options.extraArguments,
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

  // "true" option for std.manifestYamlDoc indents arrays in objects.
}, true)
