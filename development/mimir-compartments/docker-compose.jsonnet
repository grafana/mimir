std.manifestYamlDoc({
  // We explicitly list all important services here, so that it's easy to disable them by commenting out.
  services:
    self.distributors +
    self.ingesters +
    self.query_frontend +
    self.query_schedulers +
    self.querier +
    self.store_gateways(1) +
    self.compactor +
    self.block_builder +
    self.block_builder_scheduler +
    self.rulers(1) +
    self.alertmanagers(1) +
    self.usage_trackers +
    self.nginx +
    self.minio +
    self.grafana +
    self.grafana_agent +
    self.memcached +
    self.kafka_clusters +
    self.redpanda_console +
    self.tempo +
    {},

  // Two distributors, each representing a (0-based) write compartment. They shard every write across all
  // read compartments by metric name, and each produces to its own write compartment's Kafka cluster;
  // nginx load-balances across them so writes spread randomly across write compartments.
  local numWriteCompartments = 2,
  distributors:: {
    ['distributor-wc-%d' % id]: mimirService({
      name: 'distributor-wc-%d' % id,
      target: 'distributor',
      publishedHttpPort: 8000 + id,
      extraArguments: [
        // Each write compartment's distributor writes to its own Kafka cluster, sharding series across
        // read compartment topics (parameterised by read compartment ID). The <write-compartment-id>
        // placeholder in the address is resolved from -distributor.write-compartment-id. Values with
        // "<...>" placeholders are single-quoted so they aren't interpreted as shell redirections by the
        // "sh -c" command.
        '-distributor.write-compartment-id=%d' % id,
        "-ingest-storage.kafka.address='kafka-wc-<write-compartment-id>:9092'",
        "-ingest-storage.kafka.topic='mimir-ingest-rc-<read-compartment-id>'",
      ],
    }) + {
      // Share a "distributor" network alias so nginx (DISTRIBUTOR_HOST=distributor) balances across both.
      networks: { default: { aliases: ['distributor'] } },
    }
    for id in std.range(0, numWriteCompartments - 1)
  },

  // Each read compartment runs its own ingesters (one per zone). An ingester registers its partition
  // into the partition ring of its read compartment, and consumes that read compartment's topic from
  // every write compartment's Kafka cluster (one cluster per write compartment). The query path is not
  // compartment-aware yet, so only read compartment 0's data is queryable.
  local numCompartments = 2,
  local partitionsPerCompartment = 2,
  local zones = ['zone-a', 'zone-b'],
  // Ingesters belong to a read compartment. The name is ingester-<zone>-rc-<compartment>-<partition>;
  // the trailing number is the partition ID (0-based, parsed by the ingester to own that partition), and
  // -ingester.read-compartment-id selects the read compartment ring it registers into.
  ingesters:: {
    ['ingester-%s-rc-%d-%d' % [zones[zoneIdx], compartment, partition]]: mimirService({
      name: 'ingester-%s-rc-%d-%d' % [zones[zoneIdx], compartment, partition],
      target: 'ingester',
      // Based at 8100 to leave room below for the other services.
      publishedHttpPort: 8100 + ((compartment * partitionsPerCompartment + partition) * std.length(zones)) + zoneIdx,
      jaegerApp: 'ingester-%s-rc-%d-%d' % [zones[zoneIdx], compartment, partition],
      extraArguments: [
        '-ingester.ring.instance-availability-zone=%s' % zones[zoneIdx],
        '-ingester.read-compartment-id=%d' % compartment,
        // Consume the read compartment's topic from every write compartment's Kafka cluster. The
        // <write-compartment-id> placeholder is expanded per write compartment by the reader, and the
        // <read-compartment-id> placeholder in the topic is expanded to this ingester's read compartment.
        // The values are single-quoted so the "<...>" placeholders aren't interpreted as shell
        // redirections by the "sh -c" command.
        "-ingest-storage.kafka.address='kafka-wc-<write-compartment-id>:9092'",
        "-ingest-storage.kafka.topic='mimir-ingest-rc-<read-compartment-id>'",
      ],
      extraVolumes: ['.data-ingester-%s-rc-%d-%d:/data:delegated' % [zones[zoneIdx], compartment, partition]],
    })
    for compartment in std.range(0, numCompartments - 1)
    for partition in std.range(0, partitionsPerCompartment - 1)
    for zoneIdx in std.range(0, std.length(zones) - 1)
  },

  querier:: {
    querier: mimirService({
      name: 'querier',
      target: 'querier',
      publishedHttpPort: 8005,
    }),
  },

  query_frontend:: {
    'query-frontend': mimirService({
      name: 'query-frontend',
      target: 'query-frontend',
      publishedHttpPort: 8007,
      jaegerApp: 'query-frontend',
    }),
  },

  query_schedulers:: {
    'query-scheduler': mimirService({
      name: 'query-scheduler',
      target: 'query-scheduler',
      publishedHttpPort: 8008,
    }),
  },

  compactor:: {
    compactor: mimirService({
      name: 'compactor',
      target: 'compactor',
      publishedHttpPort: 8006,
    }),
  },

  store_gateways(count):: {
    ['store-gateway-zone-a-%d' % id]: mimirService({
      name: 'store-gateway-zone-a-' + id,
      target: 'store-gateway',
      publishedHttpPort: 8020 + id,
      jaegerApp: 'store-gateway-zone-a-%d' % id,
      extraArguments: ['-store-gateway.sharding-ring.instance-availability-zone=zone-a'],
    })
    for id in std.range(1, count)
  } + {
    ['store-gateway-zone-b-%d' % id]: mimirService({
      name: 'store-gateway-zone-b-' + id,
      target: 'store-gateway',
      publishedHttpPort: 8050 + id,
      jaegerApp: 'store-gateway-zone-b-%d' % id,
      extraArguments: ['-store-gateway.sharding-ring.instance-availability-zone=zone-b'],
    })
    for id in std.range(1, count)
  },

  rulers(count):: if count <= 0 then {} else {
    ['ruler-%d' % id]: mimirService({
      name: 'ruler-' + id,
      target: 'ruler',
      publishedHttpPort: 8030 + id,
      jaegerApp: 'ruler-%d' % id,
    })
    for id in std.range(1, count)
  },

  alertmanagers(count):: if count <= 0 then {} else {
    ['alertmanager-%d' % id]: mimirService({
      name: 'alertmanager-' + id,
      target: 'alertmanager',
      publishedHttpPort: 8040 + id,
      extraArguments: ['-alertmanager.web.external-url=http://localhost:%d/alertmanager' % (8040 + id)],
      jaegerApp: 'alertmanager-%d' % id,
    })
    for id in std.range(1, count)
  },

  block_builder:: {
    'block-builder-0': mimirService({
      name: 'block-builder-0',
      target: 'block-builder',
      publishedHttpPort: 8009,
    }),
  },

  block_builder_scheduler:: {
    'block-builder-scheduler-0': mimirService({
      name: 'block-builder-scheduler-0',
      target: 'block-builder-scheduler',
      publishedHttpPort: 8010,
    }),
  },

  usage_trackers:: {
    'usage-tracker-zone-a-0': mimirService({
      name: 'usage-tracker-zone-a-0',
      target: 'usage-tracker',
      publishedHttpPort: 8011,
      extraArguments: [
        '-usage-tracker.instance-ring.instance-availability-zone=zone-a',
        '-usage-tracker.partitions=16',
        '-usage-tracker.partition-reconcile-interval=10s',
        '-usage-tracker.lost-partitions-shutdown-grace-period=30s',
      ],
    }),
    'usage-tracker-zone-a-1': mimirService({
      name: 'usage-tracker-zone-a-1',
      target: 'usage-tracker',
      publishedHttpPort: 8012,
      extraArguments: [
        '-usage-tracker.instance-ring.instance-availability-zone=zone-a',
        '-usage-tracker.partitions=16',
        '-usage-tracker.partition-reconcile-interval=10s',
        '-usage-tracker.lost-partitions-shutdown-grace-period=30s',
      ],
    }),
    'usage-tracker-zone-b-0': mimirService({
      name: 'usage-tracker-zone-b-0',
      target: 'usage-tracker',
      publishedHttpPort: 8013,
      extraArguments: [
        '-usage-tracker.instance-ring.instance-availability-zone=zone-b',
        '-usage-tracker.partitions=16',
        '-usage-tracker.partition-reconcile-interval=10s',
        '-usage-tracker.lost-partitions-shutdown-grace-period=30s',
      ],
    }),
  },

  nginx:: {
    nginx: {
      hostname: 'nginx',
      image: 'nginxinc/nginx-unprivileged:1.22-alpine',
      depends_on: [
        'distributor-wc-%d' % id
        for id in std.range(0, numWriteCompartments - 1)
      ] + [
        'alertmanager-1',
        'ruler-1',
        'query-frontend',
        'compactor',
        'grafana',
      ],
      environment: [
        'NGINX_ENVSUBST_OUTPUT_DIR=/etc/nginx',
        // "distributor" is a shared network alias for both distributors; nginx resolves it to both at
        // startup and round-robins across them.
        'DISTRIBUTOR_HOST=distributor:8080',
        'ALERT_MANAGER_HOST=alertmanager-1:8080',
        'RULER_HOST=ruler-1:8080',
        'QUERY_FRONTEND_HOST=query-frontend:8080',
        'COMPACTOR_HOST=compactor:8080',
      ],
      ports: ['8080:8080'],
      volumes: ['../common/config:/etc/nginx/templates'],
    },
  },

  minio:: {
    minio: {
      image: 'minio/minio:RELEASE.2025-05-24T17-08-30Z',
      // MinIO treats top-level directories under /data as buckets. Create the buckets the dev cluster
      // needs before starting the server, since this env doesn't ship a pre-seeded data directory.
      entrypoint: ['sh', '-c', 'mkdir -p /data/mimir-blocks /data/mimir-ruler /data/mimir-alertmanager /data/usage-tracker-snapshots && exec minio server --console-address :9001 /data'],
      environment: ['MINIO_ROOT_USER=mimir', 'MINIO_ROOT_PASSWORD=supersecret'],
      ports: [
        '9000:9000',
        '9001:9001',
      ],
      volumes: ['.data-minio:/data:delegated'],
    },
  },

  // One single-broker KRaft Kafka cluster per write compartment. Each cluster is fully independent (its
  // own controller quorum and offset space), so an ingester consuming the same partition from every
  // cluster reads each cluster's records independently. Single-broker clusters use a replication factor
  // of 1. The host port is 29092 + write compartment ID.
  kafka_clusters:: {
    ['kafka-wc-%d' % wc]: {
      image: 'confluentinc/cp-kafka:latest',
      hostname: 'kafka-wc-%d' % wc,
      environment: [
        'CLUSTER_ID=zH1GDqcNTzGMDCXm5VZQdg',  // Cluster ID is required in KRaft mode; the value is a random UUID.
        'KAFKA_NUM_PARTITIONS=100',  // Default number of partitions for auto-created topics.
        'KAFKA_PROCESS_ROLES=broker,controller',
        'KAFKA_BROKER_ID=1',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
        'KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT',
        'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
        'KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-wc-%d:9093' % wc,
        // Single-broker clusters: every replication factor must be 1, otherwise creating the internal
        // topics (e.g. __consumer_offsets) fails because there aren't enough brokers.
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
        'KAFKA_DEFAULT_REPLICATION_FACTOR=1',
        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
        'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
        'KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=10000',
        'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,PLAINTEXT_HOST://:%d' % (29092 + wc),
        'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-wc-%d:9092,PLAINTEXT_HOST://localhost:%d' % [wc, 29092 + wc],

        // Decomment the following config to keep a short retention of records in Kafka.
        // This is useful to test the behaviour when Kafka records are deleted.
        // 'KAFKA_LOG_RETENTION_MINUTES=1',
        // 'KAFKA_LOG_SEGMENT_BYTES=1000000',
      ],
      ports: [
        '%d:%d' % [29092 + wc, 29092 + wc],
      ],
      healthcheck: {
        test: 'kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1',
        start_period: '1s',
        interval: '1s',
        timeout: '1s',
        retries: '30',
      },
    }
    for wc in std.range(0, numWriteCompartments - 1)
  },
  // One Redpanda console per write compartment, each connected to that write compartment's Kafka cluster
  // (every cluster is independent, so a single console would only show one cluster). The host port is
  // 8090 + write compartment ID.
  redpanda_console:: {
    ['redpanda-console-wc-%d' % wc]: {
      image: 'docker.redpanda.com/redpandadata/console:latest',
      environment: [
        'KAFKA_BROKERS=kafka-wc-%d:9092' % wc,
      ],
      ports: [
        '%d:8080' % (8090 + wc),
      ],
    }
    for wc in std.range(0, numWriteCompartments - 1)
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

  tempo:: {
    tempo: {
      image: 'grafana/tempo:2.10.3',
      command: ['-config.file=/etc/config/tempo.yaml'],
      volumes: ['./config:/etc/config'],
      ports: [
        '3200',  // tempo: this is where metrics are scraped and where datasource queries are sent.
        '9095',  // tempo grpc
        '4317',  // otlp grpc
        '4318',  // otlp http, this is the one we use for local tracing.
      ],
    },
  },

  local otelTracingEnv(appName) = {
    // We could send traces to Alloy and let it send them to Tempo,
    // but this is local so let's skip one hop and send them directly to Tempo.
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: 'http://tempo:4318/v1/traces',
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
      } + {
        ['kafka-wc-%d' % wc]: { condition: 'service_healthy' }
        for wc in std.range(0, numWriteCompartments - 1)
      },
      env: otelTracingEnv(self.target),
      extraArguments: [],
      debug: false,
      debugPort: self.publishedHttpPort + 3000,
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
      'sh',
      '-c',
      std.join(' ', [
        (if options.debug then 'exec ./dlv exec ./mimir --listen=:%(debugPort)d --headless=true --api-version=2 --accept-multiclient --continue -- ' % options else 'exec ./mimir'),
        '-config.file=./config/mimir.yaml' % options,
        '-target=%(target)s' % options,
        '-activity-tracker.filepath=/activity/%(name)s' % options,
      ] + options.extraArguments),
    ],
    environment: [
      '%s=%s' % [key, options.env[key]]
      for key in std.objectFields(options.env)
      if options.env[key] != null
    ],
    hostname: options.name,
    // Only publish HTTP port, but not gRPC one.
    ports: ['%d:8080' % options.publishedHttpPort, '%(debugPort)d:%(debugPort)d' % options],
    depends_on: options.dependsOn,
    volumes: ['./config:/mimir/config', './activity:/activity'] + options.extraVolumes,
  },

  // "true" option for std.manifestYamlDoc indents arrays in objects.
}, true)
