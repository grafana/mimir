{
  _config+:: {
    ingest_storage_enabled: false,
    ingest_storage_kafka_backend: 'kafka',

    // Mimir ingesters migrated from classic architecture to partitions run their instances hash ring
    // on a dedicated prefix, which has been introduced as part of the migration process.
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: false,
    ingest_storage_ingester_instance_ring_dedicated_prefix: 'partition-ingesters/',

    // How many zones ingesters should be deployed to.
    ingest_storage_ingester_zones: 3,

    commonConfig+:: if !$._config.ingest_storage_enabled then {} else
      $.ingest_storage_args +

      // The following should only be configured on distributors, ingesters and query-frontends, but it's currently required to pass
      // config validation when ingest storage is enabled.
      // TODO remove once we've improved the config validation.
      $.ingest_storage_kafka_consumer_args,

    ingesterRingClientConfig+:: if !$._config.ingest_storage_enabled then {} else $.ingest_storage_ingester_ring_client_args,
  },

  //
  // Basic configuration.
  //

  assert !$._config.ingest_storage_enabled || $._config.is_microservices_deployment_mode : 'ingest storage requires microservices deployment mode',
  assert !$._config.ingest_storage_enabled || $._config.ruler_remote_evaluation_enabled : 'ingest storage requires ruler remote evaluation',

  // The generic ingest storage config that should be applied to every component.
  ingest_storage_args::
    // The Kafka client config should only be applied to distributors, ingesters and rulers, but it's currently required
    // to pass config validation when ingest storage is enabled.
    $.ingest_storage_kafka_client_args
    {
      'ingest-storage.enabled': true,
    },

  //
  // Kafka client configuration.
  //

  ingest_storage_kafka_producer_address:: 'kafka.%(namespace)s.svc.%(cluster_domain)s:9092' % $._config,
  ingest_storage_kafka_consumer_address:: 'kafka.%(namespace)s.svc.%(cluster_domain)s:9092' % $._config,

  // The default Kafka client ID settings to use for producers and consumers.
  // These key-value settings get serialised into a comma-separated string.
  ingest_storage_kafka_producer_client_id_settings:: {},
  ingest_storage_kafka_consumer_client_id_settings:: {},

  // The per-component Kafka client ID settings overrides.
  ingest_storage_distributor_kafka_client_id_settings:: $.ingest_storage_kafka_producer_client_id_settings,
  ingest_storage_ruler_kafka_client_id_settings:: $.ingest_storage_kafka_producer_client_id_settings,

  ingest_storage_ingester_kafka_client_id_settings:: $.ingest_storage_kafka_consumer_client_id_settings,
  ingest_storage_query_frontend_kafka_client_id_settings:: $.ingest_storage_kafka_consumer_client_id_settings,

  // The configuration that should be applied to Mimir components either producing to or consuming from Kafka.
  ingest_storage_kafka_client_args:: {
    'ingest-storage.kafka.topic': 'ingest',
    'ingest-storage.kafka.auto-create-topic-default-partitions': 1000,
  },

  // The configuration that should be applied to all Mimir components producing to Kafka.
  ingest_storage_kafka_producer_args:: {
    'ingest-storage.kafka.address': $.ingest_storage_kafka_producer_address,
    'ingest-storage.kafka.client-id': $.mimirKafkaClientID($.ingest_storage_kafka_producer_client_id_settings),
  },

  // The configuration that should be applied to all Mimir components consuming from Kafka.
  ingest_storage_kafka_consumer_args:: {
    'ingest-storage.kafka.address': $.ingest_storage_kafka_consumer_address,
    'ingest-storage.kafka.client-id': $.mimirKafkaClientID($.ingest_storage_kafka_consumer_client_id_settings),
  },

  //
  // Mimir components specific configuration.
  //

  distributor_args+:: if !$._config.ingest_storage_enabled then {} else
    $.ingest_storage_kafka_producer_args +
    $.ingest_storage_distributor_args,

  ruler_args+:: if !$._config.ingest_storage_enabled then {} else
    $.ingest_storage_kafka_producer_args +
    $.ingest_storage_ruler_args,

  ingester_args+:: if !$._config.ingest_storage_enabled then {} else
    $.ingest_storage_ingester_args,

  query_frontend_args+:: if !$._config.ingest_storage_enabled then {} else
    $.ingest_storage_query_frontend_args,

  ingest_storage_distributor_args+:: {
    // Apply per-component overrides to Kafka client ID.
    'ingest-storage.kafka.client-id': $.mimirKafkaClientID($.ingest_storage_distributor_kafka_client_id_settings),

    // Increase the default remote write timeout (applied to writing to Kafka too) because writing
    // to Kafka-compatible backend may be slower than writing to ingesters.
    'distributor.remote-timeout': '5s',
  },

  ingest_storage_ruler_args+:: {
    // Apply per-component overrides to Kafka client ID.
    'ingest-storage.kafka.client-id': $.mimirKafkaClientID($.ingest_storage_ruler_kafka_client_id_settings),

    // No need to increase -distributor.remote-timeout because the ruler's default is higher.
  },

  ingest_storage_ingester_args+:: {
    // Disallow Push gRPC API; everything must come from ingest storage.
    'ingester.push-grpc-method-enabled': false,

    // Reduce the LPO polling interval to improve latency of strong consistency reads.
    'ingest-storage.kafka.last-produced-offset-poll-interval': '500ms',

    // Reduce the OffsetCommit pressure, at the cost of replaying few seconds of already-ingested data in case an ingester abruptly terminates
    // (in case of a graceful shutdown, the ingester will commit the offset at shutdown too).
    'ingest-storage.kafka.consumer-group-offset-commit-interval': '5s',

    // Apply per-component overrides to Kafka client ID.
    'ingest-storage.kafka.client-id': $.mimirKafkaClientID($.ingest_storage_ingester_kafka_client_id_settings),
  },

  ingest_storage_partition_ring_client_args+:: {
    // Set no key prefix for the partition ring, like we do for all other hash rings.
    'ingester.partition-ring.prefix': '',
  },

  ingest_storage_ingester_ring_client_args+:: $.ingest_storage_partition_ring_client_args + (
    if !$._config.ingest_storage_ingester_instance_ring_dedicated_prefix_enabled then {} else {
      // Run partition ingesters on a dedicated hash ring, so that they don't clash with classic ingesters.
      'ingester.ring.prefix': $._config.ingest_storage_ingester_instance_ring_dedicated_prefix,
    }
  ),

  ingest_storage_query_frontend_args+::
    // The query-frontend uses the partitions ring to discover in-use partitions, used by
    // the logic to fetch the last produced offsets.
    $.ingest_storage_partition_ring_client_args
    {
      // Apply per-component overrides to Kafka client ID.
      'ingest-storage.kafka.client-id': $.mimirKafkaClientID($.ingest_storage_query_frontend_kafka_client_id_settings),

      // Reduce the LPO polling interval to improve latency of strong consistency reads.
      'ingest-storage.kafka.last-produced-offset-poll-interval': '500ms',
    },

  //
  // Enforce the configured ingester zones.
  //

  assert !$._config.ingest_storage_enabled || ($._config.ingest_storage_ingester_zones >= 2 && $._config.ingest_storage_ingester_zones <= 3) : 'ingest storage requires either 2 or 3 ingester zones',

  ingester_zone_c_statefulset:
    if $._config.ingest_storage_enabled && $._config.ingest_storage_ingester_zones < 3
    then null
    else super.ingester_zone_c_statefulset,

  ingester_zone_c_service:
    if $._config.ingest_storage_enabled && $._config.ingest_storage_ingester_zones < 3
    then null
    else super.ingester_zone_c_service,

  //
  // Utilities.
  //

  mimirKafkaClientID(settings)::
    // Remove all null values from the settings. There could be null values because we use null as override
    // value to remove a setting from the map.
    local cleanSettings = {
      [key]: settings[key]
      for key in std.objectFields(settings)
      if settings[key] != null
    };

    if std.length(cleanSettings) > 0 then
      // Build the Kafka client ID from settings. Sort the key-value pairs to get a stable output.
      local clientID = std.join(',', std.sort(
        [
          key + '=' + cleanSettings[key]
          for key in std.objectFields(cleanSettings)
        ]
      ));

      // The client ID can be up to 255 characters in length (limit hardcoded in franz-go), and can include
      // the following characters:
      // a-z, A-Z, 0-9, . (dot), _ (underscore), - (dash), = (equal), "," (comma).
      local isValid(input) =
        std.length(
          // Build an array of invalid characters.
          [
            char
            for char in std.stringChars(clientID)
            if !std.member('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-=,', char)
          ]
        ) == 0;

      assert std.length(clientID) < 256 : 'the Kafka client ID must be less than 256 characters (actual: %s)' % clientID;
      assert isValid(clientID) : 'the Kafka client ID contains invalid characters (actual: %s)' % clientID;

      clientID
    else
      // Explicitly use null so that the CLI flag will not be set at all (instead of getting set to an empty string).
      null,
}
