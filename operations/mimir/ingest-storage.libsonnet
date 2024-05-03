{
  _config+:: {
    ingest_storage_enabled: false,
    ingest_storage_kafka_backend: 'kafka',

    // Mimir ingesters migrated from classic architecture to partitions run their instances hash ring
    // on a dedicated prefix, which has been introduced as part of the migration process.
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: false,
    ingest_storage_ingester_instance_ring_dedicated_prefix: 'partition-ingesters/',

    commonConfig+:: if !$._config.ingest_storage_enabled then {} else
      $.ingest_storage_args +

      // The following should only be configured on distributors and ingesters, but it's currently required to pass
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

  ingest_storage_kafka_producer_client_id:: null,
  ingest_storage_kafka_consumer_client_id:: null,

  // The configuration that should be applied to Mimir components either producing to or consuming from Kafka.
  ingest_storage_kafka_client_args:: {
    'ingest-storage.kafka.topic': 'ingest',
    'ingest-storage.kafka.auto-create-topic-default-partitions': 1000,
  },

  // The configuration that should be applied to Mimir components producing to Kafka.
  ingest_storage_kafka_producer_args:: {
    'ingest-storage.kafka.address': $.ingest_storage_kafka_producer_address,
    'ingest-storage.kafka.client-id': $.ingest_storage_kafka_producer_client_id,
  },

  // The configuration that should be applied to Mimir components consuming from Kafka.
  ingest_storage_kafka_consumer_args:: {
    'ingest-storage.kafka.address': $.ingest_storage_kafka_consumer_address,
    'ingest-storage.kafka.client-id': $.ingest_storage_kafka_consumer_client_id,
  },

  //
  // Mimir components specific configuration.
  //

  distributor_args+:: if !$._config.ingest_storage_enabled then {} else
    $.ingest_storage_kafka_producer_args,

  ruler_args+:: if !$._config.ingest_storage_enabled then {} else
    $.ingest_storage_kafka_producer_args,

  ingest_storage_ingester_args+:: {
    'ingester.push-grpc-method-enabled': false,  // Disallow Push gRPC API; everything must come from ingest storage.
    'ingest-storage.kafka.last-produced-offset-poll-interval': '500ms',  // Reduce the LPO polling interval to improve latency of strong consistency reads.
  },

  ingest_storage_ingester_ring_client_args+:: {
    // Set no key prefix for the partition ring, like we do for all other hash rings.
    'ingester.partition-ring.prefix': '',
  } + (
    if !$._config.ingest_storage_ingester_instance_ring_dedicated_prefix_enabled then {} else {
      // Run partition ingesters on a dedicated hash ring, so that they don't clash with classic ingesters.
      'ingester.ring.prefix': $._config.ingest_storage_ingester_instance_ring_dedicated_prefix,
    }
  ),

  ingester_args+:: if !$._config.ingest_storage_enabled then {} else $.ingest_storage_ingester_args,
}
