local mimir = import 'mimir/mimir.libsonnet';

mimir {
  local availabilityZones = ['us-east-2a', 'us-east-2b'],

  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    multi_zone_distributor_enabled: true,
    multi_zone_distributor_availability_zones: availabilityZones,
  },

  distributor_zone_a_args+:: {
    'ingest-storage.kafka.address': 'warpstream-agent-write-zone-a.%(namespace)s.svc.cluster.local.:9092' % $._config,
    'ingest-storage.kafka.client-id': $.mimirKafkaClientID($.ingest_storage_distributor_kafka_client_id_settings {
      warpstream_az: availabilityZones[0],
    }),
  },

  distributor_zone_b_args+:: {
    'ingest-storage.kafka.address': 'warpstream-agent-write-zone-b.%(namespace)s.svc.cluster.local.:9092' % $._config,
    'ingest-storage.kafka.client-id': $.mimirKafkaClientID($.ingest_storage_distributor_kafka_client_id_settings {
      warpstream_az: availabilityZones[1],
    }),
  },
}
