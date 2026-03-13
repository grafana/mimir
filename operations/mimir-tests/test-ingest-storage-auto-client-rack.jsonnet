// Test that ingest_storage_set_client_rack sets -ingest-storage.kafka.client-rack on each ingester zone.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    // Configure features required by ingest storage.
    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',
    ruler_remote_evaluation_enabled: true,

    ingest_storage_enabled: true,
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: true,

    // ingest_storage_set_client_rack defaults to true, but set it explicitly for clarity.
    ingest_storage_set_client_rack: true,
  },
}
