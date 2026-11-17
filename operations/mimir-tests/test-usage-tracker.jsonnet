// Based on test-multi-zone.jsonnet.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    // Configure features required by ingest storage.
    ruler_enabled: true,
    ruler_storage_bucket_name: 'rules-bucket',
    ruler_remote_evaluation_enabled: true,

    ingest_storage_enabled: true,
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: true,

    usage_tracker_enabled: true,
    usage_tracker_snapshots_bucket_name: 'usage-tracker-snapshots-bucket',
    usage_tracker_deletion_protection_enabled: true,
  },

  usage_tracker_annotations+:: {
    'component-id': 'usage-tracker',
  },
}
