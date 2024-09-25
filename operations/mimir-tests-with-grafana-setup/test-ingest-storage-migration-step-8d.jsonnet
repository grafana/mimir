// Migration step 8:
// Deploy ingester-zone-a with correct annotations.
(import 'test-ingest-storage-migration-step-8c.jsonnet') {
  _config+:: {
    ingest_storage_migration_classic_ingesters_zone_a_decommission: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_a_enabled: true,
  },
}
