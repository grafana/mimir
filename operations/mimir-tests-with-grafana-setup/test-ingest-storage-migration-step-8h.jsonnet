// Migration step 8:
// Deploy ingester-zone-b with correct annotations.
(import 'test-ingest-storage-migration-step-8g.jsonnet') {
  _config+:: {
    ingest_storage_migration_classic_ingesters_zone_b_decommission: false,
    ingest_storage_ingester_migration_autoscaling_ingester_annotations_ingester_zone_b_enabled: true,
  },
}
