// Migration step 8:
// - Begin migrating ingester-zone-[abc]-partition StatefulSets to ingester-zone-[abc]. This should be done zone-by-zone,
//   but to keep the jsonnet test smaller we're going to do all zones at once. In this step we're going to scale down
//   ingester-zone-[abc]-partition replicas to 0.
(import 'test-ingest-storage-migration-step-7.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_migration_partition_ingester_zone_a_scale_down: true,
    ingest_storage_migration_partition_ingester_zone_b_scale_down: true,
    ingest_storage_migration_partition_ingester_zone_c_scale_down: true,
  },
}
