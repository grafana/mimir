// Migration step 6:
// - Decommission ingester-zone-[abc] StatefulSet and Service.
(import 'test-ingest-storage-migration-step-5b.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_migration_classic_ingesters_scale_down: false,
    ingest_storage_migration_classic_ingesters_zone_a_decommission: true,
    ingest_storage_migration_classic_ingesters_zone_b_decommission: true,
    ingest_storage_migration_classic_ingesters_zone_c_decommission: true,
  },
}
