// Migration step 9:
// - Decommission ingester-zone-[abc]-partition StatefulSet.
// - Deploy ingester-zone-[ab] StatefulSet, reducing ingester zones from 3 to 2.
(import 'test-ingest-storage-migration-step-8.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: true,

    ingest_storage_migration_partition_ingester_zone_a_scale_down: false,
    ingest_storage_migration_partition_ingester_zone_b_scale_down: false,
    ingest_storage_migration_partition_ingester_zone_c_scale_down: false,

    ingest_storage_migration_partition_ingester_zone_a_enabled: false,
    ingest_storage_migration_partition_ingester_zone_b_enabled: false,
    ingest_storage_migration_partition_ingester_zone_c_enabled: false,

    ingest_storage_migration_classic_ingesters_zone_a_decommission: false,
    ingest_storage_migration_classic_ingesters_zone_b_decommission: false,
    ingest_storage_migration_classic_ingesters_zone_c_decommission: false,

    ingest_storage_ingester_zones: 2,
  },
}
