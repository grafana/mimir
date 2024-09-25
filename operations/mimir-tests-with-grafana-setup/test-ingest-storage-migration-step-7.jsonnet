// Migration step 7:
// - Globally enable ingest storage.
(import 'test-ingest-storage-migration-step-6.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_enabled: true,
    ingest_storage_ingester_instance_ring_dedicated_prefix_enabled: true,

    // The following migration-specific settings are not required anymore because we enabled ingest storage globally.
    ingest_storage_migration_write_to_partition_ingesters_enabled: false,
    ingest_storage_migration_querier_enabled: false,

    // Keep removing downscaling annotations in inester-zone-[abc], until we are ready to switch to ingest-storage scaling.
    ingest_storage_ingester_migration_classic_ingesters_remove_downscaling_annotations: true,
    // Don't scale down to 0 anymore.
    ingest_storage_ingester_migration_classic_ingesters_scale_down: false,
  },
}
