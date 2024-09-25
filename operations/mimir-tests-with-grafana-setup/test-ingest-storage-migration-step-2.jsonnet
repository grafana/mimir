// Migration step 3:
// - Configure distributors and rulers to write both to classic ingesters and Kafka.
(import 'test-ingest-storage-migration-step-1c.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_migration_write_to_partition_ingesters_enabled: true,
    ingest_storage_migration_write_to_classic_ingesters_enabled: true,
  },
}
