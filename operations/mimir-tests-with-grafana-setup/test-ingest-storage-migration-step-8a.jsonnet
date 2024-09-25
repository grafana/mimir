// Migration step 8a:
// - Switch primary zone (used by other zones to scale up/down) to ingester-zone-b-partition, since we're going
//   to scale down ingester-zone-a-partition to 0.
(import 'test-ingest-storage-migration-step-7.jsonnet') {
  _config+:: {
    ingest_storage_ingester_migration_autoscaling_ingester_primary_zone: 'ingester-zone-b-partition',
  },
}
