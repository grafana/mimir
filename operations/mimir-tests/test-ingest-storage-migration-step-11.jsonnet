// Migration step 11:
// - Enable ingesters autoscaling.
(import 'test-ingest-storage-migration-step-10.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_ingester_autoscaling_ingester_annotations_enabled: true,
    multi_zone_ingester_replicas: 0,
    ingester_automated_downscale_enabled: false,

    // The following config is not required anymore.
    ingest_storage_migration_classic_ingesters_no_scale_down_delay: false,
  },
}
