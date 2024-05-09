// Migration step 10:
// - Deploy ingesters autoscaling HPA and ReplicaTemplates.
(import 'test-ingest-storage-migration-step-9.jsonnet') {
  _config+:: {
    // This builds on previous step.
    ingest_storage_ingester_autoscaling_enabled: true,
    ingest_storage_ingester_autoscaling_min_replicas: 3,
    ingest_storage_ingester_autoscaling_max_replicas: 30,

    // Do not configure ingesters to scale based on HPA yet.
    ingest_storage_ingester_autoscaling_ingester_annotations_enabled: false,
  },
}
