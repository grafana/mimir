// Migration step 1:
// - Deploy a temporarily dedicated set of ingesters, named ingester-zone-[abc]-partition.
(import 'test-ingest-storage-migration-step-0.jsonnet') + {
  _config+:: {
    ingest_storage_migration_partition_ingester_zone_a_enabled: true,
    ingest_storage_migration_partition_ingester_zone_b_enabled: true,
    ingest_storage_migration_partition_ingester_zone_c_enabled: true,

    ingest_storage_migration_partition_ingester_zone_a_replicas: 0,
    ingest_storage_migration_partition_ingester_zone_b_replicas: 0,
    ingest_storage_migration_partition_ingester_zone_c_replicas: 0,

    shuffle_sharding+:: {
      ingest_storage_partitions_enabled: true,
    },
  },
}
