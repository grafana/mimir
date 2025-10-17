local etcd_cluster = import 'etcd-operator/etcd-cluster.libsonnet';

// Etcd is deprecated for use with the HA tracker. Use memberlist instead.
// Etcd is still supported for other components, but memberlist is recommended for all KV store usages.
etcd_cluster {
  etcd:
    $.etcd_cluster('etcd', size=$._config.etcd_replicas, env=[{
      name: 'ETCD_AUTO_COMPACTION_RETENTION',
      value: '1h',
    }]),
}
