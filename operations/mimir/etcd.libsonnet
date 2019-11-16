local etcd_cluster = import 'etcd-operator/etcd-cluster.libsonnet';

etcd_cluster {
  etcd:
    $.etcd_cluster('etcd', env=[{
      name: 'ETCD_AUTO_COMPACTION_RETENTION',
      value: '1h',
    }]),
}
