local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {

  'cortex-scaling.json':
    $.dashboard('Cortex / Scaling')
    .addClusterSelectorTemplates()
    .addRow(
      $.row('Workload-based scaling')
      .addPanel(
        $.panel('Workload-based scaling') + { sort: { col: 1, desc: false } } +
        $.tablePanel([
          |||
            sum by (cluster, namespace, deployment) (
              kube_deployment_spec_replicas{cluster=~"$cluster", namespace=~"$namespace", deployment=~"ingester|memcached"}
              or
              label_replace(
                kube_statefulset_replicas{cluster=~"$cluster", namespace=~"$namespace", deployment=~"ingester|memcached"},
                "deployment", "$1", "statefulset", "(.*)"
              )
            )
          |||,
          |||
            quantile_over_time(0.99, sum by (cluster, namespace, deployment) (label_replace(rate(cortex_distributor_received_samples_total{cluster=~"$cluster", namespace=~"$namespace"}[1m]), "deployment", "ingester", "cluster", ".*"))[1h:])
              * 3 / 80e3
          |||,
          |||
            label_replace(
              sum by(cluster, namespace) (
                cortex_ingester_memory_series{cluster=~"$cluster", namespace=~"$namespace"}
              ) / 1e+6,
              "deployment", "ingester", "cluster", ".*"
            )
              or
            label_replace(
              sum by (cluster, namespace) (
                4 * cortex_ingester_memory_series{cluster=~"$cluster", namespace=~"$namespace", job=~".+/ingester"}
                  *
                cortex_ingester_chunk_size_bytes_sum{cluster=~"$cluster", namespace=~"$namespace", job=~".+/ingester"}
                  /
                cortex_ingester_chunk_size_bytes_count{cluster=~"$cluster", namespace=~"$namespace", job=~".+/ingester"}
              )
                /
              avg by (cluster, namespace) (memcached_limit_bytes{cluster=~"$cluster", namespace=~"$namespace", job=~".+/memcached"}),
              "deployment", "memcached", "namespace", ".*"
            )
          |||,
        ], {
          cluster: { alias: 'Cluster' },
          namespace: { alias: 'Namespace' },
          deployment: { alias: 'Deployment' },
          'Value #A': { alias: 'Current Replicas', decimals: 0 },
          'Value #B': { alias: 'Required Replicas, by ingestion rate', decimals: 0 },
          'Value #C': { alias: 'Required Replicas, by active series', decimals: 0 },
        })
      )
    )
    .addRow(
      ($.row('Resource-based scaling') + { height: '500px' })
      .addPanel(
        $.panel('Resource-based scaling') + { sort: { col: 1, desc: false } } +
        $.tablePanel([
          |||
            sum by (cluster, namespace, deployment) (
              kube_deployment_spec_replicas{cluster=~"$cluster", namespace=~"$namespace"}
              or
              label_replace(
                kube_statefulset_replicas{cluster=~"$cluster", namespace=~"$namespace"},
                "deployment", "$1", "statefulset", "(.*)"
              )
            )
          |||,
          |||
            sum by (cluster, namespace, deployment) (
              kube_deployment_spec_replicas{cluster=~"$cluster", namespace=~"$namespace"}
              or
              label_replace(
                kube_statefulset_replicas{cluster=~"$cluster", namespace=~"$namespace"},
                "deployment", "$1", "statefulset", "(.*)"
              )
            )
              *
            quantile_over_time(0.99, sum by (cluster, namespace, deployment) (label_replace(rate(container_cpu_usage_seconds_total{cluster=~"$cluster", namespace=~"$namespace"}[1m]), "deployment", "$1", "pod_name", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"))[24h:])
              /
            sum by (cluster, namespace, deployment) (label_replace(kube_pod_container_resource_requests_cpu_cores{cluster=~"$cluster", namespace=~"$namespace"}, "deployment", "$1", "pod", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"))
          |||,
          |||
            sum by (cluster, namespace, deployment) (
              kube_deployment_spec_replicas{cluster=~"$cluster", namespace=~"$namespace"}
              or
              label_replace(
                kube_statefulset_replicas{cluster=~"$cluster", namespace=~"$namespace"},
                "deployment", "$1", "statefulset", "(.*)"
              )
            )
              *
            quantile_over_time(0.99, sum by (cluster, namespace, deployment) (label_replace(container_memory_usage_bytes{cluster=~"$cluster", namespace=~"$namespace"}, "deployment", "$1", "pod_name", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"))[24h:1m])
              /
            sum by (cluster, namespace, deployment) (label_replace(kube_pod_container_resource_requests_memory_bytes{cluster=~"$cluster", namespace=~"$namespace"}, "deployment", "$1", "pod", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"))
          |||,
        ], {
          cluster: { alias: 'Cluster' },
          namespace: { alias: 'Namespace' },
          deployment: { alias: 'Deployment' },
          'Value #A': { alias: 'Current Replicas', decimals: 0 },
          'Value #B': { alias: 'Required Replicas, by CPU usage', decimals: 0 },
          'Value #C': { alias: 'Required Replicas, by Memory usage', decimals: 0 },
        })
      )
    ),
}
