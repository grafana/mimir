{
  grafanaDashboardFolder: 'Mimir',
  grafanaDashboardShards: 4,

  _config+:: {
    // The product name used when building dashboards.
    product: 'Mimir',

    // The prefix including product name used when building dashboards.
    dashboard_prefix: '%(product)s / ' % $._config.product,
    // Controls tooltip and hover highlight behavior across different panels
    // 0: Default, the cross hair will appear on only one panel
    // 1: Shared crosshair, the crosshair will appear on all panels but the
    // tooltip will  appear only on the panel under the cursor
    // 2: Shared Tooltip, both crosshair and tooltip will appear on all panels
    graph_tooltip: 0,

    // Tags for dashboards.
    tags: ['mimir'],

    // If Mimir is deployed as a single binary, set to true to
    // modify the job selectors in the dashboard queries.
    singleBinary: false,

    // These are used by the dashboards and allow for the simultaneous display of
    // microservice and single binary Mimir clusters.
    // Whenever you do any change here, please reflect it in the doc at:
    // docs/sources/operators-guide/monitoring-grafana-mimir/requirements.md
    job_names: {
      ingester: '(ingester.*|cortex|mimir|mimir-write)',  // Match also custom and per-zone ingester deployments.
      distributor: '(distributor|cortex|mimir|mimir-write)',
      querier: '(querier.*|cortex|mimir|mimir-read)',  // Match also custom querier deployments.
      ruler_querier: '(ruler-querier.*)',  // Match also custom querier deployments.
      ruler: '(ruler|cortex|mimir|mimir-backend)',
      query_frontend: '(query-frontend.*|cortex|mimir|mimir-read)',  // Match also custom query-frontend deployments.
      ruler_query_frontend: '(ruler-query-frontend.*)',  // Match also custom ruler-query-frontend deployments.
      query_scheduler: '(query-scheduler.*|mimir-backend)',  // Not part of single-binary. Match also custom query-scheduler deployments.
      ruler_query_scheduler: '(ruler-query-scheduler.*)',  // Not part of single-binary. Match also custom query-scheduler deployments.
      ring_members: ['alertmanager', 'compactor', 'distributor', 'ingester.*', 'querier.*', 'ruler', 'ruler-querier.*', 'store-gateway.*', 'cortex', 'mimir'],
      store_gateway: '(store-gateway.*|cortex|mimir|mimir-backend)',  // Match also per-zone store-gateway deployments.
      gateway: '(gateway|cortex-gw|cortex-gw-internal)',
      compactor: '(compactor.*|cortex|mimir|mimir-backend)',  // Match also custom compactor deployments.
      alertmanager: '(alertmanager|cortex|mimir|mimir-backend)',
      overrides_exporter: '(overrides-exporter|mimir-backend)',
    },

    // The label used to differentiate between different Kubernetes clusters.
    per_cluster_label: 'cluster',

    // Grouping labels, to uniquely identify and group by {jobs, clusters}
    job_labels: [$._config.per_cluster_label, 'namespace', 'job'],
    cluster_labels: [$._config.per_cluster_label, 'namespace'],

    // PromQL queries used to find clusters and namespaces with Mimir.
    dashboard_variables: {
      job_query: 'cortex_build_info',  // Only used if singleBinary is true.
      cluster_query: 'cortex_build_info',
      namespace_query: 'cortex_build_info{%s=~"$cluster"}' % $._config.per_cluster_label,
    },

    cortex_p99_latency_threshold_seconds: 2.5,

    // Whether resources dashboards are enabled (based on cAdvisor metrics).
    resources_dashboards_enabled: true,

    // Whether mimir gateway is enabled
    gateway_enabled: false,

    // The label used to differentiate between different application instances (i.e. 'pod' in a kubernetes install).
    per_instance_label: 'pod',

    // Name selectors for different application instances, using the "per_instance_label".
    instance_names: {
      local helmCompatibleName = function(name) '(.*-mimir-)?%s' % name,

      compactor: helmCompatibleName('compactor.*'),
      alertmanager: helmCompatibleName('alertmanager.*'),
      ingester: helmCompatibleName('ingester.*'),
      distributor: helmCompatibleName('distributor.*'),
      querier: helmCompatibleName('querier.*'),
      ruler: helmCompatibleName('ruler.*'),
      query_frontend: helmCompatibleName('query-frontend.*'),
      query_scheduler: helmCompatibleName('query-scheduler.*'),
      store_gateway: helmCompatibleName('store-gateway.*'),
      gateway: helmCompatibleName('(gateway|cortex-gw|cortex-gw).*'),
    },

    deployment_type: 'kubernetes',
    // System mount point where mimir stores its data, used for baremetal
    // deployment only.
    instance_data_mountpoint: '/',
    resources_panel_series: {
      kubernetes: {
        network_receive_bytes_metrics: 'container_network_receive_bytes_total',
        network_transmit_bytes_metrics: 'container_network_transmit_bytes_total',
      },
      baremetal: {
        network_receive_bytes_metrics: 'node_network_receive_bytes_total',
        network_transmit_bytes_metrics: 'node_network_transmit_bytes_total',
      },
    },
    resources_panel_queries: {
      kubernetes: {
        cpu_usage: 'sum by(%(instance)s) (rate(container_cpu_usage_seconds_total{%(namespace)s,container=~"%(instanceName)s"}[$__rate_interval]))',
        cpu_limit: 'min(container_spec_cpu_quota{%(namespace)s,container=~"%(instanceName)s"} / container_spec_cpu_period{%(namespace)s,container=~"%(instanceName)s"})',
        cpu_request: 'min(kube_pod_container_resource_requests{%(namespace)s,container=~"%(instanceName)s",resource="cpu"})',
        // We use "max" instead of "sum" otherwise during a rolling update of a statefulset we will end up
        // summing the memory of the old instance/pod (whose metric will be stale for 5m) to the new instance/pod.
        memory_working_usage: 'max by(%(instance)s) (container_memory_working_set_bytes{%(namespace)s,container=~"%(instanceName)s"})',
        memory_working_limit: 'min(container_spec_memory_limit_bytes{%(namespace)s,container=~"%(instanceName)s"} > 0)',
        memory_working_request: 'min(kube_pod_container_resource_requests{%(namespace)s,container=~"%(instanceName)s",resource="memory"})',
        // We use "max" instead of "sum" otherwise during a rolling update of a statefulset we will end up
        // summing the memory of the old instance/pod (whose metric will be stale for 5m) to the new instance/pod.
        memory_rss_usage: 'max by(%(instance)s) (container_memory_rss{%(namespace)s,container=~"%(instanceName)s"})',
        memory_rss_limit: 'min(container_spec_memory_limit_bytes{%(namespace)s,container=~"%(instanceName)s"} > 0)',
        memory_rss_request: 'min(kube_pod_container_resource_requests{%(namespace)s,container=~"%(instanceName)s",resource="memory"})',
        network: 'sum by(%(instance)s) (rate(%(metric)s{%(namespace)s,%(instance)s=~"%(instanceName)s"}[$__rate_interval]))',
        disk_writes:
          |||
            sum by(%(instanceLabel)s, %(instance)s, device) (
              rate(
                node_disk_written_bytes_total[$__rate_interval]
              )
            )
            +
            %(filterNodeDiskContainer)s
          |||,
        disk_reads:
          |||
            sum by(%(instanceLabel)s, %(instance)s, device) (
              rate(
                node_disk_read_bytes_total[$__rate_interval]
              )
            ) + %(filterNodeDiskContainer)s
          |||,
        disk_utilization:
          |||
            max by(persistentvolumeclaim) (
              kubelet_volume_stats_used_bytes{%(namespace)s} /
              kubelet_volume_stats_capacity_bytes{%(namespace)s}
            )
            and
            count by(persistentvolumeclaim) (
              kube_persistentvolumeclaim_labels{
                %(namespace)s,
                %(label)s
              }
            )
          |||,
      },
      baremetal: {
        // Somes queries does not makes sense when running mimir on baremetal
        // no need to define them
        cpu_usage: 'sum by(%(instance)s) (rate(node_cpu_seconds_total{mode="user",%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}[$__rate_interval]))',
        memory_working_usage:
          |||
            node_memory_MemTotal_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
            - node_memory_MemFree_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
            - node_memory_Buffers_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
            - node_memory_Cached_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
            - node_memory_Slab_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
            - node_memory_PageTables_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
            - node_memory_SwapCached_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
          |||,
        // From cAdvisor code, the memory RSS is:
        // The amount of anonymous and swap cache memory (includes transparent hugepages).
        memory_rss_usage:
          |||
            node_memory_Active_anon_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
            + node_memory_SwapCached_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}
          |||,
        network: 'sum by(%(instance)s) (rate(%(metric)s{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}[$__rate_interval]))',
        disk_writes:
          |||
            sum by(%(instanceLabel)s, %(instance)s, device) (
              rate(
                node_disk_written_bytes_total{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}[$__rate_interval]
              )
            )
          |||,
        disk_reads:
          |||
            sum by(%(instanceLabel)s, %(instance)s, device) (
              rate(
                node_disk_read_bytes_total{%(namespace)s,%(instance)s=~".*%(instanceName)s.*"}[$__rate_interval]
              )
            )
          |||,
        disk_utilization:
          |||
            1 - ((node_filesystem_avail_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*", mountpoint="%(instanceDataDir)s"})
                / node_filesystem_size_bytes{%(namespace)s,%(instance)s=~".*%(instanceName)s.*", mountpoint="%(instanceDataDir)s"})
          |||,
      },
    },

    // The label used to differentiate between different nodes (i.e. servers).
    per_node_label: 'instance',

    // Whether certain dashboard description headers should be shown
    show_dashboard_descriptions: {
      writes: true,
      reads: true,
      tenants: true,
      top_tenants: true,
    },

    // Whether autoscaling panels and alerts should be enabled for specific Mimir services.
    autoscaling: {
      querier_enabled: false,
      querier_hpa_name: 'keda-hpa-querier',
      ruler_querier_hpa_name: 'keda-hpa-ruler-querier',
    },

    // The routes to exclude from alerts.
    alert_excluded_routes: [],

    // The default datasource used for dashboards.
    dashboard_datasource: 'default',
    datasource_regex: '',

    // Tunes histogram recording rules to aggregate over this interval.
    // Set to at least twice the scrape interval; otherwise, recording rules will output no data.
    // Set to four times the scrape interval to account for edge cases: https://www.robustperception.io/what-range-should-i-use-with-rate/
    recording_rules_range_interval: '1m',
  },
}
