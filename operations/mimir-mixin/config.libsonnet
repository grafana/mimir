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
    graph_tooltip: 1,

    // Tags for dashboards.
    tags: ['mimir'],

    // If Mimir is deployed as a single binary, set to true to
    // modify the job selectors in the dashboard queries.
    singleBinary: false,

    // This is mapping between a Mimir component name and the regular expression that should be used
    // to match its instance and container name. Mimir jsonnet and Helm guarantee that the instance name
    // (e.g. Kubernetes Deployment) and container name always match, so it's safe to use a shared mapping.
    //
    // This mapping is intentionally local and can't be overridden. If the final user needs to customize
    // dashboards and alerts, they should override the final matcher regexp (e.g. container_names or instance_names).
    local componentNameRegexp = {
      // Microservices deployment mode. The following matchers MUST match only
      // the instance when deployed in microservices mode (e.g. "distributor"
      // matcher shouldn't match "mimir-write" too).
      compactor: 'compactor',
      alertmanager: 'alertmanager',
      alertmanager_im: 'alertmanager-im',
      ingester: 'ingester',
      distributor: 'distributor',
      querier: 'querier',
      query_frontend: 'query-frontend',
      query_scheduler: 'query-scheduler',
      ruler: 'ruler',
      ruler_querier: 'ruler-querier',
      ruler_query_frontend: 'ruler-query-frontend',
      ruler_query_scheduler: 'ruler-query-scheduler',
      store_gateway: 'store-gateway',
      overrides_exporter: 'overrides-exporter',
      gateway: '(gateway|cortex-gw|cortex-gw-internal)',

      // Read-write deployment mode. The following matchers MUST match only
      // the instance when deployed in read-write deployment mode (e.g. "mimir-write"
      // matcher shouldn't match "distributor" too).
      mimir_write: 'mimir-write',
      mimir_read: 'mimir-read',
      mimir_backend: 'mimir-backend',
    },

    // Some dashboards show panels grouping together multiple components of a given "path".
    // This mapping configures which components belong to each group.
    local componentGroups = {
      write: ['distributor', 'ingester', 'mimir_write'],
      read: ['query_frontend', 'querier', 'ruler_query_frontend', 'ruler_querier', 'mimir_read'],
      backend: ['query_scheduler', 'ruler_query_scheduler', 'ruler', 'store_gateway', 'compactor', 'alertmanager', 'overrides_exporter', 'mimir_backend'],
    },

    // These are used by the dashboards and allow for the simultaneous display of
    // microservice and single binary Mimir clusters.
    // Whenever you do any change here, please reflect it in the doc at:
    // docs/sources/mimir/manage/monitoring-grafana-mimir/requirements.md
    job_names: {
      ingester: ['ingester.*', 'cortex', 'mimir', 'mimir-write.*'],  // Match also custom and per-zone ingester deployments.
      distributor: ['distributor', 'cortex', 'mimir', 'mimir-write.*'],
      querier: ['querier.*', 'cortex', 'mimir', 'mimir-read.*'],  // Match also custom querier deployments.
      ruler_querier: ['ruler-querier.*'],  // Match also custom querier deployments.
      ruler: ['ruler', 'cortex', 'mimir', 'mimir-backend.*'],
      query_frontend: ['query-frontend.*', 'cortex', 'mimir', 'mimir-read.*'],  // Match also custom query-frontend deployments.
      ruler_query_frontend: ['ruler-query-frontend.*'],  // Match also custom ruler-query-frontend deployments.
      query_scheduler: ['query-scheduler.*', 'mimir-backend.*'],  // Not part of single-binary. Match also custom query-scheduler deployments.
      ruler_query_scheduler: ['ruler-query-scheduler.*'],  // Not part of single-binary. Match also custom query-scheduler deployments.
      ring_members: ['admin-api', 'alertmanager', 'compactor.*', 'distributor', 'ingester.*', 'querier.*', 'ruler', 'ruler-querier.*', 'store-gateway.*', 'cortex', 'mimir', 'mimir-write.*', 'mimir-read.*', 'mimir-backend.*'],
      store_gateway: ['store-gateway.*', 'cortex', 'mimir', 'mimir-backend.*'],  // Match also per-zone store-gateway deployments.
      gateway: ['gateway', 'cortex-gw', 'cortex-gw-internal'],
      compactor: ['compactor.*', 'cortex', 'mimir', 'mimir-backend.*'],  // Match also custom compactor deployments.
      alertmanager: ['alertmanager', 'cortex', 'mimir', 'mimir-backend.*'],
      overrides_exporter: ['overrides-exporter', 'mimir-backend.*'],

      // The following are job matchers used to select all components in a given "path".
      write: ['distributor', 'ingester.*', 'mimir-write.*'],
      read: ['query-frontend.*', 'querier.*', 'ruler-query-frontend.*', 'ruler-querier.*', 'mimir-read.*'],
      backend: ['ruler', 'query-scheduler.*', 'ruler-query-scheduler.*', 'store-gateway.*', 'compactor.*', 'alertmanager', 'overrides-exporter', 'mimir-backend.*'],
    },

    // Name selectors for different application instances, using the "per_instance_label".
    instance_names: {
      // Wrap the regexp into an Helm compatible matcher if the deployment type is "kubernetes".
      local helmCompatibleMatcher = function(regexp) if $._config.deployment_type == 'kubernetes' then '(.*mimir-)?%s' % regexp else regexp,
      // Wrap the regexp to match any prefix if the deployment type is "baremetal".
      local baremetalCompatibleMatcher = function(regexp) if $._config.deployment_type == 'baremetal' then '.*%s' % regexp else regexp,
      local instanceMatcher = function(regexp) baremetalCompatibleMatcher(helmCompatibleMatcher('%s.*' % regexp)),

      // Microservices deployment mode. The following matchers MUST match only
      // the instance when deployed in microservices mode (e.g. "distributor"
      // matcher shouldn't match "mimir-write" too).
      compactor: instanceMatcher(componentNameRegexp.compactor),
      alertmanager: instanceMatcher(componentNameRegexp.alertmanager),
      alertmanager_im: instanceMatcher(componentNameRegexp.alertmanager_im),
      ingester: instanceMatcher(componentNameRegexp.ingester),
      distributor: instanceMatcher(componentNameRegexp.distributor),
      querier: instanceMatcher(componentNameRegexp.querier),
      ruler: instanceMatcher(componentNameRegexp.ruler),
      ruler_query_frontend: instanceMatcher(componentNameRegexp.ruler_query_frontend),
      ruler_query_scheduler: instanceMatcher(componentNameRegexp.ruler_query_scheduler),
      ruler_querier: instanceMatcher(componentNameRegexp.ruler_querier),
      query_frontend: instanceMatcher(componentNameRegexp.query_frontend),
      query_scheduler: instanceMatcher(componentNameRegexp.query_scheduler),
      store_gateway: instanceMatcher(componentNameRegexp.store_gateway),
      overrides_exporter: instanceMatcher(componentNameRegexp.overrides_exporter),
      gateway: instanceMatcher(componentNameRegexp.gateway),

      // Read-write deployment mode. The following matchers MUST match only
      // the instance when deployed in read-write deployment mode (e.g. "mimir-write"
      // matcher shouldn't match "distributor" too).
      mimir_write: instanceMatcher(componentNameRegexp.mimir_write),
      mimir_read: instanceMatcher(componentNameRegexp.mimir_read),
      mimir_backend: instanceMatcher(componentNameRegexp.mimir_backend),

      // The following are instance matchers used to select all components in a given "path".
      // These matchers CAN match both instances deployed in "microservices" and "read-write" mode.
      local componentsGroupMatcher = function(components)
        instanceMatcher('(%s)' % std.join('|', std.map(function(name) componentNameRegexp[name], components))),

      write: componentsGroupMatcher(componentGroups.write),
      read: componentsGroupMatcher(componentGroups.read),
      backend: componentsGroupMatcher(componentGroups.backend),
    },
    all_instances: std.join('|', std.map(function(name) componentNameRegexp[name], componentGroups.write + componentGroups.read + componentGroups.backend)),

    container_names: {
      // Microservices deployment mode. The following matchers MUST match only
      // the instance when deployed in microservices mode (e.g. "distributor"
      // matcher shouldn't match "mimir-write" too).
      gateway: componentNameRegexp.gateway,
      distributor: componentNameRegexp.distributor,
      ingester: componentNameRegexp.ingester,
      query_frontend: componentNameRegexp.query_frontend,
      query_scheduler: componentNameRegexp.query_scheduler,
      querier: componentNameRegexp.querier,
      store_gateway: componentNameRegexp.store_gateway,
      ruler: componentNameRegexp.ruler,
      ruler_query_frontend: componentNameRegexp.ruler_query_frontend,
      ruler_query_scheduler: componentNameRegexp.ruler_query_scheduler,
      ruler_querier: componentNameRegexp.ruler_querier,
      alertmanager: componentNameRegexp.alertmanager,
      alertmanager_im: componentNameRegexp.alertmanager_im,
      compactor: componentNameRegexp.compactor,

      // Read-write deployment mode. The following matchers MUST match only
      // the container when deployed in read-write deployment mode (e.g. "mimir-write"
      // matcher shouldn't match "distributor" too).
      mimir_write: componentNameRegexp.mimir_write,
      mimir_read: componentNameRegexp.mimir_read,
      mimir_backend: componentNameRegexp.mimir_backend,

      // The following are container matchers used to select all components in a given "path".
      // These matchers CAN match both instances deployed in "microservices" and "read-write" mode.
      local componentsGroupMatcher = function(components) std.join('|', std.map(function(name) componentNameRegexp[name], components)),

      write: componentsGroupMatcher(componentGroups.write),
      read: componentsGroupMatcher(componentGroups.read),
      backend: componentsGroupMatcher(componentGroups.backend),
    },

    // The label used to differentiate between different Kubernetes clusters.
    per_cluster_label: 'cluster',
    per_namespace_label: 'namespace',
    per_job_label: 'job',

    // Grouping labels, to uniquely identify and group by {jobs, clusters}
    job_labels: [$._config.per_cluster_label, $._config.per_namespace_label, $._config.per_job_label],
    job_prefix: '($namespace)/',
    cluster_labels: [$._config.per_cluster_label, $._config.per_namespace_label],

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

    // Whether grafana cloud alertmanager instance-mapper is enabled
    alertmanager_im_enabled: false,

    // The label used to differentiate between different application instances (i.e. 'pod' in a kubernetes install).
    per_instance_label: 'pod',

    deployment_type: 'kubernetes',
    // System mount point where mimir stores its data, used for baremetal
    // deployment only.
    instance_data_mountpoint: '/',
    // Resource consumption threshold to accomodate node loss
    // used for baremetal deployment only
    resource_threshold: 0.66,
    alertmanager_alerts: {
      kubernetes: {
        memory_allocation: |||
          (container_memory_working_set_bytes{container="alertmanager"} / container_spec_memory_limit_bytes{container="alertmanager"}) > %(threshold)s
          and
          (container_spec_memory_limit_bytes{container="alertmanager"} > 0)
        |||,
      },
      baremetal: {
        memory_allocation: |||
          (process_resident_memory_bytes{job=~".*/alertmanager"} / on(%(per_instance_label)s) node_memory_MemTotal_bytes{}) > %(threshold)s
        |||,
      },
    },
    ingester_alerts: {
      kubernetes: {
        memory_allocation: |||
          (
            # We use RSS instead of working set memory because of the ingester's extensive usage of mmap.
            # See: https://github.com/grafana/mimir/issues/2466
            container_memory_rss{container=~"(%(ingester)s|%(mimir_write)s|%(mimir_backend)s)"}
              /
            ( container_spec_memory_limit_bytes{container=~"(%(ingester)s|%(mimir_write)s|%(mimir_backend)s)"} > 0 )
          )
          # Match only Mimir namespaces.
          * on(%(alert_aggregation_labels)s) group_left max by(%(alert_aggregation_labels)s) (cortex_build_info)
          > %(threshold)s
        |||,
      },
      baremetal: {
        memory_allocation: |||
          (
            process_resident_memory_bytes{job=~".*/(%(ingester)s|%(mimir_write)s|%(mimir_backend)s)"}
              /
            on(%(per_instance_label)s) node_memory_MemTotal_bytes{}
          ) > %(threshold)s
        |||,
      },
    },
    mimir_scaling_rules: {
      kubernetes: {
        actual_replicas_count:
          |||
            # Convenience rule to get the number of replicas for both a deployment and a statefulset.
            # Multi-zone deployments are grouped together removing the "zone-X" suffix.
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(
                kube_deployment_spec_replicas,
                # The question mark in "(.*?)" is used to make it non-greedy, otherwise it
                # always matches everything and the (optional) zone is not removed.
                "deployment", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"
              )
            )
            or
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(kube_statefulset_replicas, "deployment", "$1", "statefulset", "(.*?)(?:-zone-[a-z])?")
            )
          |||,
        cpu_usage_seconds_total:
          |||
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(
                label_replace(
                  sum by (%(alert_aggregation_labels)s, %(per_instance_label)s)(rate(container_cpu_usage_seconds_total[1m])),
                  "deployment", "$1", "%(per_instance_label)s", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                ),
                # The question mark in "(.*?)" is used to make it non-greedy, otherwise it
                # always matches everything and the (optional) zone is not removed.
                "deployment", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"
              )
            )
          |||,
        resource_requests_cpu_cores:
          |||
            # Convenience rule to get the CPU request for both a deployment and a statefulset.
            # Multi-zone deployments are grouped together removing the "zone-X" suffix.
            # This recording rule is made compatible with the breaking changes introduced in kube-state-metrics v2
            # that remove resource metrics, ref:
            # - https://github.com/kubernetes/kube-state-metrics/blob/master/CHANGELOG.md#v200-alpha--2020-09-16
            # - https://github.com/kubernetes/kube-state-metrics/pull/1004
            #
            # This is the old expression, compatible with kube-state-metrics < v2.0.0,
            # where kube_pod_container_resource_requests_cpu_cores was removed:
            (
              sum by (%(alert_aggregation_labels)s, deployment) (
                label_replace(
                  label_replace(
                    kube_pod_container_resource_requests_cpu_cores,
                    "deployment", "$1", "%(per_instance_label)s", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                  ),
                  # The question mark in "(.*?)" is used to make it non-greedy, otherwise it
                  # always matches everything and the (optional) zone is not removed.
                  "deployment", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"
                )
              )
            )
            or
            # This expression is compatible with kube-state-metrics >= v1.4.0,
            # where kube_pod_container_resource_requests was introduced.
            (
              sum by (%(alert_aggregation_labels)s, deployment) (
                label_replace(
                  label_replace(
                    kube_pod_container_resource_requests{resource="cpu"},
                    "deployment", "$1", "%(per_instance_label)s", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                  ),
                  # The question mark in "(.*?)" is used to make it non-greedy, otherwise it
                  # always matches everything and the (optional) zone is not removed.
                  "deployment", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"
                )
              )
            )
          |||,
        cpu_required_replicas_count:
          |||
            # Jobs should be sized to their CPU usage.
            # We do this by comparing 99th percentile usage over the last 24hrs to
            # their current provisioned #replicas and resource requests.
            ceil(
              %(alert_aggregation_rule_prefix)s_deployment:actual_replicas:count
                *
              quantile_over_time(0.99, %(alert_aggregation_rule_prefix)s_deployment:container_cpu_usage_seconds_total:sum_rate[24h])
                /
              %(alert_aggregation_rule_prefix)s_deployment:kube_pod_container_resource_requests_cpu_cores:sum
            )
          |||,
        memory_usage:
          |||
            # Convenience rule to get the Memory utilization for both a deployment and a statefulset.
            # Multi-zone deployments are grouped together removing the "zone-X" suffix.
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(
                label_replace(
                  container_memory_usage_bytes{image!=""},
                  "deployment", "$1", "%(per_instance_label)s", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                ),
                # The question mark in "(.*?)" is used to make it non-greedy, otherwise it
                # always matches everything and the (optional) zone is not removed.
                "deployment", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"
              )
            )
          |||,
        memory_requests:
          |||
            # Convenience rule to get the Memory request for both a deployment and a statefulset.
            # Multi-zone deployments are grouped together removing the "zone-X" suffix.
            # This recording rule is made compatible with the breaking changes introduced in kube-state-metrics v2
            # that remove resource metrics, ref:
            # - https://github.com/kubernetes/kube-state-metrics/blob/master/CHANGELOG.md#v200-alpha--2020-09-16
            # - https://github.com/kubernetes/kube-state-metrics/pull/1004
            #
            # This is the old expression, compatible with kube-state-metrics < v2.0.0,
            # where kube_pod_container_resource_requests_memory_bytes was removed:
            (
              sum by (%(alert_aggregation_labels)s, deployment) (
                label_replace(
                  label_replace(
                    kube_pod_container_resource_requests_memory_bytes,
                    "deployment", "$1", "%(per_instance_label)s", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                  ),
                  # The question mark in "(.*?)" is used to make it non-greedy, otherwise it
                  # always matches everything and the (optional) zone is not removed.
                  "deployment", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"
                )
              )
            )
            or
            # This expression is compatible with kube-state-metrics >= v1.4.0,
            # where kube_pod_container_resource_requests was introduced.
            (
              sum by (%(alert_aggregation_labels)s, deployment) (
                label_replace(
                  label_replace(
                    kube_pod_container_resource_requests{resource="memory"},
                    "deployment", "$1", "%(per_instance_label)s", "(.*)-(?:([0-9]+)|([a-z0-9]+)-([a-z0-9]+))"
                  ),
                  # The question mark in "(.*?)" is used to make it non-greedy, otherwise it
                  # always matches everything and the (optional) zone is not removed.
                  "deployment", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"
                )
              )
            )
          |||,
        memory_required_replicas_count:
          |||
            # Jobs should be sized to their Memory usage.
            # We do this by comparing 99th percentile usage over the last 24hrs to
            # their current provisioned #replicas and resource requests.
            ceil(
              %(alert_aggregation_rule_prefix)s_deployment:actual_replicas:count
                *
              quantile_over_time(0.99, %(alert_aggregation_rule_prefix)s_deployment:container_memory_usage_bytes:sum[24h])
                /
              %(alert_aggregation_rule_prefix)s_deployment:kube_pod_container_resource_requests_memory_bytes:sum
            )
          |||,
      },
      baremetal: {
        actual_replicas_count:
          |||
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(
                cortex_build_info{namespace="baremetal"},
                "deployment", "$1", "job", "baremetal/(.*)"
              )
            )
          |||,
        cpu_usage_seconds_total:
          |||
            sum by (%(alert_aggregation_labels)s, deployment) (
              irate(
                label_replace(
                  process_cpu_seconds_total{namespace="baremetal"},
                  "deployment", "$1", "job", "baremetal/(.*)"
                )[5m:]
              )
            )
          |||,
        resource_requests_cpu_cores:
          |||
            sum by (%(alert_aggregation_labels)s, deployment) (
              count without(cpu, mode) (
                label_replace(
                  node_cpu_seconds_total{mode="idle"},
                  "deployment", "$1", "instance", ".*(%(all_instances)s).*"
                )
              )
            )
          |||,
        cpu_required_replicas_count:
          |||
            ceil(
              %(alert_aggregation_rule_prefix)s_deployment:actual_replicas:count
                *
              quantile_over_time(0.99, %(alert_aggregation_rule_prefix)s_deployment:container_cpu_usage_seconds_total:sum_rate[24h])
                /
              %(alert_aggregation_rule_prefix)s_deployment:kube_pod_container_resource_requests_cpu_cores:sum
                /
              %(resource_threshold)s
            )
          |||,
        memory_usage:
          |||
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(
                process_resident_memory_bytes{namespace="baremetal"},
                "deployment", "$1", "job", "baremetal/(.*)"
              )
            )
          |||,
        memory_requests:
          |||
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(
                node_memory_MemTotal_bytes,
                "deployment", "$1", "instance", ".*(%(all_instances)s).*"
              )
            )
          |||,
        memory_required_replicas_count:
          |||
            ceil(
              %(alert_aggregation_rule_prefix)s_deployment:actual_replicas:count
                *
              quantile_over_time(0.99, %(alert_aggregation_rule_prefix)s_deployment:container_memory_usage_bytes:sum[24h])
                /
              %(alert_aggregation_rule_prefix)s_deployment:kube_pod_container_resource_requests_memory_bytes:sum
                /
              %(resource_threshold)s
            )
          |||,
      },
    },
    resources_panel_queries: {
      kubernetes: {
        cpu_usage: 'sum by(%(instanceLabel)s) (rate(container_cpu_usage_seconds_total{%(namespace)s,container=~"%(containerName)s"}[$__rate_interval]))',
        cpu_limit: 'min(container_spec_cpu_quota{%(namespace)s,container=~"%(containerName)s"} / container_spec_cpu_period{%(namespace)s,container=~"%(containerName)s"})',
        cpu_request: 'min(kube_pod_container_resource_requests{%(namespace)s,container=~"%(containerName)s",resource="cpu"})',
        // We use "max" instead of "sum" otherwise during a rolling update of a statefulset we will end up
        // summing the memory of the old instance/pod (whose metric will be stale for 5m) to the new instance/pod.
        memory_working_usage: 'max by(%(instanceLabel)s) (container_memory_working_set_bytes{%(namespace)s,container=~"%(containerName)s"})',
        memory_working_limit: 'min(container_spec_memory_limit_bytes{%(namespace)s,container=~"%(containerName)s"} > 0)',
        memory_working_request: 'min(kube_pod_container_resource_requests{%(namespace)s,container=~"%(containerName)s",resource="memory"})',
        // We use "max" instead of "sum" otherwise during a rolling update of a statefulset we will end up
        // summing the memory of the old instance/pod (whose metric will be stale for 5m) to the new instance/pod.
        memory_rss_usage: 'max by(%(instanceLabel)s) (container_memory_rss{%(namespace)s,container=~"%(containerName)s"})',
        memory_rss_limit: 'min(container_spec_memory_limit_bytes{%(namespace)s,container=~"%(containerName)s"} > 0)',
        memory_rss_request: 'min(kube_pod_container_resource_requests{%(namespace)s,container=~"%(containerName)s",resource="memory"})',
        memory_go_heap_usage: 'sum by(%(instanceLabel)s) (go_memstats_heap_inuse_bytes{%(namespace)s,container=~"%(containerName)s"})',
        network_receive_bytes: 'sum by(%(instanceLabel)s) (rate(container_network_receive_bytes_total{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"}[$__rate_interval]))',
        network_transmit_bytes: 'sum by(%(instanceLabel)s) (rate(container_network_transmit_bytes_total{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"}[$__rate_interval]))',
        disk_writes:
          |||
            sum by(%(nodeLabel)s, %(instanceLabel)s, device) (
              rate(
                node_disk_written_bytes_total[$__rate_interval]
              )
            )
            +
            %(filterNodeDiskContainer)s
          |||,
        disk_reads:
          |||
            sum by(%(nodeLabel)s, %(instanceLabel)s, device) (
              rate(
                node_disk_read_bytes_total[$__rate_interval]
              )
            ) + %(filterNodeDiskContainer)s
          |||,
        disk_utilization:
          |||
            max by(persistentvolumeclaim) (
              kubelet_volume_stats_used_bytes{%(namespaceMatcher)s} /
              kubelet_volume_stats_capacity_bytes{%(namespaceMatcher)s}
            )
            and
            count by(persistentvolumeclaim) (
              kube_persistentvolumeclaim_labels{
                %(namespaceMatcher)s,
                %(containerMatcher)s
              }
            )
          |||,
      },
      baremetal: {
        // Somes queries does not makes sense when running mimir on baremetal
        // no need to define them
        cpu_usage: 'sum by(%(instanceLabel)s) (rate(node_cpu_seconds_total{mode="user",%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}[$__rate_interval]))',
        memory_working_usage:
          |||
            node_memory_MemTotal_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
            - node_memory_MemFree_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
            - node_memory_Buffers_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
            - node_memory_Cached_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
            - node_memory_Slab_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
            - node_memory_PageTables_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
            - node_memory_SwapCached_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
          |||,
        // From cAdvisor code, the memory RSS is:
        // The amount of anonymous and swap cache memory (includes transparent hugepages).
        memory_rss_usage:
          |||
            node_memory_Active_anon_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
            + node_memory_SwapCached_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}
          |||,
        memory_go_heap_usage: 'sum by(%(instanceLabel)s) (go_memstats_heap_inuse_bytes{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"})',
        network_receive_bytes: 'sum by(%(instanceLabel)s) (rate(node_network_receive_bytes_total{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"}[$__rate_interval]))',
        network_transmit_bytes: 'sum by(%(instanceLabel)s) (rate(node_network_transmit_bytes_total{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s"}[$__rate_interval]))',
        disk_writes:
          |||
            sum by(%(nodeLabel)s, %(instanceLabel)s, device) (
              rate(
                node_disk_written_bytes_total{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}[$__rate_interval]
              )
            )
          |||,
        disk_reads:
          |||
            sum by(%(nodeLabel)s, %(instanceLabel)s, device) (
              rate(
                node_disk_read_bytes_total{%(namespace)s,%(instanceLabel)s=~"%(instanceName)s"}[$__rate_interval]
              )
            )
          |||,
        disk_utilization:
          |||
            1 - ((node_filesystem_avail_bytes{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s", mountpoint="%(instanceDataDir)s"})
                / node_filesystem_size_bytes{%(namespaceMatcher)s,%(instanceLabel)s=~"%(instanceName)s", mountpoint="%(instanceDataDir)s"})
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
      query_frontend: {
        enabled: false,
        hpa_name: 'keda-hpa-query-frontend',
      },
      ruler_query_frontend: {
        enabled: false,
        hpa_name: 'keda-hpa-ruler-query-frontend',
      },
      querier: {
        enabled: false,
        // hpa_name can be a regexp to support multiple querier deployments, like "keda-hpa-querier(-burst(-backup)?)?".
        hpa_name: 'keda-hpa-querier',
      },
      ruler_querier: {
        enabled: false,
        hpa_name: 'keda-hpa-ruler-querier',
      },
      distributor: {
        enabled: false,
        hpa_name: 'keda-hpa-distributor',
      },
      ruler: {
        enabled: false,
        hpa_name: 'keda-hpa-ruler',
      },
      gateway: {
        enabled: false,
        hpa_name: 'keda-hpa-cortex-gw.*',
      },
    },


    // The routes to exclude from alerts.
    alert_excluded_routes: [
      'debug_pprof',
    ],

    // The default datasource used for dashboards.
    dashboard_datasource: 'default',
    datasource_regex: '',

    // Tunes histogram recording rules to aggregate over this interval.
    // Set to at least twice the scrape interval; otherwise, recording rules will output no data.
    // Set to four times the scrape interval to account for edge cases: https://www.robustperception.io/what-range-should-i-use-with-rate/
    recording_rules_range_interval: '1m',

    // Used to inject rows into dashboards at specific places that support it.
    injectRows: {},

    // Used to add additional services to dashboards that support it.
    extraServiceNames: [],

    // When using rejecting inflight requests in ingesters early (using -ingester.limit-inflight-requests-using-grpc-method-limiter option),
    // rejected requests will not count towards standard Mimir metrics like cortex_request_duration_seconds_count.
    // Enabling this will make them visible on the dashboard again.
    //
    // Disabled by default, because when -ingester.limit-inflight-requests-using-grpc-method-limiter is not used (default), then rejected requests
    // are already counted as failures.
    show_rejected_requests_on_writes_dashboard: false,
  },
}
