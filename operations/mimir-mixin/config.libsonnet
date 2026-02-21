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

    // Added default flag for GEM-specific dashboards and alerts.
    gem_enabled: false,

    rollout_operator_alerts_enable: $._config.gem_enabled == false && $._config.deployment_type == 'kubernetes' && $._config.singleBinary == false,
    rollout_operator_dashboard_enable: true,
    rollout_operator_dashboard_title: 'rollout-operator',
    // This is the md5 of the rollout-operator dashboard name.
    // This is set such that if the name / uid was to change an error will be raised in dashboard generation.
    // This ensures that the uid is consistent and can be reliably linked to.
    rollout_operator_dashboard_uid: 'f40e8042a6be71a98444a29b2c4e9421',
    rollout_operator_container_name: 'rollout-operator',
    rollout_operator_links: [
      {
        asDropdown: true,
        icon: 'external link',
        includeVars: true,
        keepTime: true,
        tags: $._config.tags,
        targetBlank: false,
        title: '%(product)s dashboards' % $._config,
        type: 'dashboards',
      },
    ],
    rollout_operator_instance_matcher:
      if std.get($._config, 'helm', '') == '' then $._config.rollout_operator_container_name + '.*' else '(.*%g-)?%g.*' % [$._config.helm, $._config.rollout_operator_container_name],
    rollout_operator_resources_panel_queries: self.resources_panel_queries.kubernetes,

    // This is mapping between a Mimir component name and the regular expression that should be used
    // to match its instance and container name. Mimir jsonnet and Helm guarantee that the instance name
    // (e.g. Kubernetes Deployment) and container name always match, so it's safe to use a shared mapping.
    //
    // This mapping is intentionally local and can't be overridden. If the final user needs to customize
    // dashboards and alerts, they should override the final matcher regexp (e.g. container_names or instance_names).
    local componentNameRegexp = {
      compactor: 'compactor',
      alertmanager: 'alertmanager',
      alertmanager_im: 'alertmanager-im',
      ingester: 'ingester',
      block_builder: 'block-builder',
      block_builder_scheduler: 'block-builder-scheduler',
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
      usage_tracker: 'usage-tracker',
      federation_frontend: 'federation-frontend',
    },

    // Some dashboards show panels grouping together multiple components of a given "path".
    // This mapping configures which components belong to each group. A component can belong
    // to multiple groups.
    local componentGroups = {
      write: ['distributor', 'ingester'],
      read: ['query_frontend', 'querier', 'ruler_query_frontend', 'ruler_querier'],
      backend: ['query_scheduler', 'ruler_query_scheduler', 'ruler', 'store_gateway', 'compactor', 'alertmanager', 'overrides_exporter'],
      remote_ruler_read: ['ruler_query_frontend', 'ruler_query_scheduler', 'ruler_querier'],
    },

    // These are used by the dashboards and allow for the simultaneous display of
    // microservice and single binary Mimir clusters.
    // Whenever you do any change here, please reflect it in the doc at:
    // docs/sources/mimir/manage/monitoring-grafana-mimir/requirements.md
    job_names: {
      ingester: ['ingester.*', 'cortex', 'mimir'],  // Match also custom and per-zone ingester deployments.
      ingester_partition: ['ingester.*-partition'],  // Match exclusively temporarily partition ingesters run during the migration to ingest storage.
      block_builder: ['block-builder.*'],
      block_builder_scheduler: ['block-builder-scheduler.*'],
      distributor: ['distributor.*', 'cortex', 'mimir'],  // Match also per-zone distributor deployments.
      querier: ['querier.*', 'cortex', 'mimir'],  // Match also custom and per-zone querier deployments.
      ruler_querier: ['ruler-querier.*'],  // Match also custom and per-zone ruler-querier deployments.
      ruler: ['ruler|ruler-zone-.*', 'cortex', 'mimir'],  // Match also per-zone ruler deployments.
      query_frontend: ['query-frontend.*', 'cortex', 'mimir'],  // Match also custom and per-zone query-frontend deployments.
      ruler_query_frontend: ['ruler-query-frontend.*'],  // Match also custom and per-zone ruler-query-frontend deployments.
      query_scheduler: ['query-scheduler.*'],  // Not part of single-binary. Match also custom and per-zone query-scheduler deployments.
      ruler_query_scheduler: ['ruler-query-scheduler.*'],  // Not part of single-binary. Match also custom and per-zone ruler-query-scheduler deployments.
      ring_members: ['admin-api', 'alertmanager', 'compactor.*', 'distributor.*', 'ingester.*', 'query-frontend.*', 'querier.*', 'ruler|ruler-zone-.*', 'ruler-querier.*', 'store-gateway.*', 'cortex', 'mimir'],
      store_gateway: ['store-gateway.*', 'cortex', 'mimir'],  // Match also per-zone store-gateway deployments.
      gateway: ['gateway', 'cortex-gw.*'],  // Match also custom and per-zone gateway deployments.
      compactor: ['compactor.*', 'cortex', 'mimir'],  // Match also custom compactor deployments.
      alertmanager: ['alertmanager', 'cortex', 'mimir'],
      overrides_exporter: ['overrides-exporter'],
      usage_tracker: ['usage-tracker.*'],

      // The following are job matchers used to select all components in the read path.
      main_read_path: std.uniq(std.sort(self.query_frontend + self.query_scheduler + self.querier)),
      remote_ruler_read_path: std.uniq(std.sort(self.ruler_query_frontend + self.ruler_query_scheduler + self.ruler_querier)),

      // The following are job matchers used to select all components in a given "path".
      write: ['distributor.*', 'ingester.*'],
      read: ['query-frontend.*', 'querier.*', 'ruler-query-frontend.*', 'ruler-querier.*'],
      backend: ['ruler|ruler-zone-.*', 'query-scheduler.*', 'ruler-query-scheduler.*', 'store-gateway.*', 'compactor.*', 'alertmanager', 'overrides-exporter'],

      federation_frontend: ['federation-frontend.*'],  // Match federation-frontend deployments
    },

    // Name selectors for different application instances, using the "per_instance_label".
    instance_names: {
      // Wrap the regexp into an Helm compatible matcher if the deployment type is "kubernetes".
      local helmCompatibleMatcher = function(regexp) if $._config.deployment_type == 'kubernetes' then '(.*mimir-)?%s' % regexp else regexp,
      // Wrap the regexp to match any prefix if the deployment type is "baremetal".
      local baremetalCompatibleMatcher = function(regexp) if $._config.deployment_type == 'baremetal' then '.*%s' % regexp else regexp,
      local instanceMatcher = function(regexp) baremetalCompatibleMatcher(helmCompatibleMatcher('%s.*' % regexp)),

      // The following matchers MUST match only the individual instances.
      compactor: instanceMatcher(componentNameRegexp.compactor),
      block_builder: instanceMatcher(componentNameRegexp.block_builder),
      block_builder_scheduler: instanceMatcher(componentNameRegexp.block_builder_scheduler),
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
      usage_tracker: instanceMatcher(componentNameRegexp.usage_tracker),

      // The following are instance matchers used to select all components in a given "path".
      local componentsGroupMatcher = function(components)
        instanceMatcher('(%s)' % std.join('|', std.map(function(name) componentNameRegexp[name], components))),

      write: componentsGroupMatcher(componentGroups.write),
      read: componentsGroupMatcher(componentGroups.read),
      backend: componentsGroupMatcher(componentGroups.backend),
      remote_ruler_read: componentsGroupMatcher(componentGroups.remote_ruler_read),

      federation_frontend: instanceMatcher(componentNameRegexp.federation_frontend),
    },
    all_instances: std.join('|', std.map(function(name) componentNameRegexp[name], componentGroups.write + componentGroups.read + componentGroups.backend)),

    container_names: {
      // The following matchers MUST match only the individual instances.
      block_builder: componentNameRegexp.block_builder,
      block_builder_scheduler: componentNameRegexp.block_builder_scheduler,
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
      usage_tracker: componentNameRegexp.usage_tracker,

      // The following are container matchers used to select all components in a given "path".
      local componentsGroupMatcher = function(components) std.join('|', std.map(function(name) componentNameRegexp[name], components)),

      write: componentsGroupMatcher(componentGroups.write),
      read: componentsGroupMatcher(componentGroups.read),
      backend: componentsGroupMatcher(componentGroups.backend),

      federation_frontend: componentNameRegexp.federation_frontend,
    },

    // The label used to differentiate between different Kubernetes clusters.
    per_cluster_label: 'cluster',
    per_namespace_label: 'namespace',
    per_job_label: 'job',
    per_query_path_label: 'container',  // Used to differentiate between queriers, query-frontends and query-schedulers in different query paths (eg. queriers in ordinary query path and queriers in dedicated ruler path).
    per_component_loki_label: 'name',

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

    // Controls whether dashboards show classic or native latency histograms. Allowed values: 'classic' (default), 'native'.
    dashboards_default_latency_mode: 'classic',

    // Used to add extra labels to all alerts. Careful: takes precedence over default labels.
    alert_extra_labels: {},

    // Used to add extra annotations to all alerts, Careful: takes precedence over default annotations.
    alert_extra_annotations: {},

    // Used as the job prefix in alerts that select on job label (e.g. GossipMembersTooHigh, RingMembersMismatch). This can be set to a known namespace to prevent those alerts from firing incorrectly due to selecting similar metrics from Loki/Tempo.
    alert_job_prefix: '.*/',

    alert_cluster_variable: '{{ $labels.%s }}' % $._config.per_cluster_label,

    alert_instance_variable: '{{ $labels.%s }}' % $._config.per_instance_label,

    alert_node_variable: '{{ $labels.%s }}' % $._config.per_cluster_label,

    // The alertname is used to create a hyperlink to the runbooks. Currenlty we only have a single set of runbooks, so different products (e.g. GEM) should still use the same runbooks.
    alert_product: $._config.product,

    // The Deployment or StatefulSet names (eg. 'querier' or 'ingester-zone-a') to exclude from the MimirRolloutStuck alert.
    rollout_stuck_alert_ignore_deployments: [],
    rollout_stuck_alert_ignore_statefulsets: [],

    // Whether alerts for experimental ingest storage are enabled.
    ingest_storage_enabled: true,

    // Whether the experimental usage tracker is enabled.
    usage_tracker_enabled: false,

    cortex_p99_latency_threshold_seconds: 2.5,

    // Whether resources dashboards are enabled (based on cAdvisor metrics).
    resources_dashboards_enabled: true,

    // Whether mimir block-builder is enabled (experimental)
    block_builder_enabled: false,

    // Whether mimir gateway is enabled. The gateway is usually enabled in GEM deployments.
    gateway_enabled: $._config.gem_enabled,
    gateway_per_tenant_metrics_enabled: false,

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

    // Threshold for DistributorGcUsesTooMuchCpu
    distributor_gc_cpu_threshold: 10,

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
            container_memory_rss{container=~"(%(ingester)s)"}
              /
            ( container_spec_memory_limit_bytes{container=~"(%(ingester)s)"} > 0 )
          )
          # Match only Mimir namespaces.
          * on(%(alert_aggregation_labels)s) group_left max by(%(alert_aggregation_labels)s) (cortex_build_info)
          > %(threshold)s
        |||,
      },
      baremetal: {
        memory_allocation: |||
          (
            process_resident_memory_bytes{job=~".*/(%(ingester)s)"}
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
            #
            # Notes:
            # - Multi-zone deployments are grouped together removing the "zone-X" suffix.
            # - To avoid "vector cannot contain metrics with the same labelset" errors we need to add an additional
            #   label "deployment_without_zone" first, then run the aggregation, and finally rename "deployment_without_zone"
            #   to "deployment".
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(
                sum by (%(alert_aggregation_labels)s, deployment_without_zone) (
                  label_replace(
                    kube_deployment_spec_replicas,
                    # The question mark in "(.*?)" is used to make it non-greedy, otherwise it
                    # always matches everything and the (optional) zone is not removed.
                    "deployment_without_zone", "$1", "deployment", "(.*?)(?:-zone-[a-z])?"
                  )
                )
                or
                sum by (%(alert_aggregation_labels)s, deployment_without_zone) (
                  label_replace(kube_statefulset_replicas, "deployment_without_zone", "$1", "statefulset", "(.*?)(?:-zone-[a-z])?")
                ),
                "deployment", "$1", "deployment_without_zone", "(.*)"
              )
            )
          |||,
        cpu_usage_seconds_total:
          |||
            sum by (%(alert_aggregation_labels)s, deployment) (
              label_replace(
                label_replace(
                  sum by (%(alert_aggregation_labels)s, %(per_instance_label)s)(rate(container_cpu_usage_seconds_total[%(recording_rules_range_interval)s])),
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
        cpu_limit: 'min(kube_pod_container_resource_limits{%(namespace)s,container=~"%(containerName)s",resource="cpu"})',
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
        memory_oom_killed: |||
          group by (%(instanceLabel)s, reason) (
            kube_pod_container_status_last_terminated_reason{%(namespace)s,container=~"%(containerName)s",reason="OOMKilled"}
            unless
            # Note, this leg offsets by the "$__interval" to find the first occurrence of OOMKilled in the gauge.
            (kube_pod_container_status_last_terminated_reason{%(namespace)s,container=~"%(containerName)s",reason="OOMKilled"} offset $__interval == bool 0)
          ) != 0
        |||,
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
              kubelet_volume_stats_used_bytes{%(namespaceMatcher)s, %(persistentVolumeClaimMatcher)s} /
              kubelet_volume_stats_capacity_bytes{%(namespaceMatcher)s, %(persistentVolumeClaimMatcher)s}
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

    rollout_dashboard: {
      // workload_label_replaces is used to create label_replace(...) calls on the statefulset and deployment series when rendering the Rollout Dashboard.
      // Extendable to allow grouping multiple workloads into a single one.
      workload_label_replaces: [
        { src_label: 'deployment', regex: '(.+)', replacement: '$1' },
        { src_label: 'statefulset', regex: '(.+)', replacement: '$1' },
        { src_label: 'workload', regex: '(.*?)(?:-zone-[a-z])?', replacement: '$1' },
      ],
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
    //
    // IMPORTANT: use non-capture groups in the regular expression so that when they're used as part
    // of other regexps we don't break existing capture groups.
    autoscaling_hpa_prefix: 'keda-hpa-(?:mimir-)?',

    // The configured hpa_name can be a regexp to support multiple deployments (e.g. multi-zone deployments).
    autoscaling: {
      query_frontend: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'query-frontend.*',
      },
      ruler_query_frontend: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'ruler-query-frontend.*',
      },
      querier: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'querier.*',
      },
      ruler_querier: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'ruler-querier.*',
      },
      store_gateway: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'store-gateway-zone-a',
      },
      distributor: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'distributor.*',
      },
      ruler: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + '(?:ruler|ruler-zone-.*)',
      },
      gateway: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'cortex-gw.*',
      },
      ingester: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'ingester-zone-a',
        replica_template_name: 'ingester-zone-a',
      },
      compactor: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'compactor',
      },
      block_builder: {
        enabled: false,
        hpa_name: $._config.autoscaling_hpa_prefix + 'block-builder',
      },
    },


    // The routes to exclude from alerts.
    alert_excluded_routes: [
      'debug_pprof',
    ],

    // All query methods from IngesterServer interface. Basically everything except Push.
    ingester_read_path_routes_regex: '/cortex.Ingester/(QueryStream|QueryExemplars|LabelValues|LabelNames|UserStats|AllUserStats|MetricsForLabelMatchers|MetricsMetadata|LabelNamesAndValues|LabelValuesCardinality|ActiveSeries)',

    // All query methods from StoregatewayServer interface.
    store_gateway_read_path_routes_regex: '/gatewaypb.StoreGateway/.*',

    // The default datasource used for dashboards.
    dashboard_datasource: 'default',
    datasource_regex: '',

    // Tunes histogram recording rules to aggregate over this interval.
    // Set to at least twice the scrape interval; otherwise, recording rules will output no data.
    // Set to four times the scrape interval to account for edge cases: https://www.robustperception.io/what-range-should-i-use-with-rate/
    recording_rules_range_interval: '1m',

    // Used to calculate range interval in alerts with default range selector under 10 minutes.
    // Needed to account for edge cases: https://www.robustperception.io/what-range-should-i-use-with-rate/
    base_alerts_range_interval_minutes: 1,

    // Used to inject rows into dashboards at specific places that support it.
    injectRows: {},

    // Used to add additional services to dashboards that support it.
    extraServiceNames: [],

    // When using early rejection of inflight requests in ingesters and distributors (using -ingester.limit-inflight-requests-using-grpc-method-limiter
    // and -distributor.limit-inflight-requests-using-grpc-method-limiter options), rejected requests will not count towards standard Mimir metrics
    // like cortex_request_duration_seconds_count. Enabling this will make them visible on the dashboard again.
    //
    // Disabled by default, because when -ingester.limit-inflight-requests-using-grpc-method-limiter and -distributor.limit-inflight-requests-using-grpc-method-limiter is
    // not used (default), then rejected requests are already counted as failures.
    show_rejected_requests_on_writes_dashboard: false,

    // When ingester reactive limiters are enabled (using -ingester.push-reactive-limiter.enabled=true and -ingester.read-reactive-limiter.enabled=true), additional
    // metrics related to inflight requests and rejections can be shown on the Writes and Reads dashboards.
    show_reactive_limiter_panels: false,

    // Show panels that use queries for gRPC-based ingestion (distributor -> ingester)
    show_grpc_ingestion_panels: true,

    // Show panels that use queries for "ingest storage" ingestion (distributor -> Kafka, Kafka -> ingesters)
    show_ingest_storage_panels: true,
  },
}
