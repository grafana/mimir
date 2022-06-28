{
  grafanaDashboardFolder: 'Mimir',
  grafanaDashboardShards: 4,

  _config+:: {
    // The product name used when building dashboards.
    product: 'Mimir',

    // The prefix including product name used when building dashboards.
    dashboard_prefix: '%(product)s / ' % $._config.product,

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
      ingester: '(ingester.*|cortex|mimir)',  // Match also custom and per-zone ingester deployments.
      distributor: '(distributor|cortex|mimir)',
      querier: '(querier.*|cortex|mimir)',  // Match also custom querier deployments.
      ruler_querier: '(ruler-querier.*)',  // Match also custom querier deployments.
      ruler: '(ruler|cortex|mimir)',
      query_frontend: '(query-frontend.*|cortex|mimir)',  // Match also custom query-frontend deployments.
      ruler_query_frontend: '(ruler-query-frontend.*)',  // Match also custom ruler-query-frontend deployments.
      query_scheduler: 'query-scheduler.*',  // Not part of single-binary. Match also custom query-scheduler deployments.
      ruler_query_scheduler: 'ruler-query-scheduler.*',  // Not part of single-binary. Match also custom query-scheduler deployments.
      ring_members: ['alertmanager', 'compactor', 'distributor', 'ingester.*', 'querier.*', 'ruler', 'ruler-querier.*', 'store-gateway.*', 'cortex', 'mimir'],
      store_gateway: '(store-gateway.*|cortex|mimir)',  // Match also per-zone store-gateway deployments.
      gateway: '(gateway|cortex-gw|cortex-gw-internal)',
      compactor: 'compactor.*|cortex|mimir',  // Match also custom compactor deployments.
      alertmanager: 'alertmanager|cortex|mimir',
      overrides_exporter: 'overrides-exporter',
    },

    // The label used to differentiate between different Kubernetes clusters.
    per_cluster_label: 'cluster',

    // Grouping labels, to uniquely identify and group by {jobs, clusters}
    job_labels: [$._config.per_cluster_label, 'namespace', 'job'],
    cluster_labels: [$._config.per_cluster_label, 'namespace'],

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
