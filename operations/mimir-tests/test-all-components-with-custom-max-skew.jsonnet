// Based on test-all-components.jsonnet.
(import 'test-all-components.jsonnet') {
  _config+:: {
    // Increase all defaults by 1.
    distributor_topology_spread_max_skew+: 1,
    query_frontend_topology_spread_max_skew+: 1,
    querier_topology_spread_max_skew+: 1,
    ruler_topology_spread_max_skew+: 1,
    ruler_querier_topology_spread_max_skew+: 1,
  },
}
