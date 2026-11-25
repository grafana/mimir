// The grafana-databases-lib instance shared by Mimir's dashboards, configured from $._config.
// It is merged into dashboard-utils.libsonnet, so every dashboard can use $._databases. Each area
// of the library (autoscaling, ...) adds its own config mixin below.
local dblib = import 'grafana-databases-lib/main.libsonnet';

{
  _databases::
    // KEDA scalers of the components scaled on CPU and memory. They get one desired replicas
    // panel per scaler instead of a single one, both in their rows and as named panels.
    // $._config.autoscaling.<component>.scalers adds to (or overrides) these, e.g. for scalers of
    // components defined by mixins built on this one.
    local cpuAndMemory = { cpu: { title: 'CPU' }, memory: { title: 'memory' } };
    local scalers = {
      gateway: cpuAndMemory,
      query_frontend: cpuAndMemory,
      ruler_query_frontend: cpuAndMemory,
      distributor: cpuAndMemory,
      ruler: cpuAndMemory,
      ruler_querier: cpuAndMemory { queries: { title: 'in-flight queries' } },
    };

    // Series added to a component's replicas panel. The ingester's also show the spec and status
    // replicas of the ReplicaTemplate its HPA scales.
    local replicaTemplateQuery(field) =
      'max(kube_customresource_replicatemplate_%(field)s_replicas{%(namespace_matcher)s, name=~"%(replica_template_name)s"})' % {
        field: field,
        namespace_matcher: $.namespaceMatcher(),
        replica_template_name: $._config.autoscaling.ingester.replica_template_name,
      };
    local extraReplicaQueries = {
      ingester: [
        { expr: replicaTemplateQuery('spec'), legend: 'Template spec replicas' },
        { expr: replicaTemplateQuery('status'), legend: 'Template status replicas' },
      ],
    };

    dblib.new()
    + dblib.withConfigMixin({
      selector: $.namespaceMatcher(),
      clusterLabel: $._config.per_cluster_label,
      namespaceLabel: $._config.per_namespace_label,
      groupBy: $._config.cluster_labels,
    })
    // Autoscaling panels and rows, one set per component defined in $._config.autoscaling.
    + dblib.autoscaling.withConfigMixin({
      hpaPrefix: $._config.autoscaling_hpa_prefix,
      // Mimir's ScaledObjects name their triggers (e.g. "..._cpu_hpa_..."), which reads better
      // than the trigger metric (s0-prometheus, ...).
      triggerLegend: '{{ scaler }}',
      // Mimir's dashboards start time series at 0 (see timeseriesPanel in dashboard-utils.libsonnet).
      axisMin: 0,
      components: {
        [name]: {
          hpaName: $._config.autoscaling[name].hpa_name,
          scalers: std.get(scalers, name, {}) + std.get($._config.autoscaling[name], 'scalers', {}),
          extraReplicaQueries: std.get(extraReplicaQueries, name, []),
        }
        for name in std.objectFields($._config.autoscaling)
      },
    }),
}
