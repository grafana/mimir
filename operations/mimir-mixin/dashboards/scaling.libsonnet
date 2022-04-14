local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'mimir-scaling.json':
    ($.dashboard('Scaling') + { uid: '88c041017b96856c9176e07cf557bdcf' })
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Service scaling') + { height: '200px' })
      .addPanel({
        type: 'text',
        title: '',
        options: {
          content: |||
            This dashboard can help identify scaling-related issues by suggesting services which may benefit from scaling up.
            The table below gives a suggested number of replicas and the reason why.
            If the system is failing for one of the below reasons, try scaling up to the specified number.
            These are not meant as a prescriptive number, but rather as a helpful guideline when things go wrong.

            Reasons:
            - **sample_rate**: There are not enough replicas to handle the
              sample rate.  Applies to distributor and ingesters.
            - **active_series**: There are not enough replicas
              to handle the number of active series.  Applies to ingesters.
            - **cpu_usage**: There are not enough replicas
              based on the CPU usage of the jobs vs the resource requests.
              Applies to all jobs.
            - **memory_usage**: There are not enough replicas based on the memory
              usage vs the resource requests.  Applies to all jobs.
            - **active_series_limits**: There are not enough replicas to hold 60% of the
              sum of all the per tenant series limits.
            - **sample_rate_limits**: There are not enough replicas to handle 60% of the
              sum of all the per tenant rate limits.
          |||,
          mode: 'markdown',
        },
      })
    )
    .addRow(
      ($.row('Scaling') + { height: '400px' })
      .addPanel(
        $.panel('Workload-based scaling') + { sort: { col: 0, desc: false } } +
        $.tablePanel([
          |||
            sort_desc(
              %s_deployment_reason:required_replicas:count{%s}
                > ignoring(reason) group_left
              %s_deployment:actual_replicas:count{%s}
            )
          ||| % [$._config.alert_aggregation_rule_prefix, $.namespaceMatcher(), $._config.alert_aggregation_rule_prefix, $.namespaceMatcher()],
        ], {
          __name__: { alias: 'Cluster', type: 'hidden' },
          cluster: { alias: 'Cluster' },
          namespace: { alias: 'Namespace' },
          deployment: { alias: 'Service' },
          reason: { alias: 'Reason' },
          Value: { alias: 'Required Replicas', decimals: 0 },
        })
      )
    ),
}
