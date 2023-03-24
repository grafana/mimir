local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'runbook-mimir-ingester-reaching-series-limit.json';

(import 'dashboard-utils.libsonnet') {
  [filename]:
    ($.dashboard('Runbook / IngesterReachingSeriesLimit') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()


    .addRow(
      $.row('Description')
      .addPanel(
        $.textPanel(
          '',
          |||
            ## %(product)sIngesterReachingSeriesLimit alert
            This alert fires when the `max_series` per ingester instance limit is enabled and the actual number of in-memory series in an ingester is reaching the limit.
            Once the limit is reached, writes to the ingester will fail (5xx) for new series, while appending samples to existing ones will continue to succeed.

            ## In case of emergency

            - If the actual number of series is very close to or already hit the limit, then you can increase the limit via runtime config to gain some time.
            - Increasing the limit will increase the ingesters' memory utilization. Please monitor the ingesters' memory utilization via the [Mimir / Writes Resources](%(mimir_writes_resources_url)s) dashboard.

            ## How to fix it
            1. **Temporarily increase the limit**
               If the actual number of series is very close to or already hit the limit, or if you foresee the ingester will hit the limit before dropping the stale series as an effect of the scale up, you should also temporarily increase the limit.
            2. **Check if shuffle-sharding shard size is correct** (see the _Shuffle sharding_ row below).
            3. **Scale up ingesters**
               Scaling up ingesters will lower the number of series per ingester. However, the effect of this change will take up to 4h, because after the scale up we need to wait until all stale series are dropped from memory as the effect of TSDB head compaction, which could take up to 4h (with the default config, TSDB keeps in-memory series up to 3h old and it gets compacted every 2h).
          ||| % $._config {
            mimir_writes_resources_url: $.dashboardURL('mimir-writes-resources.json'),
          },
          { transparent: false },
        )
      )
      + { height: '350px' }
    )

    .addRow(
      $.row('Configuration')
      .addPanel(
        $.textPanel(
          'How the limit is configured',
          |||
            - The limit can be configured either on CLI (`-ingester.instance-limits.max-series`) or in the runtime config:
              ```
              ingester_limits:
                max_series: <int>
              ```
            - The jsonnet mixin configures the limit in the runtime config and can be fine-tuned via:
              ```
              _config+:: {
                ingester_instance_limits+:: {
                  max_series: <int>
                }
              }
              ```
            - When configured in the runtime config, changes are applied live without requiring an ingester restart
            - The current applied limit can be seen on the right.
          |||,
          { transparent: false },
        )
      )
      .addPanel(
        $.timeseriesPanel('Current configured max_series limit for $namespace') + { fieldConfig: { defaults: { unit: 'short' } } }
        + $.queryPanel(
          [
            'max(cortex_ingester_instance_limits{limit="max_series", %(namespace_matcher)s})' % {
              namespace_matcher: $.namespaceMatcher(),
            },
          ],
          ['max series']
        ),
      )
      + { height: '350px' }
    )

    .addRow(
      $.row('Shuffle sharding')
      .addPanel(
        $.textPanel(
          'Description',
          |||
            - When shuffle-sharding is enabled, we target up to 100K series / tenant / ingester assuming tenants on average use 50% of their max series limit.
            - The **table on the right** shows the tenants that might cause higher pressure on ingesters. The query excludes tenants which are already sharded across all ingesters.
            - Check the current shard size of each tenant in the output and, if they're not already sharded across all ingesters, you may consider to double their shard size.
            - Be warned that the when increasing the shard size for a tenant, the number of in-memory series will temporarily increase. Make sure to monitor:
              - The per-ingester number of series, to make sure that any are not close to reaching the limit. You might need to temporarily raise the ingester `max_series`.
              - The per-tenant number of series. Due to reshuffling, series will be counted multiple times (in the new and old ingesters), and therefore a tenant may risk having samples rejected because they hit the `per_user` series limit. You might need to temporarily raise the limit.
            - The in-memory series in the ingesters will be effectively reduced at the TSDB head compaction happening at least 1h after you increased the shard size for the affected tenants
          |||,
          { transparent: false },
        )
      )
      .addPanel(
        $.tablePanel(
          [
            |||
              topk by (pod) (5, # top 5 tenants per ingester
                  sum by (user, pod) ( # get in-memory series for each tenant on each pod
                      cortex_ingester_memory_series_created_total{%(namespace_matcher)s} - cortex_ingester_memory_series_removed_total{%(namespace_matcher)s}
                  )
                  and on(user) # intersection with tenants that are exceeding 50%% of their series limit (added acorss ingesters & accounting for replication)
                  (
                      sum by (user) ( # total in-memory series for the tenant across ingesters
                          cortex_ingester_memory_series_created_total{%(namespace_matcher)s} - cortex_ingester_memory_series_removed_total{%(namespace_matcher)s}
                      )
                      > 200000 # show only big tenants - with more than 200k series across ingesters
                      >
                      (
                          max by(user) (cortex_limits_overrides{%(namespace_matcher)s, limit_name="max_global_series_per_user"}) # global limit
                          *
                          scalar(max(cortex_distributor_replication_factor{%(namespace_matcher)s})) # with replication
                          *
                          0.5 # 50%%
                      )
                  )
                  and on (pod) ( # intersection with top 3 ingesters by in-memory series
                      topk(3,
                          sum by (pod) (cortex_ingester_memory_series{%(namespace_matcher)s})
                      )
                  )
                  and on(user) ( # intersection with the tenants which don't have series on all ingesters
                      count by (user) (cortex_ingester_memory_series_created_total{%(namespace_matcher)s}) # count ingesters where each tenant has series
                      !=
                      scalar(count(count by (pod) (cortex_ingester_memory_series{%(namespace_matcher)s}))) # count total ingesters: first `count` counts series by ingester (we ignore the counted number), second `count` counts rows in series per ingester, second count gives the number of ingesters
                  )
              )
            ||| % {
              namespace_matcher: $.namespaceMatcher(),
            },
          ],
          { 'Value #A': { alias: 'series' } }
        ),
      )
      + { height: '500px' }
    )

    { version: 16 },
}
