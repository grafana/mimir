local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-writes.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {
  [filename]:
    ($.dashboard('Writes') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addRowIf(
      $._config.show_dashboard_descriptions.writes,
      ($.row('Writes dashboard description') { height: '125px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows various health metrics for the write path.
            It is broken into sections for each service on the write path,
            and organized by the order in which the write request flows.
            <br/>
            Incoming metrics data travels from the gateway → distributor → ingester.
            <br/>
            For each service, there are 3 panels showing
            (1) requests per second to that service,
            (2) average, median, and p99 latency of requests to that service, and
            (3) p99 latency of requests to each instance of that service.
          </p>
          <p>
            It also includes metrics for the key-value (KV) stores used to manage
            the high-availability tracker and the ingesters.
          </p>
        |||),
      )
    ).addRow(
      ($.row('Headlines') +
       {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Samples / sec') +
        $.statPanel(
          $.queries.distributor.samplesPerSecond,
          format='short'
        )
      )
      .addPanel(
        local title = 'Exemplars / sec';
        $.panel(title) +
        $.statPanel(
          $.queries.distributor.exemplarsPerSecond,
          format='short'
        ) +
        $.panelDescription(
          title,
          |||
            The total number of received exemplars by the distributors, excluding rejected and deduped exemplars, but not necessarily ingested by the ingesters.
          |||
        )
      )
      .addPanel(
        local title = 'In-memory series';
        $.panel(title) +
        $.statPanel(|||
          sum(cortex_ingester_memory_series{%(ingester)s}
          / on(%(group_by_cluster)s) group_left
          max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s}))
        ||| % ($._config) {
          ingester: $.jobMatcher($._config.job_names.ingester),
          distributor: $.jobMatcher($._config.job_names.distributor),
        }, format='short') +
        $.panelDescription(
          title,
          |||
            The number of series not yet flushed to object storage that are held in ingester memory.
          |||
        ),
      )
      .addPanel(
        local title = 'Exemplars in ingesters';
        $.panel(title) +
        $.statPanel(|||
          sum(cortex_ingester_tsdb_exemplar_exemplars_in_storage{%(ingester)s}
          / on(%(group_by_cluster)s) group_left
          max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s}))
        ||| % ($._config) {
          ingester: $.jobMatcher($._config.job_names.ingester),
          distributor: $.jobMatcher($._config.job_names.distributor),
        }, format='short') +
        $.panelDescription(
          title,
          |||
            Number of TSDB exemplars currently in ingesters' storage.
          |||
        ),
      )
      .addPanel(
        $.panel('Tenants') +
        $.statPanel('count(count by(user) (cortex_ingester_active_series{%s}))' % $.jobMatcher($._config.job_names.ingester), format='short')
      )
      .addPanelIf(
        $._config.gateway_enabled,
        $.panel('Requests / sec') +
        $.statPanel('sum(rate(cortex_request_duration_seconds_count{%s, route=~"%s"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.gateway), $.queries.write_http_routes_regex], format='reqps')
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel($.queries.gateway.writeRequestsPerSecond)
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.write_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.gateway), $.queries.write_http_routes_regex], ''
        )
      )
    )
    .addRow(
      $.row('Distributor')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel($.queries.distributor.writeRequestsPerSecond)
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.distributor) + [utils.selector.re('route', '/distributor.Distributor/Push|/httpgrpc.*|%s' % $.queries.write_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/distributor.Distributor/Push|/httpgrpc.*|%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.distributor), $.queries.write_http_routes_regex], ''
        )
      )
    )
    .addRowIf(
      $._config.forwarding_enabled,
      $.row('Distributor Forwarding')
      .addPanel(
        $.panel('Requests / sec') +
        $.queryPanel(
          [
            |||
              (sum(rate(cortex_distributor_forward_requests_total{%(distributorMatcher)s}[$__rate_interval])))
              -
              (
                sum(rate(cortex_distributor_forward_errors_total{%(distributorMatcher)s}[$__rate_interval]))
                or vector(0)
              )
            ||| % {
              distributorMatcher: $.jobMatcher($._config.job_names.distributor),
            },
            |||
              label_replace(
                sum by (status_code) (
                  rate(
                    cortex_distributor_forward_errors_total{%(distributorMatcher)s}[$__rate_interval]
                  )
                ),
                "status_code",
                "error",
                "status_code",
                "failed"
              )
            ||| % {
              distributorMatcher: $.jobMatcher($._config.job_names.distributor),
            },
          ], [
            'success',
            '{{ status_code }}',
          ],
        ) + $.stack + {
          aliasColors: {
            '1xx': '#EAB839',
            '2xx': '#7EB26D',
            '3xx': '#6ED0E0',
            '4xx': '#EF843C',
            '5xx': '#E24D42',
            success: '#7EB26D',
            'error': '#E24D42',
          },
        },
      )
      .addPanel(
        $.panel('Latency') +
        $.latencyPanel('cortex_distributor_forward_requests_latency_seconds', '{%s}' % $.jobMatcher($._config.job_names.distributor))
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_distributor_forward_requests_latency_seconds_bucket{%s}[$__rate_interval])))' % [
            $._config.per_instance_label,
            $.jobMatcher($._config.job_names.distributor),
          ],
          '{{ %s }}' % $._config.per_instance_label
        )
      )
    )
    .addRowIf(
      $._config.autoscaling.distributor.enabled,
      $.row('Distributor - autoscaling')
      .addPanel(
        local title = 'Replicas';
        $.panel(title) +
        $.queryPanel(
          [
            'kube_horizontalpodautoscaler_spec_min_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.distributor.hpa_name],
            'kube_horizontalpodautoscaler_spec_max_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.distributor.hpa_name],
            'kube_horizontalpodautoscaler_status_current_replicas{%s, horizontalpodautoscaler="%s"}' % [$.namespaceMatcher(), $._config.autoscaling.distributor.hpa_name],
          ],
          [
            'Min',
            'Max',
            'Current',
          ],
        ) +
        $.panelDescription(
          title,
          |||
            The minimum, maximum, and current number of distributor replicas.
          |||
        ),
      )
      .addPanel(
        local title = 'Scaling metric (CPU)';
        $.panel(title) +
        $.queryPanel(
          [
            $.filterKedaMetricByHPA('(keda_metrics_adapter_scaler_metrics_value{metric=~".*cpu.*"} / 1000)', $._config.autoscaling.distributor.hpa_name),
            'kube_horizontalpodautoscaler_spec_target_metric{%s, horizontalpodautoscaler="%s", metric_name=~".*cpu.*"} / 1000' % [$.namespaceMatcher(), $._config.autoscaling.distributor.hpa_name],
          ], [
            'Scaling metric',
            'Target per replica',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            This panel shows the result of the query used as scaling metric and target/threshold used.
            The desired number of replicas is computed by HPA as: <scaling metric> / <target per replica>.
          |||
        ) +
        $.panelAxisPlacement('Target per replica', 'right'),
      )
      .addPanel(
        local title = 'Scaling metric (Memory)';
        $.panel(title) +
        $.queryPanel(
          [
            $.filterKedaMetricByHPA('keda_metrics_adapter_scaler_metrics_value{metric=~".*memory.*"}', $._config.autoscaling.distributor.hpa_name),
            'kube_horizontalpodautoscaler_spec_target_metric{%s, horizontalpodautoscaler="%s", metric_name=~".*memory.*"}' % [$.namespaceMatcher(), $._config.autoscaling.distributor.hpa_name],
          ], [
            'Scaling metric',
            'Target per replica',
          ]
        ) +
        $.panelDescription(
          title,
          |||
            This panel shows the result of the query used as scaling metric and target/threshold used.
            The desired number of replicas is computed by HPA as: <scaling metric> / <target per replica>.
          |||
        ) +
        $.panelAxisPlacement('Target per replica', 'right') +
        {
          yaxes: std.mapWithIndex(
            // Set the "bytes" unit to the right axis.
            function(idx, axis) axis + (if idx == 1 then { format: 'bytes' } else {}),
            $.yaxes('bytes')
          ),
        },
      )
      .addPanel(
        local title = 'Autoscaler failures rate';
        $.panel(title) +
        $.queryPanel(
          $.filterKedaMetricByHPA('sum by(metric) (rate(keda_metrics_adapter_scaler_errors[$__rate_interval]))', $._config.autoscaling.distributor.hpa_name),
          '{{metric}} failures'
        ) +
        $.panelDescription(
          title,
          |||
            The rate of failures in the KEDA custom metrics API server. Whenever an error occurs, the KEDA custom
            metrics server is unable to query the scaling metric from Prometheus so the autoscaler woudln't work properly.
          |||
        ),
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.panel('Requests / sec') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route="/cortex.Ingester/Push"}' % $.jobMatcher($._config.job_names.ingester))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route="/cortex.Ingester/Push"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], ''
        )
      )
    )
    .addRow(
      $.kvStoreRow('Distributor - key-value store for high-availability (HA) deduplication', 'distributor', 'distributor-hatracker')
    )
    .addRow(
      $.kvStoreRow('Distributor - key-value store for distributors ring', 'distributor', 'distributor-(lifecycler|ring)')
    )
    .addRow(
      $.kvStoreRow('Ingester - key-value store for the ingesters ring', 'ingester', 'ingester-.*')
    )
    .addRow(
      $.row('Ingester - shipper')
      .addPanel(
        $.panel('Uploaded blocks / sec') +
        $.successFailurePanel(
          'sum(rate(cortex_ingester_shipper_uploads_total{%s}[$__rate_interval])) - sum(rate(cortex_ingester_shipper_upload_failures_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_shipper_upload_failures_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ) +
        $.panelDescription(
          'Uploaded blocks / sec',
          |||
            The rate of blocks being uploaded from the ingesters
            to object storage.
          |||
        ) +
        $.stack,
      )
      .addPanel(
        $.panel('Upload latency') +
        $.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', '{%s,component="ingester",operation="upload"}' % $.jobMatcher($._config.job_names.ingester)) +
        $.panelDescription(
          'Upload latency',
          |||
            The average, median (50th percentile), and 99th percentile time
            the ingesters take to upload blocks to object storage.
          |||
        ),
      )
    )
    .addRow(
      $.row('Ingester - TSDB head')
      .addPanel(
        $.panel('Compactions / sec') +
        $.successFailurePanel(
          'sum(rate(cortex_ingester_tsdb_compactions_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_compactions_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ) +
        $.panelDescription(
          'Compactions per second',
          |||
            Ingesters maintain a local TSDB per-tenant on disk. Each TSDB maintains a head block for each
            active time series; these blocks get periodically compacted (by default, every 2h).
            This panel shows the rate of compaction operations across all TSDBs on all ingesters.
          |||
        ) +
        $.stack,
      )
      .addPanel(
        $.panel('Compactions latency') +
        $.latencyPanel('cortex_ingester_tsdb_compaction_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.ingester)) +
        $.panelDescription(
          'Compaction latency',
          |||
            The average, median (50th percentile), and 99th percentile time ingesters take to compact TSDB head blocks
            on the local filesystem.
          |||
        ),
      )
    )
    .addRow(
      $.row('Ingester - TSDB write ahead log (WAL)')
      .addPanel(
        $.panel('WAL truncations / sec') +
        $.successFailurePanel(
          'sum(rate(cortex_ingester_tsdb_wal_truncations_total{%s}[$__rate_interval])) - sum(rate(cortex_ingester_tsdb_wal_truncations_failed_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_wal_truncations_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ) +
        $.panelDescription(
          'WAL truncations per second',
          |||
            The WAL is truncated each time a new TSDB block is written. This panel measures the rate of
            truncations.
          |||
        ) +
        $.stack,
      )
      .addPanel(
        $.panel('Checkpoints created / sec') +
        $.successFailurePanel(
          'sum(rate(cortex_ingester_tsdb_checkpoint_creations_total{%s}[$__rate_interval])) - sum(rate(cortex_ingester_tsdb_checkpoint_creations_failed_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingester_tsdb_checkpoint_creations_failed_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ) +
        $.panelDescription(
          'Checkpoints created per second',
          |||
            Checkpoints are created as part of the WAL truncation process.
            This metric measures the rate of checkpoint creation.
          |||
        ) +
        $.stack,
      )
      .addPanel(
        $.panel('WAL truncations latency (includes checkpointing)') +
        $.queryPanel('sum(rate(cortex_ingester_tsdb_wal_truncate_duration_seconds_sum{%s}[$__rate_interval])) / sum(rate(cortex_ingester_tsdb_wal_truncate_duration_seconds_count{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)], 'avg') +
        { yaxes: $.yaxes('s') } +
        $.panelDescription(
          'WAL truncations latency (including checkpointing)',
          |||
            Average time taken to perform a full WAL truncation,
            including the time taken for the checkpointing to complete.
          |||
        ),
      )
      .addPanel(
        $.panel('Corruptions / sec') +
        $.queryPanel([
          'sum(rate(cortex_ingester_tsdb_wal_corruptions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
          'sum(rate(cortex_ingester_tsdb_mmap_chunk_corruptions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ], [
          'WAL',
          'mmap-ed chunks',
        ]) +
        $.stack + {
          yaxes: $.yaxes('ops'),
          aliasColors: {
            WAL: '#E24D42',
            'mmap-ed chunks': '#E28A42',
          },
        },
      )
    )
    .addRow(
      $.row('Exemplars')
      .addPanel(
        local title = 'Distributor exemplars incoming rate';
        $.panel(title) +
        $.queryPanel(
          'sum(%(group_prefix_jobs)s:cortex_distributor_exemplars_in:rate5m{%(job)s})'
          % { job: $.jobMatcher($._config.job_names.distributor), group_prefix_jobs: $._config.group_prefix_jobs },
          'incoming exemplars',
        ) +
        { yaxes: $.yaxes('ex/s') } +
        $.panelDescription(
          title,
          |||
            The rate of exemplars that have come in to the distributor, including rejected or deduped exemplars.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor exemplars received rate';
        $.panel(title) +
        $.queryPanel(
          'sum(%(group_prefix_jobs)s:cortex_distributor_received_exemplars:rate5m{%(job)s})'
          % { job: $.jobMatcher($._config.job_names.distributor), group_prefix_jobs: $._config.group_prefix_jobs },
          'received exemplars',
        ) +
        { yaxes: $.yaxes('ex/s') } +
        $.panelDescription(
          title,
          |||
            The rate of received exemplars, excluding rejected and deduped exemplars.
            This number can be sensibly lower than incoming rate because we dedupe the HA sent exemplars, and then reject based on time, see `cortex_discarded_exemplars_total` for specific reasons rates.
          |||
        ),
      )
      .addPanel(
        local title = 'Ingester ingested exemplars rate';
        $.panel(title) +
        $.queryPanel(
          |||
            sum(
              %(group_prefix_jobs)s:cortex_ingester_ingested_exemplars:rate5m{%(ingester)s}
              / on(%(group_by_cluster)s) group_left
              max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
            )
          ||| % {
            ingester: $.jobMatcher($._config.job_names.ingester),
            distributor: $.jobMatcher($._config.job_names.distributor),
            group_by_cluster: $._config.group_by_cluster,
            group_prefix_jobs: $._config.group_prefix_jobs,
          },
          'ingested exemplars',
        ) +
        { yaxes: $.yaxes('ex/s') } +
        $.panelDescription(
          title,
          |||
            The rate of exemplars ingested in the ingesters.
            Every exemplar is sent to the replication factor number of ingesters, so the sum of rates from all ingesters is divided by the replication factor.
            This ingested exemplars rate should match the distributor's received exemplars rate.
          |||
        ),
      )
      .addPanel(
        local title = 'Ingester appended exemplars rate';
        $.panel(title) +
        $.queryPanel(
          |||
            sum(
              %(group_prefix_jobs)s:cortex_ingester_tsdb_exemplar_exemplars_appended:rate5m{%(ingester)s}
              / on(%(group_by_cluster)s) group_left
              max by (%(group_by_cluster)s) (cortex_distributor_replication_factor{%(distributor)s})
            )
          ||| % {
            ingester: $.jobMatcher($._config.job_names.ingester),
            distributor: $.jobMatcher($._config.job_names.distributor),
            group_by_cluster: $._config.group_by_cluster,
            group_prefix_jobs: $._config.group_prefix_jobs,
          },
          'appended exemplars',
        ) +
        { yaxes: $.yaxes('ex/s') } +
        $.panelDescription(
          title,
          |||
            The rate of exemplars appended in the ingesters.
            This can be lower than ingested exemplars rate since TSDB does not append the same exemplar twice, and those can be frequent.
          |||
        ),
      )
    ),
}
