local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-writes.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {

  [filename]:
    assert std.md5(filename) == '8280707b8f16e7b87b840fc1cc92d4c5' : 'UID of the dashboard has changed, please update references to dashboard.';
    ($.dashboard('Writes') + { uid: std.md5(filename) })
    .addClusterSelectorTemplates()
    .addShowNativeLatencyVariable()
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
        $.statPanel(
          $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_memory_series{%s}' % [$.jobMatcher($._config.job_names.ingester)]),
          format='short'
        ) +
        $.panelDescription(
          title,
          |||
            The number of series not yet flushed to object storage that are held in ingester memory.
            With classic storage we the sum of series from all ingesters is divided by the replication factor.
            With ingest storage we take the maximum series of each ingest partition.
          |||
        ),
      )
      .addPanel(
        local title = 'Exemplars in ingesters';
        $.panel(title) +
        $.statPanel(
          $.queries.ingester.ingestOrClassicDeduplicatedQuery('cortex_ingester_tsdb_exemplar_exemplars_in_storage{%s}' % [$.jobMatcher($._config.job_names.ingester)]),
          format='short'
        ) +
        $.panelDescription(
          title,
          |||
            Number of TSDB exemplars currently in ingesters' storage.
            With classic storage we the sum of exemplars from all ingesters is divided by the replication factor.
            With ingest storage we take the maximum exemplars of each ingest partition.
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
        $.statPanel(utils.ncHistogramSumBy(utils.ncHistogramCountRate($.queries.gateway.requestsPerSecondMetric, $.queries.gateway.writeRequestsPerSecondSelector)), format='reqps')
      )
    )
    .addRowIf(
      $._config.gateway_enabled,
      $.row('Gateway')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanelNativeHistogram($.queries.gateway.requestsPerSecondMetric, $.queries.gateway.writeRequestsPerSecondSelector)
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.gateway.requestsPerSecondMetric, $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.write_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.perInstanceLatencyPanelNativeHistogram('0.99', $.queries.gateway.requestsPerSecondMetric, $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.write_http_routes_regex)])
      )
    )
    .addRow(
      $.row('Distributor')
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.panelDescription(
          'Requests / sec',
          |||
            The rate of successful, failed and rejected requests to distributor.
            Rejected requests are requests that distributor fails to handle because of distributor instance limits.
            When distributor is configured to use "early" request rejection, then rejected requests are NOT included in other metrics.
            When distributor is not configured to use "early" request rejection, then rejected requests are also counted as "errors".
          |||
        ) +
        $.qpsPanelNativeHistogram($.queries.distributor.requestsPerSecondMetric, $.queries.distributor.writeRequestsPerSecondSelector) +
        if $._config.show_rejected_requests_on_writes_dashboard then
          {
            targets: [
              {
                legendLink: null,
                expr: 'sum (rate(cortex_distributor_instance_rejected_requests_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.distributor)],
                format: 'time_series',
                intervalFactor: 2,
                legendFormat: 'rejected',
                refId: 'B',
              },
            ] + super.targets,
          } + $.aliasColors({
            rejected: '#EAB839',
          })
        else {},
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.distributor.requestsPerSecondMetric, $.jobSelector($._config.job_names.distributor) + [utils.selector.re('route', '%s' % $.queries.distributor.writeRequestsPerSecondRouteRegex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.perInstanceLatencyPanelNativeHistogram('0.99', $.queries.distributor.requestsPerSecondMetric, $.jobSelector($._config.job_names.distributor) + [utils.selector.re('route', '%s' % $.queries.distributor.writeRequestsPerSecondRouteRegex)])
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      $.row('Distributor (ingest storage)')
      .addPanel(
        $.ingestStorageKafkaProducedRecordsRatePanel('distributor')
      )
      .addPanel(
        $.ingestStorageKafkaProducedRecordsLatencyPanel('distributor')
      )
    )
    .addRowsIf(std.objectHasAll($._config.injectRows, 'postDistributor'), $._config.injectRows.postDistributor($))
    .addRowIf(
      $._config.show_grpc_ingestion_panels,
      ($.row('Ingester'))
      .addPanel(
        $.timeseriesPanel('Requests / sec') +
        $.panelDescription(
          'Requests / sec',
          |||
            The rate of successful, failed and rejected requests to ingester.
            Rejected requests are requests that ingester fails to handle because of ingester instance limits (ingester-max-inflight-push-requests, ingester-max-inflight-push-requests-bytes, ingester-max-ingestion-rate).
            When ingester is configured to use "early" request rejection, then rejected requests are NOT included in other metrics.
            When ingester is not configured to use "early" request rejection, then rejected requests are also counted as "errors".
          |||
        ) +
        $.qpsPanelNativeHistogram($.queries.ingester.requestsPerSecondMetric, $.queries.ingester.writeRequestsPerSecondSelector) +
        if $._config.show_rejected_requests_on_writes_dashboard then
          {
            targets: [
              {
                legendLink: null,
                expr: 'sum (rate(cortex_ingester_instance_rejected_requests_total{%s, reason=~"ingester_max_inflight_push_requests|ingester_max_ingestion_rate"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
                format: 'time_series',
                intervalFactor: 2,
                legendFormat: 'rejected',
                refId: 'B',
              },
            ] + super.targets,
          } + $.aliasColors({
            rejected: '#EAB839',
          })
        else {},
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanelNativeHistogram($.queries.ingester.requestsPerSecondMetric, $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.perInstanceLatencyPanelNativeHistogram('0.99', $.queries.ingester.requestsPerSecondMetric, $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      ($.row('Ingester – Kafka records processing (ingest storage)'))
      .addPanel(
        $.timeseriesPanel('Kafka fetches / sec') +
        $.panelDescription(
          'Kafka fetches / sec',
          |||
            Rate of fetches received from Kafka brokers. A fetch can contain multiple records (a write request received on the write path is mapped into a single record).
            Read errors are any errors reported on connection to Kafka brokers, and are separate from "failed" fetches.
          |||
        ) +
        $.queryPanel(
          [
            |||
              sum (rate (cortex_ingest_storage_reader_fetches_total{%s}[$__rate_interval]))
              -
              sum (rate (cortex_ingest_storage_reader_fetch_errors_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
            'sum (rate (cortex_ingest_storage_reader_fetch_errors_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
            // cortex_ingest_storage_reader_read_errors_total metric is reported by Kafka client.
            'sum (rate (cortex_ingest_storage_reader_read_errors_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            'successful',
            'failed',
            'read errors',
          ],
        ) + $.aliasColors({ successful: $._colors.success, failed: $._colors.failed, 'read errors': $._colors.failed }) + $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Kafka records batch latency') +
        $.panelDescription(
          'Kafka records batch latency',
          |||
            This panel displays the two stages in processing a record batch from Kafka. Fetching batches happens asynchronously to processing them.

            If processing batches is the bottleneck, then "Awaiting next batch avg" will be close to zero. In this case you can consider tuning the ingestion-concurrency configuration parameters.

            If fetching is the bottleneck, then "Awaiting next batch avg" will be in the high tens of milliseconds or higher.
            It is normal for fetching to be the bottleneck during normal operation because a lot of the time is spent in waiting for new records to be produced.
          |||
        ) +
        $.withExemplars($.queryPanel(
          [
            'histogram_avg(sum(rate(cortex_ingest_storage_reader_records_batch_process_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_avg(sum(rate(cortex_ingest_storage_reader_records_batch_wait_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            'Batch processing avg',
            'Awaiting next batch avg',
          ],
        )) + {
          fieldConfig+: {
            defaults+: { unit: 's', exemplar: true },
          },
        } +
        $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Record size') +
        $.panelDescription(
          'Record size',
          |||
            Concurrent fetching estimates the size of records.
            The estimation is used to enforce the max-buffered-bytes limit and to reduce discarded bytes.
          |||
        ) +
        $.queryPanel([
          |||
            sum(rate(cortex_ingest_storage_reader_fetch_bytes_total{%s}[$__rate_interval]))
            /
            sum(rate(cortex_ingest_storage_reader_records_per_fetch_sum{%s}[$__rate_interval]))
          ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          |||
            histogram_avg(sum(rate(cortex_ingest_storage_reader_estimated_bytes_per_record{%s}[$__rate_interval])))
          |||
          % [$.jobMatcher($._config.job_names.ingester)],
        ], [
          'Actual bytes per record (avg)',
          'Estimated bytes per record (avg)',
        ]) +
        { fieldConfig+: { defaults+: { unit: 'bytes' } } },
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      $.row('')
      .addPanel(
        $.timeseriesPanel('Kafka fetch throughput') +
        $.panelDescription(
          'Kafka fetch throughput',
          |||
            Throughput of fetches received from Kafka brokers.
            This panel shows the rate of bytes fetched from Kafka brokers, and the rate of bytes discarded.
            The discarded bytes are due to concurrently fetching overlapping offsets.

            Discarded bytes amounting to up to 10% of the total fetched bytes are exepcted during startup when there is higher concurrency in fetching.
            Discarded bytes amounting to around 1% of the total fetched bytes are expected during normal operation.

            High values of discarded bytes might indicate inaccurate estimation of record size.
          |||
        ) +
        $.queryPanel([
          'sum(rate(cortex_ingest_storage_reader_fetch_bytes_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingest_storage_reader_fetched_discarded_bytes_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
        ], [
          'Fetched bytes (decompressed)',
          'Discarded bytes (decompressed)',
        ]) +
        { fieldConfig+: { defaults+: { unit: 'Bps' } } },
      )
      .addPanel(
        $.timeseriesPanel('Write request batches processed / sec') +
        $.panelDescription(
          'Write request batches processed / sec',
          |||
            Rate of write requests processed from Kafka. When concurrent fetcher is enabled, this panel shows the rate
            of write request batches processed: multiple requests could get batched together and so the resulting number
            batches processed may be lower than the number of write requests received in the distributor.

            Failed records are categorized as "client" errors (e.g. per-tenant limits) or server errors.
          |||
        ) +
        $.queryPanel(
          [
            |||
              sum(
                # This is the old metric name. We're keeping support for backward compatibility.
                rate(cortex_ingest_storage_reader_records_total{%s}[$__rate_interval])
                or
                rate(cortex_ingest_storage_reader_requests_total{%s}[$__rate_interval])
              )
              -
              sum(
                # This is the old metric name. We're keeping support for backward compatibility.
                rate(cortex_ingest_storage_reader_records_failed_total{%s}[$__rate_interval])
                or
                rate(cortex_ingest_storage_reader_requests_failed_total{%s}[$__rate_interval])
              )
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
            'sum (
                # This is the old metric name. We\'re keeping support for backward compatibility.
                rate(cortex_ingest_storage_reader_records_failed_total{%s, cause="client"}[$__rate_interval])
                or
                rate(cortex_ingest_storage_reader_requests_failed_total{%s, cause="client"}[$__rate_interval])
              )' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
            'sum (
              # This is the old metric name. We\'re keeping support for backward compatibility.
              rate(cortex_ingest_storage_reader_records_failed_total{%s, cause="server"}[$__rate_interval])
              or
              rate(cortex_ingest_storage_reader_requests_failed_total{%s, cause="server"}[$__rate_interval])
            )' % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
          ],
          [
            'successful',
            'failed (client)',
            'failed (server)',
          ],
        ) + $.aliasColors({ successful: $._colors.success, 'failed (client)': $._colors.clientError, 'failed (server)': $._colors.failed }) + $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Ingested series / sec') +
        $.panelDescription(
          'Ingested series',
          |||
            Concurrent ingestion estimates the number of timeseries per batch to choose the optimal concurrency settings.

            Normally one timeseries contains one sample, but it can contain multiple samples.
          |||
        ) +
        $.queryPanel([
          'histogram_sum(sum(rate(cortex_ingest_storage_reader_pusher_timeseries_per_flush{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          'sum(rate(cortex_ingest_storage_reader_pusher_estimated_timeseries_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
        ], [
          'Actual series',
          'Estimated series',
        ]) +
        { fieldConfig+: { defaults+: { unit: 'short' } } },
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      $.row('Ingester – end-to-end latency (ingest storage)')
      .addPanel(
        $.ingestStorageIngesterEndToEndLatencyWhenRunningPanel(),
      )
      .addPanel(
        $.ingestStorageIngesterEndToEndLatencyOutliersWhenRunningPanel(),
      )
      .addPanel(
        $.ingestStorageIngesterEndToEndLatencyWhenStartingPanel(),
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      ($.row('Ingester – last consumed offset (ingest storage)'))
      .addPanel(
        $.timeseriesPanel('Last consumed offset commits / sec') +
        $.panelDescription(
          'Last consumed offset commits / sec',
          |||
            Rate of "last consumed offset" commits issued by ingesters to Kafka.
          |||
        ) +
        $.queryPanel(
          [
            |||
              sum (rate (cortex_ingest_storage_reader_offset_commit_requests_total{%s}[$__rate_interval]))
              -
              sum (rate (cortex_ingest_storage_reader_offset_commit_failures_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
            'sum (rate (cortex_ingest_storage_reader_offset_commit_failures_total{%s}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            'successful',
            'failed',
          ],
        ) + $.aliasColors({ successful: $._colors.success, failed: $._colors.failed }) + $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Last consumed offset commits latency') +
        $.panelDescription(
          'Kafka record processing latency',
          |||
            Time spent to commit "last consumed offset" by ingesters to Kafka.
          |||
        ) +
        $.queryPanel(
          [
            'histogram_avg(sum(rate(cortex_ingest_storage_reader_offset_commit_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_offset_commit_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_offset_commit_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_offset_commit_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            'avg',
            '99th percentile',
            '99.9th percentile',
            '100th percentile',
          ],
        ) + {
          fieldConfig+: {
            defaults+: { unit: 's' },
          },
        },
      )
    )
    .addRowIf(
      $._config.gateway_enabled && $._config.autoscaling.gateway.enabled,
      $.cpuAndMemoryBasedAutoScalingRow('Gateway'),
    )
    .addRowIf(
      $._config.autoscaling.distributor.enabled,
      $.cpuAndMemoryBasedAutoScalingRow('Distributor'),
    )
    .addRowIf(
      $._config.autoscaling.ingester.enabled,
      $.row('Ingester – autoscaling')
      .addPanel(
        local replicaTemplateQueries = [
          'max(kube_customresource_replicatemplate_spec_replicas{%(namespace_matcher)s, name=~"%(replica_template_name)s"})' % {
            namespace_matcher: $.namespaceMatcher(),
            replica_template_name: $._config.autoscaling.ingester.replica_template_name,
          },
          'max(kube_customresource_replicatemplate_status_replicas{%(namespace_matcher)s, name=~"%(replica_template_name)s"})' % {
            namespace_matcher: $.namespaceMatcher(),
            replica_template_name: $._config.autoscaling.ingester.replica_template_name,
          },
        ];

        local replicaTemplateLegends = [
          'Template spec replicas',
          'Template status replicas',
        ];

        $.autoScalingActualReplicas('ingester', replicaTemplateQueries, replicaTemplateLegends) + { title: 'Replicas (HPA + ReplicaTemplate)' } +
        $.panelDescription(
          'Replicas (HPA + ReplicaTemplate)',
          |||
            The minimum, maximum, and current number of replicas reported by the HPA for the ReplicaTemplate object.
            If available, also the spec and status replicas fields for the ReplicaTemplate object itself.
            Rollout-operator will keep ingester replicas updated based on the ReplicaTemplate spec field, and then update the template's status field once the ingester count changes.
          |||
        )
      )
      .addPanel(
        $.timeseriesPanel('Replicas (Ingesters)') +
        $.panelDescription(
          'Replicas (Ingesters)',
          |||
            Number of up ingester replicas per zone.
            Also show the number of read-only replicas per zone, or number of Inactive partitions for ingest storage.
          |||
        ) + $.queryPanel(
          [
            'sum by (%s) (up{%s})' % [$._config.per_job_label, $.jobMatcher($._config.job_names.ingester)],
            'sum by (%s) (cortex_lifecycler_read_only{%s}) unless on (%s) (cortex_partition_ring_partitions{name="ingester-partitions"})' % [$._config.per_job_label, $.jobMatcher($._config.job_names.ingester), $._config.per_job_label],
            'max(cortex_partition_ring_partitions{%s,name="ingester-partitions",state="Inactive"})' % [$.namespaceMatcher()],
          ],
          [
            'up ({{ %(per_job_label)s }})' % $._config.per_job_label,
            'read-only ({{ %(per_job_label)s }})' % $._config.per_job_label,
            'inactive partitions',
          ],
        ),
      )
      .addPanel(
        $.autoScalingDesiredReplicasByAverageValueScalingMetricPanel('ingester', '', '') + { title: 'Desired replicas (ReplicaTemplate)' }
      )
      .addPanel(
        $.autoScalingFailuresPanel('ingester') + { title: 'Autoscaler failures rate (ReplicaTemplate)' }
      ),
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
      $.row('Ingester – shipper')
      .addPanel(
        $.timeseriesPanel('Uploaded blocks / sec') +
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
        $.timeseriesPanel('Upload latency') +
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
      $.row('Ingester – TSDB head')
      .addPanel(
        $.timeseriesPanel('Compactions / sec') +
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
        $.timeseriesPanel('Compactions latency') +
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
      $.row('Ingester – TSDB write ahead log (WAL)')
      .addPanel(
        $.timeseriesPanel('WAL truncations / sec') +
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
        $.timeseriesPanel('Checkpoints created / sec') +
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
        $.timeseriesPanel('WAL truncations latency (includes checkpointing)') +
        $.queryPanel(
          |||
            sum(rate(cortex_ingester_tsdb_wal_truncate_duration_seconds_sum{%s}[$__rate_interval]))
            /
            sum(rate(cortex_ingester_tsdb_wal_truncate_duration_seconds_count{%s}[$__rate_interval])) >= 0
          ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)], 'avg'
        ) +
        { fieldConfig+: { defaults+: { unit: 's', noValue: '0' } } } +
        $.panelDescription(
          'WAL truncations latency (including checkpointing)',
          |||
            Average time taken to perform a full WAL truncation,
            including the time taken for the checkpointing to complete.
          |||
        ),
      )
      .addPanel(
        $.timeseriesPanel('Corruptions / sec') +
        $.queryPanel([
          'sum(rate(cortex_ingester_tsdb_wal_corruptions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
          'sum(rate(cortex_ingester_tsdb_mmap_chunk_corruptions_total{%s}[$__rate_interval]))' % $.jobMatcher($._config.job_names.ingester),
        ], [
          'WAL',
          'mmap-ed chunks',
        ]) +
        $.stack +
        { fieldConfig+: { defaults+: { unit: 'ops', noValue: '0' } } } +
        $.aliasColors({
          WAL: '#E24D42',
          'mmap-ed chunks': '#E28A42',
        }),
      )
    )
    .addRow(
      $.row('Exemplars')
      .addPanel(
        local title = 'Distributor exemplars incoming rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(%(group_prefix_jobs)s:cortex_distributor_exemplars_in:rate5m{%(job)s})'
          % { job: $.jobMatcher($._config.job_names.distributor), group_prefix_jobs: $._config.group_prefix_jobs },
          'incoming exemplars',
        ) +
        { fieldConfig+: { defaults+: { unit: 'ex/s' } } } +
        $.panelDescription(
          title,
          |||
            The rate of exemplars that have come in to the distributor, including rejected or deduped exemplars.
          |||
        ),
      )
      .addPanel(
        local title = 'Distributor exemplars received rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          'sum(%(group_prefix_jobs)s:cortex_distributor_received_exemplars:rate5m{%(job)s})'
          % { job: $.jobMatcher($._config.job_names.distributor), group_prefix_jobs: $._config.group_prefix_jobs },
          'received exemplars',
        ) +
        { fieldConfig+: { defaults+: { unit: 'ex/s' } } } +
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
        $.timeseriesPanel(title) +
        $.queryPanel(
          $.queries.ingester.ingestOrClassicDeduplicatedQuery('%(group_prefix_jobs)s:cortex_ingester_ingested_exemplars:rate5m{%(ingester)s}' % {
            ingester: $.jobMatcher($._config.job_names.ingester),
            group_prefix_jobs: $._config.group_prefix_jobs,
          }),
          'ingested exemplars',
        ) +
        { fieldConfig+: { defaults+: { unit: 'ex/s' } } } +
        $.panelDescription(
          title,
          |||
            The rate of exemplars ingested in the ingesters.
            Every exemplar is replicated to a number of ingesters. With classic storage we the sum of rates from all ingesters is divided by the replication factor.
            With ingest storage we take the maximum rate of each ingest partition.
            This ingested exemplars rate should match the distributor's received exemplars rate.
          |||
        ),
      )
      .addPanel(
        local title = 'Ingester appended exemplars rate';
        $.timeseriesPanel(title) +
        $.queryPanel(
          $.queries.ingester.ingestOrClassicDeduplicatedQuery('%(group_prefix_jobs)s:cortex_ingester_tsdb_exemplar_exemplars_appended:rate5m{%(ingester)s}' % {
            ingester: $.jobMatcher($._config.job_names.ingester),
            group_prefix_jobs: $._config.group_prefix_jobs,
          }),
          'appended exemplars',
        ) +
        { fieldConfig+: { defaults+: { unit: 'ex/s' } } } +
        $.panelDescription(
          title,
          |||
            The rate of exemplars appended in the ingesters.
            This can be lower than ingested exemplars rate since TSDB does not append the same exemplar twice, and those can be frequent.
          |||
        ),
      )
    )
    .addRow(
      $.row('Instance Limits')
      .addPanel(
        $.timeseriesPanel('Ingester per %s blocked requests' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'sum by (%s) (cortex_ingester_adaptive_limiter_blocked_requests{%s, request_type="push"})'
          % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], '',
        ) +
        { fieldConfig+: { defaults+: { unit: 'req' } } }
      )
      .addPanel(
        $.timeseriesPanel('Ingester per %s inflight requests' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'sum by (%s) (cortex_ingester_adaptive_limiter_inflight_requests{%s, request_type="push"})'
          % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], '',
        ) +
        { fieldConfig+: { defaults+: { unit: 'req' } } }
      )
      .addPanel(
        $.timeseriesPanel('Ingester per %s inflight request limit' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'sum by (%s) (cortex_ingester_adaptive_limiter_inflight_limit{%s, request_type="push"})'
          % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], '',
        ) +
        { fieldConfig+: { defaults+: { unit: 'req' } } }
      ),
    )
    .addRow(
      $.row('')
      .addPanel(
        $.timeseriesPanel('Rejected distributor requests') +
        $.queryPanel(
          'sum by (reason) (rate(cortex_distributor_instance_rejected_requests_total{%s}[$__rate_interval]))'
          % $.jobMatcher($._config.job_names.distributor),
          '{{reason}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } }
      )
      .addPanel(
        $.timeseriesPanel('Rejected ingester requests') +
        $.queryPanel(
          'sum by (reason) (rate(cortex_ingester_instance_rejected_requests_total{%s}[$__rate_interval]))'
          % $.jobMatcher($._config.job_names.ingester),
          '{{reason}}',
        ) +
        { fieldConfig+: { defaults+: { unit: 'reqps' } } }
      )
      .addPanel(
        $.timeseriesPanel('Ingester per %s rejection rate' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'sum by (%s) (cortex_ingester_rejection_rate{%s})'
          % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], '',
        ) +
        { fieldConfig+: { defaults+: { unit: 'percentunit', max: '1' } } }
      ),
    ),
}
