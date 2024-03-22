local utils = import 'mixin-utils/utils.libsonnet';
local filename = 'mimir-writes.json';

(import 'dashboard-utils.libsonnet') +
(import 'dashboard-queries.libsonnet') {

  [filename]:
    assert std.md5(filename) == '8280707b8f16e7b87b840fc1cc92d4c5' : 'UID of the dashboard has changed, please update references to dashboard.';
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
        $.timeseriesPanel('Requests / sec') +
        $.qpsPanel($.queries.gateway.writeRequestsPerSecond)
      )
      .addPanel(
        $.timeseriesPanel('Latency') +
        $.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', $.queries.write_http_routes_regex)])
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
        $.qpsPanel($.queries.distributor.writeRequestsPerSecond) +
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
        $.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.distributor) + [utils.selector.re('route', '/distributor.Distributor/Push|/httpgrpc.*|%s' % $.queries.write_http_routes_regex)])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/distributor.Distributor/Push|/httpgrpc.*|%s"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.distributor), $.queries.write_http_routes_regex], ''
        )
      )
      .addPanelIf(
        $._config.show_ingest_storage_panels,
        $.timeseriesPanel('Sync write to Kafka latency (ingest storage)') +
        $.panelDescription(
          'Sync write to Kafka latency (ingest storage)',
          |||
            Latency of synchronous write operation used to store data into Kafka.
          |||
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.5, sum(rate(cortex_ingest_storage_writer_latency_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.distributor)],
            'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_writer_latency_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.distributor)],
            'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_writer_latency_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.distributor)],
            'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_writer_latency_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.distributor)],
          ],
          [
            '50th percentile',
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
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route="/cortex.Ingester/Push"}' % $.jobMatcher($._config.job_names.ingester)) +
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
        $.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.eq('route', '/cortex.Ingester/Push')])
      )
      .addPanel(
        $.timeseriesPanel('Per %s p99 latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route="/cortex.Ingester/Push"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], ''
        )
      )
    )
    .addRowIf(
      $._config.show_ingest_storage_panels,
      ($.row('Ingester (ingest storage)'))
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
        $.timeseriesPanel('Kafka records / sec') +
        $.panelDescription(
          'Kafka records / sec',
          |||
            Rate of processed records from Kafka. Failed records are categorized as "client" errors (e.g. per-tenant limits) or server errors.
          |||
        ) +
        $.queryPanel(
          [
            |||
              sum(rate(cortex_ingest_storage_reader_records_total{%s}[$__rate_interval]))
              -
              sum(rate(cortex_ingest_storage_reader_records_failed_total{%s}[$__rate_interval]))
            ||| % [$.jobMatcher($._config.job_names.ingester), $.jobMatcher($._config.job_names.ingester)],
            'sum (rate (cortex_ingest_storage_reader_records_failed_total{%s, cause="client"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
            'sum (rate (cortex_ingest_storage_reader_records_failed_total{%s, cause="server"}[$__rate_interval]))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            'successful',
            'failed (client)',
            'failed (server)',
          ],
        ) + $.aliasColors({ successful: $._colors.success, 'failed (client)': $._colors.clientError, 'failed (server)': $._colors.failed }) + $.stack,
      )
      .addPanel(
        $.timeseriesPanel('Kafka record processing latency') +
        $.panelDescription(
          'Kafka record processing latency',
          |||
            Time used to process a single record (write request). This time is spent by appending data to per-tenant TSDB.
          |||
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.5, sum(rate(cortex_ingest_storage_reader_processing_time_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_processing_time_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_processing_time_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_processing_time_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            '50th percentile',
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
      $._config.show_ingest_storage_panels,
      ($.row('Ingester (ingest storage – end-to-end latency)'))
      .addPanel(
        $.timeseriesPanel('Kafka record end-to-end latency when ingesters are running') +
        $.panelDescription(
          'Kafka record end-to-end latency when ingesters are running',
          |||
            Time between writing request by distributor to Kafka and reading the record by ingester, when ingesters are running.
          |||
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.5, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="running"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="running"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="running"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="running"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            '50th percentile',
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
      .addPanel(
        $.timeseriesPanel('Kafka record end-to-end latency when starting') +
        $.panelDescription(
          'Kafka record end-to-end latency when starting',
          |||
            Time between writing request by distributor to Kafka and reading the record by ingester during catch-up phase, when ingesters are starting.
            If ingesters are not starting and catching up in the selected time range, this panel will be empty.
          |||
        ) +
        $.queryPanel(
          [
            'histogram_quantile(0.5, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="starting"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="starting"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="starting"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_receive_delay_seconds{%s, phase="starting"}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            '50th percentile',
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
      $._config.show_ingest_storage_panels,
      ($.row('Ingester (ingest storage - last consumed offset)'))
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
            'histogram_quantile(0.5, sum(rate(cortex_ingest_storage_reader_offset_commit_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.99, sum(rate(cortex_ingest_storage_reader_offset_commit_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(0.999, sum(rate(cortex_ingest_storage_reader_offset_commit_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
            'histogram_quantile(1.0, sum(rate(cortex_ingest_storage_reader_offset_commit_request_duration_seconds{%s}[$__rate_interval])))' % [$.jobMatcher($._config.job_names.ingester)],
          ],
          [
            '50th percentile',
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
      $.row('Ingester - TSDB head')
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
      $.row('Ingester - TSDB write ahead log (WAL)')
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
        { fieldConfig+: { defaults+: { unit: 'ex/s' } } } +
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
        $.timeseriesPanel(title) +
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
      ),
    ),
}
