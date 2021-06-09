local utils = import 'mixin-utils/utils.libsonnet';

(import 'dashboard-utils.libsonnet') {
  'cortex-reads.json':
    ($.dashboard('Cortex / Reads') + { uid: '8d6ba60eccc4b6eedfa329b24b1bd339' })
    .addClusterSelectorTemplates()
    .addRow(
      ($.row('Reads Summary') { height: '175px', showTitle: false })
      .addPanel(
        $.textPanel('', |||
          <p>
            This dashboard shows various health metrics for the Cortex read path.
            It is broken into sections for each service on the read path, and organized by the order in which the read request flows.
            <br/>
            Incoming queries travel from the gateway → query frontend → query scheduler → querier → ingester and/or store-gateway (depending on the age of the query).
          </p> 
          <p>
            The dashboard shows metrics for the 4 optional caches that can be deployed with Cortex: 
            the query results cache, the metadata cache, the chunks cache, and the index cache. 
            <br/>
            These panels will show “no data” if the caches are not deployed. 
          </p>
          <p>
            Lastly, it also includes metrics for how the ingester and store-gateway interact with object storage. 
          </p>
        |||),
      )
    )
    .addRow(
      ($.row('Headlines') +
       {
         height: '100px',
         showTitle: false,
       })
      .addPanel(
        $.panel('Instant Queries / s') +
        $.statPanel(|||
          sum(
            rate(
              cortex_request_duration_seconds_count{
                %(queryFrontend)s,
                route=~"(prometheus|api_prom)_api_v1_query"
              }[1h]
            )
          ) + 
          sum(
            rate(
              cortex_prometheus_rule_evaluations_total{
                %(ruler)s
              }[1h]
            )
          )
        ||| % {
          queryFrontend: $.jobMatcher($._config.job_names.query_frontend),
          ruler: $.jobMatcher($._config.job_names.ruler),
        }, format='reqps') +
        $.panelDescription(
          'Instant Queries Per Second',
          |||
            Rate of instant queries per second being made to the system.
            Includes both queries made to the <tt>/prometheus</tt> API as 
            well as queries from the ruler.
          |||
        ),
      )
      .addPanel(
        $.panel('Range Queries / s') +
        $.statPanel(|||
          sum(
            rate(
              cortex_request_duration_seconds_count{
                %(queryFrontend)s,
                route=~"(prometheus|api_prom)_api_v1_query_range"
              }[1h]
            )
          )
        ||| % {
          queryFrontend: $.jobMatcher($._config.job_names.query_frontend),
        }, format='reqps') +
        $.panelDescription(
          'Range Queries Per Second',
          |||
            Rate of range queries per second being made to 
            Cortex via the <tt>/prometheus</tt> API. 
            (The ruler does not issue range queries).
          |||
        ),
      )
    )
    .addRow(
      $.row('Gateway')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.gateway)) +
        $.panelDescriptionRps('gateway')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.gateway) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')]) +
        $.panelDescriptionLatency('gateway')
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.gateway)], ''
        ) +
        { yaxes: $.yaxes('s') } +
        $.panelDescriptionP99Latency('gateway')
      )
    )
    .addRow(
      $.row('Query Frontend')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.query_frontend)) +
        $.panelDescriptionRps('query frontend')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')]) +
        $.panelDescriptionLatency('query frontend')
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.query_frontend)], ''
        ) +
        { yaxes: $.yaxes('s') } +
        $.panelDescriptionP99Latency('query frontend')
      )
    )
    .addRow(
      $.row('Query Scheduler')
      .addPanel(
        $.textPanel(
          '',
          |||
            <p>
              The query scheduler is an optional service that moves
              the internal queue from the query frontend into a
              separate component.
              If this service is not deployed, 
              these panels will show "No Data."
            </p>
          |||
        )
      )
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_query_scheduler_queue_duration_seconds_count{%s}' % $.jobMatcher($._config.job_names.query_scheduler)) +
        $.panelDescriptionRps('query scheduler')
      )
      .addPanel(
        $.panel('Latency (Time in Queue)') +
        $.latencyPanel('cortex_query_scheduler_queue_duration_seconds', '{%s}' % $.jobMatcher($._config.job_names.query_scheduler)) +
        $.panelDescriptionLatency('query scheduler')
      )
    )
    .addRow(
      $.row('Cache - Query Results')
      .addPanel(
        $.textPanel('', |||
          <p>
            The query results cache is one of 4 optional caches
            that can be deployed as part of a GEM cluster to improve query performance. 
            It is used by the query-frontend to cache entire results of queries.
          </p>
        |||)
      )
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{method=~"frontend.+", %s}' % $.jobMatcher($._config.job_names.query_frontend)) +
        $.panelDescriptionRps('query results')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector($._config.job_names.query_frontend) + [utils.selector.re('method', 'frontend.+')]) +
        $.panelDescriptionLatency('query results')
      )
    )
    .addRow(
      $.row('Querier')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_querier_request_duration_seconds_count{%s, route=~"(prometheus|api_prom)_api_v1_.+"}' % $.jobMatcher($._config.job_names.querier)) +
        $.panelDescriptionRps(
          'querier'
        )
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_querier_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.re('route', '(prometheus|api_prom)_api_v1_.+')]) +
        $.panelDescriptionLatency('querier')
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_querier_request_duration_seconds_bucket{%s, route=~"(prometheus|api_prom)_api_v1_.+"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.querier)], ''
        ) +
        { yaxes: $.yaxes('s') } +
        $.panelDescriptionP99Latency('querier')
      )
    )
    .addRow(
      $.row('Ingester')
      .addPanel(
        $.textPanel(
          '',
          |||
            <p>
              For short term queries, queriers go 
              to the ingester to fetch the data. 
            </p>
          |||
        )
      )
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata"}' % $.jobMatcher($._config.job_names.ingester)) +
        $.panelDescriptionRps('ingester')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.ingester) + [utils.selector.re('route', '/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata')]) +
        $.panelDescriptionLatency('ingester')
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/cortex.Ingester/Query(Stream)?|/cortex.Ingester/MetricsForLabelMatchers|/cortex.Ingester/LabelValues|/cortex.Ingester/MetricsMetadata"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.ingester)], ''
        ) +
        { yaxes: $.yaxes('s') } +
        $.panelDescriptionP99Latency('ingester')
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.row('Store-gateway')
      .addPanel(
        $.textPanel(
          '',
          |||
            <p>
              For longer term queries, queriers go to the store-gateways to 
              fetch the data. 
              Store-gateways are responsible for fetching the data from object 
              storage. 
            </p>
          |||
        )
      )
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_request_duration_seconds_count{%s,route=~"/gatewaypb.StoreGateway/.*"}' % $.jobMatcher($._config.job_names.store_gateway)) +
        $.panelDescriptionRps('store gateway')
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_request_duration_seconds', $.jobSelector($._config.job_names.store_gateway) + [utils.selector.re('route', '/gatewaypb.StoreGateway/.*')]) +
        $.panelDescriptionLatency('store gateway')
      )
      .addPanel(
        $.panel('Per %s p99 Latency' % $._config.per_instance_label) +
        $.hiddenLegendQueryPanel(
          'histogram_quantile(0.99, sum by(le, %s) (rate(cortex_request_duration_seconds_bucket{%s, route=~"/gatewaypb.StoreGateway/.*"}[$__rate_interval])))' % [$._config.per_instance_label, $.jobMatcher($._config.job_names.store_gateway)], ''
        ) +
        { yaxes: $.yaxes('s') } +
        $.panelDescriptionP99Latency('store gateway')
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks'),
      $.row('Memcached - Chunks storage - Index')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{%s,method="store.index-cache-read.memcache.fetch"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('method', 'store.index-cache-read.memcache.fetch')])
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks'),
      $.row('Memcached - Chunks storage - Chunks')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_cache_request_duration_seconds_count{%s,method="chunksmemcache.fetch"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cache_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('method', 'chunksmemcache.fetch')])
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.row('Memcached – Blocks Storage – Block Index (Store-gateway)')  // Resembles thanosMemcachedCache
      .addPanel(
        $.textPanel(
          '',
          |||
            <p>
              The block index cache is an optional component that the 
              store-gateway will check before going to object storage.
              This helps reduce calls to the object store.
            </p>
          |||
        )
      )
      .addPanel(
        $.panel('Requests Per Second') +
        $.queryPanel(
          |||
            sum by(operation) (
              rate(
                thanos_memcached_operations_total{
                  component="store-gateway",
                  name="index-cache",
                  %s
                }[$__rate_interval]
              )
            )
          ||| % $.jobMatcher($._config.job_names.store_gateway), '{{operation}}'
        ) +
        $.stack +
        { yaxes: $.yaxes('ops') } +
        $.panelDescription(
          'Requests Per Second',
          |||
            Requests per second made to 
            the block index cache
            from the store-gateway,
            separated into request type.
          |||
        ),
      )
      .addPanel(
        $.panel('Latency (getmulti)') +
        $.latencyPanel(
          'thanos_memcached_operation_duration_seconds',
          |||
            {
              %s,
              operation="getmulti",
              component="store-gateway",
              name="index-cache"
            }
          ||| % $.jobMatcher($._config.job_names.store_gateway)
        ) +
        $.panelDescription(
          'Latency (getmulti)',
          |||
            The average, median (50th percentile) and 99th percentile
            time to satisfy a “getmulti” request 
            from the store-gateway, 
            which retrieves multiple items from the cache. 
          |||
        )
      )
      .addPanel(
        $.panel('Hit ratio') +
        $.queryPanel(
          |||
            sum by(item_type) (
              rate(
                thanos_store_index_cache_hits_total{
                  component="store-gateway",
                  %s
                }[$__rate_interval]
              )
            ) 
            / 
            sum by(item_type) (
              rate(
                thanos_store_index_cache_requests_total{
                  component="store-gateway",
                  %s
                }[$__rate_interval]
              )
            )
          ||| % [
            $.jobMatcher($._config.job_names.store_gateway),
            $.jobMatcher($._config.job_names.store_gateway),
          ],
          '{{item_type}}'
        ) +
        { yaxes: $.yaxes('percentunit') } +
        $.panelDescription(
          'Hit Ratio',
          |||
            The fraction of requests to the 
            block index cache that successfully return data. 
            Requests that miss the cache must go to 
            object storage for the underlying data. 
          |||
        ),
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.thanosMemcachedCache(
        'Memcached – Blocks Storage – Chunks (Store-gateway)',
        $._config.job_names.store_gateway,
        'store-gateway',
        'chunks-cache'
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.thanosMemcachedCache(
        'Memcached – Blocks Storage – Metadata (Store-gateway)',
        $._config.job_names.store_gateway,
        'store-gateway',
        'metadata-cache'
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'blocks'),
      $.thanosMemcachedCache(
        'Memcached – Blocks Storage – Metadata (Querier)',
        $._config.job_names.querier,
        'querier',
        'metadata-cache'
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'cassandra'),
      $.row('Cassandra')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_cassandra_request_duration_seconds_count{%s, operation="SELECT"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_cassandra_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('operation', 'SELECT')])
      )
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'bigtable'),
      $.row('BigTable')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_bigtable_request_duration_seconds_count{%s, operation="/google.bigtable.v2.Bigtable/ReadRows"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_bigtable_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('operation', '/google.bigtable.v2.Bigtable/ReadRows')])
      ),
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_index_backend + $._config.chunk_store_backend, 'dynamodb'),
      $.row('DynamoDB')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_dynamo_request_duration_seconds_count{%s, operation="DynamoDB.QueryPages"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_dynamo_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('operation', 'DynamoDB.QueryPages')])
      ),
    )
    .addRowIf(
      std.member($._config.storage_engine, 'chunks') &&
      std.member($._config.chunk_store_backend, 'gcs'),
      $.row('GCS')
      .addPanel(
        $.panel('Requests Per Second') +
        $.qpsPanel('cortex_gcs_request_duration_seconds_count{%s, operation="GET"}' % $.jobMatcher($._config.job_names.querier))
      )
      .addPanel(
        $.panel('Latency') +
        utils.latencyRecordingRulePanel('cortex_gcs_request_duration_seconds', $.jobSelector($._config.job_names.querier) + [utils.selector.eq('operation', 'GET')])
      )
    )
    // Object store metrics for the store-gateway.
    .addRowsIf(
      std.member($._config.storage_engine, 'blocks'),
      $.getObjectStoreRows('Store-gateway - Blocks Object Store', 'store-gateway')
    )
    // Object store metrics for the querier.
    .addRowsIf(
      std.member($._config.storage_engine, 'blocks'),
      $.getObjectStoreRows('Querier - Blocks Object Store', 'querier')
    ),
} +
(
  {
    panelDescriptionRps(service)::
      $.panelDescription(
        'Requests Per Second',
        |||
          Read requests per second made to the %s(s).
        ||| % service
      ),

    panelDescriptionLatency(service)::
      $.panelDescription(
        'Latency',
        |||
          Across all %s instances, the average, median 
          (50th percentile), and 99th percentile time to respond 
          to a request.  
        ||| % service
      ),

    panelDescriptionP99Latency(service)::
      $.panelDescription(
        'Per Instance P99 Latency',
        |||
          The 99th percentile latency for each individual 
          instance of the %s service.
        ||| % service
      ),
  }
)
