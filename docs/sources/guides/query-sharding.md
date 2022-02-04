---
title: Query sharding
weight: 1100
---

# Query sharding

Mimir includes the ability to process queries in parallel. This is
achieved by breaking the dataset into smaller pieces. These smaller pieces are
called shards. Each shard then gets queried in a partial query, and those
partial queries are distributed by the query-frontend to run on different
queries in parallel. The results of those partial queries are aggregated by the
query-frontend to return the full query result.

## Query sharding at glance

Not all queries are shardable. While the full query is not shardable, the inner
parts of a query could still be shardable.

In particular associative aggregations (like `sum`, `min`, `max`, `count`,
`avg`) are shardable, while query functions (like`absent`, `absent_over_time`,
`histogram_quantile`, `sort_desc`, `sort`) are not.

In the following examples we look at a concrete example with a shard count of
3. All the partial queries that include a label selector `__query_shard__` are
executed in parallel.

### Example 1: Full query is shardable

```promql
sum(rate(metric[1m]))
```
Is executed as (assuming a shard count of 3):

```promql
sum(
  sum(rate(metric{__query_shard__="1_of_3"}[1m]))
  sum(rate(metric{__query_shard__="2_of_3"}[1m]))
  sum(rate(metric{__query_shard__="3_of_3"}[1m]))
)
```

### Example 2: Inner part is shardable

```promql
histogram_quantile(0.99, sum by(le) (rate(metric[1m])))
```

Is executed as (assuming a shard count of 3):

```promql
histogram_quantile(0.99, sum by(le) (
  sum by(le) (rate(metric{__query_shard__="1_of_3"}[1m]))
  sum by(le) (rate(metric{__query_shard__="2_of_3"}[1m]))
  sum by(le) (rate(metric{__query_shard__="3_of_3"}[1m]))
))
```

## Query-related configuration

Configure these options for query sharding:

- Set the command-line flag
  `-query-frontend.parallelize-shardable-queries=true` or set the query
  frontend configuration parameter `parallelize-shardable-queries` to true.

- Set the shard count for each query to be an integer greater than two. The
  shard count for a query is set by the first item set in this ordered list:

  - The HTTP header `Sharding-Control` specified as part of the query request

  - The tenant override value for the limit
    `query_sharding_total_shards`

  - The value of the command-line configuration flag
    `-frontend.query-sharding-total-shards`

- For a microservices deployment, set the query frontend configuration to the
  same values as are in their equivalent querier configuration command-line
  flags:

  - -querier.max-concurrent
  - -querier.timeout
  - -querier.max-samples
  - -querier.at-modifier-enabled
  - -querier.default-evaluation-interval
  - -querier.active-query-tracker-dir
  - -querier.lookback-delta

## Flow of a sharded query

![Flow of a query with query sharding](../../images/query-sharding.png)

[//]: <> (TODO: Line out how `-frontend.query-sharding-max-sharded-queries` related to all of this)

[//]: <> (TODO: Finish an overview, which shows how the query-frontend splits queries to explain)
[Mermaid Graph]:https://mermaid.live/edit/#eyJjb2RlIjoiZmxvd2NoYXJ0IExSXG4gIHN1YmdyYXBoIFFGW1F1ZXJ5LUZyb250ZW5kXVxuICAgIGRpcmVjdGlvbiBMUlxuICAgIHN1YmdyYXBoIFRTXG4gICAgICAgIGRpcmVjdGlvbiBSTFxuICAgICAgICBUUzFbc3RhcnQ9dHMgZW5kIHQ9bl1cbiAgICAgICAgVFMyW3N0YXJ0PXRuIGVuZCB0PW4rMV1cbiAgICAgICAgVFNuWy4uLi5dXG4gICAgZW5kXG4gICAgc3ViZ3JhcGggUVNbU3BsaXR0aW5nIHF1ZXJpZXMgXFxuIGludG8gMyBxdWVyeSBzaGFyZHNdXG4gICAgICAgIGRpcmVjdGlvbiBSTFxuICAgICAgICBRUzFhWzEvM11cbiAgICAgICAgUVMxYlsyLzNdXG4gICAgICAgIFFTMWNbMy8zXVxuICAgICAgICBRUzJhWzEvM11cbiAgICAgICAgUVMyYlsyLzNdXG4gICAgICAgIFFTMmNbMy8zXVxuICAgICAgICBRU25hWzEvM11cbiAgICAgICAgUVNuYlsyLzNdXG4gICAgICAgIFFTbmNbMy8zXVxuICAgIGVuZFxuICBlbmRcbiAgVFMxIC0tPiBRUzFhXG4gIFRTMSAtLT4gUVMxYlxuICBUUzEgLS0-IFFTMWNcbiAgVFMyIC0tPiBRUzJhXG4gIFRTMiAtLT4gUVMyYlxuICBUUzIgLS0-IFFTMmNcbiAgVFNuIC0tPiBRU25hXG4gIFRTbiAtLT4gUVNuYlxuICBUU24gLS0-IFFTbmNcbiAgVVtVc2VyXSAtLT4gUUZcbiIsIm1lcm1haWQiOiJ7XG4gIFwidGhlbWVcIjogXCJkZWZhdWx0XCJcbn0iLCJ1cGRhdGVFZGl0b3IiOnRydWUsImF1dG9TeW5jIjp0cnVlLCJ1cGRhdGVEaWFncmFtIjp0cnVlfQ

[//]: <> (TODO query_range.split_queries_by_interval: "24h")

## Operational considerations

Splitting a single query into sharded queries increases the quantity of queries
that must be processed. Parallelization decreases the query processing time
latency, but increases the load on querier components and their underlying data
stores (ingesters for recent data and store-gateway for historic data). The
caching layer for chunks and indexes will also experience an increased load.

## Verification

### Query statistics

The query statistics of the query-frontend allow to check if query sharding was
used for an individual query. The field `sharded_queries` contains the amount
of parallelly executed sub-queries.

When `sharded_queries` is `0`, either the query is not shardable or query
sharding is disabled for cluster or tenant. This is a log line of an
unshardable query:

```
sharded_queries=0  param_query="absent(up{job=\"my-service\"})"
```

When `sharded_queries` matches the configured shard count, query sharding is
operational and the query has only a single leg (assuming time splitting is
disabled). The following log line represents that case with a shard count of
`16`:

```
sharded_queries=16 query="sum(rate(prometheus_engine_queries[5m]))"
```

When `sharded_queries` is a multiple of the configured shard count, query
sharding is operational and the query has only a multiple legs (assuming time
splitting is disabled). The following log line shows a query with two legs and
with a configured shard count of `16`:

```
sharded_queries=32 query="sum(rate(prometheus_engine_queries{engine=\"ruler\"}[5m]))/sum(rate(prometheus_engine_queries[5m]))"
```

The query-frontend also exposes metrics, which can be useful to understand the
query workload's parallelism as a whole.

To get the ratio of queries which are shardable:
queries the following PromQL query can be used. A value of 1.0 would mean all queries are shardable wh:

```promql
sum(rate(cortex_frontend_query_sharding_rewrites_succeeded_total[$__rate_interval])) /
sum(rate(cortex_frontend_query_sharding_rewrites_attempted_total[$__rate_interval]))
```

The histogram `cortex_frontend_sharded_queries_per_query` allows to understand
how many sharded sub queries are generated per query.
