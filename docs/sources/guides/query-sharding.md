---
title: Query sharding
weight: 1100
---

**NOTE:** Query sharding is an experimental feature. As such, the configuration
settings, command line flags, or specifics of the implementation are subject to
change.

## Overview

Since version 1.6, Grafana Enterprise Metrics (GEM) includes the ability to
process range queries in parallel. This is achieved by breaking down the whole
dataset into smaller pieces (which we'll refer to a "shards") and running a
query for each piece in parallel. Those partial results are then in a second
step aggregated to get the full query result.

## Configuration

For query sharding to be used, all of the following conditions need to be met:

- The query needs to be a range query and shardable (see [Limitations](#limitations)).

- The flag `-query-frontend.parallelize-shardable-queries=true` needs to be
  set.

- The shard count for that particular query needs to be an integer bigger than
  `2`. The shard count for a particular query is determined in the following
  order:

  - If the HTTP header `Sharding-Control` is set as part of the query request,
    its value takes precedence.

  - Failing that, the tenant override value for the limit
    `query_sharding_total_shards` is used.

  - Failing that, the value of the configuration flag
    `-frontend.query-sharding-total-shards` is used.

Query sharding is implemented in the `query-frontend` component. In case you
are running a micro-services deployment, it is important that you specify
querier specific flags for the `query-frontend` as well with the same value as
specified on the queriers:

```
-querier.max-concurrent
-querier.timeout
-querier.max-samples
-querier.at-modifier-enabled
-querier.default-evaluation-interval
-querier.active-query-tracker-dir
-querier.lookback-delta
```

### Flow of a sharded query

[//]: <> (TODO: I think it is important to explain how parallelism in the query path happens with time split, multiple legs per query and shard count)

[//]: <> (TODO: Line out how `-frontend.query-sharding-max-sharded-queries` related to all of this)

![Flow of a query with query sharding](../../images/query-sharding.png)

## Operational considerations

For every additional shard, the amount of queries the system needs to process
increases, at the benefit of reduced query processing time. This will put
additional load especially onto queriers and their underlying data stores
(ingesters for recent data and store-gateway for historic data). Also the
caching layer for chunks and indexes will experience increased load.

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

## Limitations

[//]: <> (The conditions were derived from https://github.com/grafana/mimir/blob/cad5243915a739e026ba3352ce9b7bdff3de97d6/pkg/frontend/querymiddleware/astmapper/parallel.go)

The implementation is only able to shard queries, which meet all of the
following conditions:

- All binary expressions contain a scalar value on one of the sides.

[//]: <> (List of functions should be kept in sync with https://github.com/grafana/mimir/blob/cad5243915a739e026ba3352ce9b7bdff3de97d6/pkg/frontend/querymiddleware/astmapper/parallel.go#L24-L32)

- There is no call to one of those functions: `absent`,`absent_over_time`,
  `histogram_quantile`, `sort_desc`, `sort`.

[//]: <> (List of functions should be kept in sync with https://github.com/grafana/mimir/blob/cad5243915a739e026ba3352ce9b7bdff3de97d6/pkg/frontend/querymiddleware/astmapper/parallel.go#L16-L22)

- The only aggregations used are: `sum`, `min`, `max`, `count`, `avg`.

- There are no nested aggregations.
