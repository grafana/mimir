---
description: The Mimir query engine (MQE) is responsible for evaluating PromQL queries.
menuTitle: Mimir query engine
title: Grafana Mimir query engine
weight: 100
---

The Mimir query engine (MQE) is an experimental alternative to Prometheus' query engine.
It is used in [queriers]({{< relref "./components/querier" >}}) to evaluate PromQL queries.

MQE produces equivalent results to Prometheus' engine, generally uses less memory and CPU
than Prometheus' engine, and evaluates queries at least as fast, if not faster.
It supports almost all stable PromQL features, and transparently falls back to Prometheus'
engine for queries that use unsupported features.

## How to enable MQE

MQE is experimental and disabled by default. It can be enabled by setting the
`-querier.query-engine=mimir` CLI flag on queriers, or setting the equivalent YAML
configuration file option.

## Fallback to Prometheus' engine

By default, MQE will fall back to Prometheus' engine for any queries that use unsupported
features.

This behaviour can be disabled by setting the `-querier.enable-query-engine-fallback=false`
CLI flag on queriers, or setting the equivalent YAML configuration file option. If fallback
is disabled and MQE receives a query it does not support, then the query will fail.

You can force a query supported by MQE to use Prometheus' engine by adding the
`X-Mimir-Force-Prometheus-Engine: true` HTTP header to the query request. This header only
has an effect if fallback is enabled.

## Query memory consumption limit

MQE supports enforcing a per-query memory consumption limit. This allows you to ensure that
a single memory-hungry query cannot monopolise a large proportion of available memory in a
querier, or cause it to exhaust all available memory and crash.

While evaluating a query, MQE estimates the memory consumed by the query, such as memory used
for the final result and any intermediate calculations, and aborts the query with an
[`err-mimir-max-estimated-memory-consumption-per-query`]({{< relref "../../manage/mimir-runbooks#err-mimir-max-estimated-memory-consumption-per-query" >}})
error if the estimate exceeds the configured limit.

The estimate is based on the memory consumed by samples currently held in memory for query
evaluation. This includes both raw samples decoded from chunks and samples held in memory as
intermediate results of calculations, or the final result. It also includes some other large
sources of memory consumption for intermediate results.

The estimate is limited, for example:

- it does not consider the memory consumed by series labels
- it does not consider the memory consumed by chunks that are currently in memory (although
  the max chunks and max chunks bytes limits continue to be enforced)
- it makes an assumption about the memory consumed by each native histogram, rather than
  accurately calculating the memory consumed by each histogram

By default, no limit is enforced. The default limit for all tenants can be configured by
setting the `-querier.max-estimated-memory-consumption-per-query` CLI flag, or setting the
equivalent YAML configuration file option. The default limit can be overridden on a per-tenant
basis by setting `max_estimated_memory_consumption_per_query` for that tenant. Setting the
limit to 0 disables it.

The limit is not enforced for queries that run through Prometheus' engine, and setting the limit
has no impact if MQE is disabled or the query falls back to Prometheus' engine.

## Known differences compared to Prometheus' engine

### Binary operations that produces no series

If MQE can determine that a binary operation such as `+`, `-`, `/` or `and` will produce no series
based on the series labels on both sides, it will skip evaluating both sides.

For example, if the query is `foo / bar`, and `foo` selects a single series `foo{env="test"}`, and
`bar` selects a single series `bar{env="prod"}`, then the query cannot produce any series and so the
data for each side is not evaluated.

This can have some noticeable side effects, including:

- `aborted stream because query was cancelled: context canceled: query execution finished` might be
  logged by the querier, as streaming data from ingesters and store-gateways is aborted without reading
  all the data (because it is not needed).

- Some annotations may not be emitted. For example, if the query above was
  `rate(foo[1m]) / sum(rate(bar[1m]))`, Prometheus' engine will emit annotations like
  `metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "foo"` and
  `metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "bar"`. MQE will not
  emit these annotations as they are only emitted during the evaluation of the series data, not during
  evaluation of the series labels.

- `found duplicate series for the match group` errors may not be returned by MQE if a match group has no
  series on one side but multiple series on the other side and those series have samples that conflict
  with each other.

### `topk` and `bottomk`

It can appear that MQE and Prometheus' engine produce different results for queries that use `topk`
and `bottomk` if different series have samples with the same values. Prometheus' engine does not
have deterministic behaviour in this case and may select different series on each evaluation of the
query, and MQE's implementation differs from Prometheus' engine, which can also lead to different results.
