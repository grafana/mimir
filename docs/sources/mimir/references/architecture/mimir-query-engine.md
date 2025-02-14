---
description: Use the Mimir query engine (MQE) to evaluate PromQL queries.
menuTitle: Mimir query engine
title: Grafana Mimir query engine
weight: 100
---

# Grafana Mimir query engine

The Mimir Query Engine (MQE) is an experimental alternative to Prometheus' query engine.
You can use it in [queriers](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/querier)
to evaluate PromQL queries.

MQE produces equivalent results to Prometheus' engine, generally uses less memory and CPU
than Prometheus' engine, and evaluates queries at least as fast, if not faster.
It supports almost all stable PromQL features and transparently falls back to Prometheus'
engine for queries that use unsupported features.

## How to enable MQE

MQE is experimental and disabled by default. To enable it, either set the
`-querier.query-engine=mimir` CLI flag on queriers or set the equivalent YAML
configuration file option.

## Fallback to Prometheus' engine

By default, MQE falls back to Prometheus' engine for any queries that use unsupported
features.

To disable this behaviour, either set the `-querier.enable-query-engine-fallback=false`
CLI flag on queriers, or set the equivalent YAML configuration file option. If fallback
is disabled and MQE receives a query it does not support, then the query fails.

To force a query supported by MQE to use Prometheus' engine, add the
`X-Mimir-Force-Prometheus-Engine: true` HTTP header to the query request. This header only
has an effect if fallback is enabled.

## Query memory consumption limit

MQE supports enforcing a per-query memory consumption limit. This allows you to ensure that
a single memory-hungry query cannot monopolize a large proportion of available memory in a
querier, or cause it to exhaust all available memory and crash.

While evaluating a query, MQE estimates the memory consumed by the query, such as memory used
for the final result and any intermediate calculations, and stops the query with an
[`err-mimir-max-estimated-memory-consumption-per-query`](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/mimir-runbooks#err-mimir-max-estimated-memory-consumption-per-query)
error if the estimate exceeds the configured limit.

The estimate is based on the memory consumed by samples currently held in memory for query
evaluation. This includes both raw samples decoded from chunks, and samples held in memory as
intermediate results of calculations or as the final result. It also includes some other large
sources of memory consumption for intermediate results.

This estimate has the following limitations:

- It doesn't consider the memory consumed by series labels.
- It doesn't consider the memory consumed by chunks that are currently in memory.
  However, the maximum chunks and maximum chunks bytes limits continue to be enforced.
- It makes an assumption about the memory consumed by each native histogram, rather than
  accurately calculating the memory consumed by each histogram.

By default, no limit is enforced. To configure the default limit for all tenants, set either
the `-querier.max-estimated-memory-consumption-per-query` CLI flag, or set the
equivalent YAML configuration file option. You can override this default limit on a per-tenant
basis by setting `max_estimated_memory_consumption_per_query` for that tenant. Setting the
limit to 0 disables it.

The limit is not enforced for queries that run through Prometheus' engine, and setting the limit
has no impact if MQE is disabled or if the query falls back to Prometheus' engine.

## Known differences compared to Prometheus' engine

The following are known differences between MQE and Prometheus' engine:

### Binary operations that produces no series

If MQE can determine that a binary operation such as `+`, `-`, `/` or `and` produce no series
based on the series labels on both sides, it skips evaluating both sides.

For example, if the query is `foo / bar`, and `foo` selects a single series `foo{env="test"}`, and
`bar` selects a single series `bar{env="prod"}`, then the query cannot produce any series and so the
data for each side is not evaluated.

This has some noticeable side effects, including:

- `aborted stream because query was cancelled: context canceled: query execution finished` might be
  logged by the querier, as streaming data from ingesters and store-gateways is aborted without reading
  all the data, as it isn't needed.

- Some annotations aren't emitted. For example, if the query above was
  `rate(foo[1m]) / sum(rate(bar[1m]))`, Prometheus' engine emits annotations such as
  `metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "foo"` and
  `metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "bar"`. In contrast,
  MQE doesn't emit these annotations, as they are only emitted during the evaluation of the series data,
  and not during the evaluation of the series labels.

- `found duplicate series for the match group` errors aren't returned by MQE if a match group has no
  series on one side but multiple series on the other side and those series have samples that conflict
  with each other.

### `topk` and `bottomk`

MQE and Prometheus' engine produce different results for queries that use `topk`
and `bottomk` if different series have samples with the same values. Prometheus' engine does not
have deterministic behavior in this case and selects different series on each evaluation of the
query. MQE's implementation differs from Prometheus' engine, which can also lead to different results.
