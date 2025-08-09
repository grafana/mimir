---
description: Learn best practices for querying Grafana Mimir.
title: Best practices for querying Grafana Mimir
menuTitle: Query best practices
weight: 1
keywords:
  - query
  - best practices
  - performance
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# Best practices for querying Grafana Mimir

The way you write queries for Grafana Mimir affects the speed and quality of your results. Follow these best practices to ensure optimal performance and reliability for queries.

## Use precise label selectors

To reduce the initial dataset size, start with the most specific label selectors. For example, if you have labels like `namespace` and `app_name` where `app_name` is more specific, lead with `app_name="myapp"` rather than starting with the broader `namespace` selector.

To further reduce the number of series that require processing, include as many label selectors as possible for your query.

Whenever possible, use an exact label match, such as `label="value"`, instead of a regular expression, such as `label=~"pattern"`. Regular expressions are more computationally expensive.

For more information about how to query metric labels, refer to [Query metric labels](https://grafana.com/docs/mimir/<MIMIR_VERSION>/query/query-metric-labels/)

## Narrow down your time range

Limit your query to a specific time period to reduce the number of metrics Mimir needs to process. As a best practice, query the shortest feasible time range to narrow down your results. Larger time ranges require more computing resources and increase query latency.

If you don't specify a time range, consider using the `-store.max-labels-query-length` configuration parameter to limit the maximum time range.

Additionally, choose a step interval that matches your requirements for running the query. Smaller step intervals increase a query's cost but provide higher resolution.

## Avoid high cardinality operations

Avoid high cardinality operations to avoid performance issues or latency with your queries. High cardinality operations typically involve processing exceptionally large datasets.

High cardinality typically results from the following circumstances:

- Using a large number of labels
- Attaching a large number of values to a label.

To reduce cardinality in your queries, limit operations that involve many labels, such as the `group_by` function.

## Use recording rules

If you need to run a particularly expensive or complex query, consider creating a recording rule to minimize its load. Recording rules in Prometheus involve running queries at a predetermined time and then precomputing the query results to save for faster retrieval later.

Recording rules are useful for the following types of queries:

- Dashboard queries that run frequently
- Complex aggregations across high-cardinality metrics
- Queries spanning long time ranges

For more information about recording rules, refer to [Defining recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) in the Prometheus documentation.
