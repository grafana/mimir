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

Whenever possible, use an exact label match, such as `label="value"`, instead of a regular expression, such as label=~"pattern". Regular expressions are more computationally expensive.

### Mimir label querying best practices

Refer to "Query metric labels"

## Narrow down your time range

Limit your query to a specific time period to reduce the number of metrics Mimir needs to process. As a best practice, query the shortest feasible time range to narrow down your results. Larger time ranges require more computing resources and increase query latency.

If you don't specify a time range, consider using the `-store.max-labels-query-length` configuration parameter to limit the maximum time range.

Additionally, choose a step interval that matches your requirements for running the query. Smaller step intervals increase a query's cost but provides higher resolution.

## Avoid high cardinality operations

Avoid high cardinality operations: Be cautious with functions that can significantly increase cardinality, such as `group_by` with many labels.

## Use recording rules

Use recording rules: Pre-compute expensive queries using [recording rules](../../manage/rule-evaluation/recording-rules/), especially for dashboard queries that run frequently, complex aggregations across high-cardinality metrics, or queries that span long time ranges.


