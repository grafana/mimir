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

## General query guidelines

### Query construction order

Structure your queries to eliminate unwanted results as early as possible:

1. Start with the most selective label selectors: Use the most specific labels first to reduce the initial dataset size
2. Apply time range constraints: Narrow down to the shortest feasible time range
3. Add functions and aggregations: Apply PromQL functions after narrowing the series set

### Use precise label selectors

- Start with the most selective labels: If you have labels like `namespace` and `app_name` where `app_name` is more specific, lead with `app_name="myapp"` rather than starting with the broader `namespace` selector.
- Include as many label selectors as possible to reduce the number of series that need to be processed.
- Prefer exact matches over regular expressions: Use exact label matching (`label="value"`) instead of regular expressions (`label=~"pattern"`) whenever possible. Regular expressions are more computationally expensive.

### Use appropriate time ranges

- Limit query time ranges: Query only the time range you need. Larger time ranges require more compute resources and increase query latency.
- Consider the `-store.max-labels-query-length` limit: When querying labels without specifying time ranges, Mimir will query the whole retention period or up to this configured limit.
- Use appropriate step intervals: Choose step intervals that match your visualization requirements. Smaller steps increase query cost but provide higher resolution.

### Optimize PromQL queries

- Avoid high cardinality operations: Be cautious with functions that can significantly increase cardinality, such as `group_by` with many labels.
- Use recording rules: Pre-compute expensive queries using [recording rules](../../manage/rule-evaluation/recording-rules/), especially for dashboard queries that run frequently, complex aggregations across high-cardinality metrics, or queries that span long time ranges.

## Mimir label querying best practices

Refer to "Query metric labels"

## Recommended values

Use values other than default (find in Helm chart)
