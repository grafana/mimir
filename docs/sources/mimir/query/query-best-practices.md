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

# Best practices for querying Grafana Mimir

The way you write queries for Grafana Mimir affects the speed and quality of your results. Follow these best practices to ensure optimal performance and reliability for queries.

## Use label matchers

To reduce the initial size of your dataset, apply label matchers to your query. Label matchers help you to filter your data based on its associated labels. For example, you if you have a label called `app_name`, you can append `app_name="myapp"` to your query.

Whenever possible, use an exact label match, such as `label="value"`, instead of a regular expression, such as `label=~"pattern"`. Regular expressions are more computationally expensive.

For more information about how to query metric labels, refer to [Query metric labels](https://grafana.com/docs/mimir/<MIMIR_VERSION>/query/query-metric-labels/).

## Narrow down your time range

Limit your query to a specific time period to reduce the number of metrics and samples Mimir needs to process. As a best practice, query the shortest feasible time range to narrow down your results. Larger time ranges require more computing resources and increase query latency.

Follow these guidelines for setting a time range based on your query type:

- Dashboards: For dashboards, use a time range that matches the typical viewing window. While users can zoom out to view more history, the default view should be efficient and focused on recent, relevant data.
- Alerting rules: Alerting rules should almost always operate on short, recent time windows to detect current states or recent trends, rather than long-term historical patterns. Timely alerts depend on the quick evaluations of fresh data.
- Ad-hoc analysis: When exploring data, start with small time ranges and gradually expand if more historical context is required for your investigation. This iterative approach prevents unnecessary strain on the system during exploratory phases.

Additionally, choose a step interval that matches your requirements for running the query. Smaller step intervals increase a query's cost but provide higher resolution.

## Consider your query evaluation frequency and time range

Consider the evaluation frequency of your query when determining the time range. For instance, if a query runs every minute, such as for an alerting rule or a frequently refreshing dashboard panel, there's no need for it to look back over a long time period, such as 30 days.

Queries with very long look-back periods, such as `rate(my_metric[30d])`, can be computationally expensive. They require Mimir to process a large volume of historical data, even if only the latest value is of interest.

## Filter early in the query

Avoid running broad, large-scale queries and then filtering the results within Grafana. This approach is computationally expensive and inefficient. Instead, filter your data as early as possible in the query itself.

Filtering data in the query provides the following benefits:

- Reduced data transfer: Filtering at the query level means Mimir sends only the data you need to Grafana, which reduces network overhead.
- Lower processing load: Mimir can apply filters at the storage layer, allowing it to discard irrelevant time series and data points before they are retrieved, processed, and aggregated. This significantly reduces the computational load on the Mimir cluster.
- Improved query speed: Queries that are pre-filtered are generally faster to run and return results.

Use these strategies to implement early filtering:

- Use label selectors: Leverage precise label selectors within your PromQL query to narrow down the dataset from the start. For more information, refer to [Use Label Selectors](#use-label-selectors).
- Apply functions and aggregations: If you need to transform or aggregate data, do so within the query. For example, `sum by (job) (metric_name)` is more efficient than retrieving all `metric_name` series and then summing them in Grafana.

## Use recording rules

If you need to run a particularly expensive or complex query, consider creating a recording rule to minimize its load. Recording rules in Mimir involve precomputing queries at a predetermined time and then saving the results for faster retrieval later.

Recording rules are useful for the following types of queries:

- Dashboard queries that run frequently
- Complex aggregations across high-cardinality metrics
- Queries spanning long time ranges

Recording rules are most beneficial for queries with results that you frequently evaluate. If you use a recording rule for a query and don't frequently evaluate its results, it could lead to unnecessary computation and data storage.

For more information about recording rules, refer to [Defining recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) in the Prometheus documentation.
