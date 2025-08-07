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

This topic provides best practices for querying metric data from Grafana Mimir to ensure optimal performance and reliability.

## Mimir-specific query optimizations

### Leverage Mimir's query caching

- **Align queries for caching**: Ensure your range queries are aligned (both `start` and `end` parameters are multiples of the `step`) to benefit from Mimir's query result caching. Aligned queries are cached by default, significantly improving performance for repeated queries.
- **Configure unaligned query caching**: If needed, enable caching for unaligned queries on a per-tenant basis using the `cache_unaligned_requests` parameter in your [limits configuration](../../configure/configuration-parameters/#limits).
- **Understand cache behavior**: Grafana automatically aligns queries for you, but when using the Mimir HTTP API directly, ensure alignment for optimal caching benefits.

### Work with TSDB block boundaries

- **Understand block granularity**: Mimir stores metrics in TSDB blocks with specific time ranges - `2h` for recent metrics and up to `24h` for historical metrics with default configuration.
- **Align time ranges to block boundaries**: When querying labels or historical data, your `start` and `end` parameters are automatically rounded to TSDB block boundaries, which can affect the actual data range returned.
- **Optimize for block structure**: Design your queries to work efficiently with Mimir's block structure rather than fighting against it.

### Use Mimir's specialized APIs

- **Choose the right cardinality API**: Use [label cardinality APIs](../query-metric-labels/#querying-label-names) specifically for understanding series cardinality rather than extracting this information from general queries.
- **Leverage label-specific endpoints**: Use dedicated label querying APIs (`/api/v1/labels`, `/api/v1/label/{name}/values`) rather than extracting labels from series or instant query results.
- **Understand API trade-offs**: Label cardinality APIs query only in-memory series in ingesters, while other APIs can query historical data from blocks.

## General query guidelines

### Use appropriate time ranges

- **Limit query time ranges**: Query only the time range you need. Larger time ranges require more compute resources and increase query latency.
- **Consider the `-store.max-labels-query-length` limit**: When querying labels without specifying time ranges, Mimir will query the whole retention period or up to this configured limit.
- **Use appropriate step intervals**: Choose step intervals that match your visualization requirements. Smaller steps increase query cost but provide higher resolution.

### Optimize PromQL queries

- **Use specific label selectors**: Include as many label selectors as possible to reduce the number of series that need to be processed.
- **Avoid high cardinality operations**: Be cautious with functions that can significantly increase cardinality, such as `group_by` with many labels.
- **Use recording rules**: Pre-compute expensive queries using recording rules, especially for dashboard queries that run frequently.

## Performance considerations

### Query complexity

- **Minimize regex usage**: Regular expressions in label selectors can be expensive. Use exact matches when possible.
- **Limit aggregation scope**: When using aggregation functions like `sum()` or `avg()`, group by only the necessary labels.
- **Use rate() for counters**: Always use `rate()` or `irate()` functions when querying counter metrics.

### Resource optimization

- **Batch similar queries**: When possible, combine multiple queries into a single, more complex query to reduce overhead.
- **Use subqueries carefully**: Subqueries can be powerful but may significantly increase query cost.
- **Monitor query performance**: Use Grafana Mimir's query performance metrics to identify slow or expensive queries.

## API usage best practices

### HTTP API optimization

- **Use POST for complex queries**: For queries with many parameters or long PromQL expressions, use POST requests instead of GET.
- **Implement client-side timeouts**: Set appropriate timeouts for your HTTP clients to handle slow queries gracefully.
- **Handle rate limiting**: Implement proper retry logic with exponential backoff for rate-limited requests.

### Mimir label querying best practices

- **Specify time ranges**: Always include `start` and `end` parameters when querying label names or values to limit the scope. Without these, Mimir queries the whole retention period or up to the configured `-store.max-labels-query-length`.
- **Choose the right API for your use case**:
  - Use **[Get label names](../../references/http-api/#get-label-names)** over **[Label names cardinality](../../references/http-api/#label-names-cardinality)** unless you specifically need cardinality counts
  - For multiple label values, issue multiple **Get label values** requests rather than a single **Label values cardinality** call with multiple label names
- **Use series selectors judiciously**: Including series selectors (`match[]` or `selector` parameters) is computationally more expensive but can significantly reduce result set size when the selector is highly selective.
- **Understand data freshness trade-offs**: Cardinality APIs query only in-memory series from ingesters (freshest data), while other APIs can access historical block data.
- **Consider alternatives for small series sets**: If querying label values with a highly selective series selector (few thousand series), consider using the **[Get series by label matchers](../../references/http-api/#get-series-by-label-matchers)** API instead.

## Monitoring and alerting

### Query observability

- **Track query latency**: Monitor query response times to identify performance degradation.
- **Monitor query success rate**: Track the ratio of successful to failed queries.
- **Alert on query timeouts**: Set up alerts for queries that consistently timeout or fail.

### Resource monitoring

- **Watch query load**: Monitor the query load on your Mimir cluster to ensure adequate capacity.
- **Track cache hit rates**: Monitor query result cache hit rates to optimize caching configuration.
- **Monitor ingester load**: Keep an eye on ingester resource usage during high query loads.

## Mimir multi-tenancy considerations

### Tenant isolation and limits

- **Understand tenant-specific limits**: Mimir enforces query limits on a per-tenant basis. Monitor and configure appropriate limits for query concurrency, memory usage, and timeout values.
- **Configure tenant-specific caching**: Use the `cache_unaligned_requests` parameter to enable caching for unaligned queries on specific tenants that require it.
- **Plan for tenant scaling**: Different tenants may have vastly different query patterns and resource requirements. Monitor per-tenant metrics to identify scaling needs.

### Cross-tenant query considerations

- **Tenant data isolation**: Queries are automatically scoped to the requesting tenant's data. There's no risk of accidentally accessing another tenant's metrics.
- **Resource sharing awareness**: High-volume queries from one tenant can impact cluster performance for others. Monitor cluster-wide resource usage alongside per-tenant metrics.

## Configuration optimization

### Query frontend settings

- **Tune caching parameters**: Configure appropriate TTL values for query result caching based on your data freshness requirements and query patterns.
- **Optimize query splitting**: Adjust query sharding and parallelization settings based on your cluster size and query complexity.
- **Configure appropriate timeouts**: Set reasonable query timeout values that balance user experience with resource protection.

### Store gateway optimization

- **Block storage considerations**: Ensure your object storage can handle the query load, especially for historical data queries that access many blocks.
- **Index caching**: Configure appropriate index cache sizes to speed up label and series lookups.
- **Compaction awareness**: Understand how Mimir's compaction process affects query performance and plan maintenance windows accordingly.
