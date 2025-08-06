---
description: Learn best practices for querying metric data from Grafana Mimir.
title: Query best practices
menuTitle: Query best practices
weight: 1
keywords:
  - query
  - best practices
  - performance
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# Query best practices

This topic provides best practices for querying metric data from Grafana Mimir to ensure optimal performance and reliability.

## General query guidelines

### Use appropriate time ranges

- **Limit query time ranges**: Query only the time range you need. Larger time ranges require more compute resources and increase query latency.
- **Align queries for caching**: Ensure your range queries are aligned (both `start` and `end` parameters are multiples of the `step`) to benefit from query result caching.
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

## Troubleshooting slow queries

### Diagnosis steps

1. **Check query complexity**: Review the PromQL query for unnecessary complexity or high cardinality operations.
2. **Verify time range**: Ensure the query time range is appropriate for your use case.
3. **Examine series cardinality**: Use label cardinality APIs to understand the number of series your query needs to process.
4. **Review query alignment**: Verify that range queries are properly aligned for caching benefits.

### Common issues

- **High cardinality label combinations**: Queries that select series with high cardinality label combinations can be slow.
- **Long time ranges**: Queries spanning long time periods require more resources and time to execute.
- **Complex aggregations**: Multiple levels of aggregation or grouping can impact query performance.

## API usage best practices

### HTTP API optimization

- **Use POST for complex queries**: For queries with many parameters or long PromQL expressions, use POST requests instead of GET.
- **Implement client-side timeouts**: Set appropriate timeouts for your HTTP clients to handle slow queries gracefully.
- **Handle rate limiting**: Implement proper retry logic with exponential backoff for rate-limited requests.

### Label querying

- **Specify time ranges**: Always include `start` and `end` parameters when querying label names or values to limit the scope.
- **Use series selectors**: Include series selectors in label queries to reduce the dataset size.
- **Prefer specific APIs**: Use dedicated label querying APIs rather than extracting labels from general query results.

## Monitoring and alerting

### Query observability

- **Track query latency**: Monitor query response times to identify performance degradation.
- **Monitor query success rate**: Track the ratio of successful to failed queries.
- **Alert on query timeouts**: Set up alerts for queries that consistently timeout or fail.

### Resource monitoring

- **Watch query load**: Monitor the query load on your Mimir cluster to ensure adequate capacity.
- **Track cache hit rates**: Monitor query result cache hit rates to optimize caching configuration.
- **Monitor ingester load**: Keep an eye on ingester resource usage during high query loads.