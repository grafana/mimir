---
title: Configure queries to block
description: Control what queries are sent to your Mimir installation.
weight: 210
---

# Configure queries to block

In certain situations, you might want to control what queries are being sent to your Mimir installation. These queries
might be intentionally or unintentionally expensive to run, and they might affect the overall stability or cost of running
your service.

You can block queries using [per-tenant overrides](../about-runtime-configuration/):

```yaml
overrides:
  "tenant-id":
    blocked_queries:
      # block this query exactly
      - pattern: 'sum(rate(node_cpu_seconds_total{env="prod"}[1m]))'

      # block any query matching this regex pattern
      - pattern: '.*env="prod".*'
        regex: true

      # block this query only if it's a range query and the time range is not aligned to the step (i.e. not eligible for range query result caching)
      - pattern: 'sum(rate(node_cpu_seconds_total{env="prod"}[1m]))'
        unaligned_range_queries: true

      # block queries longer than 7 days
      - time_range_longer_than: 7d
        reason: "queries longer than 7 days are not allowed"

      # combine pattern matching with time range filtering
      - pattern: ".*expensive.*"
        regex: true
        time_range_longer_than: 24h
        reason: "expensive queries over 1 day are blocked"
```

The blocking is enforced on instant and range queries as well as remote read queries.

For instant and range queries the pattern is evaluated against the query, for remote read requests, the pattern is evaluated against each set of matchers, as if the matchers formed a vector selector. If any set of matchers is blocked, the whole remote read request is rejected.

Setting `time_range_longer_than` on a rule blocks queries where the time range duration (calculated as `end - start`) exceeds the specified threshold.
Time range filtering is automatically skipped for instant queries since they query a single point in time.
When combined with pattern matching, both the pattern must match AND the time range must exceed the threshold for the query to be blocked.

Setting `unaligned_range_queries: true` on a rule causes the rule to only block range queries where the time range is not aligned to the step.
Such queries are not eligible for [range query result caching](https://grafana.com/docs/mimir/latest/references/architecture/components/query-frontend/#caching) by default.
This can be useful to discourage unaligned queries without impacting clients that already send aligned requests.
Matching instant queries and remote read requests are not blocked.

For example the remote read query that contains the matcher `__name__` regex matched to `foo.*` is interpreted as `{__name__=~"foo.*"}`. To restrict the blocking to such selectors, include the curly braces in your pattern, e.g. `\{.*foo.*\}`.

To set up runtime overrides, refer to [runtime configuration](../about-runtime-configuration/).

{{% admonition type="note" %}}
The order of patterns is preserved, so the first matching pattern will be used.
{{% /admonition %}}

## Format queries to block

You can ignore this section if you're not using a regular expression. If you are, you need to ensure that it matches against formatted queries as follows.

Use Mimirtool's `mimirtool promql format <query>` command to apply the Prometheus formatter to a query that is expected to be blocked by the regular expression pattern, and then check the formatted query will match the regular expression.

Mimir parses queries into PromQL expressions before blocking is applied. When a regular expression is not used, the pattern from the blocked queries is also similarly parsed before being compared against the formatted representation of the parsed query. When a regular expression is used, the pattern is used as-is. This process allows for consistent query blocking behavior regardless of formatting differences in the submitted queries.

Among other transformations the Prometheus formatter may reorder operators, remove empty selector braces, and eliminate newlines, extraneous whitespace, and comments.

### Formatted query examples

Empty selector braces removed:

```bash
mimirtool promql format 'foo{}'
```

```console
foo
```

Operators reordered:

```bash
mimirtool promql format 'sum(container_memory_rss) by (namespace)'
```

```console
sum by (namespace) (container_memory_rss)
```

Newlines, extra whitespace, and comments eliminated:

```bash
mimirtool promql format '
rate(
  metric_counter[15m] # comment 1
) /
rate(
  other_counter[15m] # comment 2
)
'
```

```console
rate(metric_counter[15m]) / rate(other_counter[15m])
```

## View blocked queries

Blocked queries are logged, as well as counted in the `cortex_query_frontend_rejected_queries_total` metric on a per-tenant basis.
