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
```

## Block queries based on time range

You can block queries based on their time range duration (calculated as `end - start` from the query parameters). This is useful for preventing queries that span too long or too short of a time period:

```yaml
overrides:
  "tenant-id":
    blocked_queries:
      # Block queries longer than 7 days
      - time_range_longer_than: 7d
        reason: "queries longer than 7 days are not allowed"

      # Block queries shorter than 5 minutes
      - time_range_shorter_than: 5m
        reason: "queries shorter than 5 minutes are not useful"

      # Block queries OUTSIDE acceptable window (too short or too long)
      # This blocks queries < 7d OR > 21d, allowing 7-21 day queries
      - time_range_shorter_than: 7d
        time_range_longer_than: 21d
        reason: "queries must be between 7 and 21 days"

      # Block queries INSIDE a problematic window
      # This blocks queries between 2-3h (both conditions must be true)
      - time_range_longer_than: 2h
        time_range_shorter_than: 3h
        reason: "queries between 2 and 3 hours hit a performance cliff"

      # Combine pattern matching with time range filtering
      - pattern: ".*expensive.*"
        regex: true
        time_range_longer_than: 24h
        reason: "expensive queries over 1 day are blocked"
```

### Time range filtering details

- **Duration format**: Supports Prometheus duration strings: `ms`, `s`, `m`, `h`, `d`, `w`, `y`
  - Examples: `5m`, `1h`, `24h`, `7d`, `2w`
- **Query time range**: The time range is calculated as `end - start` from the query parameters
- **Instant queries**: Time range filtering is automatically skipped for instant queries since they query a single point in time
- **Optional fields**: Both `time_range_longer_than` and `time_range_shorter_than` are optional. You can use one, both, or neither
- **Window behavior**: When both thresholds are specified, the blocking mode depends on their relationship:
  - **Outside window** (`longer_than >= shorter_than`): Blocks queries too short OR too long
    - Example: `shorter_than: 7d, longer_than: 21d` blocks queries < 7d OR > 21d (allows 7-21 day queries)
    - Use case: Define an acceptable query duration range
  - **Inside window** (`longer_than < shorter_than`): Blocks queries falling between the thresholds (both conditions must be true)
    - Example: `longer_than: 2h, shorter_than: 3h` blocks queries between 2-3 hours (allows queries < 2h or > 3h)
    - Use case: Block a specific problematic time range while allowing shorter and longer queries
- **Combination with patterns**: When both pattern and time range filters are specified, the pattern must match AND the time range must violate the threshold for the query to be blocked
- **Debug logging**: Blocked queries include a `time_range_position` field in logs indicating whether the query duration was "before" (too short), "after" (too long), or "inside" the configured thresholds

### Use cases

- **Prevent expensive long-range queries**: Block queries spanning more than a specific duration to reduce system load
- **Enforce query granularity**: Block very short queries that may not provide useful results for your use case
- **Protect specific metrics**: Combine pattern matching with time range limits to protect expensive metrics only over long periods
- **Block problematic time ranges**: Block queries in a specific duration range that causes performance issues while allowing shorter and longer queries

The blocking is enforced on instant and range queries as well as remote read queries.

For instant and range queries the pattern is evaluated against the query, for remote read requests, the pattern is evaluated against each set of matchers, as if the matchers formed a vector selector. If any set of matchers is blocked, the whole remote read request is rejected.

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
