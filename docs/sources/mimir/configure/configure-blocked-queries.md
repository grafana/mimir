---
title: Configure queries to block
description: Control what queries are sent to your Mimir installation.
weight: 210
---

# Configure queries to block

In certain situations, you might want to control what queries are being sent to your Mimir installation. These queries
might be intentionally or unintentionally expensive to run, and they might affect the overall stability or cost of running
your service.

Each rule must include a `pattern` field; rules without a `pattern` are silently skipped.
To match all queries, use `pattern: ".*"` with `regex: true`.
Optional filter conditions (`time_range_longer_than`, `minimum_step_size`, `unaligned_range_queries`) narrow which matching queries are blocked; all configured conditions must be satisfied to block.

You can block queries using [per-tenant overrides](../about-runtime-configuration/):

```yaml
overrides:
  "tenant-id":
    blocked_queries:
      # exact match
      - pattern: 'sum(rate(node_cpu_seconds_total{env="prod"}[1m]))'

      # regex match
      - pattern: '.*env="prod".*'
        regex: true

      # match all queries longer than 7 days
      - pattern: ".*"
        regex: true
        time_range_longer_than: 7d
        reason: "queries longer than 7 days are not allowed"

      # match expensive queries longer than 1 day
      - pattern: ".*expensive.*"
        regex: true
        time_range_longer_than: 24h
        reason: "expensive queries over 1 day are blocked"

      # match all queries with a step smaller than 1 minute
      - pattern: ".*"
        regex: true
        minimum_step_size: 1m
        reason: "step resolution too fine-grained"

      # match this query only when the time range is not aligned to the step
      - pattern: 'sum(rate(node_cpu_seconds_total{env="prod"}[1m]))'
        unaligned_range_queries: true
```

The blocking is enforced on instant and range queries as well as remote read queries.

For instant and range queries the pattern is evaluated against the query; for remote read requests, the pattern is evaluated against each set of matchers, as if the matchers formed a vector selector. If any set of matchers is blocked, the whole remote read request is rejected.

Setting `time_range_longer_than` on a rule blocks queries where the time range duration (calculated as `end - start`) exceeds the specified threshold.
Instant queries are never blocked by this filter.

Setting `minimum_step_size` on a rule blocks queries where the step is smaller than the configured duration.
Instant queries and queries with no step are never blocked by this filter.

Setting `unaligned_range_queries: true` on a rule limits it to range queries where the time range is not aligned to the step.
Such queries are not eligible for [range query result caching](https://grafana.com/docs/mimir/latest/references/architecture/components/query-frontend/#caching) by default.
This can be useful to discourage unaligned queries without impacting clients that already send aligned requests.
Instant queries and remote read requests are not blocked.

For remote read requests, each set of matchers is evaluated as a vector selector. For example, a matcher on `__name__` regex-matched to `foo.*` is interpreted as `{__name__=~"foo.*"}`. To restrict the blocking to such selectors, include the curly braces in your pattern, e.g. `\{.*foo.*\}`.

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

Blocked queries are logged with `msg="query blocked"` at info level, including the query text, duration, step, rule index, and reason. Use these fields to identify which rule matched and why.

Blocked queries are also counted in the `cortex_query_frontend_rejected_queries_total` metric per tenant (`user` label) with `reason="blocked"`. Note that the same metric tracks rate-limited queries under `reason="limited"`.
