---
title: Configure queries to block
description: Control what queries are sent to your Mimir installation.
weight: 100
---

# Configure queries to block

In certain situations, you might want to control what queries are being sent to your Mimir installation. These queries
might be intentionally or unintentionally expensive to run, and they might affect the overall stability or cost of running
your service.

You can block queries using [per-tenant overrides]({{< relref "./about-runtime-configuration" >}}):

```yaml
overrides:
  "tenant-id":
    blocked_queries:
      # block this query exactly
      - pattern: 'sum(rate(node_cpu_seconds_total{env="prod"}[1m]))'

      # block any query matching this regex pattern
      - pattern: '.*env="prod".*'
        regex: true
```

The blocking is enforced on instant and range queries as well as remote read queries.

For instant and range queries the pattern is evaluated against the query, for remote read requests, the pattern is evaluated against each set of matchers, as if the matchers formed a vector selector. If any set of matchers is blocked, the whole remote read request is rejected.

For example the remote read query that contains the matcher `__name__` regex matched to `foo.*` is interpreted as `{__name__=~"foo.*"}`. To restrict the blocking to such selectors, include the curly braces in your pattern, e.g. `\{.*foo.*\}`.

To set up runtime overrides, refer to [runtime configuration]({{< relref "./about-runtime-configuration" >}}).

{{% admonition type="note" %}}
The order of patterns is preserved, so the first matching pattern will be used.
{{% /admonition %}}

## Format queries to block

Use Mimirtool's `mimirtool promql format <query>` command to apply the Prometheus formatter to a query
for use in a blocked query `pattern`.

Queries received by Mimir are parsed into PromQL expressions before blocking is applied.
The `pattern` from the blocked queries is compared against the formatted representation of the parsed query,
in order to allow consistent query blocking behavior regardless of formatting differences in the submitted queries.

Among other transformations the Prometheus formatter may reorder operators, remove empty selector braces,
and eliminate newlines, extraneous whitespace, and comments.

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
