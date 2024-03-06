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

To set up runtime overrides, refer to [runtime configuration]({{< relref "./about-runtime-configuration" >}}).

{{% admonition type="note" %}}
The order of patterns is preserved, so the first matching pattern will be used.
{{% /admonition %}}

## View blocked queries

Blocked queries are logged, as well as counted in the `cortex_query_frontend_rejected_queries_total` metric on a per-tenant basis.
