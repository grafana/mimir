---
description: Learn how to query metric data from Grafana Mimir.
title: Query metric data from Grafana Mimir
menuTitle: Query
weight: 44
keywords:
  - query
---

# Query metric data from Grafana Mimir

You can query data from Grafana Mimir via Grafana or the Grafana Mimir HTTP API.

The [Grafana Mimir HTTP API](https://grafana.com/docs/mimir/latest/references/http-api/) is compatible with the [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/).

To understand how you can query Prometheus data from within Mimir, refer to [Querying Prometheus](https://prometheus.io/docs/prometheus/latest/querying/basics/), which introduces you to Prometheus Query Language (PromQL).

## Understand how range queries are cached

By default, Mimir [caches](https://grafana.com/docs/mimir/latest/references/architecture/components/query-frontend/#caching) a [range query](https://grafana.com/docs/mimir/latest/references/http-api/#range-query) only if the query is aligned. A query is aligned when both the `start` and `end` parameters are multiples of its `step`.

Mathematically, it looks as follows:

```
start modulo step = 0
```

AND

```
end modulo step = 0
```

Otherwise, a query is unaligned and it is not cached.

If you are querying from within Grafana, queries are aligned for you automatically.
If you invoke the [Grafana Mimir HTTP API](https://grafana.com/docs/mimir/latest/references/http-api/) directly, make sure that your [range queries](https://grafana.com/docs/mimir/latest/references/http-api/#range-query) are aligned in order to benefit from caching.

{{% admonition type="caution" %}}
If you do want to cache unaligned queries, configure Mimir to enable caching on a per-tenant basis via the `cache_unaligned_requests` parameter. For more information, refer to [limits](https://grafana.com/docs/mimir/next/configure/configuration-parameters/#limits).
{{% /admonition %}}

{{< section menuTitle="true" >}}
