---
title: "0001: Query blocking"
description: "Query blocking"
draft: true
---

# 0001: Query blocking

**Author:** @wilfriedroset

**Date:** 05/2023

**Sponsor(s):** @dimitarvdimitrov

**Type:** Feature

**Status:** Accepted

**Related issues/PRs:**

- https://github.com/grafana/mimir/pull/5609

---

## Background

The `query-frontend` is a component that is responsible for query acceleration through splitting, sharding, caching, and other techniques. It is also responsible for enforcing limits on queries such as total query time range.

## Problem Statement

In certain situations, you may not be able to control the queries being sent to your Mimir installation. These queries may be intentionally or unintentionally expensive to run, and they may affect the overall stability or cost of running your service.

## Goals

- Prevent harmful queries to be processed by Mimir's read path.

## Non-Goals (optional)

- Rewrite queries in a non harmful way.
- Optimize queries on the fly.
- Blocked remote read.
- Block similar queries with a single definition. Blocking `metric_name{label1=value1, label2=value2}` does not automatically block `metric_name{label2=value2, label1=value1}`.
- Lint or re-format queries.

## Proposals

### Proposal 0: Do nothing

Current tenants isolation through `shuffle-sharding` are sufficient to keep the read path healthy. Mimir's operators can use limits such as `max_fetched_chunks_per_query`, `max_queriers_per_tenant` or `store_gateway_tenant_shard_size` to limits tenants processing capabilities.

**Pros:**

- Nothing to be done

**Cons:**

- Harmful queries are still processed by Mimir but in a limited part of the cluster

### Proposal 1: Query blocking

Based on the configuration, Mimir's [limitsMiddleware](https://github.com/grafana/mimir/blob/main/pkg/frontend/querymiddleware/limits.go)) ran by the `query-frontend` would be responsible for blocking queries. Operators would be able to block queries either for all tenants or on a per-tenant basis via the _runtime configuration_.

```yaml
overrides:
  "tenant-id":
    blocked_queries:
      # block this query exactly
      - pattern: 'sum(rate({env="prod"}[1m]))'

      # block any query matching this regex pattern
      - pattern: '="\.\*"' # all queries with a label value `.*`
        regex: true
```

The order of patterns is preserved, so the first matching pattern will be used.

PromQL queries might contains special characters used in regexp (`[](){},-`) or regexp.
As such, a list of blocked queries, either regex or non-regex, is preferred over a single regex. This would avoid a eventual escaping problems.
To accommodate multi-lines promQL queries, Mimir's operators can use YAML multi-line strings.

Blocked queries must be logged, as well as counted in the `cortex_query_frontend_blocked_queries_total` metric on a per-tenant basis.

**Pros:**

- Mimir's operators have a new way to ensure the read path stability.
- Blocked queries can be defined either at the cluster level or tenant level.

**Cons:**

- Depending on the configuration assessing if a query should be blocked or not could be expensive, however the cost saving out weight this cons.
- Any changes made to the queries would allow the tenants to escape the protection.

## Other Notes

- Loki has a [similar feature](https://grafana.com/docs/loki/latest/operations/blocking-queries/).
