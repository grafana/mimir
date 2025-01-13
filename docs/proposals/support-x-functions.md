---
title: "0004: Support x-functions"
description: "Support x-functions"
draft: true
---

-

# 0004: Support x-functions

**Authors:** @wilfriedroset

**Date:** 12/2024

**Sponsor(s):** @username of maintainer(s) willing to shepherd this MID

**Type:** Feature

**Status:** Draft

**Related issues/PRs:**

---

## Background

Mimir can be configured to use its own _experimental_ PromQL engine. This new engine provides improved performance and reduced querier resource consumption. It could be used to extend the list of functions supported by the standard PromQL engine.

## Problem statement

Rate and friends functions are known to produce undesired/unexpected results. This is a long standing debates in the Prometheus community. This proposal aims to address the point made by @free in the [original proposal](https://github.com/prometheus/prometheus/issues/3746) and introduce x-functions (`xrate`, `xincrease`, `xdelta`) as defined. Since then other backends have added support for x-functions such as [Thanos](https://thanos.io/tip/components/query.md/) or other types of implementation like [VictoriaMetrics](https://docs.victoriametrics.com/metricsql/#).
This leave users in a complex position:

- Use Mimir as it fit their operational needs vs something else that fit less nicely but have the x-functions.
- Maintain a fork of Mimir with the x-functions.

## Goals

- Give users freedom of choice regarding new proposed implementation of rates.

## Non-Goals

- Add support of x-functions in Prometheus itself
- Challenge the accuracy of Prometheus' rate and friends functions

## Proposals

Note: All proposal are based on the fact that Mimir runs with `-querier.query-engine=mimir`.

### Proposal 0: Do nothing

In this proposal, we consider that the problem should be addressed by the standard PromQL engine.

**Pros:**

- Nothing to be done

**Cons:**

- Although Mimir has the means to address the feature request it will not be addressed.

### Proposal 1: Add x-functions in Mimir PromQL engine

The flag `-query.enable-x-functions` can be used to enable the x-functions. All tenants can use them such as

```
xrate(cortex_cache_memory_items_count{}[$__rate_interval]))
```

**Pros:**

- Tenants can choose between `rate` and `xrate` according to the behavior they expect

**Cons:**

- Tenants must modify all queries no matter where they are (dashboards, rules and adhoc queries). This is also true for community mixins.
- Grafana query builder is not aware of the x-functions and is therefore not as useful as it could be.

### Proposal 2: Replace rate and friends by x-functions in Mimir PromQL engine

This proposal is similar to the proposal 1, except that the `rate` is actually a `xrate`.
This behavior can be controlled by a dedicated flag such as `-querier.replace-rate-by-x-functions`.

**Pros:**

- Tenants do not need to modify all queries
- Grafana query builder stays as it is now and does not need to know about x-functions.

**Cons:**

- Tenants are unaware of which rate algorithm is used for the rate computation.
- Switching of the `rate` functions is instance wide, and may have impacts on existing dashboards created by users on the instance that relies on the previous behavior of `rate`

### Proposal 3: Allow to enable x-functions at the tenant level with Mimir PromQL engine

Note: This is an alternative to proposal 1 which allow a fine-grain configuration

The `x-functions` can be enabled via tenant-level limits:

```
overrides:
  tenantA:
    enable_x_functions: true
```

**Pros:**

- Same as proposal 1
- x-functions can be enabled at the tenant level

**Cons:**

- Same as proposal 1

### Proposal 4: Enable to replace rate and friends by x-functions at the tenant level with Mimir PromQL engine

Note: This is an alternative of the proposal 2

The flag `replace-rate-by-x-functions` can be enabled via the limits at the tenant level

```
overrides:
  tenantA:
    replace_rate_by_x_functions: true
```

**Pros:**

- Same as proposal 2
- x-functions can be replaced at the tenant level, lowering impacts on existing dashboards

**Cons:**

- Tenants are unaware of which rate algorithm is used for the rate computation.

### Proposal 5: Fine grain behavior configuration at the tenant level with Mimir PromQL engine

This proposal aims to balance all Pros/Cons of the above proposals by giving more control to the operator.
Mimir supports a `x_functions` limit defined at the tenant level that controls how rate queries are processed:

- `default`: Only the standard rate and friends functions are available
- `add`: rate and friends co-exists with x-functions
- `replace`: x-functions replace rate and friends

```
overrides:
  tenantA:
    x_functions: <default|add|replace>
```

**Pros:**

- With `replace` Tenants do not need to modify all queries and Grafana query builder remains as it is now and does not need to know about x-functions.
- With `add` Tenants can choose between `rate` and `xrate` according to the behavior they expect.
- As the limit is at the tenant level, operators can adjust the configuration to suit the tenant's needs.

**Cons:**

- Might be more complex to implement than the other proposal

## Other Notes

### References

@free original work:

- https://docs.google.com/document/d/1y2Mp041_2v0blnKnZk7keCnJZICeK2YWUQuXH_m4DVc/edit?tab=t.0#heading=h.bupciudrwmna
- https://github.com/prometheus/prometheus/issues/3746
- https://github.com/prometheus/prometheus/issues/3806

Most recent proposals regarding the same problem:
@fatpat work https://github.com/prometheus/prometheus/pull/13436
@ColinDKelley work https://docs.google.com/document/d/1CF5jhyxSD437c2aU2wHcvg88i8CjSPO3kMHsEaDRe2w/edit?tab=t.0#heading=h.bupciudrwmna

Prometheus [DevSummit notes](https://docs.google.com/document/d/11LC3wJcVk00l8w5P3oLQ-m3Y37iom6INAMEu2ZAGIIE/edit?pli=1&tab=t.0#heading=h.xwbekevs3tsq) where @gouthamve asks `Can we add xrate behind the same feature flag?`
