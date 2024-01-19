---
aliases:
  - ../operators-guide/configure/configure-spread-minimizing-tokens/
description: Learn how to migrate ingesters to spread-minimizing tokens.
menuTitle: Spread-minimizing tokens
title: Migrate ingesters to spread-minimizing tokens
---

# Migrate ingesters to spread-minimizing tokens

If you are running Grafana Mimir, and you want to use the _spread-minimizing token generation strategy_ for your ingesters, we provide a step by step guide on how to enable it. We assume that the ingester time series replication is [configured with enabled zone-awareness](https://grafana.com/docs/mimir/latest/configure/configure-zone-aware-replication/#configuring-ingester-time-series-replication). Note: we strongly recommend to enable this feature if [shuffle sharding](https://grafana.com/docs/mimir/latest/configure/configure-shuffle-sharding/#ingesters-shuffle-sharding) is disabled in your ingesters, or if it is enabled, but most of the tenants of your system use all available ingesters.

For simplicity, let’s assume that there are 3 configured availability zones named `zone-a`, `zone-b` and `zone-c`. Migration to the _spread-minimizing token generation strategy_ is a complex process that has to be performed zone by zone, in order to prevent any data loss.

## Step 1: disable write requests to ingesters from zone-a

In order to do this, configure the following flag on the distributor and the ruler:

```
-ingester.ring.excluded-zones=zone-a
```

Before proceeding to the next step, ensure that there are no write requests to the ingesters from `zone-a`. This can be verified by the following query:

```
sum by(route) (
  rate(
    cortex_request_duration_seconds_count{
      namespace="<your-namespace>",
      container="ingester",
      pod=~"ingester-zone-a-.*",
      route="/cortex.Ingester/Push"}[5m]
  )
)
```

You should see something like this:

![No More Write Requests](no-more-write-requests.png)

## Step 2: shut down ingesters from zone-a

In this step we want to ensure that all the in-memory series of all ingesters from `zone-a` have been flushed to long-term storage, as well as that the ingesters from `zone-a` have been forgotten from the ring. In order to do this, the [/ingester/shutdown](https://github.com/grafana/mimir/blob/main/docs/sources/mimir/references/http-api/index.md#shutdown) endpoint needs to be invoked on all the ingesters from `zone-a`. Before proceeding to the next step, ensure that all the calls have been successfully completed.

## Step 3: enable spread-minimizing tokens for ingesters in zone-a

In order to achieve this, configure the following flags on the ingesters from `zone-a`:

```
-ingester.ring.tokens-file-path=
-ingester.ring.token-generation-strategy=spread-minimizing
-ingester.ring.spread-minimizing-zones=zone-a,zone-b,zone-c
```

**Note**: in our example we used `zone-a,zone-b,zone-c` to denote a comma-separated list of configured availability zones.

Before proceeding to the next step, ensure that all the ingester pods related to `zone-a` are up and running with the new configuration.

### Optional step: in-order registration of ingesters

Mimir gives you a possibility to force the ring to perform an in-order registration of ingesters, i.e., to allow an ingester to register its tokens within the ring only after all previous ingesters (with ID lower than its own ID) have already been registered. Enabling this feature minimizes a probability that a write request that should be handled by an ingester actually arrives to the ring before the ingester is registered within the ring, and therefore gets handled by another ingester, the situation that would introduce some deviation from an optimal load distribution. This feature can be configured as follows:

```
-ingester.ring.spread-minimizing-join-ring-in-order=true
```

## Step 4: re-enable write requests to ingesters from zone-a

In order to achieve this, revert [link Step 1: disable write requests to ingesters from zone-a](#step-1-disable-write-requests-to-ingesters-from-zone-a).

At this point, you can check the number of in-memory time series of ingesters from `zone-a` by the following query:

```
sum by(pod) (
  cortex_ingester_memory_series{
    namespace="<your-namespace>",
    pod=~"ingester-zone-a-.*"}
)
```

If everything went smoothly, you should see something like this:

![Successful migration of ingesters from a zone](migration-of-a-zone.png)

## Step 5: migrate ingesters from zone-b

In order to fulfill this step, repeat steps 1 to 4, where all the occurrences of `zone-a` need to be replaced with `zone-b`.

## Step 6: migrate ingesters from zone-c

In order to fulfill this step, repeat steps 1 to 4, where all the occurrences of `zone-a` need to be replaced with `zone-c`.
