---
title: "Configure HA deduplication"
description: "How to configure Grafana Mimir to handle HA Prometheus server deduplication."
weight: 10
---

# Configure HA deduplication

## Context

You can have more than a single Prometheus scraping the same metrics for redundancy. Grafana Mimir already does replication for redundancy and it doesn't make sense to ingest the same data twice. So in Grafana Mimir, we made sure we can dedupe the data we receive from HA Pairs of Prometheus. We do this via the following:

Assume that there are two teams, each running their own Prometheus, monitoring different services. Let's call the Prometheis `T1` and `T2`. Now, if the teams are running HA pairs, let's call the individual Prometheis, `T1.a`, `T1.b` and `T2.a` and `T2.b`.

Grafana Mimir only ingests from one of `T1.a` and `T1.b`, and only from one of `T2.a` and `T2.b`. It does this by electing a leader replica for each cluster of Prometheus. For example, in the case of `T1`, let it be `T1.a`. As long as `T1.a` is the leader, we drop the samples received from `T1.b`. And if Grafana Mimir sees no new samples from `T1.a` for a short period (30s by default), it'll switch the leader to be `T1.b`.

This means if `T1.a` goes down for a few minutes Grafana Mimir's HA sample handling will have switched and elected `T1.b` as the leader. The failure timeout ensures we don't drop too much data before failover to the other replica. Note that with the default scrape period of 15s, and the default timeouts in Grafana Mimir, in most cases you'll only lose a single scrape of data in the case of a leader election failover. For any rate queries the rate window should be at least 4x the scrape period to account for any of these failover scenarios, for example with the default scrape period of 15s you should calculate rates over at least 1m periods.

Now we do the same leader election process `T2`.

## Config

### How to configure Prometheus

For Grafana Mimir to achieve this, set two identifiers for each Prometheus server: one for the cluster (`T1` or `T2`, for example), and one to identify the replica in the cluster (`a` or `b`, for example). The easiest way to do this is to set [external labels](https://prometheus.io/docs/prometheus/latest/configuration/configuration/). The default labels are `cluster` and `__replica__`. For example:

```
global:
  external_labels:
    cluster: prom-team1
    __replica__: replica1
```

and

```
global:
  external_labels:
    cluster: prom-team1
    __replica__: replica2
```

Note: These are external labels and have nothing to do with `remote_write` config.

These two label names are configurable on a per-tenant basis within Grafana Mimir. For example, if the label name of one cluster is used by some workloads, set the label name of another cluster to be something else that uniquely identifies the second cluster. Some examples might include, `team`, `cluster`, or `prometheus`.

The replica label should be set so that the value for each Prometheus is unique in that cluster. Note: Grafana Mimir drops this label when ingesting data, but preserves the cluster label. This way, your timeseries won't change when replicas change.

### How to configure Grafana Mimir

The minimal configuration requires:

- Enabling the HA tracker via `-distributor.ha-tracker.enable=true` CLI flag (or its YAML config option)
- Configuring the KV store for the HA Tracker. Only `consul` and `etcd` are currently supported. The `multi` KV store backend should be used only for live migrations between two different KV store backends.
- Enable the HA tracker for all tenants via `-distributor.ha-tracker.enable-for-all-users=true` (or its YAML config option). Alternatively, to enable the HA Tracker only on a per-tenant basis, you can keep the default `-distributor.ha-tracker.enable-for-all-users=false` and override it on a per-tenant basis using `accept_ha_samples` in the overrides section of the runtime configuration.

The following configuration snippet shows an example to enable the HA tracker for all tentants via YAML config file:

```yaml
limits:
  accept_ha_samples: true
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      [store: <string> | default = "consul"]
      [consul | etcd: <config>]
```

For further configuration file documentation, see the [distributor section](../configuration/config-file-reference.md#distributor). The HA Tracker flags are all prefixed with `-distributor.ha-tracker.*`.
