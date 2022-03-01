---
title: "Configure HA deduplication"
description: "How to configure Grafana Mimir to handle HA Prometheus server deduplication."
weight: 10
---

# Configure high-availability deduplication

You can have more than one Prometheus instance that scrapes the same metrics for redundancy. Grafana Mimir already performs replication for redundancy,
so you do not need to ingest the same data twice. In Grafana Mimir, you can deduplicate the data that you receive from HA pairs of Prometheus instances.

Assume that there are two teams, each running their own Prometheus instance, which monitors different services: Prometheus `T1` and Prometheus `T2`.
If the teams are running HA pairs, the individual Prometheus instances would be `T1.a` and `T1.b`, and `T2.a` and `T2.b`.

Grafana Mimir only ingests from either `T1.a` or `T1.b`, and only from `T2.a` or `T2.b`. It does this by electing a leader replica for each 
cluster of Prometheus. For example, in the case of `T1`, the leader replica would be `T1.a`. As long as `T1.a` is the leader, the samples
that `T1.b` receives are dropped. And if Grafana Mimir does not see any new samples from `T1.a` for a short period of time (30 seconds by default), it switches the leader to `T1.b`.

If `T1.a` goes down for a few minutes, Grafana Mimir’s HA sample handling will have switched and elected `T1.b` as the leader. The failure 
timeout ensures that too much data is not dropped before failover to the other replica. 

> **Note:** In a scenario where the default scrape period is 15 seconds, and the timeouts in Grafana Mimir are set to the default values, 
> when a leader-election failover occurs, you'll likely only lose a single scrape of data. For any rate query, make the rate window 
> at least four times that of the scrape period to account for any of these failover scenarios. 
> For example with the default scrape period of 15 seconds, calculate rates longer than at least 1-minute intervals.

Repeat the leader-election process for `T2`.

## Distributor high-availability (HA) tracker

The [distributor]({{<relref "../architecture/distributor.md">}}) includes a high-availability (HA) tracker.

The HA tracker deduplicates incoming samples based on a cluster and replica label expected on each incoming series.
The cluster label uniquely identifies the cluster of redundant Prometheus servers for a given tenant.
The replica label uniquely identifies the replica within the Prometheus cluster.
Incoming samples are considered duplicated (and thus dropped) if received from any replica which is not the currently elected as leader within a cluster.

In the event the HA tracker is enabled but incoming samples contain only one or none of the cluster and replica labels, these samples will be accepted by default and never deduplicated.

> Note: for performance reasons, the HA tracker only checks the cluster and replica label of the first series in the request to decide whether all series in the request should be deduplicated. This assumes that all series inside the request have the same cluster and replica labels, which is typically true when Prometheus is configured with external labels. We recommend you to ensure this requirement is honored if you're having a non standard Prometheus setup (eg. you're using Prometheus federation or have a metrics proxy in between).

## Configuration

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

The minimal configuration requires to:

- Enable the HA tracker.
- Configure the HA tracker KV store.
- Configure expected label names for cluster and replica.

#### Enable the HA tracker

To enable the HA tracker feature you need to set the `-distributor.ha-tracker.enable=true` CLI flag (or its YAML config option) in the distributor.

The next step is to decide whether you want to enable it for all tenants or just a subset of them.
To enable it for all tenants you need to set `-distributor.ha-tracker.enable-for-all-users=true`.
Alternatively, you can enable the HA Tracker only on a per-tenant basis, keeping the default `-distributor.ha-tracker.enable-for-all-users=false` and overriding it on a per-tenant basis setting `accept_ha_samples` in the overrides section of the runtime configuration.

#### Configure the HA tracker KV store

The HA Tracker requires a key-value (KV) store to coordinate which replica is currently elected.
The supported KV stores for the HA tracker are `consul` and `etcd`.

> Note: `memberlist` is not supported. Memberlist-based KV stores propagate updates using the gossip protocol, which is too slow for HA tracker. The result would be that different distributors may see a different Prometheus server elected as leaders at the same time.

The following CLI flags (and their respective YAML config options) are available to configure the HA tracker KV store:

- `-distributor.ha-tracker.store`: The backend storage to use (eg. `consul` or `etcd`).
- `-distributor.ha-tracker.consul.*`: The Consul client configuration (should be used only if `consul` is the configured backend storage).
- `-distributor.ha-tracker.etcd.*`: The etcd client configuration (should be used only if `etcd` is the configured backend storage).

#### Configure expected label names for cluster and replica

The HA tracker deduplicates incoming series that have cluster and replica labels.
The name of these labels can be configured both globally and on a per-tenant basis.

Configure the default cluster and replica label names using the following CLI flags (or their respective YAML config options):

- `-distributor.ha-tracker.cluster`: name of the label whose value uniquely identifies a Prometheus HA cluster (defaults to `cluster`).
- `-distributor.ha-tracker.replica`: name of the label whose value uniquely identifies a Prometheus replica within the HA cluster (defaults to `__replica__`).

_The HA label names can be overridden on a per-tenant basis by setting `ha_cluster_label` and `ha_replica_label` in the overrides section of the runtime configuration._

#### Example configuration

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
