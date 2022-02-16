---
title: "Configure HA deduplication"
description: "How to configure Grafana Mimir to handle HA Prometheus server deduplication."
weight: 10
---

# Configure HA deduplication

## Context

You can have more than a single Prometheus scraping the same metrics for redundancy. Grafana Mimir already does replication for redundancy and it doesn't make sense to ingest the same data twice. So in Grafna Mimir, we made sure we can dedupe the data we receive from HA Pairs of Prometheus. We do this via the following:

Assume that there are two teams, each running their own Prometheus, monitoring different services. Let's call the Prometheis T1 and T2. Now, if the teams are running HA pairs, let's call the individual Prometheis, T1.a, T1.b and T2.a and T2.b.

Grafana Mimir only ingests from one of T1.a and T1.b, and only from one of T2.a and T2.b. It does this by electing a leader replica for each cluster of Prometheus. For example, in the case of T1, let it be T1.a. As long as T1.a is the leader, we drop the samples received from T1.b. And if Grafana Mimir sees no new samples from T1.a for a short period (30s by default), it'll switch the leader to be T1.b.

This means if T1.a goes down for a few minutes Grafana Mimir's HA sample handling will have switched and elected T1.b as the leader. The failure timeout ensures we don't drop too much data before failover to the other replica. Note that with the default scrape period of 15s, and the default timeouts in Grafana Mimir, in most cases you'll only lose a single scrape of data in the case of a leader election failover. For any rate queries the rate window should be at least 4x the scrape period to account for any of these failover scenarios, for example with the default scrape period of 15s then you should calculate rates over at least 1m periods.

Now we do the same leader election process T2.

## Config

### Client Side

For Grafana Mimir to achieve this, set two identifiers for each Prometheus server: one for the cluster (T1 or T2, for example), and one to identify the replica in the cluster (a or b, for example). The easiest way to do this is to set [external labels](https://prometheus.io/docs/prometheus/latest/configuration/configuration/). The default labels are cluster and __replica__. For example:

```
cluster: prom-team1
__replica__: replica1
```

and

```
cluster: prom-team1
__replica__: replica2
```

Note: These are external labels and have nothing to do with remote_write config.

These two label names are configurable per-tenant within Grafana Mimir, and should be set to something sensible. For example, cluster label is already used by some workloads, and you should set the label to be something else that uniquely identifies the cluster. Good examples for this label-name would be `team`, `cluster`, `prometheus`, etc.

The replica label should be set so that the value for each Prometheus is unique in that cluster. Note: Grafana Mimir drops this label when ingesting data, but preserves the cluster label. This way, your timeseries won't change when replicas change.

### Server Side

The minimal configuration requires:

- Enabling the HA tracker via `-distributor.ha-tracker.enable=true` CLI flag (or its YAML config option)
- Configuring the KV store for the HA Tracker. Only Consul and etcd are currently supported. The multi KV store backend should be used only for live migrations between two different KV store backends.
- Setting the limits configuration to accept samples via `-distributor.ha-tracker.enable-for-all-users=true` (or its YAML config option). To enable the HA Tracker only on a per-tenant basis, one can use the default value of `false` and override it on a per-tenant basis using `accept_ha_samples`.

The following configuration snippet shows an example of the HA tracker config via YAML config file:

```yaml
limits:
  ...
  accept_ha_samples: true
  ...
distributor:
  ...
  ha_tracker:
    enable_ha_tracker: true
    ...
    kvstore:
      [store: <string> | default = "consul"]
      [consul | etcd: <config>]
      ...
  ...
```

For further configuration file documentation, see the [distributor section](../configuration/config-file-reference.md#distributor_config).

The following is a summary of the HA Tracker configuration options:
```
  -distributor.ha-tracker.cluster string
    	Prometheus label to look for in samples to identify a Prometheus HA cluster. (default "cluster")
  -distributor.ha-tracker.consul.acl-token string
    	ACL Token used to interact with Consul.
  -distributor.ha-tracker.consul.client-timeout duration
    	HTTP timeout when talking to Consul (default 20s)
  -distributor.ha-tracker.consul.consistent-reads
    	Enable consistent reads to Consul.
  -distributor.ha-tracker.consul.hostname string
    	Hostname and port of Consul. (default "localhost:8500")
  -distributor.ha-tracker.consul.watch-burst-size int
    	Burst size used in rate limit. Values less than 1 are treated as 1. (default 1)
  -distributor.ha-tracker.consul.watch-rate-limit float
    	Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit. (default 1)
  -distributor.ha-tracker.enable
    	Enable the distributors HA tracker so that it can accept samples from Prometheus HA replicas gracefully (requires labels).
  -distributor.ha-tracker.enable-for-all-users
    	Flag to enable, for all users, handling of samples with external labels identifying replicas in an HA Prometheus setup.
  -distributor.ha-tracker.etcd.dial-timeout duration
    	The dial timeout for the etcd connection. (default 10s)
  -distributor.ha-tracker.etcd.endpoints value
    	The etcd endpoints to connect to.
  -distributor.ha-tracker.etcd.max-retries int
    	The maximum number of retries to do for failed ops. (default 10)
  -distributor.ha-tracker.etcd.password string
    	Etcd password.
  -distributor.ha-tracker.etcd.tls-ca-path string
    	Path to the CA certificates file to validate server certificate against. If not set, the host's root CA certificates are used.
  -distributor.ha-tracker.etcd.tls-cert-path string
    	Path to the client certificate file, which will be used for authenticating with the server. Also requires the key path to be configured.
  -distributor.ha-tracker.etcd.tls-enabled
    	Enable TLS.
  -distributor.ha-tracker.etcd.tls-insecure-skip-verify
    	Skip validating server certificate.
  -distributor.ha-tracker.etcd.tls-key-path string
    	Path to the key file for the client certificate. Also requires the client certificate to be configured.
  -distributor.ha-tracker.etcd.tls-server-name string
    	Override the expected name on the server certificate.
  -distributor.ha-tracker.etcd.username string
    	Etcd username.
  -distributor.ha-tracker.failover-timeout duration
    	If we don't receive any samples from the accepted replica for a cluster in this amount of time we will failover to the next replica we receive a sample from. This value must be greater than the update timeout (default 30s)
  -distributor.ha-tracker.max-clusters int
    	Maximum number of clusters that HA tracker will keep track of for single user. 0 to disable the limit.
  -distributor.ha-tracker.multi.mirror-enabled
    	Mirror writes to secondary store.
  -distributor.ha-tracker.multi.mirror-timeout duration
    	Timeout for storing value to secondary store. (default 2s)
  -distributor.ha-tracker.multi.primary string
    	Primary backend storage used by multi-client.
  -distributor.ha-tracker.multi.secondary string
    	Secondary backend storage used by multi-client.
  -distributor.ha-tracker.prefix string
    	The prefix for the keys in the store. Should end with a /. (default "ha-tracker/")
  -distributor.ha-tracker.replica string
    	Prometheus label to look for in samples to identify a Prometheus HA replica. (default "__replica__")
  -distributor.ha-tracker.store string
    	Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi. (default "consul")
  -distributor.ha-tracker.update-timeout duration
    	Update the timestamp in the KV store for a given cluster/replica only after this amount of time has passed since the current stored timestamp. (default 15s)
  -distributor.ha-tracker.update-timeout-jitter-max duration
    	Maximum jitter applied to the update timeout, in order to spread the HA heartbeats over time. (default 5s)
```