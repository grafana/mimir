---
title: "About the key-value store"
description: ""
weight: 10
---

# About the key-value store

A key-value (KV) store is a database that store data indexed by key.
Grafana Mimir requires a key-value store for the following features:

- [Hash ring]({{< relref "./about-the-hash-ring.md" >}})
- [(Optional) Distributor high-availability tracker]({{< relref "../operating-grafana-mimir/configure-ha-deduplication.md" >}})

## Supported key-value store backends

Grafana Mimir supports the following key-value (KV) store backends:

- Gossip-based [memberlist](https://github.com/hashicorp/memberlist) protocol (default)
- [Consul](https://www.consul.io)
- [Etcd](https://etcd.io)

### Gossip-based memberlist protocol (default)

By default, Grafana Mimir instances use a gossip-based protocol to join a memberlist cluster.
The data is shared between the instances using peer-to-peer communication, and no external dependency is required.

We recommend that you use memberlist when you run Grafana Mimir.

To configure memberlist, refer to [memberlist]({{< relref "../configuration/reference-configuration-parameters.md#memberlist">}}).

### Consul

Grafana Mimir supports [Consul](https://www.consul.io) as a backend KV store.
If you decide to use Consul, you must install it. The Grafana Mimir installation does not include Consul.

To configure Consul, refer to [consul]({{< relref "../configuration/reference-configuration-parameters.md#consul">}}).

### Etcd

Grafana Mimir supports [etcd](https://etcd.io) as a backend KV store.
If you choose to use etcd, you must install it. The Grafana Mimir installation does not include etcd.

To configure etcd, refer to [etcd]({{< relref "../configuration/reference-configuration-parameters.md#etcd">}}).
