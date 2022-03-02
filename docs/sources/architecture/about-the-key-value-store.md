---
title: "About the key-value store"
description: ""
weight: 10
---

# About the key-value store

A key-value (KV) store is a database storing data indexed by key.
Grafana Mimir requires a key-value store for two features:

- [Hash ring]({{<relref "./about-the-hash-ring.md">}})
- [(Optional) Distributor high-availability tracker]({{<relref "../operating-grafana-mimir/configure-ha-deduplication.md">}})

## Supported key-value store backends

Grafana Mimir supports the following key-value (KV) store backends:

- Gossip-based [memberlist](https://github.com/hashicorp/memberlist) protocol (default)
- [Consul](https://www.consul.io)
- [Etcd](https://etcd.io)

### Gossip-based memberlist protocol (default)

By default, Grafana Mimir instances join a memberlist cluster using a Gossip based protocol.
The data is shared between the instances using a peer-to-peer communication and no external dependency is required.

This is the recommended way to run Grafana Mimir.

To configure memberlist, refer to the [memberlist]({{< relref "../configuration/reference-configuration-parameters.md#memberlist">}})
block section of the configuration.

### Consul

Grafana Mimir supports [Consul](https://www.consul.io) as a backend KV store.
If you decided to use Consul, you need to install it alongside Grafana Mimir.

To configure Consul, refer to the [consul]({{< relref "../configuration/reference-configuration-parameters.md#consul">}})
block section of the configuration.

### Etcd

Grafana Mimir supports [etcd](https://etcd.io) as a backend KV store.
If you choose to use etcd, you need to install it alongside Grafana Mimir.

Refer to the [etcd](../configuration/reference-configuration-parameters.md#etcd)
block section of the configuration for details on how to configure it.
