---
aliases:
  - ../../operators-guide/architecture/key-value-store/
description: The key-value store is a database that stores data indexed by key.
menuTitle: Key-value store
title: Grafana Mimir key-value store
weight: 70
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# Grafana Mimir key-value store

A key-value (KV) store is a database that stores data indexed by key.
Grafana Mimir requires a key-value store for the following features:

- [Hash ring](../hash-ring/)
- [(Optional) Distributor high-availability tracker](../../../configure/configure-high-availability-deduplication/)

## Supported key-value store backends

Grafana Mimir supports the following key-value (KV) store backends:

- Gossip-based [memberlist](https://github.com/hashicorp/memberlist) protocol

[Consul](https://www.consul.io) and [Etcd](https://etcd.io) backends are deprecated as of 2.17.

### Gossip-based memberlist protocol

By default, Grafana Mimir instances use a Gossip-based protocol to join a memberlist cluster.
The data is shared between the instances using peer-to-peer communication and no external dependency is required.

We recommend that you use memberlist to run Grafana Mimir.

To configure memberlist, refer to [configuring hash rings](../../../configure/configure-hash-rings/).

As of [Grafana Mimir versions 2.17](https://github.com/grafana/mimir/releases/tag/mimir-2.17.0), the Gossip-based memberlist protocol is supported for the [optional distributor high-availability tracker](../../../configure/configure-high-availability-deduplication/).
