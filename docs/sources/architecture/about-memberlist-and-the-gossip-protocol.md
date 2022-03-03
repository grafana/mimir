---
title: "About memberlist and the gossip protocol"
description: ""
weight: 10
---

# About memberlist and the gossip protocol

[Memberlist](https://github.com/hashicorp/memberlist) is a Go library that manages cluster membership, node failure detection, and message passing using a gossip based protocol.
Memberlist is eventually consistent and network partitions are partially tolerated by attempting to communicate to potentially dead nodes through multiple routes.

By default, Grafana Mimir uses memberlist to implement a [key-value (KV) store]({{< relref "./about-the-key-value-store.md">}}) to share the [hash ring]({{< relref "./about-the-hash-ring.md">}}) data structures between instances.

When using a memberlist-based KV store, each instance maintains a copy of the hash rings.
Each Mimir instance updates a hash ring locally and uses memberlist to propagate the changes to other instances.
Updates generated locally, and updates received from other instances are merged together to form the current state of the ring on the instance itself.

To configure memberlist, refer to [configuring hash rings]({{< relref "../operating-grafana-mimir/configure-hash-ring.md">}}).

## How memberlist propagates hash ring changes

When using a memberlist-based KV store, every Grafana Mimir instance propagates the hash ring data structures to other instances using the following techniques:

1. Propagating only the differences introduced in recent changes.
1. Propagating the full hash ring data structure.

Every `-memberlist.gossip-interval` an instance randomly selects a subset of all Grafana Mimir cluster instances configured by `-memberlist.gossip-nodes` and sends them the latest changes.
This operation is done very frequently and it's the primary technique used to propagate changes.

In addition, every `-memberlist.pullpush-interval` an instance randomly select another instance in the Grafana Mimir cluster and transfers the full content of KV store with all hash rings to it (unless `-memberlist.pullpush-interval` is zero, which disables this behavior).
Once this operation is completed, the two instances have the same exact content of KV store.
This operation is computationally more expensive, it's done less frequently and is used to ensure that the hash rings periodically reconcile to a common state.
