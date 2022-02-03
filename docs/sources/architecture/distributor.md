---
title: "Distributor"
description: "Overview of the distributor microservice."
weight: 20
---

# Distributor

The **distributor** service is responsible for handling incoming samples from Prometheus. It's the first stop in the write path for series samples. Once the distributor receives samples from Prometheus, each sample is validated for correctness and to ensure that it is within the configured tenant limits, falling back to defaults in case limits have not been overridden for the specific tenant. Valid samples are then split into batches and sent to multiple [ingesters]({{<relref "./ingester.md">}}) in parallel.

The validation done by the distributor includes:

- The metric labels name are formally correct
- The configured max number of labels per metric is respected
- The configured max length of a label name and value is respected
- The timestamp is not older/newer than the configured min/max time range

Distributors are **stateless** and can be scaled up and down as needed.

## High Availability Tracker

The distributor features a **High Availability (HA) Tracker**. When enabled, the distributor deduplicates incoming samples from redundant Prometheus servers. This allows you to have multiple HA replicas of the same Prometheus servers, writing the same series to Mimir and then deduplicate these series in the Mimir distributor.

The HA Tracker deduplicates incoming samples based on a cluster and replica label. The cluster label uniquely identifies the cluster of redundant Prometheus servers for a given tenant, while the replica label uniquely identifies the replica within the Prometheus cluster. Incoming samples are considered duplicated (and thus dropped) if received by any replica which is not the current primary within a cluster.

The HA Tracker requires a key-value (KV) store to coordinate which replica is currently elected. The distributor will only accept samples from the current leader. Samples with one or no labels (of the replica and cluster) are accepted by default and never deduplicated.

The supported KV stores for the HA tracker are:

- [Consul](https://www.consul.io)
- [Etcd](https://etcd.io)

Note: Memberlist is not supported. Memberlist-based KV stores propagate updates using the gossip protocol, which is very slow for HA purposes: the result is that different distributors may see a different Prometheus server elected as an HA replica, which is definitely not desirable.

For more information, please refer to [config for sending HA pairs data to Mimir](guides/ha-pair-handling.md) in the documentation.

## Hashing

Distributors use consistent hashing, in conjunction with a configurable replication factor, to determine which ingester instance(s) should receive a given series.

The hash is calculated using the metric name, labels and tenant ID.

There is a trade-off associated with including labels in the hash. Writes are more balanced across ingesters, but each query needs to talk to all ingesters since a metric could be spread across multiple ingesters given different label sets.

### The hash ring

A hash ring (stored in a key-value store) is used to achieve consistent hashing for the series sharding and replication across the ingesters. All [ingesters]({{<relref "./ingester.md">}}) register themselves into the hash ring with a set of tokens they own; each token is a random unsigned 32-bit integer. Each incoming series is [hashed](#hashing) in the distributor and then pushed to the ingester which owns the token range for the series hash number plus N-1 subsequent ingesters in the ring, where N is the replication factor.

To do the hash lookup, distributors find the smallest appropriate token whose value is larger than the [hash of the series](#hashing). When the replication factor is larger than 1, the subsequent tokens (clockwise in the ring) that belong to different ingesters will also be included in the result.

The effect of this hash set up is that each token that an ingester owns is responsible for a range of hashes. If there are three tokens with values 0, 25, and 50, then a hash of 3 would be given to the ingester that owns the token 25; the ingester owning token 25 is responsible for the hash range of 1-25.

The supported KV stores for the hash ring are:

- [Consul](https://www.consul.io)
- [Etcd](https://etcd.io)
- Gossip [memberlist](https://github.com/hashicorp/memberlist)

#### Quorum consistency

Since all distributors share access to the same hash ring, write requests can be sent to any distributor and you can setup a stateless load balancer in front of it.

To ensure consistent query results, Mimir uses [Dynamo-style](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) quorum consistency on reads and writes. This means that the distributor will wait for a positive response of at least one half plus one of the ingesters to send the sample to before successfully responding to the Prometheus write request.

## Load balancing across distributors

We recommend randomly load balancing write requests across distributor instances. For example, if you're running Mimir in a Kubernetes cluster, you could run the distributors as a Kubernetes [Service](https://kubernetes.io/docs/concepts/services-networking/service/).
