# Compartments Design

## What is a compartment

A compartment is a group of Mimir pods that handles a portion of the load of the entire cell. All compartment pods run in the same namespace, but they're managed by different Deployments and StatefulSets.

There are two, distinct, sets of compartments:

* Write compartments
* Read compartments

The number of write compartments is not required to be the same as the number of read compartments.

## Write path

The flow of write requests is:

* The cortex-gw receives a write request
* The cortex-gw sends the request to a random distributor running in a random compartment
* The distributor shards the series by **read** compartment first, and then by the number of partitions in each read compartment

Each write compartment runs a dedicated pool of Warpstream write agents, running on a dedicated Warpstream virtual cluster (VC). This allows scaling the Warpstream RSM by sharding the processing of RSM mutations across multiple CPU cores (the RSM processing of each single tenant is single threaded, but by using different VCs each VC RSM is processed in parallel).

Each Warpstream VC runs on a dedicated object storage bucket to overcome rate limiting issues. In the write path, to successfully ingest a write request, Warpstream needs to write to 1 single bucket (the bucket of the write compartment where the request is handled).

Each write compartment runs the same number of distributor pods. Each write compartment load is balanced (no hotspot compartments).

## Sharding

The sharding technique used in write and read compartments is different:

* **Write compartments**: Write requests are randomly distributed between compartments
* **Read compartments**: Series are sharded to read compartments based on the metric name, and then – within a compartment – they're sharded between partitions using the series labels hash sharding already used by Mimir

## Read path

Each read compartment runs ingesters, block-builders, store-gateways and compactors, responsible to manage the series within that compartment. Ingesters are the most demanding component given they need real time consumption from Warpstream.

An ingester owns a partition of a read compartment, and it consumes its own partition from all VCs used by write compartments. Each read compartment uses a dedicated topic, to simplify partition management (we have an ingester-0 in each read compartment, and each ingester-0 owns partition 0 but of a different topic).

For example, the ingester-0 in the read-comp-1 consumes partition 0 from the "read-comp-1" topic of all VCs used by write compartments.

## Implementation Notes

### Topic model

Each read compartment has a dedicated Kafka/Warpstream topic. The distributor writes to the correct topic based on which read compartment a series is assigned to. This means the Writer must support writing to multiple topics (not just one default topic).
