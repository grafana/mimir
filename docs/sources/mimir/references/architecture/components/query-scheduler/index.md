---
aliases:
  - ../../../operators-guide/architecture/components/query-scheduler/
description: The query-scheduler distributes work to queriers.
menuTitle: (Optional) Query-scheduler
title: (Optional) Grafana Mimir query-scheduler
weight: 120
---

# (Optional) Grafana Mimir query-scheduler

The query-scheduler is an optional, stateless component that retains a queue of queries to execute, and distributes the workload to available [queriers]({{< relref "../querier" >}}).

![Query-scheduler architecture](query-scheduler-architecture.png)

[//]: # "Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit"

The following flow describes how a query moves through a Grafana Mimir cluster:

1. The [query-frontend]({{< relref "../query-frontend" >}}) receives queries, and then either splits and shards them, or serves them from the cache.
1. The query-frontend enqueues the queries into a query-scheduler.
1. The query-scheduler stores the queries in an in-memory queue where they wait for a querier to pick them up.
1. Queriers pick up the queries, and executes them.
1. The querier sends results back to query-frontend, which then forwards the results to the client.

## Benefits of using the query-scheduler

Query-scheduler enables the scaling of query-frontends. You might experience challenges when you scale query-frontend. To learn more about query-frontend scalability limits, refer to [Why query-frontend scalability is limited]({{< relref "../query-frontend#why-query-frontend-scalability-is-limited" >}}).

### How query-scheduler solves query-frontend scalability limits

When you use the query-scheduler, the queue is moved from the query-frontend to the query-scheduler, and the query-frontend can be scaled to any number of replicas.

The query-scheduler is affected by the same scalability limits as the query-frontend, but because a query-scheduler replica can handle high amounts of query throughput, scaling the query-scheduler to a number of replicas greater than `-querier.max-concurrent` is typically not required, even for very large Grafana Mimir clusters.

## Configuration

To use the query-scheduler, query-frontends and queriers need to discover the addresses of query-scheduler instances.
The query-scheduler supports two service discovery mechanisms:

- DNS-based service discovery
- Ring-based service discovery

### DNS-based service discovery

To use the query-scheduler with DNS-based service discovery, configure the query-frontends and queriers to connect to the query-scheduler:

- Query-frontend: `-query-frontend.scheduler-address`
- Querier: `-querier.scheduler-address`

{{< admonition type="note" >}}
The configured query-scheduler address should be in the `host:port` format.

If multiple query-schedulers are running, the host should be a DNS name resolving to all query-scheduler instances.
{{< /admonition >}}

{{< admonition type="note" >}}
The querier pulls queries only from the query-frontend or the query-scheduler, but not both.
`-querier.frontend-address` and `-querier.scheduler-address` options are mutually exclusive and you can only set one option.
{{< /admonition >}}

### Ring-based service discovery

To use the query-scheduler with ring-based service discovery, configure the query-schedulers to join their hash ring, and the query-frontends and queriers to discover query-scheduler instances via the ring:

1. [Configure the hash ring]({{< relref "../../../../configure/configure-hash-rings" >}}) for the query-scheduler.
1. Set `-query-scheduler.service-discovery-mode=ring` (or its respective YAML configuration parameter) to query-scheduler, query-frontend and querier.
1. Set the `-query-scheduler.ring.*` flags (or their respective YAML configuration parameters) to query-scheduler, query-frontend and querier.

#### Migrate from DNS-based to ring-based service discovery

To migrate the query-scheduler from [DNS-based service discovery](#dns-based-service-discovery) to [ring-based service discovery](#ring-based-service-discovery), perform the following steps:

1. Configure the **query-scheduler** instances to join a ring:

   ```
   -query-scheduler.service-discovery-mode=ring

   # Configure the query-scheduler ring backend (e.g. "memberlist").
   -query-scheduler.ring.store=<backend>

   # If the configured <backend> is "memberlist", then ensure memberlist is configured for the query-scheduler.
   -memberlist.join=<same as other Mimir components>

   # If the configured <backend> is "consul" or "etcd", then set their backend configuration
   # for the query-scheduler ring:
   # - Consul: -query-scheduler.ring.consul.*
   # - Ecd:    -query-scheduler.ring.etcd.*
   ```

1. Wait until the query-scheduler instances have completed rolling out.
1. Ensure the changes have been successfully applied; open the [query-scheduler ring status]({{< relref "../../../http-api#query-scheduler-ring-status" >}}) page and ensure all query-scheduler instances are registered to the ring.
   At this point, queriers and query-frontend are still discovering query-schedulers via DNS.
1. Configure **query-frontend** and **querier** instances to discover query-schedulers via the ring:

   ```
   -query-scheduler.service-discovery-mode=ring

   # Remove the DNS-based service discovery configuration:
   # -query-frontend.scheduler-address

   # Configure the query-scheduler ring backend (e.g. "memberlist").
   -query-scheduler.ring.store=<backend>

   # If the configured <backend> is "memberlist", then ensure memberlist is configured for the query-scheduler.
   -memberlist.join=<same as other Mimir components>

   # If the configured <backend> is "consul" or "etcd", then set their backend configuration
   # for the query-scheduler ring:
   # - Consul: -query-scheduler.ring.consul.*
   # - Ecd:    -query-scheduler.ring.etcd.*
   ```

{{< admonition type="note" >}}
If you deploy your Mimir cluster with Jsonnet, refer to [Migrate query-scheduler from DNS-based to ring-based service discovery]({{< relref "../../../../set-up/jsonnet/migrate-query-scheduler-from-dns-to-ring-based-service-discovery" >}}).
{{< /admonition >}}

## Operational considerations

For high-availability, run two query-scheduler replicas.

If you're running a Grafana Mimir cluster with a very high query throughput, you can add more query-scheduler replicas.
If you scale the query-scheduler, ensure that the number of replicas you add is less or equal than the configured `-querier.max-concurrent`.
