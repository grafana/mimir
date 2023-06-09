---
aliases:
  - ../../operators-guide/deploy-grafana-mimir/jsonnet/migrate-query-scheduler-from-dns-to-ring-based-service-discovery/
description: Learn how to migrate query-scheduler from DNS-based to ring-based service discovery
menuTitle: Migrate query-scheduler to ring-based service discovery
title: Migrate query-scheduler from DNS-based to ring-based service discovery
weight: 50
---

# Migrate query-scheduler from DNS-based to ring-based service discovery

The query-scheduler supports two service discovery mechanisms:

- [DNS-based service discovery]({{< relref "../../references/architecture/components/query-scheduler/index.md#dns-based-service-discovery" >}})
- [Ring-based service discovery]({{< relref "../../references/architecture/components/query-scheduler/index.md#ring-based-service-discovery" >}})

To migrate the query-scheduler from DNS-based to ring-based service discovery when your Mimir cluster is deployed using Jsonnet:

1. Configure the query-scheduler instances to join a ring, but keep the querier and query-frontend instances discovering query-schedulers via DNS:
   ```jsonnet
   {
     _config+:: {
       query_scheduler_service_discovery_mode: 'ring',
       query_scheduler_service_discovery_ring_read_path_enabled: false,
     }
   }
   ```
1. Wait until query-scheduler changes have been applied.
1. Open the [query-scheduler ring status]({{< relref "../../references/http-api/index.md#query-scheduler-ring-status" >}}) page and ensure all query-scheduler instances are registered to the ring.
1. Configure query-frontend and querier instances to discover query-schedulers via the ring:
   ```jsonnet
   {
     _config+:: {
       query_scheduler_service_discovery_mode: 'ring',
       query_scheduler_service_discovery_ring_read_path_enabled: true,
     }
   }
   ```
