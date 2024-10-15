---
aliases:
  - ../../operators-guide/deploy-grafana-mimir/jsonnet/migrate-from-microservices-to-read-write/
description:
  Migrate from a cluster deployed as microservices to one in read-write
  mode.
menuTitle: Migrate from microservices to read-write mode
title: Migrate from microservices to read-write mode without downtime
weight: 40
---

# Migrate from microservices to read-write mode without downtime

{{< admonition type="warning" >}}
Read-Write mode, and migrating between modes, is experimental.
{{< /admonition >}}

At a high level, the steps involved are as follows:

1. Deploy read-write components alongside microservices; they both join the same ring.
1. Switch over end points in your ingress.
1. Decommission microservices.

Steps can be applied vice-versa for migrating from read-write mode to microservices.

## Step 1: Configure prerequisite zone-awareness

Read-write mode requires that you enable [multi-zone ingesters and store-gateways]({{< relref "../../configure/configure-zone-aware-replication#enabling-zone-awareness-via-the-grafana-mimir-jsonnet" >}}).

```jsonnet
{
  _config+:: {
    multi_zone_ingester_enabled: true,
    multi_zone_store_gateway_enabled: true,
  },
}
```

[Ruler remote evaluation]({{< relref "./configure-ruler#operational-modes" >}}) is also required to be disabled, however this is done later in the migration process if you are presently using it in microservices.

## Step 2: Deploy read-write components with 0 replicas

By setting the `deployment_mode` to `migration`, jsonnet will configure the components for both read-write and microservices. To begin with we want to set their replicas to 0.

```jsonnet
{
  _config+:: {
    deployment_mode: 'migration',

    mimir_write_replicas: 0,
    mimir_read_replicas: 0,
    mimir_backend_replicas: 0,
    autoscaling_mimir_read_enabled: false,
  },
}
```

### Optionally double check configuration

Optionally at this point you may like to compare the component configuration between microservice and read-write modes.

For example:

```bash
# Export all of the Kubernetes objects to yaml:

kubectl get -o yaml deployment distributor > distributor.yaml; yq eval -i '.spec' distributor.yaml
kubectl get -o yaml deployment overrides-exporter > overrides-exporter.yaml; yq eval -i '.spec' overrides-exporter.yaml
kubectl get -o yaml deployment querier > querier.yaml; yq eval -i '.spec' querier.yaml
kubectl get -o yaml deployment query-frontend > query-frontend.yaml; yq eval -i '.spec' query-frontend.yaml
kubectl get -o yaml deployment query-scheduler > query-scheduler.yaml; yq eval -i '.spec' query-scheduler.yaml
kubectl get -o yaml deployment ruler > ruler.yaml; yq eval -i '.spec' ruler.yaml
kubectl get -o yaml deployment ruler-querier > ruler-querier.yaml; yq eval -i '.spec' ruler-querier.yaml
kubectl get -o yaml deployment ruler-query-frontend > ruler-query-frontend.yaml; yq eval -i '.spec' ruler-query-frontend.yaml
kubectl get -o yaml deployment ruler-query-scheduler > ruler-query-scheduler.yaml; yq eval -i '.spec' ruler-query-scheduler.yaml
kubectl get -o yaml deployment mimir-read > mimir-read.yaml; yq eval -i '.spec' mimir-read.yaml
kubectl get -o yaml statefulset compactor > compactor.yaml; yq eval -i '.spec' compactor.yaml
kubectl get -o yaml statefulset ingester-zone-a > ingester-zone-a.yaml; yq eval -i '.spec' ingester-zone-a.yaml
kubectl get -o yaml statefulset ingester-zone-b > ingester-zone-b.yaml; yq eval -i '.spec' ingester-zone-b.yaml
kubectl get -o yaml statefulset ingester-zone-c > ingester-zone-c.yaml; yq eval -i '.spec' ingester-zone-c.yaml
kubectl get -o yaml statefulset store-gateway-zone-a > store-gateway-zone-a.yaml; yq eval -i '.spec' store-gateway-zone-a.yaml
kubectl get -o yaml statefulset store-gateway-zone-b > store-gateway-zone-b.yaml; yq eval -i '.spec' store-gateway-zone-b.yaml
kubectl get -o yaml statefulset store-gateway-zone-c > store-gateway-zone-c.yaml; yq eval -i '.spec' store-gateway-zone-c.yaml
kubectl get -o yaml statefulset mimir-write-zone-a > mimir-write-zone-a.yaml; yq eval -i '.spec' mimir-write-zone-a.yaml
kubectl get -o yaml statefulset mimir-write-zone-b > mimir-write-zone-b.yaml; yq eval -i '.spec' mimir-write-zone-b.yaml
kubectl get -o yaml statefulset mimir-write-zone-c > mimir-write-zone-c.yaml; yq eval -i '.spec' mimir-write-zone-c.yaml
kubectl get -o yaml statefulset mimir-backend-zone-a > mimir-backend-zone-a.yaml; yq eval -i '.spec' mimir-backend-zone-a.yaml
kubectl get -o yaml statefulset mimir-backend-zone-b > mimir-backend-zone-b.yaml; yq eval -i '.spec' mimir-backend-zone-b.yaml
kubectl get -o yaml statefulset mimir-backend-zone-c > mimir-backend-zone-c.yaml; yq eval -i '.spec' mimir-backend-zone-c.yaml

# Diff deployments and statefulsets:

## Write
diff --color=always distributor.yaml mimir-write-zone-a.yaml
diff --color=always ingester-zone-a.yaml mimir-write-zone-a.yaml
diff --color=always ingester-zone-b.yaml mimir-write-zone-b.yaml
diff --color=always ingester-zone-c.yaml mimir-write-zone-c.yaml

## Read
diff --color=always query-frontend.yaml mimir-read.yaml
diff --color=always querier.yaml mimir-read.yaml
diff --color=always ruler-query-frontend.yaml mimir-read.yaml
diff --color=always ruler-querier.yaml mimir-read.yaml

## Backend
diff --color=always overrides-exporter.yaml mimir-backend-zone-a.yaml
diff --color=always query-scheduler.yaml mimir-backend-zone-a.yaml
diff --color=always ruler-query-scheduler.yaml mimir-backend-zone-a.yaml
diff --color=always ruler.yaml mimir-backend-zone-a.yaml
diff --color=always compactor.yaml mimir-backend-zone-a.yaml
diff --color=always store-gateway-zone-a.yaml mimir-backend-zone-a.yaml
diff --color=always store-gateway-zone-b.yaml mimir-backend-zone-b.yaml
diff --color=always store-gateway-zone-c.yaml mimir-backend-zone-c.yaml
```

## Step 3: Migrate read-path to read-write service

### Step 3.1: Scale up read component

Scale up the Mimir read component using either the autoscaler or explicitly setting the number of replicas. (Keep the current microservices at their present level of replicas or autoscaling).

```jsonnet
{
  _config+:: {
    deployment_mode: 'migration',

    mimir_write_replicas: 0,
    mimir_read_replicas: 3,
    mimir_backend_replicas: 0,
    autoscaling_mimir_read_enabled: false,
  },
}
```

```jsonnet
{
  _config+:: {
    deployment_mode: 'migration',

    mimir_write_replicas: 0,
    mimir_backend_replicas: 0,
    autoscaling_mimir_read_enabled: true,
    autoscaling_mimir_read_min_replicas: 3,
    autoscaling_mimir_read_max_replicas: 30,
  },
}
```

The Read-write querier at this point will start running queries from the `query-scheduler` (as they share the same ring).

### Step 3.2: Check mimir-read is working

Perform a test query by port-forwarding to `mimir-read`.

Ensure `mimir-read` is running queries:

`sum by (pod) (rate(cortex_querier_request_duration_seconds_count{job=~".*mimir-read.*))", route=~"(prometheus|api_prom)_api_v1_.+"}[1m]))`

### Step 3.3: Route traffic to mimir-read

Configure your load-balancer to route read requests to `mimir-read`.

Ensure the `query-frontend` microservice is no longer getting requests:

`sum by(pod) (rate(cortex_query_frontend_queries_total{pod!~"ruler-query-frontend.*"}[1m]))`

## Step 4: Migrate backend components to backend service

### Step 4.1: Scale up backend component

Scale up the Mimir backend component.

```jsonnet
{
  _config+:: {
    mimir_backend_replicas: 3,
  },
}
```

### Step 4.2: Check mimir-backend is working

Check the following rings:

- Query-scheduler ring should include both microservices and read-write components.
- Store-gateway ring should include both microservices and read-write components.
- Compactor ring should include both microservices and read-write components.
- Ruler ring should include both microservices and read-write components.

Run a few test queries on long-term data.

### Step 4.3: Route traffic to mimir-backend

Configure your load-balancer to route `compactor` and `ruler` endpoints to `mimir-backend`.

## Step 5: Scale down microservice read and backend components

Now that we have `mimir-read` and `mimir-backend` scaled up and receiving traffic, we can safely decommission the microservices on those paths.

First though, configure the microservice `store-gateway` to leave the ring:

```jsonnet
{
  // Configure microservices store-gateway to leave the ring.
  store_gateway_args+:: {
    'store-gateway.sharding-ring.unregister-on-shutdown': true,
  },

  mimir_backend_args+:: {
    'store-gateway.sharding-ring.unregister-on-shutdown': false,
  },
}
```

Then scale down the replicas for all components:

```jsonnet
{
  _config+:: {
    multi_zone_store_gateway_replicas: 0,
    autoscaling_querier_enabled: false,
  },

  query_frontend_deployment+:
    deployment.mixin.spec.withReplicas(0),

  query_scheduler_deployment+:
    deployment.mixin.spec.withReplicas(0),

  querier_deployment+:
    deployment.mixin.spec.withReplicas(0),

  ruler_deployment+:
    deployment.mixin.spec.withReplicas(0),

  overrides_exporter_deployment+:
    deployment.mixin.spec.withReplicas(0),

  compactor_statefulset+:
    statefulSet.mixin.spec.withReplicas(0),
}

```

Ensure backend components (`query-scheduler`, `compactor`, `ruler`, `store-gateway`) correctly left their respective rings (Query-scheduler, Compactor, Ruler, Store-gateway).

It is now safe to disable [ruler remote evaluation]({{< relref "./configure-ruler#operational-modes" >}}). (This needs to be done after the microservices ruler has been scaled down, otherwise rule evaluations may fail).

```jsonnet
{
  _config+:: {
    ruler_remote_evaluation_enabled: false,
    autoscaling_ruler_querier_enabled: false,
  },
}
```

## Step 6: Migrate write path to read-write deployment

### Step 6.1: Scale up write component

Scale up `mimir-write`.

```jsonnet
{
  _config+:: {
    mimir_write_replicas: 3,
  },
}
```

### Step 6.2: Route traffic to mimir-write

Configure your load-balancer to route write requests to `mimir-write`.

Ensure the microservice distributor is no longer receiving write requests:

`sum by (job) (rate(cortex_request_duration_seconds_count{job=~".*distributor.*", route=~"/distributor.Distributor/Push|/httpgrpc.*|api_(v1|prom)_push|otlp_v1_metrics"}[1m]))`

## Step 7: Scale down write microservices

### Step 7.1: Scale down distributors

Set `distributor` replicas to `0`:

```jsonnet
{
  distributor_deployment+:
    deployment.mixin.spec.withReplicas(0),
}
```

Wait the next TSDB head compaction for ingesters (2 hours).

### Step 7.2: Scale down ingesters

{{< admonition type="warning" >}}
You must follow the shutdown ingester procedure to avoid data loss.
{{< /admonition >}}

Follow the procedure for [shutting down ingesters]({{< relref "../../manage/run-production-environment/scaling-out.md#scaling-down-ingesters-deployed-in-multiple-zones" >}}) in `ingester-zone-a`.

Scale down zone-a replicas (this can be done before waiting for step 4 in the shutdown procedure):

```jsonnet
{
  ingester_zone_a_statefulset+:
    statefulSet.mixin.spec.withReplicas(0),
}
```

Wait the required amount of time as per step 4 in the shutdown procedure.

Repeat shutting down and scaling down ingesters for zone-b and zone-c waiting the required amount of time between each zone.

## Step 8: Final cleanup

Now that migration is complete you can clean up your deployment and jsonnet.

Changing `deployment_mode` to the final state of `read-write` will remove all of the microservice Kubernetes objects.

```jsonnet
{
  _config+:: {
    deployment_mode: 'read-write',
  },
}
```

As the objects such as `query_frontend_deployment` are no longer defined, you'll also need to remove the scaling that we did for those components. This is a good time to remove any other left over scaling or microservice configuration you may have set.

Lastly you can remove and release any now unused volumes from microservices. For example, to get a list of unused PVC's:

`kubectl get pvc --no-headers | grep -E '(ingester|store-gateway|compactor)' | awk '{print $1}'`
