---
description: Learn how to configure Grafana Mimir deployment mode.
menuTitle: Configure deployment mode
title: Configure deployment mode
weight: 40
---

# Configure deployment mode

Grafana Mimir supports multiple [deployment modes](). By default the provided jsonnet will deploy in microservices mode. Monolithic mode is not current supported in jsonnet.

## Use Read-Write deployment mode

> **Warning:**
> Read-Write deployment mode is currently considered experimental and not ready for production use.

### Requirements

Read-Write deployment mode requires that multi-zone ingesters and multi-zone store gateways are used. Additionally rule evaluation is performed within Mimir-Backend, so ruler remote evaluation must be disabled.

The deployment mode is set by the `deployment_mode` configuration variable.

```jsonnet
{
  _config+:: {
    deployment_mode: 'read-write',

    mimir_write_replicas: 15,
    mimir_read_replicas: 6,
    mimir_backend_replicas: 9,

    // Requirements.
    multi_zone_ingester_enabled: true,
    multi_zone_store_gateway_enabled: true,
    ruler_remote_evaluation_enabled: false,
    // Disable microservice autoscaling.
    autoscaling_querier_enabled: false,
    autoscaling_ruler_querier_enabled: false,
  }

}
```

Autoscaling can be configured for the read path:

```jsonnet
{
  _config+:: {
    autoscaling_mimir_read_enabled: true,
    autoscaling_mimir_read_min_replicas: 2,
    autoscaling_mimir_read_max_replicas: 20,
  }
}
```

CLI Flags for Read-Write components are inherited from the microservices.

```jsonnet
{
  _config+:: {
    // This change will apply to mimir-backend too.
    store_gateway_args+:: {
      'blocks-storage.bucket-store.sync-interval': '5m',
    },
  }
}
```

⚠️ Pitfall: Kubernetes resources overrides not inherited. Remember to apply overrides both microservices and read-write components, when changing:

- Container spec (e.g. env vars)
- Deployment
- StatefulSet
- Service
- PodDisruptionBudget

```jsonnet
{
  _config+:: {
    // This change will NOT be applied to mimir-write too.
    ingester_container+::
      container.withEnvMixin(
        [envVar.new('GOGC', '50')]
      ),

    mimir_write_container+::
      container.withEnvMixin(
        [envVar.new('GOGC', '50')]
      ),

  }
}
```

### Migrating from Microservices to Read-Write deployment mode

> **Warning:**
> Migration between deployment modes is not supported.

1. Deploy read-write components along side microservices with `deployment_mode: 'migration'` (they join the same ring).
1. Switch over end points in your ingress.
1. Decommission microservices.

Steps can be applied vice-versa for migrating from read-write mode to microservices.
