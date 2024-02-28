---
aliases:
  - ../../operators-guide/deploy-grafana-mimir/jsonnet/configure-deployment-mode/
description: Learn how to configure Grafana Mimir deployment mode.
menuTitle: Configure deployment mode
title: Configure deployment mode
weight: 40
---

# Configure deployment mode

Grafana Mimir supports multiple [deployment modes]({{< relref "../../references/architecture/deployment-modes" >}}). By default, the provided Jsonnet deploys in microservices mode. Monolithic mode is not supported in Jsonnet.

## Use Read-Write deployment mode

{{< admonition type="warning" >}}
Read-Write deployment mode is experimental.
{{< /admonition >}}

### Requirements

Read-write deployment mode requires that you use multi-zone ingesters and multi-zone store gateways. Additionally, rule evaluation is performed within `mimir-backend`, so you must disable ruler remote evaluation.

You can set the deployment mode via the `deployment_mode` configuration variable:

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

    // Disable microservices autoscaling.
    autoscaling_querier_enabled: false,
    autoscaling_ruler_querier_enabled: false,
  }

}
```

You can configure autoscaling for the read path:

```jsonnet
{
  _config+:: {
    autoscaling_mimir_read_enabled: true,
    autoscaling_mimir_read_min_replicas: 2,
    autoscaling_mimir_read_max_replicas: 20,
  }
}
```

CLI flags for read-write components are inherited from the microservices. For example:

```jsonnet
{
  _config+:: {
    // This change also applies to mimir-backend.
    store_gateway_args+:: {
      'blocks-storage.bucket-store.sync-interval': '5m',
    },
  }
}
```

⚠️ Pitfall: Kubernetes resources’ overrides are not inherited. Remember to apply overrides to both microservices and read-write components, if you make changes to any of the following items:

- Container specification, for example environment variables
- Deployment
- StatefulSet
- Service
- PodDisruptionBudget

For example:

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
