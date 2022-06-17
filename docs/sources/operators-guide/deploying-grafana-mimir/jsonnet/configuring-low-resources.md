---
title: "Configuring Grafana Mimir to use low resources with Jsonnet"
menuTitle: "Configuring low resources"
description: "Learn how to configure Grafana Mimir when using Jsonnet."
weight: 20
---

# Configuring Grafana Mimir to use low resources with Jsonnet

This page describes how to configure Jsonnet to deploy Grafana Mimir in a Kubernetes cluster with low CPU and memory resources available.

## Anti-affinity

Given the distributed nature of Mimir, both performance and reliability are improved when pods are spread across different nodes.
For example, losing multiple ingesters can cause data loss, so it's better to distribute them across different nodes.

For this reason, by default, anti-affinity rules are applied to some Kubernetes Deployments and StatefulSets.
These anti-affinity rules can become an issue when playing with Mimir in a single-node Kubernetes cluster.
You can disable anti-affinity by setting the configuration values `_config.<component>_allow_multiple_replicas_on_same_node`.

### Example: disable anti-affinity

```jsonnet
local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    distributor_allow_multiple_replicas_on_same_node: true,
    ingester_allow_multiple_replicas_on_same_node: true,
    ruler_allow_multiple_replicas_on_same_node: true,
    querier_allow_multiple_replicas_on_same_node: true,
    query_frontend_allow_multiple_replicas_on_same_node: true,
    store_gateway_allow_multiple_replicas_on_same_node: true,
  },
}
```

## Resources

Default scaling of Mimir components in the provided Jsonnet is opinionated and based on engineers’ years of experience running it at Grafana Labs.
The default resource requests and limits are also fine-tuned for the provided alerting rules.
For more information, see [Monitoring Grafana Mimir]({{< relref "../../monitoring-grafana-mimir/_index.md" >}}).

However, there are use cases where you might want to change the default resource requests, their limits, or both.
For example, if you are just testing Mimir and you want to run it on a small (possibly one-node) Kubernetes cluster, and you do not have tens of gigabytes of memory or multiple cores to schedule the components, consider overriding the scaling requirements as follows:

```jsonnet
local k = import 'github.com/grafana/jsonnet-libs/ksonnet-util/kausal.libsonnet',
      deployment = k.apps.v1.deployment,
      statefulSet = k.apps.v1.statefulSet;
local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    // ... configuration values
  },

  compactor_container+: k.util.resourcesRequests('100m', '128Mi'),
  compactor_statefulset+: statefulSet.mixin.spec.withReplicas(1),

  distributor_container+: k.util.resourcesRequests('100m', '128Mi'),
  distributor_deployment+: deployment.mixin.spec.withReplicas(2),

  ingester_container+: k.util.resourcesRequests('100m', '128Mi'),
  ingester_statefulset+: statefulSet.mixin.spec.withReplicas(3),

  querier_container+: k.util.resourcesRequests('100m', '128Mi'),
  querier_deployment+: deployment.mixin.spec.withReplicas(2),

  query_frontend_container+: k.util.resourcesRequests('100m', '128Mi'),
  query_frontend_deployment+: deployment.mixin.spec.withReplicas(2),

  store_gateway_container+: k.util.resourcesRequests('100m', '128Mi'),
  store_gateway_statefulset+: statefulSet.mixin.spec.withReplicas(1),

  local smallMemcached = {
    cpu_requests:: '100m',
    memory_limit_mb:: 64,
    memory_request_overhead_mb:: 8,
    statefulSet+: statefulSet.mixin.spec.withReplicas(1),
  },

  memcached_chunks+: smallMemcached,
  memcached_frontend+: smallMemcached,
  memcached_index_queries+: smallMemcached,
  memcached_metadata+: smallMemcached,
}
```

## Ruler

The ruler is an optional component and is therefore not deployed by default when using the jsonnet deployment.
For more information about the ruler, see [Grafana Mimir ruler]({{< relref "../../architecture/components/ruler/index.md" >}}).

It is enabled by adding the following to the `_config` section of the jsonnet:

```jsonnet
  _config+:: {
    ruler_enabled: true
    ruler_client_type: '<type>',
  }
```

The `ruler_client_type` option must be one of: 'local', 'azure', 'aws', 's3'.
For more information about the options available for storing ruler state, see [Grafana Mimir ruler: State]({{< relref "../../architecture/components/ruler/index.md#state" >}}).

To get started, the 'local' can be used for initial testing:

```jsonnet
  _config+:: {
    ruler_enabled: true
    ruler_client_type: 'local',
    ruler_local_directory: '/path/to/local/directory',
  }
```

This type is generally not recommended for production use because:

- If sharding rules over multiple ruler replicas, the same file must be available on all ruler pods and kept in sync.
- It is read-only and therefore does not support rule configuration via the API.

If using object storage, additional configuration options are required:

- Amazon S3 (`s3`)

  - `ruler_storage_bucket_name`
  - `aws_region`

- Google Cloud Storage (`gcs`)

  - `ruler_storage_bucket_name`

- Azure (`azure`)
  - `ruler_storage_bucket_name`
  - `ruler_storage_azure_account_name`
  - `ruler_storage_azure_account_key`

Note: Currently the storage credentials for `s3` and `gcs` must be manually provided using additional command line arguments as necessary.

## Operational modes

The ruler has two operational modes, internal and remote. By default, the jsonnet deploys the ruler using the internal operational mode.
For more information about these modes, see [Operational modes]({{< relref "../../architecture/components/ruler/index.md#operational-modes" >}}).

To enable the remote operational mode, add the following to the jsonnet:

```jsonnet
  _config+:: {
    ruler_remote_evaluation_enabled: true
  }
```

Note that to support this operational mode, a separate query path is deployed to evaluate rules, consisting of three additional Kubernetes Deployments:

- `ruler-query-frontend`
- `ruler-query-scheduler`
- `ruler-querier`

### Migration to remote evaluation

To perform a zero downtime migration from internal to remote rule evaluation, follow these steps:

1. Deploy the following changes to enable remote evaluation in migration mode. This will cause the three new Deployments listed to be started, but will not reconfigure the ruler to use them just yet. Check that pods are successfully starting before moving to the next step.

```jsonnet
  _config+:: {
    ruler_remote_evaluation_enabled: true
    ruler_remote_evaluation_migration_enabled: true
  }
```

1. Deploy the following changes to reconfigure ruler pods to perform remote evaluation.

```jsonnet
  _config+:: {
    ruler_remote_evaluation_enabled: true
  }
```
