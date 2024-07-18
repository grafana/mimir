---
title: "Migrate from single zone to zone-aware replication in Mimir Helm chart version 4.0"
menuTitle: "Migrate from single zone to zone-aware replication in Mimir Helm chart version 4.0"
description: "Learn how to migrate from having a single availability zone to full zone-aware replication using the Grafana Mimir Helm chart"
weight: 10
---

# Migrate from single zone to zone-aware replication in Mimir Helm chart version 4.0

The `mimir-distributed` Helm chart version 4.0 enables zone-aware replication by default. This is a breaking change for existing installations and requires a migration.

This document explains how to migrate stateful components from single zone to [zone-aware replication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-zone-aware-replication/) with Helm. The three components in question are the [alertmanager](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/alertmanager/), the [store-gateway](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/store-gateway/) and the [ingester](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/ingester/).

The migration path of Alertmanager and store-gateway is straight forward, however migrating ingesters is more complicated.

This document is applicable to both Grafana Mimir and Grafana Enterprise Metrics.

## Prerequisite

Depending on what version of the `mimir-distributed` Helm chart is installed currently, make sure to meet the following requirements.

- If the current version of the `mimir-distributed` Helm chart is less than 4.0.0 (version < 4.0.0).

  1.  Follow the upgrade instructions for 4.0.0 in the [CHANGELOG.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/CHANGELOG.md#400).
      In particular make sure to disable zone awareness before upgrading the chart:

      ```yaml
      ingester:
        zoneAwareReplication:
          enabled: false
      store_gateway:
        zoneAwareReplication:
          enabled: false
      rollout_operator:
        enabled: false
      ```

      {{% admonition type="note" %}}
      A direct upgrade from non-zone aware ingesters to zone-aware ingesters will cause data loss.
      {{% /admonition %}}

  1.  If you have modified the `mimir.config` value, either make sure to merge in the latest version from the chart, or consider using `mimir.structuredConfig` instead.

      For more information, see [Manage the configuration of Grafana Mimir with Helm]({{< relref "../run-production-environment-with-helm/configuration-with-helm" >}}).

- If the current version of the `mimir-distributed` Helm chart is greater than 4.0.0 (version >= 4.0.0).

  1.  Make sure that zone-aware replication is turned off for the component in question.

      For example, the store-gateway:

      ```yaml
      store_gateway:
        zoneAwareReplication:
          enabled: false
      ```

  1.  If you have modified the `mimir.config` value, either make sure to merge in the latest version from the chart, or consider using `mimir.structuredConfig` instead.

      For more information, see [Manage the configuration of Grafana Mimir with Helm]({{< relref "../run-production-environment-with-helm/configuration-with-helm" >}}).

## Migrate alertmanager to zone-aware replication

Using zone-aware replication for alertmanager is optional and is only available if alertmanager is deployed as a StatefulSet.

### Configure zone-aware replication for Alertmanagers

This section is about planning and configuring the availability zones defined under the `alertmanager.zoneAwareReplication` Helm value.

There are two use cases in general:

1. Speeding up rollout of alertmanagers in case there are more than 3 replicas. In this case use the default value in the `small.yaml`, `large.yaml`, `capped-small.yaml` or `capped-large.yaml`. The default value defines 3 "virtual" zones and sets affinity rules so that alertmanagers from different zones do not mix, but it allows multiple alertmanagers of the same zone on the same node:

   ```yaml
   alertmanager:
     zoneAwareReplication:
       topologyKey: "kubernetes.io/hostname" # Triggers creating anti-affinity rules
   ```

1. Geographical redundancy. In this case you need to set a suitable [nodeSelector](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/) value to choose where the pods of each zone are to be placed. Setting `topologyKey` will instruct the Helm chart to create anti-affinity rules so that alertmanagers from different zones do not mix, but it allows multiple alertmanagers of the same zone on the same node. For example:
   ```yaml
   alertmanager:
     zoneAwareReplication:
       topologyKey: "kubernetes.io/hostname" # Triggers creating anti-affinity rules
       zones:
         - name: zone-a
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-a
         - name: zone-b
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-b
         - name: zone-c
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-c
   ```

> **Note**: as the `zones` value is an array, you must copy and modify it to make changes to it, there is no way to overwrite just parts of the array!

Set the chosen configuration in your custom values (e.g. `custom.yaml`).

> **Note**: The number of alertmanager Pods that will be started is derived from `alertmanager.replicas`. Each zone will start `alertmanager.replicas / number of zones` pods, rounded up to the nearest integer value. For example if you have 3 zones, then `alertmanager.replicas=3` will yield 1 alertmanager per zone, but `alertmanager.replicas=4` will yield 2 per zone, 6 in total.

### Migrate Alertmanager

Before starting this procedure, set up your zones according to [Configure zone-aware replication for alertmanagers](#configure-zone-aware-replication-for-alertmanagers).

1. Create a new empty YAML file called `migrate.yaml`.

1. Start the migration.

   Copy the following into the `migrate.yaml` file:

   ```yaml
   alertmanager:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "alertmanager-step1"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

   In this step zone-awareness is enabled with the default zone and new StatefulSets are created for zone-aware alertmanagers, but no new pods are started.

1. Wait until all alertmanagers are restarted and are ready.

1. Scale up zone-aware alertmanagers.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   alertmanager:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true
         writePath: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "alertmanager-step2"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait until all new zone-aware alertmanagers are started and are ready.

1. Set the final configuration.

   **Merge** the following values into your custom Helm values file:

   ```yaml
   alertmanager:
     zoneAwareReplication:
       enabled: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "alertmanager-step3"

1. Upgrade the installation with the `helm` command using your regular command line flags.

1. Ensure that the Service and StatefulSet resources of the non zone-aware alertmanagers have been deleted.
   The previous step also removes the Service and StatefulSet manifests of the old non zone-aware alertmanagers.
   In some cases, such as when using Helm from Tanka, these resources will not be automatically deleted from your Kubernetes cluster
   even if the Helm chart no longer renders them. If the old resources still exist, delete them manually.
   If not deleted, some of the pods may be scraped multiple times when using the Prometheus operator for metamonitoring.

1. Wait until old non zone-aware alertmanagers are terminated.

## Migrate store-gateways to zone-aware replication

### Configure zone-aware replication for store-gateways

This section is about planning and configuring the availability zones defined under the `store_gateway.zoneAwareReplication` Helm value.

There are two use cases in general:

1. Speeding up rollout of store-gateways in case there are more than 3 replicas. In this case use the default value in the `small.yaml`, `large.yaml`, `capped-small.yaml` or `capped-large.yaml`. The default value defines 3 "virtual" zones and sets affinity rules so that store-gateways from different zones do not mix, but it allows multiple store-gateways of the same zone on the same node:

   ```yaml
   store_gateway:
     zoneAwareReplication:
       enabled: false # Do not turn on zone-awareness without migration because of potential query errors
       topologyKey: "kubernetes.io/hostname" # Triggers creating anti-affinity rules
   ```

1. Geographical redundancy. In this case you need to set a suitable [nodeSelector](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/) value to choose where the pods of each zone are to be placed. Setting `topologyKey` will instruct the Helm chart to create anti-affinity rules so that store-gateways from different zones do not mix, but it allows multiple store-gateways of the same zone on the same node. For example:
   ```yaml
   store_gateway:
     zoneAwareReplication:
       enabled: false # Do not turn on zone-awareness without migration because of potential query errors
       topologyKey: "kubernetes.io/hostname" # Triggers creating anti-affinity rules
       zones:
         - name: zone-a
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-a
         - name: zone-b
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-b
         - name: zone-c
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-c
   ```

> **Note**: as `zones` value is an array, you must copy and modify it to make changes to it, there is no way to overwrite just parts of the array!

Set the chosen configuration in your custom values (e.g. `custom.yaml`).

> **Note**: The number of store-gateway pods that will be started is derived from `store_gateway.replicas`. Each zone will start `store_gateway.replicas / number of zones` pods, rounded up to the nearest integer value. For example if you have 3 zones, then `store_gateway.replicas=3` will yield 1 store-gateway per zone, but `store_gateway.replicas=4` will yield 2 per zone, 6 in total.

### Decide which migration path to take for store-gateways

There are two ways to do the migration:

1. With downtime. In this [procedure](#migrate-store-gateways-with-downtime) old non zone-aware store-gateways are stopped, which will cause queries that look back more than 12 hours (or whatever `querier.query_store_after` Mimir parameter is set to) to fail. Ingestion is not impacted. This is the quicker and simpler way.
1. Without downtime. This is a multi step [procedure](#migrate-store-gateways-without-downtime) which requires additional hardware resources as the old and new store-gateways run in parallel for some time.

### Migrate store-gateways with downtime

Before starting this procedure, set up your zones according to [Configure zone-aware replication for store-gateways](#configure-zone-aware-replication-for-store-gateways).

1. Create a new empty YAML file called `migrate.yaml`.

1. Scale the current store-gateways to 0.

   Copy the following into the `migrate.yaml` file:

   ```yaml
   store_gateway:
     replicas: 0
     zoneAwareReplication:
       enabled: false
   ```

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait until all store-gateways have terminated.

1. Set the final configuration.

   **Merge** the following values into your custom Helm values file:

   ```yaml
   store_gateway:
     zoneAwareReplication:
       enabled: true

   rollout_operator:
     enabled: true
   ```

   These values are actually the default, which means that removing the values `store_gateway.zoneAwareReplication.enabled` and `rollout_operator.enabled` is also a valid step.

1. Upgrade the installation with the `helm` command using your regular command line flags.

1. Ensure that the Service and StatefulSet resources of the non zone-aware store-gateways have been deleted.
   The previous step also removes the Service and StatefulSet manifests of the old non zone-aware store-gateways.
   In some cases, such as when using Helm from Tanka, these resources will not be automatically deleted from your Kubernetes cluster
   even if the Helm chart no longer renders them. If the old resources still exist, delete them manually.
   If not deleted, some of the pods may be scraped multiple times when using the Prometheus operator for metamonitoring.

1. Wait until all store-gateways are running and ready.

### Migrate store-gateways without downtime

Before starting this procedure, set up your zones according to [Configure zone-aware replication for store-gateways](#configure-zone-aware-replication-for-store-gateways).

1. Create a new empty YAML file called `migrate.yaml`.

1. Create the new zone-aware store-gateways

   Copy the following into the `migrate.yaml` file:

   ```yaml
   store_gateway:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "store-gateway-step1"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait for all new store-gateways to start up and be ready.

1. Make the read path use the new zone-aware store-gateways.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   store_gateway:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true
         readPath: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "store-gateway-step2"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait for all queriers and rulers to restart and become ready.

1. Set the final configuration.

   **Merge** the following values into your custom Helm values file:

   ```yaml
   store_gateway:
     zoneAwareReplication:
       enabled: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "store-gateway-step3"

   These values are actually the default, which means that removing the values `store_gateway.zoneAwareReplication.enabled` and `rollout_operator.enabled` is also a valid step.

1. Upgrade the installation with the `helm` command using your regular command line flags.

1. Ensure that the Service and StatefulSet resources of the non zone-aware store-gateways have been deleted.
   The previous step also removes the Service and StatefulSet manifests of the old non zone-aware store-gateways.
   In some cases, such as when using Helm from Tanka, these resources will not be automatically deleted from your Kubernetes cluster
   even if the Helm chart no longer renders them. If the old resources still exist, delete them manually.
   If not deleted, some of the pods may be scraped multiple times when using the Prometheus operator for metamonitoring.

1. Wait for non zone-aware store-gateways to terminate.

## Migrate ingesters to zone-aware replication

### Configure zone-aware replication for ingesters

This section is about planning and configuring the availability zones defined under the `ingester.zoneAwareReplication` Helm value.

There are two use cases in general:

1. Speeding up rollout of ingesters in case there are more than 3 replicas. In this case use the default value in the `small.yaml`, `large.yaml`, `capped-small.yaml` or `capped-large.yaml`. The default value defines 3 "virtual" zones and sets affinity rules so that ingesters from different zones do not mix, but it allows multiple ingesters of the same zone on the same node:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: false # Do not turn on zone-awareness without migration because of potential data loss
       topologyKey: "kubernetes.io/hostname" # Triggers creating anti-affinity rules
   ```

1. Geographical redundancy. In this case you need to set a suitable [nodeSelector](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/) value to choose where the pods of each zone are to be placed. Setting `topologyKey` will instruct the Helm chart to create anti-affinity rules so that ingesters from different zones do not mix, but it allows multiple ingesters of the same zone on the same node. For example:
   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: false # Do not turn on zone-awareness without migration because of potential data loss
       topologyKey: "kubernetes.io/hostname" # Triggers creating anti-affinity rules
       zones:
         - name: zone-a
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-a
         - name: zone-b
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-b
         - name: zone-c
           nodeSelector:
             topology.kubernetes.io/zone: us-central1-c
   ```

> **Note**: as `zones` value is an array, you must copy and modify it to make changes to it, there is no way to overwrite just parts of the array!

Set the chosen configuration in your custom values (e.g. `custom.yaml`).

> **Note**: The number of ingester pods that will be started is derived from `ingester.replicas`. Each zone will start `ingester.replicas / number of zones` pods, rounded up to the nearest integer value. For example if you have 3 zones, then `ingester.replicas=3` will yield 1 ingester per zone, but `ingester.replicas=4` will yield 2 per zone, 6 in total.

### Decide which migration path to take for ingesters

There are two ways to do the migration:

1. With downtime. In this [procedure](#migrate-ingesters-with-downtime) ingress is stopped to the cluster while ingesters are migrated. This is the quicker and simpler way. The time it takes to execute this migration depends on how fast ingesters restart and upload their data to object storage, but in general should be finished in an hour.
1. Without downtime. This is a multi step [procedure](#migrate-ingesters-without-downtime) which requires additional hardware resources as the old and new ingesters run in parallel for some time. This is a complex migration that can take days and requires monitoring for increased resource utilization. The minimum time it takes to do this migration can be calculated as (`querier.query_store_after`) + (2h TSDB blocks range period + `blocks_storage.tsdb.head_compaction_idle_timeout`) \* (1 + number_of_ingesters / 21). With the default values this means 12h + 3h \* (1 + number of ingesters / 21) = 15h + 3h \* (number_of_ingesters / 21). Add an extra 12 hours if shuffle sharding is enabled.

### Migrate ingesters with downtime

Before starting this procedure, set up your zones according to [Configure zone-aware replication for ingesters](#configure-zone-aware-replication-for-ingesters).

1. Create a new empty YAML file called `migrate.yaml`.

1. Enable flushing data from ingesters to storage on shutdown.

   Copy the following into the `migrate.yaml` file:

   ```yaml
   mimir:
     structuredConfig:
       blocks_storage:
         tsdb:
           flush_blocks_on_shutdown: true
       ingester:
         ring:
           unregister_on_shutdown: true

   ingester:
     zoneAwareReplication:
       enabled: false
   ```

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait for all ingesters to restart and be ready.

1. Turn off traffic to the installation.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   mimir:
     structuredConfig:
       blocks_storage:
         tsdb:
           flush_blocks_on_shutdown: true
       ingester:
         ring:
           unregister_on_shutdown: true

   ingester:
     zoneAwareReplication:
       enabled: false

   nginx:
     replicas: 0
   gateway:
     replicas: 0
   ```

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait until there is no nginx or gateway running.

1. Scale the current ingesters to 0.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   mimir:
     structuredConfig:
       blocks_storage:
         tsdb:
           flush_blocks_on_shutdown: true
       ingester:
         ring:
           unregister_on_shutdown: true

   ingester:
     replicas: 0
     zoneAwareReplication:
       enabled: false

   nginx:
     replicas: 0
   gateway:
     replicas: 0
   ```

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait until no ingesters are running.

1. Start the new zone-aware ingesters.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: true

   nginx:
     replicas: 0
   gateway:
     replicas: 0

   rollout_operator:
     enabled: true
   ```

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait until all requested ingesters are running and are ready.

1. Enable traffic to the installation.

   **Merge** the following values into your custom Helm values file:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: true

   rollout_operator:
     enabled: true
   ```

   These values are actually the default, which means that removing the values `ingester.zoneAwareReplication.enabled` and `rollout_operator.enabled` is also a valid step.

1. Ensure that the Service and StatefulSet resources of the non zone-aware ingesters have been deleted.
   The previous step also removes the Service and StatefulSet manifests of the old non zone-aware ingesters.
   In some cases, such as when using Helm from Tanka, these resources will not be automatically deleted from your Kubernetes cluster
   even if the Helm chart no longer renders them. If the old resources still exist, delete them manually.
   If not deleted, some of the pods may be scraped multiple times when using the Prometheus operator for metamonitoring.

1. Upgrade the installation with the `helm` command using your regular command line flags.

### Migrate ingesters without downtime

Before starting this procedure, set up your zones according to [Configure zone-aware replication for ingesters](#configure-zone-aware-replication-for-ingesters)

1. Double the series limits for tenants and the ingesters.

   Explanation: while new ingesters are being added, some series will start to be written to new ingesters, however the series will also exist on old ingesters, thus the series will count twice towards limits. Not updating the limits might lead to writes to be refused due to limits violation.

   The `limits.max_global_series_per_user` Mimir configuration parameter has a non-zero default value of 150000. Double the default or your value by setting:

   ```yaml
   mimir:
     structuredConfig:
       limits:
         max_global_series_per_user: 300000 # <-- or your value doubled
   ```

   If you have set the Mimir configuration parameter `ingester.instance_limits.max_series` via `mimir.config` or `mimir.structuredConfig` or via runtime overrides, double it for the duration of the migration.

   If you have set per tenant limits in the Mimir configuration parameters `limits.max_global_series_per_user`, `limits.max_global_series_per_metric` via `mimir.config` or `mimir.structuredConfig` or via runtime overrides, double the set limits. For example:

   ```yaml
   runtimeConfig:
     ingester_limits:
       max_series: X # <-- double it
     overrides:
       tenantA:
         max_global_series_per_metric: Y # <-- double it
         max_global_series_per_user: Z # <-- double it
   ```

1. Create a new empty YAML file called `migrate.yaml`.

1. Start the migration.

   Copy the following into the `migrate.yaml` file:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true
         replicas: 0

   rollout_operator:
     enabled: true
   ```

   [//]: # "ingester-step1"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

   In this step new zone-aware StatefulSets are created - but no new pods are started yet. The parameter `ingester.ring.zone_awareness_enabled: true` is set in the Mimir configuration via the `mimir.config` value. The flag `-ingester.ring.zone-awareness-enabled=false` is set on distributors, rulers and queriers. The flags `-blocks-storage.tsdb.flush-blocks-on-shutdown` and `-ingester.ring.unregister-on-shutdown` are set to true for the ingesters.

1. Wait for all Mimir components to restart and be ready.

1. Add zone-aware ingester replicas, maximum 21 at a time.

   Explanation: while new ingesters are being added, some series will start to be written to new ingesters, however the series will also exist on old ingesters, thus the series will count twice towards limits. Adding only 21 replicas at a time reduces the number of series affected and thus the likelihood of breaching maximum series limits.

   1. Replace the contents of the `migrate.yaml` file with:

      ```yaml
      ingester:
        zoneAwareReplication:
          enabled: true
          migration:
            enabled: true
            replicas: <N>

      rollout_operator:
        enabled: true
      ```

      [//]: # "ingester-step2"

      > **Note**: replace `<N>` with the number of replicas in each step until `<N>` reaches the same number as in `ingester.replicas`, do not increase `<N>` with more than 21 in each step.

   1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

   1. Once the new ingesters are started and are ready, wait at least 3 hours.

      The 3 hours is calculated from 2h TSDB block range period + `blocks_storage.tsdb.head_compaction_idle_timeout` Grafana Mimir parameters to give enough time for ingesters to remove stale series from memory. Stale series will be there due to series being moved between ingesters.

   1. If the current `<N>` above in `ingester.zoneAwareReplication.migration.replicas` is less than `ingester.replicas`, go back and increase `<N>` with at most 21 and repeat these four steps.

1. If you are using [shuffle sharding](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-shuffle-sharding/), it must be turned off on the read path at this point.

   1. Update your configuration with these values and keep them until otherwise instructed.

      ```yaml
      querier:
        extraArgs:
          "querier.shuffle-sharding-ingesters-enabled": "false"
      ruler:
        extraArgs:
          "querier.shuffle-sharding-ingesters-enabled": "false"
      ```

   1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

   1. Wait until queriers and rulers have restarted and are ready.

   1. Monitor resource utilization of queriers and rulers and scale up if necessary. Turning off shuffle sharding may increase resource utilization.

1. Enable zone-awareness on the write path.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true
         writePath: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "ingester-step3"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

   In this step the flag `-ingester.ring.zone-awareness-enabled=false` is removed from distributors and rulers.

1. Once all distributors and rulers have restarted and are ready, wait 12 hours.

   The 12 hours is calculated from the `querier.query_store_after` Grafana Mimir parameter.

1. Enable zone-awareness on the read path.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true
         writePath: true
         readPath: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "ingester-step4"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

   In this step the flag `-ingester.ring.zone-awareness-enabled=false` is removed from queriers.

1. Wait until all queriers have restarted and are ready.

1. Exclude non zone-aware ingesters from the write path.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true
         writePath: true
         readPath: true
         excludeDefaultZone: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "ingester-step5"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

   In this step the flag `-ingester.ring.excluded-zones=zone-default` is added to distributors and rulers.

1. Wait until all distributors and rulers have restarted and are ready.

1. Scale down non zone-aware ingesters to 0.

   Replace the contents of the `migrate.yaml` file with:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: true
       migration:
         enabled: true
         writePath: true
         readPath: true
         excludeDefaultZone: true
         scaleDownDefaultZone: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "ingester-step6"

1. Upgrade the installation with the `helm` command and make sure to provide the flag `-f migrate.yaml` as the last flag.

1. Wait until all non zone-aware ingesters are terminated.

1. Delete the default zone.

   **Merge** the following values into your custom Helm values file:

   ```yaml
   ingester:
     zoneAwareReplication:
       enabled: true

   rollout_operator:
     enabled: true
   ```

   [//]: # "ingester-step7"

   These values are actually the default, which means that removing the values `ingester.zoneAwareReplication.enabled` and `rollout_operator.enabled` from your `custom.yaml` is also a valid step.

1. Upgrade the installation with the `helm` command using your regular command line flags.

1. Ensure that the Service and StatefulSet resources of the non zone-aware ingesters have been deleted.
   The previous step also removes the Service and StatefulSet manifests of the old non zone-aware ingesters.
   In some cases, such as when using Helm from Tanka, these resources will not be automatically deleted from your Kubernetes cluster
   even if the Helm chart no longer renders them. If the old resources still exist, delete them manually.
   If not deleted, some of the pods may be scraped multiple times when using the Prometheus operator for metamonitoring.

1. Wait at least 3 hours.

   The 3 hours is calculated from 2h TSDB block range period + `blocks_storage.tsdb.head_compaction_idle_timeout` Grafana Mimir parameters to give enough time for ingesters to remove stale series from memory. Stale series will be there due to series being moved between ingesters.

1. If you are using [shuffle sharding](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-shuffle-sharding/):

   1. Wait an extra 12 hours.

      The 12 hours is calculated from the `querier.query_store_after` Grafana Mimir parameter. After this time, no series are stored outside their dedicated shard, meaning that shuffle sharding on the read path can be safely enabled.

   1. Remove these values from your configuration:

      ```yaml
      querier:
        extraArgs:
          "querier.shuffle-sharding-ingesters-enabled": "false"
      ruler:
        extraArgs:
          "querier.shuffle-sharding-ingesters-enabled": "false"
      ```

   1. Upgrade the installation with the `helm` command using your regular command line flags.

   1. Wait until queriers and rulers have restarted and are ready.

   1. The resource utilization of queriers and rulers should return to pre-migration levels and you can scale them down to previous numbers.

1. Undo the doubling of series limits done in the first step.

1. Upgrade the installation with the `helm` command using your regular command line flags.
