---
title: "Manage the configuration of Grafana Mimir with Helm"
menuTitle: "Manage the configuration of Grafana Mimir with Helm"
description: "Learn how to customize, secure and update the Grafana Mimir configuration using the Helm chart."
weight: 80
---

# Manage the configuration of Grafana Mimir with Helm

The `mimir-distributed` Helm chart provides a single interface to set Grafana Mimir [configuration parameters]({{< relref "../configure/reference-configuration-parameters/" >}}) and customize how Grafana Mimir is deployed on a Kubernetes cluster. This document is about the configuration parameters.

## Overview

The Grafana Mimir configuration can be managed through the Helm chart or supplied via a user managed object.

If you want to manage the configuration via the Helm chart, see [Manage the configuration with Helm](#manage-the-configuration-with-helm).

If you want to manage the configuration directly, see [Manage the configuration directly](#manage-the-configration-directly).

Handling sensitive information, such as credentials is common between the two methods, see [Injecting credentials](#injecting-credentials).

## Manage the configuration with Helm

There are three ways configuration parameters can be modified:

1. Copying the whole `mimir.config` value and modifying the configuration as text
1. Setting parameters via the `mimir.structuredConfig` value (recommended)
1. Setting extra CLI flags for components individually

> **Limitation:**: it is not possible to delete configuration parameter settings via `mimir.structuredConfig` that were set in `mimir.config`. Set the configuration parameter to its default or to some other value instead.

Grafana Mimir components are run with a configuration that is calculated from all three:

1. The configuration YAML in `mimir.config` is evaulated for Helm templates. This step is intended to ensure that the configuration applies to the Kubernetes cluster where it will be installed. For example setting up cluster specific addresses.
1. The values from `mimir.structuredConfig` are merged on top and the result is again evaulated for Helm templates. This steps is intended to apply user specific customizations. For example S3 storage details.
1. The resulting YAML configuration is then sorted alphabetically and stored in a `ConfigMap` (or `Secret` depending on the value of `configStorageType`) and provided to all Grafana Mimir components.
1. When Grafana Mimir components are run in pods, the configuration file as well as any extra CLI flags are provided to the component.
1. Each component evaulates the configuration, substituting environment variables as required. Note that extra CLI flags take precedence over the configuration file.

See the [Example](#example-of-configuration-managed-with-helm) for a practical application.

> **Note:**: CLI flags are component specific, thus they will not show up in the generated `ConfigMap` (or `Secret`), making it less obvious what configuration is running. Use only when absolutly necessary.


### Upgrade and apply changes to the configuration

Follow these steps to inspect what change will be applied to the configuration.

Preparation:

1. Install the [helm diff](https://github.com/databus23/helm-diff) plugin.
1. Make sure to use `configStorageType` is set to `ConfigMap`

Inspecting changes with the `helm diff` sub command:

```bash
helm -n mimir-test diff upgrade grafana/mimir-distributed -f custom.yaml
```

This command shows the differences between the running deployment and the deployment that would result from executing the `helm upgrade` command. Search for `name: mimir-config` in the output to see the difference in configuration settings.

> **Note:** CLI flags and their difference are found in the `Deployment` and `StatefulSet` objects.

## Manage the configuration directly

Prepare the configuration as text. It can not include Helm template functions, value evaulations. The configuration may include references to environment variables as explained in [Injecting credentials](#injecting-credentials).

Decide whether you want to use a `ConfigMap` or `Secret` to store the configuration. Handling `ConfigMap` is a little bit simpler, but be ware of sensitive information.

### Use external ConfigMap

Prepare a `ConfigMap` object where the configuration is placed under the `mimir.yaml` data key.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-mimir-config
data:
  mimir.yaml: |
    <configuration>
```

Replace `\<configuration\>` with the configuration as text, be mindful of indentation. The name `my-mimir-config` is just an example.

Set the following value for the Helm chart:

```yaml
useExternalConfig: true
externalConfigSecretName: my-mimir-config
externalConfigVersion: '0'
```

### Use external Secret

Prepare a `Secret` object where the configuration is base64 encoded and placed under the `mimir.yaml` data key.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-mimir-config
data:
  mimir.yaml: <configuration-base64>
```

Replace `\<configuration\>` with the configuration encoded base64 format text. The name `my-mimir-config` is just an example.

Set the following value for the Helm chart:

```yaml
useExternalConfig: true
externalConfigSecretName: my-mimir-config
configStorageType: Secret
externalConfigVersion: '0'
```

### Update the configuration

In order to make components aware of configuration changes, either:

 * Update the value in `externalConfigVersion` and run `helm update`
 * or restart components affected by the configuration change manually

## Injecting credentials

Credentials should be kept in `Secret` objects or in a credential vault. The Helm chart value `global.extraEnvFrom` can be used to inject the credentials into the runtime environment variables of the Grafana Mimir components. The data keys will become environment variables and usable in the Grafana Mimir configuration. For example `AWS_SECRET_ACCESS_KEY` can be referenced as `${AWS_SECRET_ACCESS_KEY}` in the configuration. See the [Example](#example-of-configuration-managed-with-helm) for a practical application.

Grafana Mimir will not keep track of changes to the credentials. If the credentials change, Grafana Mimir services should be restarted to use the new value. An easy way to trigger such restart is to provide a `global.podAnnotation` which will be applied to all Grafana Mimir components. Changing the value of the global annotation will instruct Kubernetes to restart the components. For example changing `global.podAnnotations.bucketSecretVersion` from `'0'` to `'1'` triggers a restart - note that [pod annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/) can only be strings.

## Example of configuration managed with Helm

This example show how to set up the configuration to use an S3 bucket for blocks storage. We assume that the namespace in use is called `mimir-test`.

1. Set up the external blocks storage, in this case S3 with buckets named for example `my-blocks-bucket`, `my-ruler-bucket` and in case of Grafana Enterprise Metrics `my-admin-bucket`.

1. Create an external secret with the S3 credentials by writing the following to a `mysecret.yaml` file:

   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: mimir-bucket-secret
   data:
     AWS_ACCESS_KEY_ID: FAKEACCESSKEY
     AWS_SECRET_ACCESS_KEY: FAKESECRETKEY
   ```

1. Apply the secret to your cluster with the `kubectl` command:

   ```bash
   kubectl -n mimir-test apply -f `mysecret.yaml`
   ```

1. Prepare your custom values file called `custom.yaml`:

   ```yaml
   global:
     extraEnvFrom:
       - secretRef:
         name: mimir-bucket-secret
     podAnnotations:
       bucketSecretVersion: "0"

   mimir:
     structuredConfig:
       # Uncomment in case of Grafana Enterprise Metrics 
       # admin_client:
       #   storage:
       #     s3:
       #       bucket_name: my-admin-bucket
       alertmanager_storage:
         s3:
           bucket_name: my-ruler-bucket
       blocks_storage:
         backend: s3
         s3:
           bucket_name: my-blocks-bucket
       ruler_storage:
         s3:
           bucket_name: my-ruler-bucket
       common:
         storage:
           backend: s3
           s3:
             access_key_id: ${AWS_ACCESS_KEY_ID}
             endpoint: s3.amazonaws.com
             secret_access_key: ${AWS_SECRET_ACCESS_KEY}
   ```

1. Check the resulting configuration with the `helm` command before installing:

   ```bash
   helm -n mimir-test template mimir grafana/mimir-distributed -f custom.yaml -s templates/mimir-config.yaml
   ---
   # Source: mimir-distributed/templates/mimir-config.yaml
   apiVersion: v1
   kind: ConfigMap
   metadata:
   name: mimir-config
   labels:
       helm.sh/chart: mimir-distributed-3.0.0
       app.kubernetes.io/name: mimir
       app.kubernetes.io/instance: mimir
       app.kubernetes.io/version: "2.2.0"
       app.kubernetes.io/managed-by: Helm
   namespace: "mimir-test"
   data:
   mimir.yaml: |

    activity_tracker:
      filepath: /data/metrics-activity.log
    alertmanager:
      data_dir: /data
      enable_api: true
      external_url: /alertmanager
    alertmanager_storage:
      s3:
        bucket_name: my-ruler-bucket
    blocks_storage:
      backend: s3
      bucket_store:
        sync_dir: /data/tsdb-sync
      s3:
        bucket_name: my-blocks-bucket
      tsdb:
        dir: /data/tsdb
    common:
      storage:
        backend: s3
        s3:
          access_key_id: ${AWS_ACCESS_KEY_ID}
          endpoint: s3.amazonaws.com
          secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    compactor:
      data_dir: /data
    frontend:
      align_queries_with_step: true
      log_queries_longer_than: 10s
    frontend_worker:
      frontend_address: mimir-query-frontend-headless.mimir-test.svc:9095
    ingester:
      ring:
        final_sleep: 0s
        num_tokens: 512
        unregister_on_shutdown: false
    ingester_client:
      grpc_client_config:
        max_recv_msg_size: 104857600
        max_send_msg_size: 104857600
    limits: {}
    memberlist:
      abort_if_cluster_join_fails: false
      compression_enabled: false
      join_members:
      - dns+mimir-gossip-ring.mimir-test.svc.cluster.local:7946
    ruler:
      alertmanager_url: dnssrvnoa+http://_http-metrics._tcp.mimir-alertmanager-headless.mimir-test.svc.cluster.local/alertmanager
      enable_api: true
      rule_path: /data
    ruler_storage:
      s3:
        bucket_name: my-ruler-bucket
    runtime_config:
      file: /var/mimir/runtime.yaml
    server:
      grpc_server_max_concurrent_streams: 1000
   ```
