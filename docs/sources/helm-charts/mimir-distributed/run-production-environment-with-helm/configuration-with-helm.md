---
title: "Manage the configuration of Grafana Mimir with Helm"
menuTitle: "Manage the configuration of Grafana Mimir with Helm"
description: "Learn how to customize, secure and update the Grafana Mimir configuration using the Helm chart."
weight: 80
aliases:
  - docs/mimir/latest/operators-guide/run-production-environment-with-helm/configuration-with-helm
  - docs/mimir/latest/operators-guide/running-production-environment-with-helm/configuration-with-helm
---

# Manage the configuration of Grafana Mimir with Helm

The `mimir-distributed` Helm chart provides interfaces to set Grafana Mimir [configuration parameters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/) and to customize how to deploy Grafana Mimir on a Kubernetes cluster.

## Overview

You can either manage the configuration of Grafana Mimir through the Helm chart or supply the configuration through a user-managed object.

If you want to manage the configuration through the Helm chart, refer to [Manage the configuration with Helm](#manage-the-configuration-with-helm).

If you want to manage the configuration externally, refer to [Manage the configuration externally](#manage-the-configuration-externally).

Handling sensitive information, such as credentials, is common between the two methods. Refer to [Inject credentials](#injecting-credentials).

## Manage the configuration with Helm

There are three ways that you can modify configuration parameters:

1. Setting parameters via the `mimir.structuredConfig` value (recommended).
1. Copying the entire `mimir.config` value and modifying the configuration as text (discouraged, unless you want to prevent upgrades of the chart from automatically updating the configuration).
1. Setting extra CLI flags for components individually (discouraged, except for setting availability zones).

Refer to the [Example](#example-of-configuration-managed-with-helm) for a practical application.

> **Limitation:** It's not possible to delete configuration parameters that were set in `mimir.config` through `mimir.structuredConfig`. Instead, set the configuration parameter to its default or to some other value.

### How Grafana Mimir applies the configuration

Grafana Mimir components run with a configuration calculated by the following process:

1. The configuration YAML in `mimir.config` is evaluated as a Helm template. This step ensures that the configuration applies to the Kubernetes cluster where it will be installed. For example, setting up cluster-specific addresses.
1. The values from `mimir.structuredConfig` are recursively merged with `mimir.config`. The values from `mimir.structuredConfig` take precedence over the values in `mimir.config`. The result is again evaluated as a Helm template. This step applies user-specific customizations. For example, S3 storage details.
1. The resulting YAML configuration is then sorted alphabetically and stored in a `ConfigMap` (or `Secret`, depending on the value of `configStorageType`) and provided to all Grafana Mimir components.
1. The configuration file, as well as any extra CLI flags, are provided to the Mimir pods.
1. Each component evaluates the configuration, substituting environment variables as required. Note that extra CLI flags take precedence over the configuration file.

> **Note:** CLI flags are component-specific, and therefore, don't show up in the generated `ConfigMap` (or `Secret`). This makes it less obvious which configuration is running. Use these flags only when necessary.

### Inspect changes to the configuration before upgrade

Follow these steps to inspect which change apply to the configuration.

Preparation:

1. Install the [helm diff](https://github.com/databus23/helm-diff) plugin.
1. Set `configStorageType` value to `ConfigMap`.
1. Inspect changes using the `helm diff` sub command:

```bash
helm -n mimir-test diff upgrade grafana/mimir-distributed -f custom.yaml
```

This command shows the differences between the running installation and the installation that would result from running the `helm upgrade` command. Search for `name: mimir-config` in the output to see the difference in configuration settings. Refer to [Example output of helm diff command](#example-output-of-helm-diff-command) for a concrete example.

> **Note:** You can find CLI flags and their differences in the `Deployment` and `StatefulSet` objects.

### Manage runtime configuration

You can also use the Helm chart to manage runtime configuration files. Runtime configuration files contain configuration parameters and are periodically reloaded while Mimir is running. This process allows you to change a subset of Grafana Mimir’s configuration without having to restart the Grafana Mimir component or instance.

To manage runtime configuration with the Helm chart, add the following stanza to your Helm values file:

```yaml
runtimeConfig:
  overrides:
    tenant-1: # limits for tenant-1 that the whole cluster enforces
      ingestion_tenant_shard_size: _`<SAMPLE_VALUE>`_
      max_global_series_per_user: _`<SAMPLE_VALUE>`_
      max_fetched_series_per_query: _`<SAMPLE_VALUE>`_
```

For more information about runtime configuration in Grafana Mimir, refer to [About Grafana Mimir runtime configuration](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-runtime-configuration/).

## Manage the configuration externally

Prepare the configuration as text, without including Helm template functions or value evaluations. You can include references to environment variables, as explained in [Injecting credentials](#injecting-credentials).

Decide whether you want to use a `ConfigMap` or `Secret` to store the configuration. Handling `ConfigMap` is simpler, but beware of sensitive information.

### Use external ConfigMap

Prepare a `ConfigMap` object that places the configuration under the `mimir.yaml` data key.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-mimir-config
data:
  mimir.yaml: |
    <configuration>
```

Replace `<configuration>` with the configuration as multi-line text, being mindful of indentation. The name `my-mimir-config` is just an example.

Set the following values in your custom values file, or on the Helm command line:

```yaml
useExternalConfig: true
externalConfigSecretName: my-mimir-config
externalConfigVersion: "0"
```

### Use external Secret

Prepare a `Secret` object, where the configuration is base64-encoded and placed under the `mimir.yaml` data key.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-mimir-config
data:
  mimir.yaml: <configuration-base64>
```

Replace `<configuration-base64>` with the configuration encoded as base64 format string. The name `my-mimir-config` is just an example.

Set the following values in your custom values file, or on the Helm command line:

```yaml
useExternalConfig: true
externalConfigSecretName: my-mimir-config
configStorageType: Secret
externalConfigVersion: "0"
```

### Update the configuration

To make components aware of configuration changes, choose one of the following options:

- Update the value in `externalConfigVersion` and run `helm update`.
- Manually restart components affected by the configuration change.

## Inject credentials

You can use the Helm chart value `global.extraEnvFrom` to inject credentials into the runtime environment variables of Grafana Mimir components. The data keys become environment variables and usable in the Grafana Mimir configuration. For example, you can reference `AWS_SECRET_ACCESS_KEY` as `${AWS_SECRET_ACCESS_KEY}` in the configuration. Refer to the [Example](#example-of-configuration-managed-with-helm) for a practical application.

To avoid complications, make sure that the key names in the secret contain only ASCII letters, numbers, and underscores.
Prefer `AWS_SECRET_ACCESS_KEY` over `secret-access-key.aws`.

Grafana Mimir does not track changes to the credentials. If you update the credentials, restart Grafana Mimir pods to use the updated value. To trigger a restart, provide a global [pod annotation](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/) in `global.podAnnotation`, which is applied to all Grafana Mimir pods. Changing the value of the global annotation makes Kubernetes recreate all pods. For example, changing `global.podAnnotations.bucketSecretVersion` from `"0"` to `"1"` triggers a restart. Note that pod annotations can only be strings.

## Example configuration managed with Helm

This example shows how to set up the configuration to use an S3 bucket for blocks storage in a namespace called `mimir-test`.

1. Set up the external blocks storage, in this case S3 with buckets named, for example, `my-blocks-bucket` and `my-ruler-bucket`.

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

   Replace `FAKEACCESSKEY` and `FAKESECRETKEY` with the actual value encoded in base64.

1. Apply the secret to your cluster with the `kubectl` command:

   ```bash
   kubectl -n mimir-test apply -f mysecret.yaml
   ```

1. Prepare your custom values file called `custom.yaml`:

   ```yaml
   global:
     extraEnvFrom:
       - secretRef:
           name: mimir-bucket-secret
     podAnnotations:
       bucketSecretVersion: "0"

   # This turns of the built-in MinIO support
   minio:
     enabled: false

   mimir:
     structuredConfig:
       alertmanager_storage:
         s3:
           bucket_name: my-ruler-bucket
           access_key_id: ${AWS_ACCESS_KEY_ID}
           endpoint: s3.amazonaws.com
           secret_access_key: ${AWS_SECRET_ACCESS_KEY}
       blocks_storage:
         backend: s3
         s3:
           bucket_name: my-blocks-bucket
           access_key_id: ${AWS_ACCESS_KEY_ID}
           endpoint: s3.amazonaws.com
           secret_access_key: ${AWS_SECRET_ACCESS_KEY}
       ruler_storage:
         s3:
           bucket_name: my-ruler-bucket
           access_key_id: ${AWS_ACCESS_KEY_ID}
           endpoint: s3.amazonaws.com
           secret_access_key: ${AWS_SECRET_ACCESS_KEY}
   ```

1. Check the resulting configuration with the `helm` command before installing:

   ```bash
   helm -n mimir-test template mimir grafana/mimir-distributed -f custom.yaml -s templates/mimir-config.yaml
   ```

   You should see the following output:

   ```yaml
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
         filepath: /active-query-tracker/activity.log
       alertmanager:
         data_dir: /data
         enable_api: true
         external_url: /alertmanager
       alertmanager_storage:
         s3:
           access_key_id: ${AWS_ACCESS_KEY_ID}
           bucket_name: my-ruler-bucket
           endpoint: s3.amazonaws.com
           secret_access_key: ${AWS_SECRET_ACCESS_KEY}
       blocks_storage:
         backend: s3
         bucket_store:
           sync_dir: /data/tsdb-sync
         s3:
           access_key_id: ${AWS_ACCESS_KEY_ID}
           bucket_name: my-blocks-bucket
           endpoint: s3.amazonaws.com
           secret_access_key: ${AWS_SECRET_ACCESS_KEY}
         tsdb:
           dir: /data/tsdb
       compactor:
         data_dir: /data
       frontend:
         align_queries_with_step: true
         log_queries_longer_than: 10s
       frontend_worker:
         frontend_address: mimir-query-frontend-headless.test.svc:9095
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
         - dns+mimir-gossip-ring.test.svc.cluster.local:7946
       ruler:
         alertmanager_url: dnssrvnoa+http://_http-metrics._tcp.mimir-alertmanager-headless.test.svc.cluster.local/alertmanager
         enable_api: true
         rule_path: /data
       ruler_storage:
         s3:
           access_key_id: ${AWS_ACCESS_KEY_ID}
           bucket_name: my-ruler-bucket
           endpoint: s3.amazonaws.com
           secret_access_key: ${AWS_SECRET_ACCESS_KEY}
       runtime_config:
         file: /var/mimir/runtime.yaml
       server:
         grpc_server_max_concurrent_streams: 1000
   ```

1. Install the chart with the `helm` command:

   ```bash
   helm -n mimir-test install mimir grafana/mimir-distributed -f custom.yaml
   ```

## Example output of helm diff command

The example is generated with the following steps:

1. Install Grafana Mimir with the `helm` command:

   ```bash
   helm -n test install mimir grafana/mimir-distributed --version 3.0.0
   ```

1. Create a `custom.yaml` file with the following content:

   ```yaml
   mimir:
     structuredConfig:
       alertmanager:
         external_url: https://example.com/alerts
       server:
         log_level: debug
   ```

1. Produce the diff with the `helm` command:

   ```bash
   helm -n test diff upgrade mimir grafana/mimir-distributed --version 3.0.0  -f custom.yaml
   ```

   The output is an excerpt of the real output to reduce the size:

   ```diff
   #... cut for size ...

   test, mimir-config, ConfigMap (v1) has changed:
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
       namespace: "test"
     data:
       mimir.yaml: |
          activity_tracker:
           filepath: /active-query-tracker/activity.log
         alertmanager:
           data_dir: /data
           enable_api: true
   -       external_url: /alertmanager
   +       external_url: https://example.com/alerts
         alertmanager_storage:
           backend: s3
           s3:
             access_key_id: grafana-mimir
             bucket_name: mimir-ruler
             endpoint: mimir-minio.test.svc:9000
             insecure: true
             secret_access_key: supersecret
         blocks_storage:
           backend: s3
           bucket_store:
             sync_dir: /data/tsdb-sync
           s3:
             access_key_id: grafana-mimir
             bucket_name: mimir-tsdb
             endpoint: mimir-minio.test.svc:9000
             insecure: true
             secret_access_key: supersecret
           tsdb:
             dir: /data/tsdb
         compactor:
           data_dir: /data
         frontend:
           align_queries_with_step: true
           log_queries_longer_than: 10s
         frontend_worker:
           frontend_address: mimir-query-frontend-headless.test.svc:9095
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
           - mimir-gossip-ring
         ruler:
           alertmanager_url: dnssrvnoa+http://_http-metrics._tcp.mimir-alertmanager-headless.test.svc.cluster.local/alertmanager
           enable_api: true
           rule_path: /data
         ruler_storage:
           backend: s3
           s3:
             access_key_id: grafana-mimir
             bucket_name: mimir-ruler
             endpoint: mimir-minio.test.svc:9000
             insecure: true
             secret_access_key: supersecret
         runtime_config:
           file: /var/mimir/runtime.yaml
         server:
           grpc_server_max_concurrent_streams: 1000
   +       log_level: debug

   #... cut for size ...

   test, mimir-distributor, Deployment (apps) has changed:
     # Source: mimir-distributed/templates/distributor/distributor-dep.yaml
     apiVersion: apps/v1
     kind: Deployment
     metadata:
       name: mimir-distributor
       labels:
         helm.sh/chart: mimir-distributed-3.0.0
         app.kubernetes.io/name: mimir
         app.kubernetes.io/instance: mimir
         app.kubernetes.io/component: distributor
         app.kubernetes.io/part-of: memberlist
         app.kubernetes.io/version: "2.2.0"
         app.kubernetes.io/managed-by: Helm
       annotations:
         {}
       namespace: "test"
     spec:
       replicas: 1
       selector:
         matchLabels:
           app.kubernetes.io/name: mimir
           app.kubernetes.io/instance: mimir
           app.kubernetes.io/component: distributor
       strategy:
         rollingUpdate:
           maxSurge: 0
           maxUnavailable: 1
         type: RollingUpdate
       template:
         metadata:
           labels:
             helm.sh/chart: mimir-distributed-3.0.0
             app.kubernetes.io/name: mimir
             app.kubernetes.io/instance: mimir
             app.kubernetes.io/version: "2.2.0"
             app.kubernetes.io/managed-by: Helm
             app.kubernetes.io/component: distributor
             app.kubernetes.io/part-of: memberlist
           annotations:
   -         checksum/config: bad33a421a56693ebad68b64ecf407b5e897c3679b1a33b65672dbc4e98e918f
   +         checksum/config: 02f080c347a1fcd6c9e49a38280330378d3afe12efc7151cd679935c96b35b83
           namespace: "test"
         spec:
           serviceAccountName: mimir
           securityContext:
             {}
           initContainers:
             []
           containers:
             - name: distributor
               image: "grafana/mimir:2.2.0"
               imagePullPolicy: IfNotPresent
               args:
                 - "-target=distributor"
                 - "-config.expand-env=true"
                 - "-config.file=/etc/mimir/mimir.yaml"
               volumeMounts:
                 - name: config
                   mountPath: /etc/mimir
                 - name: runtime-config
                   mountPath: /var/mimir
                 - name: storage
                   mountPath: "/data"
                   subPath:
               ports:
                 - name: http-metrics
                   containerPort: 8080
                   protocol: TCP
                 - name: grpc
                   containerPort: 9095
                   protocol: TCP
                 - name: memberlist
                   containerPort: 7946
                   protocol: TCP
               livenessProbe:
                 null
               readinessProbe:
                 httpGet:
                   path: /ready
                   port: http-metrics
                 initialDelaySeconds: 45
               resources:
                 requests:
                   cpu: 100m
                   memory: 512Mi
               securityContext:
                 readOnlyRootFilesystem: true
               env:
               envFrom:
           nodeSelector:
             {}
           affinity:
             podAntiAffinity:
               requiredDuringSchedulingIgnoredDuringExecution:
               - labelSelector:
                   matchExpressions:
                   - key: target
                     operator: In
                     values:
                     - distributor
                 topologyKey: kubernetes.io/hostname
           tolerations:
             []
           terminationGracePeriodSeconds: 60
           volumes:
             - name: config
               configMap:
                 name: mimir-config
                 items:
                   - key: "mimir.yaml"
                     path: "mimir.yaml"
             - name: runtime-config
               configMap:
                 name: mimir-runtime
             - name: storage
               emptyDir: {}
    #... cut for size ...
   ```

   Lines starting with "`-`" are removed and lines starting with "`+`" are added. The change to the annotation `checksum/config` means the pods are restarted when this change is applied.
