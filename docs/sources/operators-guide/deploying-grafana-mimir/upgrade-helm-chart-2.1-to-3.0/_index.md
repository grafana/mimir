---
title: "Upgrade the Grafana Mimir Helm chart from version 2.1 to 3.0"
menuTitle: "Upgrade Helm chart 2.1 to 3.0"
description: "Upgrade the Grafana Mimir Helm chart from version 2.1 to 3.0"
weight: 100
---

# Upgrade the Grafana Mimir Helm chart from version 2.1 to 3.0

There are breaking changes between the Grafana Mimir Helm chart versions 2.1 and 3.0.
Several parameters that were available in version 2.1 of the mimir-distributed Helm chart have changed.

**To upgrade from Helm chart 2.1 to 3.0:**

1. Decide whether or not you need to change the storage location of your configuration:

   The configuration of Mimir is now stored in a Kubernetes ConfigMap by default, instead of a Kubernetes Secret.

   - If you are using external configuration (`useExternalConfig: true`), then you must set `configStorageType: Secret`.

     > **Note:** It is now possible to use a ConfigMap to manage your external configuration instead. For more information see [(Optional) Use the new, simplified configuration method](#optional-use-the-new-simplified-configuration-method).

   - If you are not using external configuration (`useExternalConfig: false`), and your Mimir configuration contains secrets, chose one of two options:

     - Keep the previous location as-is by setting `configStorageType: Secret`.
     - Externalize secrets:

       1. Move secrets from the Mimir configuration to a [Kubernetes Secret](https://kubernetes.io/docs/concepts/configuration/secret/#working-with-secrets).
       2. Mount the Kubernetes Secret via `global.extraEnvFrom`:

          ```yaml
          global:
            extraEnvFrom:
              - secretRef:
                  name: mysecret
          ```

          For more information, see [Secrets - Use case: As container environment variables](https://kubernetes.io/docs/concepts/configuration/secret/#use-case-as-container-environment-variables).

       3. Replace the values in the Mimir configuration with environment variables.

          For example:

          ```yaml
          mimir:
            structuredConfig:
              blocks_storage:
                s3:
                  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
          ```

          For more information about `mimir.structuredConfig` see [(Optional) Use the new, simplified configuration method](#optional-use-the-new-simplified-configuration-method).

   - If you are not using an external configuration (`useExternalConfig: false`), and your Mimir configuration does not contain secrets, then the storage location is automatically changed by Helm and you do not need to do anything.

1. Decide whether or not you need to update the memcached configuration, which has changed:

   The mimir-distributed Helm chart supports multiple cache types.
   If you have not enabled any memcached caches,
   and you are not overriding the values of `memcached`, `memcached-queries`, `memcached-metadata`, or `memcached-results` sections,
   then you do not need to update the memcached configuration.

   Otherwise, check to see if you need to change any of the following configuration parameters:

   - The `memcached` section was repurposed, and `chunks-cache` was added.
   - The contents of the `memcached` section now contain the following common values that are shared across all memcached instances: `image`, `podSecurityContext`, and `containerSecurityContext`.
   - The following sections were renamed:
     - `memcached-queries` is now `index-cache`
     - `memcached-metadata` is now `metadata-cache`
     - `memcached-results` is now `results-cache`
   - The `memcached*.replicaCount` values were renamed:
     - `memcached.replicaCount` is now `chunks-cache.replicas`
     - `memcached-queries.replicaCount` is now `index-cache.replicas`
     - `memcached-metadata.replicaCount` is now `metadata-cache.replicas`
     - `memcached-results.replicaCount` is now `results-cache.replicas`
   - The `memcached*.architecture` values were removed.
   - The `memcached*.arguments` values were removed.
   - The default arguments are now encoded in the Helm chart templates; the values `*-cache.allocatedMemory`, `*-cache.maxItemMemory` and `*-cache.port` control the arguments `-m`, `-I` and `-u`. To provide additional arguments, use `*-cache.extraArgs`.
   - The `memcached*.metrics` values were consolidated under `memcachedExporter`.
   - The following examples show memcached configurations in version 2.1 and version 3.0:

     Version 2.1:

     ```yaml
     memcached:
       replicaCount: 12
       arguments:
         - -m 2048
         - -I 128m
         - -u 12345
       image:
         repository: memcached
         tag: 1.6.9-alpine

     memcached-queries:
       replicaCount: 3
       architecture: modern
       image:
         repository: memcached
         tag: 1.6.9-alpine
     ```

     Version 3.0:

     ```yaml
     memcached:
       image:
         repository: memcached
         tag: 1.6.9-alpine

     chunks-cache:
       allocatedMemory: 2048
       maxItemMemory: 128
       port: 12345
       replicas: 12

     index-cache:
       replicas: 3
     ```

1. (Conditional) If you have enabled `serviceMonitor`, or you are overriding the value of anything under the `serviceMonitor` section, or both, then move the `serviceMonitor` section under `metaMonitoring`.

1. Update the `rbac` section, based on the following changes:

   - If you are not overriding the value of anything under the `rbac` section, then skip this step.
   - The `rbac.pspEnabled` value was removed.
   - To continue using Pod Security Policy (PSP), set `rbac.create` to `true` and `rbac.type` to `psp`.
   - To start using Security Context Constraints (SCC) instead of PSP, set `rbac.create` to `true` and `rbac.type` to `scc`.

1. Update the `mimir.config` value, based on the following information:
   - Consider first using the [new simplified configuration method](#optional-use-the-new-simplified-configuration-method). It may significantly reduce the size of `mimir.config`.
   - Compare your overridden value of `mimir.config` with the one in the `values.yaml` file in the chart. If you are not overriding the value of `mimir.config`, then skip this step.
   - The service names for memcached caches have changed.
     If you previously copied the value of `mimir.config` into your values file,
     then take the latest version of the `memcached` configuration in the `mimir.config` from the `values.yaml` file in the Helm chart.
1. Decide whether or not to update the `nginx` configuration:

   - Unless you have overridden the value of `nginx.nginxConfig.file`,
     and you are using the default `mimir.config`, then skip this step.
   - Otherwise, compare the overridden `nginx.nginxConfig.file` value
     to the one in the `values.yaml` file in the Helm chart,
     and incorporate the differences.
     Pay attention to the sections that contain `x_scope_orgid`.
     The value in the `values.yaml` file contains Nginx configuration
     that adds the `X-Scope-OrgId` header to incoming requests that do not already set it.

     > **Note:** This change allows Mimir clients to keep sending requests without needing to specify a tenant ID, even though multi-tenancy is now enabled by default.

## (Optional) Use the new, simplified configuration method

In version 3.0.0, we've made several improvements to the Mimir configuration:

- You can override individual properties without having to copy the entire `mimir.config` value to your values file.
  Specify properties you want to override under the `mimir.structuredConfig` section and they will be merged with the default `mimir.config`.
- You can move secrets outside the Mimir configuration via external secrets and environment variables. You can use environment variables to externalize secrets from the configuration file.
- Finally, storing the configuration in a ConfigMap which greatly improves visibility into exactly what is changing which each release.

See [Example migrated values file](#example-migrated-values-file) for an example on migrating an example values file.

## Example migrated values file

Below is an example values file compatible with version 2.1.0 of mimir-distributed.
This file demonstrates a few things:

- All four caches are enabled and have been scaled according to the cluster's needs
- The default pod security policy is disabled
- ServiceMonitors are enabled
- Object storage credentials for block storage have been specified directly in the `mimir.config` value. The unmodified part of the default `mimir.config` are omitted for brevity, even though in a real 2.1.0 values file they needed to be included.

```yaml
rbac:
  pspEnabled: false

memcached:
  enabled: true
  replicaCount: 1

memcached-queries:
  enabled: true
  replicaCount: 1

memcached-metadata:
  enabled: true
  replicaCount: 1

memcached-results:
  enabled: true
  replicaCount: 1

serviceMonitor:
  enabled: true

mimir:
  # The unmodified part of the default mimir.config are omitted for brevity, even though in a real 2.1.0 values file, they needed to be included.
  config: |-
    ...
    #######
    # default contents omitted for brevity
    #######

    blocks_storage:
      backend: s3
      s3:
        endpoint: s3.amazonaws.com
        bucket_name: my-blocks-bucket
        access_key_id: FAKEACCESSKEY
        secret_access_key: FAKESECRETKEY

    #######
    # default contents omitted for brevity
    #######
    ...
```

After applying the migration steps listed in this guide, the equivalent file for version 3.0.0 is below. This is the complete version of the 3.0.0 values file, no parts have been omitted. The parts that were missing in the 2.1.0 version are automatically included.

First, we create an external secret to store the credentials:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mimir-bucket-secret
data:
  AWS_ACCESS_KEY_ID: FAKEACCESSKEY
  AWS_SECRET_ACCESS_KEY: FAKESECRETKEY
```

Then we rewrite the values file:

```yaml
rbac:
  create: false

chunks-cache:
  enabled: true
  replicas: 1

index-cache:
  enabled: true
  replicas: 1

metadata-cache:
  enabled: true
  replicas: 1

results-cache:
  enabled: true
  replicas: 1

metaMonitoring:
  serviceMonitor:
    enabled: true

mimir:
  structuredConfig:
    blocks_storage:
      backend: s3
      s3:
        access_key_id: ${AWS_ACCESS_KEY_ID}
        bucket_name: my-blocks-bucket
        endpoint: s3.amazonaws.com
        secret_access_key: ${AWS_SECRET_ACCESS_KEY}

global:
  extraEnvFrom:
    - secretRef:
        name: mimir-bucket-secret
```
