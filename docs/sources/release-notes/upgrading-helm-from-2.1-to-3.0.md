---
title: "TODO: Grafana Mimir Helm chart version 3.0 Upgrade Guide"
menuTitle: "TODO: Helm chart v3.0"
description: "Upgrade guide for the Grafana Mimir Helm chart version 3.0"
weight: 100
---

# TODO: Grafana Mimir Helm chart version 3.0 Upgrade Guide

## Breaking Changes

Several parameters available in version 2.1 of the mimir-distributed Helm chart have changed.
Follow these steps to migrate your Helm values file to 3.0.

- **Configuration Storage has changed**
  - Mimir configuration is now stored in a ConfigMap by default instead of a Secret.
  - If you are providing the configuration in a user managed Secret (**`useExternalConfig: true`**), then you must now set `configStorageType: Secret`. Conversely, it is now possible to use a ConfigMap to manage your external configuration.
  - See [(Optional) Use the new, simplified configuration method](#optional-use-the-new-simplified-configuration-method) for tips on securing credentials in this new format.
  - If you Mimir configuration contains secrets, there are two options:
    1. Restore the previous behavior by setting `configStorageType: Secret`
    2. Externalize secrets:
       1. Move secrets to an external [Kubernetes secret](https://kubernetes.io/docs/concepts/configuration/secret/#working-with-secrets).
       2. Mount secret via `global.extraEnvFrom`. Refer to [Secrets - Use case: As container environment variables](https://kubernetes.io/docs/concepts/configuration/secret/#use-case-as-container-environment-variables).
       3. Replace values in Mimir configuration with environment variable reference. e.g. `secret_access_key: ${AWS_SECRET_ACCESS_KEY}`
- **Update memcached configuration**
  - The mimir-distributed chart supports four different caches. The configuration parameters have changed as follows:
  - If you have not enabled any memcached caches, and you are not overriding the value of anything under the `memcached`, `memcached-queries`, `memcached-metadata`, or `memcached-results` sections, then you can safely skip this section.
  - The `memcached` section now contains common values shared across all memcached instances.
  - New `memcachedExporter` section was added to configure memcached metrics exporter.
  - New `chunks-cache` section was added that refers to previous `memcached` configuration.
  - The section `memcached-queries` is renamed to `index-cache`.
  - The section `memcached-metadata` is renamed to `metadata-cache`.
  - The section `memcached-results` is renamed to `results-cache`.
  - The value `memcached-*.replicaCount` is replaced with `*-cache.replicas` to align with the rest of the services.
    - Renamed `memcached.replicaCount` to `chunks-cache.replicas`.
    - Renamed `memcached-queries.replicaCount` to `index-cache.replicas`.
    - Renamed `memcached-metadata.replicaCount` to `metadata-cache.replicas`.
    - Renamed `memcached-results.replicaCount` to `results-cache.replicas`.
  - All memcached instances now share the same `ServiceAccount` that the chart uses for its services.
  - The value `memcached-*.architecture` was removed.
  - The value `memcached-*.arguments` was removed, the default arguments are now encoded in the template. Use `*-cache.extraArgs` to provide additional arguments as well as the values `*-cache.allocatedMemory`, `*-cache.maxItemMemory` and `*-cache.port` to set the memcached command line flags `-m`, `-I` and `-u`.
  - The remaining arguments are aligned with the rest of the chart's services, please consult the `values.yaml` file to check whether a parameter exists or was renamed.
  - **Note**: The internal service names for memcached pods have changed. If you have previously copied the value of `mimir.config` into your values file, please see the point below about updating `mimir.config` to ensure the memcached addresses are updated properly in your configuration.
- **Update serviceMonitor configuration**
  - If you have not enabled `serviceMonitor` and you are not overriding the value of anything under the `serviceMonitor` section, then you can safely skip this section.
  - `serviceMonitor` was moved to `metaMonitoring.serviceMonitor`.
- **Update rbac configuration**
  - PodSecurity has been made more flexible to support OpenShift deployment.
  - If you are not overriding the value of anything under the `rbac` section, then you can safely skip this section.
  - `rbac.pspEnabled` has been removed.
  - Use `rbac.create` along with `rbac.type` instead to select either Pod Security Policy or Security Context Constraints.
  - The default behavior is unchanged.
- **Update `mimir.config`**
  - If you are not overriding the value of `mimir.config`, then you can safely skip this section.
  - Before migrating your `mimir.config` value, take a look at how to [use the new simplified configuration method](#optional-use-the-new-simplified-configuration-method).
  - Compare your overridden value of `mimir.config` with the one in the `values.yaml` file in the chart.
  - In particular, pay special attention to any of the memcached addresses since these have changed.
- **Multi-tenancy is enabled by default**
  - The default `mimir.config` now always has multi-tenancy enabled instead of only for enterprise installations.
  - Unless you have overridden the value of `nginx.nginxConfig.file` _and_ you are using the default `mimir.config`, you can safely skip this section.
  - If you have overridden the value of `nginx.nginxConfig.file` _and_ you are using the default `mimir.config`, then make sure to compare the overridden `nginx.nginxConfig.file` to the version in the `values.yaml` file in the chart to incorporate the updates. In particular pay special attention to the sections that contain `x_scope_orgid`.

## (Optional) Use the new, simplified configuration method

In version 2.1.0 of the Helm chart, `mimir.config` is the recommended way to set Mimir configuration properties.
This has lead to a couple problems:

- Changing any property requires copying the entire configuration string and carefully incorporating updates from the chart on each release.
- The configuration must be stored in a Secret since it will always contain at least one credential for object storage.
- Common tools like `helm template` or `helm diff` don't display Secrets in a human-readable format, making it even more difficult to gain confidence when things change.

In version 3.0.0, we've made several improvements to configuration that aimed to solve the listed problems:

- You can override individual properties without copying the entire `mimir.config` block. Specify properties you want to override under the `mimir.structuredConfig`.
- You can move secrets outside the Mimir configuration via external secrets and environment variables. Environment variables can be used to externalize secrets from the config file.
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
