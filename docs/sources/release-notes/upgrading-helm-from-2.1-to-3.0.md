---
title: "TODO: Upgrading to mimir-distributed 3.0"
menuTitle: "TODO: Helm chart v3.0"
description: "Upgrade guide for the Grafana Mimir Helm chart version 3.0"
weight: 100
---

# Grafana Mimir Helm chart version 3.0 Upgrade Guide

## Breaking Changes

Several parameters available in version 2.1 of the mimir-distributed Helm chart have changed.
Follow these steps to migrate your Helm values file to 3.0.

- **Configuration Storage has changed**
    - Mimir configuration is now stored in a ConfigMap by default instead of a Secret
    - In case of providing the configuration in a user managed Secret (**`useExternalConfig=true`**), you must set `configStorageType=Secret`. Conversely, it is now possible to use a ConfigMap to manage your configuration.
    - See [(Optional) Use the new, simplified configuration method](#optional-use-the-new-simplified-configuration-method) for tips on securing credentials in this new format
    - The previous behavior can be enabled by setting `configStorageType` to `Secret`
- **Update memcached configuration**
    - The mimir-distributed chart supports four different caches. The configuration parameters have changed as follows:
    - If you have not enabled any memcached caches,  and you are not overriding the value of anything under the `memcached`, `memcached-queries`, `memcached-metadata`, or `memcached-results` sections, then you can safely skip this section.
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
    - The remaining arguments are aligned with the rest of the chart's services, please consult the values file to check whether a parameter exists or was renamed.
    - **Note**: The internal service names for memcached pods have changed. If you have previously copied the value of `mimir.config` into your values file, please see the point below about updating `mimir.config` to ensure the memcached addresses are updated properly in your configuration.
- **Update serviceMonitor configuration**
    - All meta-monitoring related settings have been consolidated under the `metaMonitoring` section
    - If you have not enabled `serviceMonitor` and you are not overriding the value of anything under the `serviceMonitor` section, then you can safely skip this section.
    - `serviceMonitor.enabled` was renamed to `metaMonitoring.serviceMonitor.enabled`
- **Update rbac configuration**
    - PodSecurity has been made more flexible to support OpenShift deployment
    - If you are not overriding the value of anything under the `rbac` section, then you can safely skip this section.
    - `rbac.pspEnabled` has been removed
    - Use `rbac.create` along with `rbac.type` instead to select either Pod Security Policy or Security Context Constraints
    - The default behavior is unchanged
- **Update `mimir.config`**
    - If you are not overriding the value of `mimir.config`, then you can safely skip this section.
    - Before migrating your `mimir.config` value, take a look at how to [use the new simplified configuration method](#optional-use-the-new-simplified-configuration-method).
    - Compare your overridden value of `mimir.config` with the one in <TODO: link>
    - In particular, pay special attention to any of the memcached addresses since these have changed.
- **Multitenancy is enabled by default**
    - All requests will now require a tenant ID is passed via the `X-Scope-OrgID` header
    - When `enterprise.enabled` is false, the nginx gateway will transparently add `X-Scope-OrgId: anonymous` when no tenant id is present in a request. This ensures compatibility with installations previously running with multitenancy disabled.
    - If you have overridden the nginx gateway configuration, make sure to compare it to the version in <TODO: link> to incorporate any updates.

* (optional) start using the simplified configuration method
    * Move secrets to an external secret
    * Add secret to global.extraEnvFrom
    * Replace values in config with env var reference
    * Use structuredConfig

## (Optional) Use the new, simplified configuration method

In version 2.1.0, `mimir.config` is the recommended way to set Mimir configuration properties.
This has lead to a couple problems:
* Changing any property requires copying the entire configuration string and carefully incorporating updates from the chart on each release.
* The configuration must be stored in a Secret since it will always contain at least one credential for object storage.
* Common tools like `helm template` or `helm diff` don't display Secrets in a human-readable format, making it even more difficult to gain confidence when things change.

In version 3.0.0, we've made several improvements to configuration that are aimed at solving the problems listed above:
* Properties can be overridden individually without copying the entire `mimir.config` block
* Environment variables can be used to externalize credentials from the config file
* Finally, the configuration can safely be stored in a ConfigMap which greatly improves visibility into exactly what is changing which each release.

## Example migrated values.yaml

Below is an example `values.yaml` file compatible with version 2.1.0 of mimir-distributed.
This file demonstrates a few things:
* All four caches are enabled and have been scaled according to the cluster's needs
* The default pod security policy is disabled
* ServiceMonitors are enabled
* Object storage credentials for block storage have been specified directly in the `mimir.config` value

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
  config: |-
    {{- if not .Values.enterprise.enabled -}}
    multitenancy_enabled: false
    {{- end }}
    limits: {}
    activity_tracker:
      filepath: /data/metrics-activity.log
    alertmanager:
      data_dir: '/data'
      enable_api: true
      external_url: '/alertmanager'
    {{- if .Values.minio.enabled }}
    alertmanager_storage:
      backend: s3
      s3:
        endpoint: {{ .Release.Name }}-minio.{{ .Release.Namespace }}.svc:9000
        bucket_name: {{ include "mimir.minioBucketPrefix" . }}-ruler
        access_key_id: {{ .Values.minio.accessKey }}
        secret_access_key: {{ .Values.minio.secretKey }}
        insecure: true
    {{- end }}
    frontend_worker:
      frontend_address: {{ template "mimir.fullname" . }}-query-frontend-headless.{{ .Release.Namespace }}.svc:{{ include "mimir.serverGrpcListenPort" . }}
    ruler:
      enable_api: true
      rule_path: '/data'
      alertmanager_url: dnssrvnoa+http://_http-metrics._tcp.{{ template "mimir.fullname" . }}-alertmanager-headless.{{ .Release.Namespace }}.svc.{{ .Values.global.clusterDomain }}/alertmanager
    server:
      grpc_server_max_recv_msg_size: 104857600
      grpc_server_max_send_msg_size: 104857600
      grpc_server_max_concurrent_streams: 1000
    frontend:
      log_queries_longer_than: 10s
      align_queries_with_step: true
      {{- if index .Values "memcached-results" "enabled" }}
      results_cache:
        backend: memcached
        memcached:
          addresses: dns+{{ .Release.Name }}-memcached-results.{{ .Release.Namespace }}.svc:11211
          max_item_size: {{ (index .Values "memcached-results").maxItemMemory }}
      cache_results: true
      {{- end }}
    compactor:
      data_dir: "/data"
    ingester:
      ring:
        final_sleep: 0s
        num_tokens: 512
        unregister_on_shutdown: false
    ingester_client:
      grpc_client_config:
        max_recv_msg_size: 104857600
        max_send_msg_size: 104857600
    runtime_config:
      file: /var/{{ include "mimir.name" . }}/runtime.yaml
    memberlist:
      abort_if_cluster_join_fails: false
      compression_enabled: false
      join_members:
      - {{ include "mimir.fullname" . }}-gossip-ring
    # This configures how the store-gateway synchronizes blocks stored in the bucket. It uses Minio by default for getting started (configured via flags) but this should be changed for production deployments.
    blocks_storage:
      backend: s3
      tsdb:
        dir: /data/tsdb
      bucket_store:
        sync_dir: /data/tsdb-sync
        {{- if .Values.memcached.enabled }}
        chunks_cache:
          backend: memcached
          memcached:
            addresses: dns+{{ .Release.Name }}-memcached.{{ .Release.Namespace }}.svc:11211
            max_item_size: {{ .Values.memcached.maxItemMemory }}
        {{- end }}
        {{- if index .Values "memcached-metadata" "enabled" }}
        metadata_cache:
          backend: memcached
          memcached:
            addresses: dns+{{ .Release.Name }}-memcached-metadata.{{ .Release.Namespace }}.svc:11211
            max_item_size: {{ (index .Values "memcached-metadata").maxItemMemory }}
        {{- end }}
        {{- if index .Values "memcached-queries" "enabled" }}
        index_cache:
          backend: memcached
          memcached:
            addresses: dns+{{ .Release.Name }}-memcached-queries.{{ .Release.Namespace }}.svc:11211
            max_item_size: {{ (index .Values "memcached-queries").maxItemMemory }}
        {{- end }}
      s3:
        endpoint: s3.amazonaws.com
        bucket_name: my-blocks-bucket
        access_key_id: FAKEACCESSKEY
        secret_access_key: FAKESECRETKEY
    {{- if .Values.minio.enabled }}
    ruler_storage:
      backend: s3
      s3:
        endpoint: {{ .Release.Name }}-minio.{{ .Release.Namespace }}.svc:9000
        bucket_name: {{ include "mimir.minioBucketPrefix" . }}-ruler
        access_key_id: {{ .Values.minio.accessKey }}
        secret_access_key: {{ .Values.minio.secretKey }}
        insecure: true
    {{- end }}
    {{- if .Values.enterprise.enabled }}
    multitenancy_enabled: true
    admin_api:
      leader_election:
        enabled: true
        ring:
          kvstore:
            store: "memberlist"
    {{- if .Values.minio.enabled }}
    admin_client:
      storage:
        type: s3
        s3:
          endpoint: {{ .Release.Name }}-minio.{{ .Release.Namespace }}.svc:9000
          bucket_name: enterprise-metrics-admin
          access_key_id: {{ .Values.minio.accessKey }}
          secret_access_key: {{ .Values.minio.secretKey }}
          insecure: true
    {{- end }}
    auth:
      type: enterprise
    cluster_name: "{{ .Release.Name }}"
    license:
      path: "/license/license.jwt"
    {{- if .Values.gateway.useDefaultProxyURLs }}
    gateway:
      proxy:
        default:
          url: http://{{ template "mimir.fullname" . }}-admin-api.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}
        admin_api:
          url: http://{{ template "mimir.fullname" . }}-admin-api.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}
        alertmanager:
          url: http://{{ template "mimir.fullname" . }}-alertmanager.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}
        compactor:
          url: http://{{ template "mimir.fullname" . }}-compactor.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}
        distributor:
          url: dns:///{{ template "mimir.fullname" . }}-distributor-headless.{{ .Release.Namespace }}.svc.{{ .Values.global.clusterDomain }}:{{ include "mimir.serverGrpcListenPort" . }}
        ingester:
          url: http://{{ template "mimir.fullname" . }}-ingester.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}
        query_frontend:
          url: http://{{ template "mimir.fullname" . }}-query-frontend.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}
        ruler:
          url: http://{{ template "mimir.fullname" . }}-ruler.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}
        store_gateway:
          url: http://{{ template "mimir.fullname" . }}-store-gateway.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}
    {{- end }}
    instrumentation:
      enabled: true
      distributor_client:
        address: 'dns:///{{ template "mimir.fullname" . }}-distributor-headless.{{ .Release.Namespace }}.svc.{{ .Values.global.clusterDomain }}:{{ include "mimir.serverGrpcListenPort" . }}'
    {{- end }}
```

After applying the migration steps listed in this guide, the equivalent `values.yaml` for version 3.0.0 looks like this:

First we create an external secret to store the credentials:

```yaml
apiVersion: v1
kind: Secret
metadata:
    name: mimir-bucket-secret
data:
    AWS_ACCESS_KEY_ID: FAKEACCESSKEY
    AWS_SECRET_ACCESS_KEY: FAKESECRETKEY
```

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
