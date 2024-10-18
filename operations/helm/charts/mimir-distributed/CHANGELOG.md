# Changelog

## Deprecated features

This section contains deprecated features and interfaces that the chart exposes. The deprecation policy of the chart is to
remove a deprecated item from the third major release after it has been deprecated it.

### List

* GEM gateway: remove port 8080 on the Service resource. Deprecated in `3.1.0` and will be removed in `6.0.0`.
  * __How to migrate__: replace usages of port 8080 with port 80; these usages can be in dashboards, Prometheus remote-write configurations, or automation for updating rules.
* NGINX configuration via `nginx` top-level values sections is being merged with by the `gateway` section. The
  `nginx` section is deprecated in `4.0.0` and will be removed in `7.0.0`.
  * __How to migrate__: refer to [Migrate to using the unified proxy deployment for NGINX and GEM gateway](https://grafana.com/docs/helm-charts/mimir-distributed/latest/migration-guides/migrate-to-unified-proxy-deployment/)

## Format of changelog

This changelog is continued from `enterprise-metrics` after Grafana Enterprise Metrics was added to `mimir-distributed` in PR #1203.
All notable changes to this chart will be documented in this file.

Entries should be ordered as follows:

* [CHANGE]
* [FEATURE]
* [ENHANCEMENT]
* [BUGFIX]

Entries should include a reference to the Pull Request that introduced the change.

## main / unreleased

* [ENHANCEMENT] Add support for setting type and internal traffic policy for Kubernetes service. Set `internalTrafficPolicy=Cluster` by default in all services with type `ClusterIP`. #9619

## 5.5.0

* [ENHANCEMENT] Dashboards: allow switching between using classic or native histograms in dashboards.
  * Overview dashboard: status, read/write latency and queries/ingestion per sec panels, `cortex_request_duration_seconds` metric. #7674
  * Writes dashboard: `cortex_request_duration_seconds` metric. #8757
  * Reads dashboard: `cortex_request_duration_seconds` metric. #8752
  * Rollout progress dashboard: `cortex_request_duration_seconds` metric. #8779
  * Alertmanager dashboard: `cortex_request_duration_seconds` metric. #8792
  * Ruler dashboard: `cortex_request_duration_seconds` metric. #8795
  * Queries dashboard: `cortex_request_duration_seconds` metric. #8800
  * Remote ruler reads dashboard: `cortex_request_duration_seconds` metric. #8801
* [ENHANCEMENT] Memcached: Update to Memcached 1.6.28 and memcached-exporter 0.14.4. #8557
* [ENHANCEMENT] Add missing fields in multiple topology spread constraints. #8533
* [ENHANCEMENT] Add support for setting the image pull secrets, node selectors, tolerations and topology spread constraints for the Grafana Agent pods used for metamonitoring. #8670
* [ENHANCEMENT] Add support for setting resource requests and limits in the Grafana Agent pods used for metamonitoring. #8715
* [ENHANCEMENT] Add support for setting namespace for dashboard config maps. #8813
* [ENHANCEMENT] Add support for string `extraObjects` for better support with templating. #8825
* [ENHANCEMENT] Allow setting read and write urls in continous-test. #8741
* [ENHANCEMENT] Add support for running continuous-test with GEM. #8837
* [ENHANCEMENT] Alerts: `RequestErrors` and `RulerRemoteEvaluationFailing` have been enriched with a native histogram version. #9004
* [ENHANCEMENT] Add support for sigv4 authentication for remote write in metamonitoring. #9279
* [ENHANCEMENT] Ingester: set GOMAXPROCS to help with Go scheduling overhead when running on machines with lots of CPU cores. #9283
* [ENHANCEMENT] GEM: enable logging of access policy name and token name that execute query in query-frontend. #9348
* [ENHANCEMENT] Update rollout-operator to `v0.19.1` (Helm chart version `v0.18.0`). #9388
* [BUGFIX] Add missing container security context to run `continuous-test` under the restricted security policy. #8653
* [BUGFIX] Add `global.extraVolumeMounts` to the exporter container on memcached statefulsets #8787
* [BUGFIX] Fix helm releases failing when `querier.kedaAutoscaling.predictiveScalingEnabled=true`. #8731
* [BUGFIX] Alertmanager: Set -server.http-idle-timeout to avoid EOF errors in ruler. #8192
* [BUGFIX] Helm: fix second relabeling in ServiceMonitor and PVC template in compactor to not show diff in ArgoCD. #9195
* [BUGFIX] Helm: create query-scheduler `PodDisruptionBudget` only when the component is enabled. #9270

## 5.4.1

* [CHANGE] Upgrade GEM version to v2.13.1.

## 5.4.0

* [FEATURE] Add support for a dedicated query path for the ruler. This allows for the isolation of ruler and user query paths. Enable it via `ruler.remoteEvaluationDedicatedQueryPath: true`. #7964
* [CHANGE] Fine-tuned `terminationGracePeriodSeconds` for the following components: #7361 #7364
  * Alertmanager: changed from `60` to `900`
  * Distributor: changed from `60` to `100`
  * Ingester: changed from `240` to `1200`
  * Overrides-exporter: changed from `60` to `30`
  * Ruler: changed from `180` to `600`
  * Store-gateway: changed from `240` to `120`
  * Compactor: changed from `240` to `900`
  * Chunks-cache: changed from `60` to `30`
  * Index-cache: changed from `60` to `30`
  * Metadata-cache: changed from `60` to `30`
  * Results-cache: changed from `60` to `30`
* [CHANGE] Smoke-test: remove the `smoke_test.image` and `continuous_test.image` sections and reuse the main Mimir image from the top `image` section using the new `-target=continuous-test` CLI flag. #7923
* [ENHANCEMENT] Dashboards: allow switching between using classic of native histograms in dashboards. #7627
  * Overview dashboard, Status panel, `cortex_request_duration_seconds` metric.
* [ENHANCEMENT] Alerts: exclude `529` and `598` status codes from failure codes in `MimirRequestsError`. #7889
* [ENHANCEMENT] The new value `metaMonitoring.grafanaAgent.logs.clusterLabel` controls whether to add a `cluster` label and with what content to PodLogs logs. #7764
* [ENHANCEMENT] The new values `global.extraVolumes` and `global.extraVolumeMounts` adds volumes and volumeMounts to all pods directly managed by mimir-distributed. #7922
* [ENHANCEMENT] Smoke-test: Parameterized `backoffLimit` for smoke tests in Helm chart to accommodate slower startup environments like k3d. #8025
* [ENHANCEMENT] Add a volumeClaimTemplates section to the `chunks-cache`, `index-cache`, `metadata-cache`, and `results-cache` components. #8016
* [ENHANCEMENT] Add 'gateway.nginx.config.clientMaxBodySize' to the `gateway` to allow setting the maximum allowed size of the client request body. #7960 #8497
* [ENHANCEMENT] Update rollout-operator to `v0.17.0`. #8399
* [ENHANCEMENT] Omit rendering empty `subPath`, `args`, `env`, and `envFrom` in resource manifests. #7587
* [BUGFIX] Helm: Allowed setting static NodePort for nginx gateway via `gateway.service.nodePort`. #6966
* [BUGFIX] Helm: Expose AM configs in the `gateway` NGINX configuration. #8248
* [BUGFIX] Helm: fix ServiceMonitor and PVC template to not show diff in ArgoCD. #8829

## 5.3.0

* [CHANGE] Do not render resource blocks for `initContainers`, `nodeSelector`, `affinity` and `tolerations` if they are empty. #7559
* [CHANGE] Rollout-operator: remove default CPU limit. #7125
* [CHANGE] Ring: relaxed the hash ring heartbeat period and timeout for distributor, ingester, store-gateway and compactor: #6860
  * `-distributor.ring.heartbeat-period` set to `1m`
  * `-distributor.ring.heartbeat-timeout` set to `4m`
  * `-ingester.ring.heartbeat-period` set to `2m`
  * `-ingester.ring.heartbeat-timeout` set to `10m`
  * `-store-gateway.sharding-ring.heartbeat-period` set to `1m`
  * `-store-gateway.sharding-ring.heartbeat-timeout` set to `4m`
  * `-compactor.ring.heartbeat-period` set to `1m`
  * `-compactor.ring.heartbeat-timeout` set to `4m`
* [CHANGE] Ruler: Set `-distributor.remote-timeout` to 10s in order to accommodate writing large rule results to the ingester. #7143
* [CHANGE] Remove `-server.grpc.keepalive.max-connection-age` and `-server.grpc.keepalive.max-connection-age-grace` from default config. The configuration now applied directly to distributor, fixing parity with jsonnet. #7269
* [CHANGE] Remove `-server.grpc.keepalive.max-connection-idle` from default config. The configuration now applied directly to distributor, fixing parity with jsonnet. #7298
* [CHANGE] Distributor: termination grace period increased from 60s to 100s. #7361
* [CHANGE] Memcached: Change default read timeout for chunks and index caches to `750ms` from `450ms`. #7778
* [FEATURE] Added experimental feature for deploying [KEDA](https://keda.sh) ScaledObjects as part of the helm chart for the components: distributor, querier, query-frontend and ruler. #7282 #7392 #7431 #7679
  * Autoscaling can be enabled via `distributor.kedaAutoscaling`, `ruler.kedaAutoscaling`, `query_frontend.kedaAutoscaling`, and `querier.kedaAutoscaling`.
  * Global configuration of `promtheusAddress`, `pollingInterval` and `customHeaders` can be found in `kedaAutoscaling`section.
  * Requires metamonitoring or custom installed Prometheus compatible solution, for more details on metamonitoring see [Monitor the health of your system](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/monitor-system-health/).
  * For migration please use `preserveReplicas` option of each component. This option should be enabled, when first time enabling KEDA autoscaling for a component, to preserve the current number of replicas. After the autoscaler takes over and is ready to scale the component, this option can be disabled. After disabling this option, the `replicas` field inside the components deployment will be ignored and the autoscaler will manage the number of replicas.
* [FEATURE] Gateway: Allow to configure whether or not NGINX binds IPv6 via `gateway.nginx.config.enableIPv6`. #7421
* [ENHANCEMENT] Add `jaegerReporterMaxQueueSize` Helm value for all components where configuring `JAEGER_REPORTER_MAX_QUEUE_SIZE` makes sense, and override the Jaeger client's default value of 100 for components expected to generate many trace spans. #7068 #7086 #7259
* [ENHANCEMENT] Rollout-operator: upgraded to v0.13.0. #7469
* [ENHANCEMENT] Query-frontend: configured `-shutdown-delay`, `-server.grpc.keepalive.max-connection-age` and termination grace period to reduce the likelihood of queries hitting terminated query-frontends. #7129
* [ENHANCEMENT] Distributor: reduced `-server.grpc.keepalive.max-connection-age` from `2m` to `60s` and configured `-shutdown-delay` to `90s` in order to reduce the chances of failed gRPC write requests when distributors gracefully shutdown. #7361
* [ENHANCEMENT] Add the possibility to create a dedicated serviceAccount for the `ruler` component by setting `ruler.serviceAccount.create` to true in the values. #7132
* [ENHANCEMENT] nginx, Gateway: set `proxy_http_version: 1.1` to proxy to HTTP 1.1. #5040
* [ENHANCEMENT] Gateway: make Ingress/Route host templateable. #7218
* [ENHANCEMENT] Make the PSP template configurable via `rbac.podSecurityPolicy`. #7190
* [ENHANCEMENT] Recording rules: add native histogram recording rules to `cortex_request_duration_seconds`. #7528
* [ENHANCEMENT] Make the port used in ServiceMonitor for kube-state-metrics configurable. #7507
* [ENHANCEMENT] Produce a clearer error messages when multiple X-Scope-OrgID headers are present. #7704
* [ENHANCEMENT] Add `querier.kedaAutoscaling.predictiveScalingEnabled` to scale querier based on inflight queries 7 days ago. #7775
* [BUGFIX] Metamonitoring: update dashboards to drop unsupported `step` parameter in targets. #7157
* [BUGFIX] Recording rules: drop rules for metrics removed in 2.0: `cortex_memcache_request_duration_seconds` and `cortex_cache_request_duration_seconds`. #7514
* [BUGFIX] Store-gateway: setting "resources.requests.memory" with a quantity that used power-of-ten SI suffix, caused an error. #7506
* [BUGFIX] Do nor render empty fields for `subPath`, `args`, `env` & `envFrom` #7587

## 5.2.3

* [BUGFIX] admin-cache: set max connections to fix failure to start #7632

## 5.2.2

* [BUGFIX] Updated GEM image to v2.11.2. #7555

## 5.2.1

* [BUGFIX] Revert [PR 6999](https://github.com/grafana/mimir/pull/6999), introduced in 5.2.0, which broke installations relying on the default value of `blocks_storage.backend: s3`. If `mimir.structuredConfig.blocks_storage.backend: s3` wasn't explicitly set, then Mimir would fail to connect to S3 and will instead try to read and write blocks to the local filesystem. #7199

## 5.2.0

* [CHANGE] Remove deprecated configuration parameter `blocks_storage.bucket_store.max_chunk_pool_bytes`. #6673
* [CHANGE] Reduce `-server.grpc-max-concurrent-streams` from 1000 to 500 for ingester and to 100 for all components. #5666
* [CHANGE] Changed default `clusterDomain` from `cluster.local` to `cluster.local.` to reduce the number of DNS lookups made by Mimir. #6389
* [CHANGE] Change the default timeout used for index-queries caches from `200ms` to `450ms`. #6786
* [FEATURE] Added option to enable StatefulSetAutoDeletePVC for StatefulSets for compactor, ingester, store-gateway, and alertmanager via `*.persistance.enableRetentionPolicy`, `*.persistance.whenDeleted`, and `*.persistance.whenScaled`. #6106
* [FEATURE] Add pure Ingress option instead of the gateway service. #6932
* [ENHANCEMENT] Update the `rollout-operator` subchart to `0.10.0`. #6022 #6110 #6558 #6681
* [ENHANCEMENT] Add support for not setting replicas for distributor, querier, and query-frontend. #6373
* [ENHANCEMENT] Make Memcached connection limit configurable. #6715
* [BUGFIX] Let the unified gateway/nginx config listen on IPv6 as well. Followup to #5948. #6204
* [BUGFIX] Quote `checksum/config` when using external config. This allows setting `externalConfigVersion` to numeric values. #6407
* [BUGFIX] Update memcached-exporter to 0.14.1 due to CVE-2023-39325. #6861

## 5.1.4

* [BUGFIX] Update memcached-exporter to 0.14.1 due to CVE-2023-39325.

## 5.1.3

* [BUGFIX] Updated Mimir image to 2.10.4 and GEM images to v2.10.4. #6654

## 5.1.2

* [BUGFIX] Update Mimir image to 2.10.3 and GEM image to v2.10.3. #6427

## 5.1.1

* [BUGFIX] Update Mimir image to 2.10.2 and GEM image to v2.10.2. #6371

## 5.1.0

* [ENHANCEMENT] Update Mimir image to 2.10.0 and GEM image to v2.10.1. #6077
* [ENHANCEMENT] Make compactor podManagementPolicy configurable. #5902
* [ENHANCEMENT] Distributor: dynamically set `GOMAXPROCS` based on the CPU request. This should reduce distributor CPU utilization, assuming the CPU request is set to a value close to the actual utilization. #5588
* [ENHANCEMENT] Querier: dynamically set `GOMAXPROCS` based on the CPU request. This should reduce noisy neighbour issues created by the querier, whose CPU utilization could eventually saturate the Kubernetes node if unbounded. #5646
* [ENHANCEMENT] Sets the `appProtocol` value to `tcp` for the `gossip-ring-svc` service template. This allows memberlist to work with istio protocol selection. #5673
* [ENHANCEMENT] Update the `rollout-operator` subchart to `0.8.0`. #5718
* [ENHANCEMENT] Make store_gateway podManagementPolicy configurable. #5757
* [ENHANCEMENT] Set `maxUnavailable` to 0 for `distributor`, `overrides-exporter`, `querier`, `query-frontend`, `query-scheduler`, `ruler-querier`, `ruler-query-frontend`, `ruler-query-scheduler`, `nginx`, `gateway`, `admin-api`, `graphite-querier` and `graphite-write-proxy` deployments, to ensure they don't become completely unavailable during a rollout. #5924
* [ENHANCEMENT] Nginx: listen on IPv6 addresses. #5948
* [BUGFIX] Fix `global.podLabels` causing invalid indentation. #5625

## 5.0.0

* [CHANGE] Changed max unavailable ingesters and store-gateways in a zone to 50. #5327
* [CHANGE] Don't render PodSecurityPolicy on Kubernetes >=1.24. (was >= 1.25). This helps with upgrades between 1.24 and 1.25. To use a PSP in 1.24, toggle `rbac.forcePSPOnKubernetes124: true`. #5357
* [ENHANCEMENT] Ruler: configure the ruler storage cache when the metadata cache is enabled. #5326 #5334
* [ENHANCEMENT] Helm: support metricRelabelings in the monitoring serviceMonitor resources via `metaMonitoring.serviceMonitor.metricRelabelings`. #5340
* [ENHANCEMENT] Service Account: allow adding labels to the service account. #5355
* [ENHANCEMENT] Memcached: enable providing additional extended options (`-o/--extended`) via `<cache-section>.extraExtendedOptions`. #5353
* [ENHANCEMENT] Memcached exporter: enable adding additional CLI arguments via `memcachedExporter.extraArgs`. #5353
* [ENHANCEMENT] Memcached: allow mounting additional volumes to the memcached and exporter containers via `<cache-section>.extraVolumes` and `<cache-section>.extraVolumeMounts`. #5353

## 4.5.0

* [CHANGE] Query-frontend: enable cardinality estimation via `frontend.query_sharding_target_series_per_shard` in the Mimir configuration for query sharding by default if `results-cache.enabled` is true. #5128
* [CHANGE] Remove `graphite-web` component from the graphite proxy. The `graphite-web` component had several configuration issues which meant it was failing to process requests. #5133
* [ENHANCEMENT] Set `nginx` and `gateway` Nginx read timeout (`proxy_read_timeout`) to 300 seconds (increase from default 60 seconds), so that it doesn't interfere with the querier's default 120 seconds timeout (`mimir.structuredConfig.querier.timeout`). #4924
* [ENHANCEMENT] Update nginx image to `nginxinc/nginx-unprivileged:1.24-alpine`. #5066
* [ENHANCEMENT] Update the `rollout-operator` subchart to `0.5.0`. #4930
* [ENHANCEMENT] Store-gateway: set `GOMEMLIMIT` to the memory request value. This should reduce the likelihood the store-gateway may go out of memory, at the cost of an higher CPU utilization due to more frequent garbage collections when the memory utilization gets closer or above the configured requested memory. #4971
* [ENHANCEMENT] Store-gateway: dynamically set `GOMAXPROCS` based on the CPU request. This should reduce the likelihood a high load on the store-gateway will slow down the entire Kubernetes node. #5104
* [ENHANCEMENT] Add global.podLabels which can add POD labels to PODs directly controlled by this chart (mimir services, nginx). #5055
* [ENHANCEMENT] Enable the `track_sizes` feature for Memcached pods to help determine cache efficiency. #5209
* [BUGFIX] Fix Pod Anti-Affinity rule to allow ingesters of from the same zone to run on same node, by using `zone` label since the old `app.kubernetes.io/component` did not allow for this. #5031
* [ENHANCEMENT] Enable `PodDisruptionBudget`s by default for admin API, alertmanager, compactor, distributor, gateway, overrides-exporter, ruler, querier, query-frontend, query-scheduler, nginx, Graphite components, chunks cache, index cache, metadata cache and results cache.

## 4.4.1

* [CHANGE] Change number of Memcached max idle connections to 150. #4591
* [CHANGE] Set `unregister_on_shutdown` for `store-gateway` to `false` by default. #4690
* [FEATURE] Add support for Vault Agent. When enabled, the Pod annotations for TLS configurable components are updated to allow a running Vault Agent to fetch secrets from Vault and to inject them into a Pod. The annotations are updated for the following components: `admin-api`, `alertmanager`, `compactor`, `distributor`, `gateway`, `ingester`, `overrides-exporter`, `querier`, `query-frontend`, `query-scheduler`, `ruler`, `store-gateway`. #4660
* [FEATURE] Add documentation to use external Redis support for chunks-cache, metadata-cache and results-cache. #4348
* [FEATURE] Allow for deploying mixin dashboards as part of the helm chart. #4618
* [ENHANCEMENT] Update the `rollout-operator` subchart to `0.4.2`. #4524 #4659 #4780
* [ENHANCEMENT] Update the `memcached-exporter` to `v0.11.2`. #4570
* [ENHANCEMENT] Update memcached to `memcached:1.6.19-alpine`. #4581
* [ENHANCEMENT] Allow definition of multiple topology spread constraints. #4584
* [ENHANCEMENT] Expose image repo path as helm vars for containers created by grafana-agent-operator #4645
* [ENHANCEMENT] Update minio subchart to `5.0.7`. #4705
* [ENHANCEMENT] Configure ingester TSDB head compaction interval to 15m. #4870
* [ENHANCEMENT] Configure ingester TSDB WAL replay concurrency to 3. #4864
* [ENHANCEMENT] Configure compactor's first level compaction wait period to 25m. #4872
* [ENHANCEMENT] You can now configure `storageClass` per zone for Alertmanager, StoreGateway and Ingester. #4234
* [ENHANCEMENT] Add suffix to minio create buckets job to avoid mimir-distributed helm chart fail to upgrade when minio image version changes. #4936
* [BUGFIX] Helm-Chart: fix route to service port mapping. #4728
* [BUGFIX] Include podAnnotations on the tokengen Job. #4540
* [BUGFIX] Add http port in ingester and store-gateway headless services. #4573
* [BUGFIX] Set `gateway` and `nginx` HPA MetricTarget type to Utilization to align with usage of averageUtilization. #4642
* [BUGFIX] Add missing imagePullSecrets configuration to the `graphite-web` deployment template. #4716

## 4.3.1

* [BUGFIX] Updated Go version in Mimir and GEM images to 1.20.3 to fix CVE-2023-24538. #4803

## 4.3.0

* [CHANGE] Ruler: changed ruler deployment max surge from `0` to `50%`, and max unavailable from `1` to `0`. #4381
* [FEATURE] Add cache support for GEM's admin bucket. The cache will be enabled by default when you use the
  small.yaml, large.yaml, capped-small.yaml or capped-large.yaml Helm values file. #3740
  > **Note:** For more information, refer to the [Grafana Enterprise Metrics configuration](https://grafana.com/docs/enterprise-metrics/latest/config).
* [ENHANCEMENT] Update GEM image grafana/enterprise-metrics to v2.7.0. #4533
* [ENHANCEMENT] Support autoscaling/v2 HorizontalPodAutoscaler for nginx autoscaling starting with Kubernetes 1.23. #4285
* [ENHANCEMENT] Set default pod security context under `rbac.podSecurityContext` for easier install on OpenShift. #4272
* [BUGFIX] Allow override of Kubernetes version for nginx HPA. #4299
* [BUGFIX] Do not generate query-frontend-headless service if query scheduler is enabled. Fixes parity with jsonnet. #4353
* [BUGFIX] Apply `clusterLabel` to ServiceMonitors for kube-state-metrics, kubelet, and cadvisor. #4126
* [BUGFIX] Add http port in distributor headless service. Fixes parity with jsonnet. #4392
* [BUGFIX] Generate the pod security context on the pod level in graphite web deployment, instead of on container level. #4272
* [BUGFIX] Fix kube-state-metrics metricRelabelings dropping pods and deployments. #4485
* [BUGFIX] Allow for single extraArg flags in templated memcached args. #4407

## 4.2.1

* [BUGFIX] Updated Go version in Mimir and GEM images to 1.20.3 and 1.19.8 to fix CVE-2023-24538. #4818

## 4.2.0

* [ENHANCEMENT] Allow NGINX error log level to be overridden and access log to be disabled. #4230
* [ENHANCEMENT] Update GEM image grafana/enterprise-metrics to v2.6.0. #4279

## 4.1.0

* [CHANGE] Configured `max_total_query_length: 12000h` limit to match Mimir jsonnet-based deployment. #3879
* [ENHANCEMENT] Enable users to specify additional Kubernetes resource manifests using the `extraObjects` variable. #4102
* [ENHANCEMENT] Update the `rollout-operator` subchart to `0.2.0`. #3624
* [ENHANCEMENT] Add ability to manage PrometheusRule for metamonitoring with Prometheus operator from the Helm chart. The alerts are disabled by default but can be enabled with `prometheusRule.mimirAlerts` set to `true`. To enable the default rules, set `mimirRules` to `true`. #2134 #2609
* [ENHANCEMENT] Update memcached image to `memcached:1.6.17-alpine`. #3914
* [ENHANCEMENT] Update minio subchart to `5.0.4`. #3942
* [BUGFIX] Enable `rollout-operator` to use PodSecurityPolicies if necessary. #3686
* [BUGFIX] Fixed gateway's checksum/config when using nginx #3780
* [BUGFIX] Disable gateway's serviceMonitor when using nginx #3781
* [BUGFIX] Expose OTLP ingestion in the `gateway` NGINX configuration. #3851
* [BUGFIX] Use alertmanager headless service in `gateway` NGINX configuration. #3851
* [BUGFIX] Use `50Gi` persistent volume for ingesters in `capped-small.yaml`. #3919
* [BUGFIX] Set server variables in NGINX configuration so that IP addresses are re-resolved when TTLs expire. #4124
* [BUGFIX] Do not include namespace for the PodSecurityPolicy definition as it is not needed and some tools reject it outright. #4164

## 4.0.1

* [ENHANCEMENT] Bump Grafana Enterprise Metrics image version to 2.5.1 #3902

## 4.0.0

* [FEATURE] Support deploying NGINX via the `gateway` section. The `nginx` section will be removed in `7.0.0`. See
  [Migrate to using the unified proxy deployment for NGINX and GEM gateway](https://grafana.com/docs/helm-charts/mimir-distributed/latest/migration-guides/migrate-to-unified-proxy-deployment/)
* [CHANGE] **breaking change** **Data loss without action.** Enables [zone-aware replication](https://grafana.com/docs/mimir/latest/configure/configure-zone-aware-replication/) for ingesters and store-gateways by default. #2778
  - If you are **upgrading** an existing installation:
    - Turn off zone-aware replication, by setting the following values:
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
    - After the upgrade you can migrate to the new zone-aware replication setup, see [Migrate from single zone to zone-aware replication with Helm](https://grafana.com/docs/mimir/latest/migration-guide/migrating-from-single-zone-with-helm/) guide.
  - If you are **installing** the chart:
    - Ingesters and store-gateways are installed with 3 logical zones, which means both ingesters and store-gateways start 3 replicas each.
* [CHANGE] **breaking change** Reduce the number of ingesters in small.yaml form 4 to 3. This should be more accurate size for the scale of 1M AS. Before upgrading refer to [Scaling down ingesters](https://grafana.com/docs/mimir/latest/operators-guide/run-production-environment/scaling-out/#scaling-down-ingesters) to scale down `ingester-3`. Alternatively override the number of ingesters to 4. #3035
* [CHANGE] **breaking change** Update minio subchart from `4.0.12` to `5.0.0`, which inherits the breaking change of minio gateway mode being removed. #3352
* [CHANGE] Nginx: uses the headless service of alertmanager, ingester and store-gateway as backends, because there are 3 separate services for each zone. #2778
* [CHANGE] Gateway: uses the headless service of alertmanager as backend, because there are 3 separate services for each zone. #2778
* [CHANGE] Update sizing plans (small.yaml, large.yaml, capped-small.yaml, capped-large.yaml). These reflect better how we recommend running Mimir and GEM in production. most plans have adjusted number of replicas and resource requirements. The only **breaking change** is in small.yaml which has reduced the number of ingesters from 4 to 3; for scaling down ingesters refer to [Scaling down ingesters](https://grafana.com/docs/mimir/latest/operators-guide/run-production-environment/scaling-out/#scaling-down-ingesters). #3035
* [CHANGE] Change default securityContext of Mimir and GEM Pods and containers, so that they comply with a [Restricted pod security policy](https://kubernetes.io/docs/concepts/security/pod-security-standards/).
  This changes what user the containers run as from `root` to `10001`. The files in the Pods' attached volumes should change ownership with the `fsGroup` change;
  most CSI drivers support changing the value of `fsGroup`, or kubelet is able to do the ownership change instead of the CSI driver. This is not the case for the HostPath driver.
  If you are using HostPath or another driver that doesn't support changing `fsGroup`, then you have a couple of options: A) set the `securityContext` of all Mimir and GEM components to `{}` in your values file; B) delete PersistentVolumes and PersistentVolumeClaims and upgrade the chart; C) add an initContainer to all components that use a PVC that changes ownership of the mounted volumes.
  If you take no action and `fsGroup` is not supported by your CSI driver, then components will fail to start. #3007
* [CHANGE] Restrict Pod seccomp profile to `runtime/default` in the default PodSecurityPolicy of the chart. #3007
* [CHANGE] Use the chart's service account for metamonitoring instead of creating one specific to metamonitoring. #3350
* [CHANGE] Use mimir for the nginx ingress example #3336
* [ENHANCEMENT] Metamonitoring: If enabled and no URL is configured, then metamonitoring metrics will be sent to
  Mimir under the `metamonitoring` tenant; this enhancement does not apply to GEM. #3176
* [ENHANCEMENT] Improve default rollout strategies. Now distributor, overrides_exporter, querier, query_frontend, admin_api, gateway, and graphite components can be upgraded more quickly and also can be rolled out with a single replica without downtime. #3029
* [ENHANCEMENT] Metamonitoring: make scrape interval configurable. #2945
* [ENHANCEMENT] Documented how to prevent a user from using a mismatched Helm chart `values.yaml` file. #3197
* [ENHANCEMENT] Update compactor configuration to match Jsonnet. #3353
  * This also now matches production configuration from Grafana Cloud
  * Set `compactor.compaction_interval` to `30m` (Decreased from `1h`)
  * Set `compactor.deletion_delay` to `2h` (Decreased from `12h`)
  * Set `compactor.max_closing_blocks_concurrency` to `2` (Increased from `1`)
  * Set `compactor.max_opening_blocks_concurrency` to `4` (Increased from `1`)
  * Set `compactor.symbols_flushers_concurrency` to `4` (Increased from `1`)
  * Set `compactor.sharding_ring.wait_stability_min_duration` to `1m` (Increased from `0`)
* [ENHANCEMENT] Update read path configuration to match Jsonnet #2998
  * This also now matches production configuration from Grafana Cloud
  * Set `blocks_storage.bucket_store.max_chunk_pool_bytes` to `12GiB` (Increased from `2GiB`)
  * Set `blocks_storage.bucket_Store.index_cache.memcached.max_item_size` to `5MiB` (Decreased from `15MiB`)
  * Set `frontend.grpc_client_config.max_send_msg_size` to `400MiB` (Increased from `100MiB`)
  * Set `limits.max_cache_freshness` to `10m` (Increased from `1m`)
  * Set `limits.max_query_parallelism` to `240` (Increased from `224`)
  * Set `query_scheduler.max_outstanding_requests_per_tenant` to `800` (Decreased from `1600`)
  * Set `store_gateway.sharding_ring.wait_stability_min_duration` to `1m` (Increased from `0`)
  * Set `frontend.results_cache.memcached.timeout` to `500ms` (Increased from `100ms`)
  * Unset `frontend.align_queries_with_step` (Was `true`, now defaults to `false`)
  * Unset `frontend.log_queries_longer_than` (Was `10s`, now defaults to `0`, which is disabled)
* [ENHANCEMENT] Added `usage_stats.installation_mode` configuration to track the installation mode via the anonymous usage statistics. #3294
* [ENHANCEMENT] Update grafana-agent-operator subchart to 0.2.8. Notable changes are being able to configure Pod's SecurityContext and Container's SecurityContext. #3350
* [ENHANCEMENT] Add possibility to configure fallbackConfig for alertmanager and set it by default. Now tenants without an alertmanager config will not see errors accessing the alertmanager UI or when using the alertmanager API. #3360
* [ENHANCEMENT] Add ability to set a `schedulerName` for alertmanager, compactor, ingester and store-gateway. This is needed for example for some storage providers. #3140
* [BUGFIX] Fix an issue that caused metamonitoring secrets to be created incorrectly #3170
* [BUGFIX] Nginx: fixed `imagePullSecret` value reference inconsistency. #3208
* [BUGFIX] Move the activity tracker log from /data to /active-query-tracker to remove ignore log messages. #3169
* [BUGFIX] Fix invalid ingress NGINX configuration due to newline in prometheusHttpPrefix Helm named templates. #3087
* [BUGFIX] Added missing endpoint for OTLP in NGINX #3479

## 3.3.0

* [ENHANCEMENT] Update GEM image grafana/enterprise-metrics to v2.4.0. #3445

## 3.2.0

* [CHANGE] Nginx: replace topology key previously used in `podAntiAffinity` (`failure-domain.beta.kubernetes.io/zone`) with a different one `topologySpreadConstraints` (`kubernetes.io/hostname`). #2722
* [CHANGE] Use `topologySpreadConstraints` instead of `podAntiAffinity` by default. #2722
  - **Important**: if you are not using the sizing plans (small.yaml, large.yaml, capped-small.yaml, capped-large.yaml) in production, you should reintroduce pod affinity rules for the ingester and store-gateway. This also fixes a missing label selector for the ingester.
     Merge the following to your custom values file:
     ```yaml
     ingester:
       affinity:
         podAntiAffinity:
           requiredDuringSchedulingIgnoredDuringExecution:
              - labelSelector:
                  matchExpressions:
                    - key: target
                      operator: In
                      values:
                        - ingester
                topologyKey: 'kubernetes.io/hostname'
              - labelSelector:
                  matchExpressions:
                    - key: app.kubernetes.io/component
                      operator: In
                      values:
                        - ingester
                topologyKey: 'kubernetes.io/hostname'
     store_gateway:
       affinity:
         podAntiAffinity:
           requiredDuringSchedulingIgnoredDuringExecution:
              - labelSelector:
                  matchExpressions:
                    - key: target
                      operator: In
                      values:
                        - store-gateway
                topologyKey: 'kubernetes.io/hostname'
              - labelSelector:
                  matchExpressions:
                    - key: app.kubernetes.io/component
                      operator: In
                      values:
                        - store-gateway
                topologyKey: 'kubernetes.io/hostname'
     ```
* [CHANGE] Ingresses for the GEM gateway and nginx will no longer render on Kubernetes versions <1.19. #2872
* [FEATURE] Add support for OpenShift Routes for Nginx #2908
* [FEATURE] Add support for `topologySpreadConstraints` to all components; add `topologySpreadConstraints` to GEM gateway, admin-api, and alertmanager, which did not have `podAntiAffinity` previously. #2722
* [ENHANCEMENT] Document `kubeVersionOverride`. If you rely on `helm template`, use this in your values to set the Kubernetes version. If unset helm will use the kubectl client version as the Kubernetes version with `helm template`, which may cause the chart to render incompatible manifests for the actual server version. #2872
* [ENHANCEMENT] Support autoscaling/v2 HorizontalPodAutoscaler for nginx autoscaling. This is used when deploying on Kubernetes >= 1.25. #2848
* [ENHANCEMENT] Monitoring: Add additional flags to conditionally enable log / metric scraping. #2936
* [ENHANCEMENT] Add podAntiAffinity to sizing plans (small.yaml, large.yaml, capped-small.yaml, capped-large.yaml). #2906
* [ENHANCEMENT] Add ability to configure and run mimir-continuous-test. #3117
* [BUGFIX] Fix wrong label selector in ingester anti affinity rules in the sizing plans. #2906
* [BUGFIX] Query-scheduler no longer periodically terminates connections from query-frontends and queriers. This caused some queries to time out and EOF errors in the logs. #3262

## 3.1.0

* [CHANGE] **breaking change** Update minio deprecated helm chart (<https://helm.min.io/>) to the supported chart's version (<https://charts.min.io/>). #2427
  - Renamed helm config values `minio.accessKey` to `minio.rootUser`.
  - Renamed helm config values `minio.secretKey` to `minio.rootPassword`.
  - Minio container images are now loaded from quay.io instead of Docker Hub. Set `minio.image.repository` value to override the default behavior.
* [CHANGE] Enable [query sharding](https://grafana.com/docs/mimir/latest/operators-guide/architecture/query-sharding/) by default. If you override the value of `mimir.config`, then take a look at `mimir.config` in the `values.yaml` from this version of the chart and incorporate the differences. If you override `mimir.config`, then consider switching to `mimir.structuredConfig`. To disable query sharding set `mimir.structuredConfig.frontend.parallelize_shardable_queries` to `false`. #2655
* [FEATURE] Add query-scheduler, which is now enabled by default. If you have copied the `mimir.config`, then update it to correctly configure the query-frontend and the querier. #2087
* [FEATURE] Added support to run graphite-proxy alongside GEM. It is disabled by default. Set `graphite.enabled=true` in your values config to get it running. #2711
* [ENHANCEMENT] Add backfill endpoints to Nginx configuration. #2478
* [ENHANCEMENT] Add `namespace` to smoke-test helm template to allow the job to be deployed within the same namespace as the rest of the deployment. #2515
* [ENHANCEMENT] Memberlist now uses DNS service-discovery by default. #2549 #2561
* [ENHANCEMENT] The Mimir configuration parameters `server.http_listen_port` and `server.grpc_listen_port` are now configurable in `mimir.structuredConfig`. #2561
* [ENHANCEMENT] Default to injecting the `no_auth_tenant` from the Mimir configuration as the value for `X-Scope-OrgID` in nginx. #2614
* [ENHANCEMENT] Default `ingester.ring.tokens-file-path` and `store-gateway.sharding-ring.tokens-file-path` to `/data/tokens` to prevent resharding on restarts. #2726
* [ENHANCEMENT] Upgrade memcached image tag to `memcached:1.6.16-alpine`. #2740
* [ENHANCEMENT] Upgrade nginx image tag to `nginxinc/nginx-unprivileged:1.22-alpine`. #2742
* [ENHANCEMENT] Upgrade minio subchart to `4.0.12`. #2759
* [ENHANCEMENT] Update agent-operator subchart to `0.2.5`. #3009
* [BUGFIX] `nginx.extraArgs` are now actually passed to the nginx container. #2336
* [BUGFIX] Add missing `containerSecurityContext` to alertmanager and tokengen job. #2416
* [BUGFIX] Add missing `containerSecutiryContext` to memcached exporter containers. #2666
* [BUGFIX] Do not use undocumented `mulf` function in templates. #2752
* [BUGFIX] Open port 80 for the Enterprise `gateway` service so that the read and write address reported by NOTES.txt is correct. Also deprecate the current default of 8080. #2860
* [BUGFIX] Periodically rebalance gRPC connection between GEM gateway and distributors after scale out of the distributors. #2862
* [BUGFIX] Remove PodSecurityPolicy when running against Kubernetes >= 1.25. #2870

## 3.0.0

* [CHANGE] **breaking change** The minimal Kubernetes version is now 1.20. This reflects the fact that Grafana does not test with older versions. #2297
* [CHANGE] **breaking change** Make `ConfigMap` the default for `configStorageType`. This means that the Mimir (or Enterprise Metrics) configuration is now created in and loaded from a ConfigMap instead of a Secret. #2277
  - Set to `Secret` to keep existing way of working. See related #2031, #2017, #2089.
  - In case the configuration is loaded from an external Secret, `useExternalConfig=true`, then `configStorageType` must be set to `Secret`.
  - Having the configuration in a ConfigMap means that `helm template` now shows the configuration directly and `helm diff upgrade` can show the changes to the configuration.
* [CHANGE] Enable multi-tenancy by default. This means `multitenancy_enabled` is now `true` for both Mimir and Enterprise Metrics. Nginx will inject `X-Scope-OrgID=anonymous` header if the header is not present, ensuring backwards compatibility. #2117
* [CHANGE] **breaking change** The value `serviceMonitor` and everything under it is moved to `metaMonitoring.serviceMonitor` to group all meta-monitoring settings under one section. #2236
* [CHANGE] Added support to install on OpenShift. #2219
  - **breaking change** The value `rbac.pspEnabled` was removed.
  - Added new `rbac.type` option. Allowed values are `psp` and `scc`, for Pod Security Policy and Security Context Constraints (OpenShift) respectively.
  - Added `rbac.create` option to enable/disable RBAC configuration.
  - mc path in Minio changed to be compatible with OpenShift security.
* [CHANGE] **breaking change** Chart now uses custom memcached templates to remove bitnami dependency. There are changes to the Helm values, listed below. #2064
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
* [CHANGE] Change default value for `blocks_storage.bucket_store.chunks_cache.memcached.timeout` to `450ms` to increase use of cached data. #2035
* [CHANGE] Remove setting `server.grpc_server_max_recv_msg_size` and `server.grpc_server_max_send_msg_size` to 100MB, since it is the default now, see #1884. #2300
* [FEATURE] Add `mimir-continuous-test` in smoke-test mode. Use `helm test` to run a smoke test of the read + write path.
* [FEATURE] Add meta-monitoring via the Grafana Agent Kubernetes operator: scrape metrics and collect logs from Mimir pods and ship them to a remote. #2068
* [ENHANCEMENT] Update memcached statefulset manifest #2321
  - Added imagePullSecrets block to pull images from private registry
  - Added resources block for memcachedExporter
* [ENHANCEMENT] ServiceMonitor object will now have default values based on release namesapce in the `namespace` and `namespaceSelector` fields. #2123
* [ENHANCEMENT] Set the `namespace` metadata field for all kubernetes objects to enable using `--namespace` correctly with Helm even if the specified namespace does not exist. #2123
* [ENHANCEMENT] The new value `serviceMonitor.clusterLabel` controls whether to add a `cluster` label and with what content to ServiceMonitor metrics. #2125
* [ENHANCEMENT] Set the flag `ingester.ring.instance-availability-zone` to `zone-default` for ingesters. This is the first step of introducing multi-zone ingesters. #2114
* [ENHANCEMENT] Add `mimir.structuredConfig` for adding and modifing `mimir.config` values after template evaulation. It can be used to alter individual values in the configuration and it's structured YAML instead of text. #2100
* [ENHANCEMENT] Add `global.podAnnotations` which can add POD annotations to PODs directly controlled by this chart (mimir services, nginx). #2099
* [ENHANCEMENT] Introduce the value `configStorageType` which can be either `ConfigMap` or `Secret`. This value sets where to store the Mimir/GEM application configuration. When using the value `ConfigMap`, make sure that any secrets, passwords, keys are injected from the environment from a separate `Secret`. See also: #2031, #2017. #2089
* [ENHANCEMENT] Add `global.extraEnv` and `global.extraEnvFrom` to values. This enables setting common environment variables and common injection of secrets to the POD environment of Mimir/GEM services and Nginx. Memcached and minio are out of scope for now. #2031
* [ENHANCEMENT] Add `extraEnvFrom` capability to all Mimir services to enable injecting secrets via environment variables. #2017
* [ENHANCEMENT] Enable `-config.expand-env=true` option in all Mimir services to be able to take secrets/settings from the environment and inject them into the Mimir configuration file. #2017
* [ENHANCEMENT] Add a simple test for enterprise installation #2027
* [ENHANCEMENT] Check for the containerSecurityContext in values file. #2112
* [ENHANCEMENT] Add `NOTES.txt` to show endpoints URLs for the user at install/upgrade. #2189
* [ENHANCEMENT] Add ServiceMonitor for overrides-exporter. #2068
* [ENHANCEMENT] Add `nginx.resolver` for allow custom resolver in nginx configuration and `nginx.extraContainers` which allow add side containers to the nginx deployment #2196

## 2.1.0

* [ENHANCEMENT] Bump image version to 2.1 #2001
  - For Grafana Mimir, see the release notes here: [Grafana Mimir 2.1](https://grafana.com/docs/mimir/latest/release-notes/v2.1/)
  - For Grafana Enterprise Metrics, see the release notes here: [Grafana Enterprise Metrics 2.1](https://grafana.com/docs/enterprise-metrics/v2.1.x/release-notes/v2-1/)
* [ENHANCEMENT] Disable `ingester.ring.unregister-on-shutdown` and `distributor.extend-writes` #1994
  - This will prevent resharding every series during a rolling ingester restart
  - Under some circumstances the previous values (both enabled) could cause write path degredation during rolling restarts
* [ENHANCEMENT] Add support for the results cache used by the query frontend #1993
  - This will result in additional resource usage due to the addition of one or
    more memcached replicas. This applies when using small.yaml, large.yaml,
    capped-large.yaml, capped-small.yaml, or when setting
    `memcached-results.enabled=true`
* [BUGFIX] Set up using older bitnami chart repository for memcached as old charts were deleted from the current one. #1998
* [BUGFIX] Use grpc round-robin for distributor clients in GEM gateway and self-monitoring
  - This utilizes an additional headless service for the distributor pods

## 2.0.14

* [BUGFIX] exclude headless services from ServiceMonitors to prevent duplication of prometheus scrape targets #1308

## 2.0.13

* [ENHANCEMENT] Removed `rbac.create` option. #1317

## 2.0.12

* [ENHANCEMENT] Add memberlist named port to container spec. #1311

## 2.0.11

* [ENHANCEMENT] Turn `ruler` and `override-exporter` into optional components. #1304

## 2.0.10

* [ENHANCEMENT] Reorder some values for consistency. #1302
* [BUGFIX] Add missing `admin_api.env`, `gateway.env` and `overrides_exporter.env` values. #1302
* [BUGFIX] Remove `<service>.extraPorts` from values as it has no effect. #1302

## 2.0.9

* [ENHANCEMENT] Disable gateway ingress by default. #1303
* [BUGFIX] Fix null port at gateway ingress definition. #1303

## 2.0.8

* [ENHANCEMENT] Add validation if `activity_tracker.filepath` is missing in `mimir.config`. #1290
* [ENHANCEMENT] Add validation if `server.http_listen_port` or `server.grpc_listen_port` is set in `mimir.config`. #1290
* [BUGFIX] Add missing empty array definition for `extraVolumeMounts` in admin_api, gateway and override-exporter. #1290
* [BUGFIX] Fix wrong template called in nginx helper. #1290

## 2.0.7

* [ENHANCEMENT] Add option to modify the port for the GEM gateway service. #1270

## 2.0.6

* [ENHANCEMENT] Add option for an ingress on GEM gateway. #1266

## 2.0.5

* [BUGFIX] Use new component name system for gateway ingress. This regression has been introduced with #1203. #1260

## 2.0.4

* [ENHANCEMENT] Determine PodDisruptionBudget APIVersion based on running version of k8s #1229

## 2.0.3

* [ENHANCEMENT] Update README.md with helm-docs version 1.8.1 instead of old 1.4.0. #1230

## 2.0.2

* [ENHANCEMENT] Update Grafana Enterprise Metrics docker image tag to v2.0.1 #1241

## 2.0.1

* [BUGFIX] Honor `global.clusterDomain` when referencing internal services, e.g. alertmanager or nginx gateway. #1227

## 2.0.0

* [CHANGE] **Breaking** for existing users of `mimir-distributed`: the naming convention is changed to have shorter resource names, as in `<release>-mimir-distributed-store-gateway` is now just `<release>-mimir-store-gateway`. To have the previous names, please specify `nameOverride: mimir-distributed` in the values. #1203
* [CHANGE] The chart `enterprise-metrics` is renamed to `mimir-distributed`. #1203
* [CHANGE] **Breaking** Configuration for Grafana Enterprise Metrics is now in the value `mimir.config` as a helm template **string**.
  Please consult the [Grafana Enterprise Migration Guide](https://grafana.com/docs/enterprise-metrics/latest/migrating-from-gem-1.7/) to learn more about how to upgrade the configuration.
  Except for the following parameters specified as command line parameters in the Pod templates,
  everything is now set in this string-typed value, giving a definitive source of configuration.
  Exceptions:
    > The `-target=` must be provided individually.\
    The `-config.file=` obviously.\
    User defined arguments from `.<service>.extraArgs`.
* [CHANGE] **Breaking** Kubernetes object labels now follow the [kubernetes standard](https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/) (e.g. `app.kubernetes.io/component=ingester`). To enable smooth upgrade and compatibility with previous Grafana Enterprise Metrics Helm chart, the value `enterprise.legacyLabels` should be set to `true`.
* [CHANGE] **Breaking** Ingesters only support `StatefulSet` from now on as chunks storage was removed in favour of blocks storage.
* [CHANGE] **Breaking** Compactor is a required component, the value `compactor.enabled` is removed.
* [CHANGE] **Breaking** The configuration parameter `server.http_listen_port` and `server.grpc_listen_port` cannot be changed from their defaults.
* [CHANGE] The default for `ingester.ring.replication_factor` is now 3 and there will be 3 ingesters started even with the default `values.yaml`.
  On the other hand, Pod anti affinity is turned off by default to allow single node deployment.
* [FEATURE] Upgrade to [Grafana Enterprise Metrics v2.0.0](https://grafana.com/docs/enterprise-metrics/v2.0.x/)
* [FEATURE] Reworked chart to enable installing Grafana Mimir open source software version without licensed features.
* [FEATURE] Added the value `nameOverride` to enable migration from Cortex helm chart.
* [FEATURE] The alertmanager can be disabled with `alertmanager.enabled: false`, to support the use case of external alertmanager.
* [FEATURE] Added definitions of `ServiceMonitor` objects for Prometheus monitoring. Configuration is done via the `serviceMonitor` values. This enables partial functionality of Grafana Mimir dashboards out of the box - without alerts and recording rules pre-loaded.
* [ENHANCEMENT] Minio bucket creation is not tied to `admin-api` anymore, moved to its own job `templates/minio/create-bucket-job.yaml`.
* [BUGFIX] `.<service>.PodDisruptionBudget` was not working. Added template definition for all services. Pod disruption budget is enabled for the ingesters and store-gateways by default.
* [BUGFIX] Fix typo in value `.alertmanager.statefulset` to `.alertmanager.statefulSet`.
* [BUGFIX] Remove unused value `.useExternalLicense`.

## Entries from enterprise-metrics chart

## 1.8.1

* [ENHANCEMENT] Support Grafana Mimir monitoring mixin labels by setting container names to the component names.
  This will make it easier to select different components in cadvisor metrics.
  Previously, all containers used "enterprise-metrics" as the container name.
  Now, for example, the ingester Pod will have a container name "ingester" rather than "enterprise-metrics".

## 1.8.0

* [FEATURE] Upgrade to [Grafana Enterprise Metrics v1.7.0](https://grafana.com/docs/metrics-enterprise/latest/downloads/#v170----january-6th-2022).

## 1.7.3

* [BUGFIX] Alertmanager does not fail anymore to load configuration via the API. #945

## 1.7.2

* [CHANGE] The Ingester statefulset now uses podManagementPolicy Parallel, upgrading requires recreating the statefulset #920

## 1.7.1

* [BUGFIX] Remove chunks related default limits. #867

## 1.7.0

* [FEATURE] Upgrade to [Grafana Enterprise Metrics v1.6.1](https://grafana.com/docs/metrics-enterprise/latest/downloads/#v161----november-18th-2021). #839

## 1.6.0

* [FEATURE] Upgrade to [Grafana Enterprise Metrics v1.5.1](https://grafana.com/docs/metrics-enterprise/latest/downloads/#v151----september-21st-2021). #729
* [CHANGE] Production values set the ingester replication factor to three to avoid data loss.
  The resource calculations of these values already factored in this replication factor but did not apply it in the configuration.
  If you have not reduced the compute resources in these values then this change should have no impact besides increased resilience to ingester failure.
  If you have reduced the compute resources, consider increasing them back to the recommended values before installing this version. #729

## 1.5.6

* [BUGFIX] YAML exports are no longer included as part of the Helm chart. #726

## 1.5.5

* [BUGFIX] Ensure all PodSpecs have configurable initContainers. #708

## 1.5.4

* [BUGFIX] Adds a `Service` resource for the Compactor Pods and adds Compactor to the default set of gateway proxy URLs. In previous chart versions the Compactor would not show up in the GEM plugin "Ring Health" tab because the gateway did not know how to reach Compactor. #714

## 1.5.3

* [BUGFIX] This change does not affect single replica deployments of the
  admin-api but does fix the potential for an inconsistent state when
  running with multiple replicas of the admin-api and experiencing
  parallel writes for the same objects. #675

## 1.5.2

* [CHANGE] Removed all references to Consul in the yaml files since GEM will be focused on deploying with memberlist. Deleted the multi-kv-consul-primary-values.yaml and multi-kv-memberlist-primary-values.yaml files since they assume you're running Consul as your primary or second kvstore. #674

## 1.5.1

* [BUGFIX] Unused `ingress` configuration section removed from `values.yaml`. #658

## 1.5.0

* [FEATURE] Upgrade to [Grafana Enterprise Metrics v1.5.0](https://grafana.com/docs/metrics-enterprise/latest/downloads/#v150----august-24th-2021). #641

## 1.4.7

* [CHANGE] Enabled enterprise authentication by default.
  > **Breaking:** This change can cause losing access to the GEM cluster in case `auth.type` has not
  > been set explicitly.
  > This is a security related change and therefore released in a patch release.

## 1.4.6

* [FEATURE] Run an instance of the GEM overrides-exporter by default. #590

## 1.4.5

* [BUGFIX] Add `memberlist.join` configuration to the ruler. #618

## 1.4.4

* [CHANGE] Removed livenessProbe configuration as it can often be more detrimental than having none. Users can still configure livenessProbes with the per App configuration hooks. #594

## 1.4.3

* [ENHANCEMENT] Added values files for installations that require setting resource limits. #583

## 1.4.2

* [CHANGE] The compactor data directory configuration has been corrected to `/data`. #562
  > **Note:** The compactor is stateless and no data stored in the existing data directory needs to be moved in order to facilitate this upgrade.
  > For more information, refer to the [Cortex Compactor documentation](https://cortexmetrics.io/docs/blocks-storage/compactor/).
* [FEATURE] Upgrade to [Grafana Enterprise Metrics v1.4.2](https://grafana.com/docs/metrics-enterprise/latest/downloads/#v142----jul-21st-2021) #562

## 1.4.1

* [BUGFIX] Fixed DNS address of distributor client for self-monitoring. #569

## 1.4.0

* [CHANGE] Use updated querier response compression configuration, changed in 1.4.0. #524
* [CHANGE] Use updated alertmanager storage configuration, changed in 1.4.0. #524
* [FEATURE] Upgrade to [Grafana Enterprise Metrics v1.4.1](https://grafana.com/docs/metrics-enterprise/latest/downloads/#v141----june-29th-2021). #524
* [FEATURE] Enable [GEM self-monitoring](https://grafana.com/docs/metrics-enterprise/latest/self-monitoring/). #524

## 1.3.5

* [CHANGE] The GRPC port on the query-frontend and store-gateway Kubernetes Services have been changed to match the naming of all other services. #523
* [FEATURE] Expose GRPC port on all GEM services. #523

## 1.3.4

* [BUGFIX] Removed symlinks from chart to fix Rancher repository imports. #504

## 1.3.3

* [FEATURE] The GEM config now uses the `{{ .Release.Name }}` variable as the default value for `cluster_name` which removes the need to additionally override this setting during an initial install. #500

## 1.3.2

* [FEATURE] Chart memcached dependencies are now at the latest release. This includes the memcached and the related exporter. #467

## 1.3.1

* [BUGFIX] Use non-deprecated alertmanager flags for cluster peers. #441
* [BUGFIX] Make store-gateway Service not headless. #441

## 1.3.0

* [FEATURE] Upgrade to [Grafana Enterprise Metrics v1.3.0](https://grafana.com/docs/metrics-enterprise/latest/downloads/#v130----april-26th-2021). #415

## 1.2.0

* [CHANGE] The chart now uses memberlist for the ring key-value store removing the need to run Consul. #340
  > **Warning:** Existing clusters will need to follow an upgrade procedure.
  > **Warning:** Existing clusters should first be upgraded to `v1.1.1` and use that version for migration before upgrading to `v1.2.0`.
  To upgrade to using memberlist:
  1. Ensure you are running the `v1.1.1` version of the chart.
  2. Deploy runtime `multi_kv_config` to use Consul as a primary and memberlist as the secondary key-value store.
     The values for such a change can be found in the [`multi-kv-consul-primary-values.yaml`](./multi-kv-consul-primary-values.yaml).
  3. Verify the configuration is in use by querying the [Configuration](https://cortexmetrics.io/docs/api/#configuration) HTTP API endpoint.
  4. Deploy runtime `multi_kv_config` to use memberlist as the primary and Consul as the secondary key-value store.
     The values for such a change can be found in [`multi-kv-memberlist-primary-values.yaml`](./multi-kv-memberlist-primary-values.yaml)
  5. Verify the configuration is in use by querying the [Configuration](https://cortexmetrics.io/docs/api/#configuration) HTTP API endpoint.
  6. Deploy `v1.2.0` helm chart which configures memberlist as the sole key-value store and removes the Consul resources.

## 1.1.1

* [FEATURE] Facilitate some runtime configuration of microservices. #342
* [FEATURE] Upgrade to [Grafana Enterprise Metrics v1.2.0](https://grafana.com/docs/metrics-enterprise/latest/downloads/#v120----march-10-2021). #342

## 1.1.0

* [CHANGE] The memcached chart from the deprecated Helm stable repository has been removed and replaced with a Bitnami chart. #333
  > **Warning:** This change will result in the cycling of your memcached Pods and will invalidate the existing cache.
* [CHANGE] Memcached Pod resource limits have been lowered to match requests. #333
* [FEATURE] YAML exports have been created for all chart values files. #333
* [BUGFIX] The values for the querier/ruler/store-gateway `-<prefix>.memcached.max-item-size` have been corrected to match the limit configured on the memcached server. #333

## 1.0.0

* [FEATURE] Initial versioned release. ##168

## Entries from mimir-distributed chart

## 0.1.8

* [BUGFIX] Fix nginx routing for rules and expose buildinfo. #1233

## 0.1.7

* [BUGFIX] Remove misplaced config value and add affinity rules in `capped-small.yaml` and `capped-large.yaml`. #1225

## 0.1.6

* [CHANGE] **Breaking** Compactor is a required component, the value `compactor.enabled` is removed. #1193
* [FEATURE] The alertmanager can be disabled with `alertmanager.enabled: false`, to support the use case of external alertmanager. #1193

## 0.1.5

* [BUGFIX] Fix labels for Mimir dashboards. #1190

## 0.1.4

* [BUGFIX] Fix documentation link missing slash. #1177

## 0.1.3

* [FEATURE] Add ServiceMonitor definitions. #1156

## 0.1.2

* [BUGFIX] Fix the naming of minio configmap and secret in the parent chart. #1152

## 0.1.1

* [BUGFIX] CI fixes. #1144

## 0.1.0

* [FEATURE] Initial commit, Mimir only, derived from `enterprise-metrics` chart. #1141
