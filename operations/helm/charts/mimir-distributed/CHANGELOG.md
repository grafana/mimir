# Changelog

This changelog is continued from `enterprise-metrics` after Grafana Enterprise Metrics was added to `mimir-distributed` in PR #1203.
All notable changes to this chart will be documented in this file.

Entries should be ordered as follows:
- [CHANGE]
- [FEATURE]
- [ENHANCEMENT]
- [BUGFIX]

Entries should include a reference to the Pull Request that introduced the change.

## main / unreleased

* [CHANGE] Change default value for `blocks_storage.bucket_store.chunks_cache.memcached.timeout` to `450ms` to increase use of cached data. #2035
* [ENHANCEMENT] Add `global.extraEnv` and `global.extraEnvFrom` to values. This enables setting common environment variables and common injection of secrets to the POD environment of Mimir/GEM services and Nginx. Memcached and minio are out of scope for now. #2031
* [ENHANCEMENT] Add `extraEnvFrom` capability to all Mimir services to enable injecting secrets via environment variables. #2017
* [ENHANCEMENT] Enable `-config.expand-env=true` option in all Mimir services to be able to take secrets/settings from the environment and inject them into the Mimir configuration file. #2017
* [ENHANCEMENT] Add a simple test for enterprise installation #2027

## 2.1.0-beta.7

* [ENHANCEMENT] Bump image version to 2.1 #2001
  - For Grafana Mimir, see the release notes here: [Grafana Mimir 2.1](https://grafana.com/docs/mimir/latest/release-notes/v2.1/)
  - For Grafana Enterprise Metrics, see the release notes here: [Grafana Enterprise Metrics 2.1](https://grafana.com/docs/enterprise-metrics/v2.1.x/release-notes/v2-1/)

## 2.1.0-beta.6

* [ENHANCEMENT] Disable `ingester.ring.unregister-on-shutdown` and `distributor.extend-writes` #1994
  - This will prevent resharding every series during a rolling ingester restart
  - Under some circumstances the previous values (both enabled) could cause write path degredation during rolling restarts

## 2.1.0-beta.5

* [ENHANCEMENT] Add support for the results cache used by the query frontend #1993
  - This will result in additional resource usage due to the addition of one or
    more memcached replicas. This applies when using small.yaml, large.yaml,
    capped-large.yaml, capped-small.yaml, or when setting
    `memcached-results.enabled=true`

## 2.1.0-beta.4

* [BUGFIX] Set up using older bitnami chart repository for memcached as old charts were deleted from the current one. #1998

## 2.1.0-beta.3

* [BUGFIX] Use grpc round-robin for distributor clients in GEM gateway and self-monitoring
  - This utilizes an additional headless service for the distributor pods

## 2.1.0-beta.2

* [ENHANCEMENT] Version bump only for release tests.

## 2.1.0-beta.1

* [ENHANCEMENT] Version bump only for release tests.

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
