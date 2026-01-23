---
description: "Migrate the Helm chart from version 5.x to 6.0"
title: "Migrate the Helm chart from version 5.x to 6.0"
menuTitle: "Migrate Helm chart 5.x to 6.0"
weight: 120
---

# Migrate the Helm chart from version 5.x to 6.0

The `mimir-distributed` Helm chart version 6.0 introduces several breaking changes:

- Ingest storage and Kafka are enabled by default. The bundled Kafka deployment is for demonstration and testing purposes only and is not suitable for production use.
- Rollout-operator webhooks are enabled by default, which requires installing CRDs before upgrading.
- The top-level `nginx` values section is removed. You must migrate to the unified gateway deployment before upgrading to version 6.0.

## Before you begin

- You are running `mimir-distributed` Helm chart version 5.x.
- If you're using the rollout-operator, you have cluster permissions to install CRDs.
- If you're using the top-level `nginx` values, you have already migrated to the unified gateway deployment.

## Procedure

### Migrate to unified gateway deployment (if needed)

If your values file contains a top-level `nginx` section, you must migrate to the unified gateway deployment before upgrading to version 6.0.

Follow the [Migrate to unified proxy deployment](https://grafana.com/docs/helm-charts/mimir-distributed/v5.8.x/migration-guides/migrate-to-unified-proxy-deployment/) guide to complete this migration.

### Account for the rollout-operator

If your deployment uses the rollout-operator, you must ensure that the required CustomResourceDefinitions (CRDs) are installed. If you don't use the rollout-operator, you must explicitly disable it in your values file to avoid unnecessary components and issues with subsequent rollouts.

#### Install CRDs if using the rollout-operator

If you're using the rollout-operator, install the CRDs from the rollout-operator chart:

```bash
kubectl apply -f https://raw.githubusercontent.com/grafana/helm-charts/main/charts/rollout-operator/crds/replica-templates-custom-resource-definition.yaml
kubectl apply -f https://raw.githubusercontent.com/grafana/helm-charts/main/charts/rollout-operator/crds/zone-aware-pod-disruption-budget-custom-resource-definition.yaml
```

#### Disable the rollout-operator if not in use

If you don't use the rollout-operator, disable it in your values file to prevent the installation of related webhooks which will interfere with subsequent rollouts:

```yaml
rollout_operator:
  enabled: false
```

### Choose your ingest storage strategy

Choose one of the following options:

#### Migrate to ingest storage

If you want to migrate your existing installation to use ingest storage, follow the [Migrate from classic to ingest storage architecture](https://grafana.com/docs/mimir/latest/set-up/migrate/migrate-ingest-storage/) guide.

{{% admonition type="note" %}}
The Kafka deployment included in the Helm chart is for demonstration and testing purposes only. For production deployments, set up your own Kafka-compatible backend and configure Mimir to connect to it.
{{% /admonition %}}

To use your own Kafka cluster:

1. Set up a production-grade Kafka-compatible backend (such as Apache Kafka, Amazon MSK, or Confluent Cloud).
1. Disable the bundled Kafka deployment in your values file:
   ```yaml
   kafka:
     enabled: false
   ```
1. Configure Mimir to connect to your Kafka cluster. Refer to the [Configure the Kafka backend](https://grafana.com/docs/mimir/latest/configure/configure-kafka-backend/) documentation for configuration details.

#### Continue using classic architecture (disable ingest storage)

Classic architecture is supported in Mimir version 3.0. If you want to continue using the classic architecture without ingest storage, add the following to your values file:

```yaml
mimir:
  structuredConfig:
    ingest_storage:
      enabled: false
    ingester:
      push_grpc_method_enabled: true
kafka:
  enabled: false
```

After adding these values, upgrade to version 6.0.

### Upgrade to version 6.0

After completing the prerequisites and choosing your ingest storage strategy, upgrade the Helm release:

```bash
helm upgrade <RELEASE_NAME> grafana/mimir-distributed --version 6.0.2 -f <VALUES_FILE>
```

## Troubleshooting

Follow this guidance to recover from common issues with the rollout operator during migration.

### Rollout operator misconfiguration

If you don't install the rollout operator CustomResourceDefinitions (CRDs) and don't disable the rollout operator, your deployment enters an error state.

If you intend to use the rollout-operator, install the CRDs as described above.

If you intend to disable the rollout-operator, follow these steps:

1. Disable the rollout-operator in your values file.

```yaml
rollout-operator:
  enabled: false
```

2. Delete the rollout-operator validating and mutating webhook configurations.

```bash
kubectl delete validatingwebhookconfiguration no-downscale-<NAMESPACE>
kubectl delete validatingwebhookconfiguration pod-eviction-<NAMESPACE>
kubectl delete mutatingwebhookconfigurations prepare-downscale-<NAMESPACE>
```

3. Re-apply the updated configuration

```bash
helm upgrade <RELEASE_NAME> grafana/mimir-distributed --version 6.0.2 -f <VALUES_FILE> --reset-values
```
