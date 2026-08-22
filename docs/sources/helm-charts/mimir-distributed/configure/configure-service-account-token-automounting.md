---
description: Learn how to stop Grafana Mimir Pods from auto-mounting Kubernetes service account tokens when using Helm.
menuTitle: Configure service account token auto-mounting
title: Configure service account token auto-mounting with Helm
weight: 30
---

# Configure service account token auto-mounting with Helm

By default, Kubernetes mounts a service account token into every Pod. The CIS Kubernetes Benchmark recommends that you only mount service account tokens where they are necessary, so that a compromised container cannot reach the Kubernetes API with the Pod's identity.

No Grafana Mimir component calls the Kubernetes API during normal operation. Mimir discovers its peers through DNS rather than through the Kubernetes API, and the only role that the chart binds to the Mimir service account grants the use of a PodSecurityPolicy or a SecurityContextConstraints resource, which Kubernetes evaluates when it admits the Pod rather than at runtime.

You can therefore opt Pods out of token auto-mounting by setting `global.automountServiceAccountToken` to `false`:

```yaml
global:
  automountServiceAccountToken: false
```

Components that authenticate to HashiCorp Vault with the token are the exception, and the chart keeps their token automatically. For more information, refer to [Components that authenticate to Vault](#components-that-authenticate-to-vault).

## How the setting is resolved

The value is set on the Pod specification rather than on the ServiceAccount resource, which means it applies whether the chart creates the service account or you bring your own through `serviceAccount.create: false`.

The value has three states:

- Unset, which is `null` and the default. The chart omits the field, so the Kubernetes default applies and the token is mounted. This preserves the behavior of earlier chart versions.
- `false`. The token is not mounted.
- `true`. The token is mounted, regardless of what the service account itself specifies.

Every component that the chart deploys also accepts `automountServiceAccountToken`, which takes precedence over `global.automountServiceAccountToken`. For example, to opt out everywhere except the store-gateway:

```yaml
global:
  automountServiceAccountToken: false

store_gateway:
  automountServiceAccountToken: true
```

## Components that authenticate to Vault

Two separate integrations authenticate to HashiCorp Vault with the Pod's service account token. When you set `global.automountServiceAccountToken` to `false`, the chart keeps the token on the components that either integration covers, so that authentication keeps working. A per-component `automountServiceAccountToken` always overrides this, which is what you use when you mount a token yourself.

### Vault Agent injection

When you set `vaultAgent.enabled` to `true`, the chart annotates Pods so that the Vault Agent injector adds a sidecar, which authenticates to Vault with the Pod's token. This covers the alertmanager, compactor, distributor, gateway, ingester, overrides-exporter, querier, query-frontend, query-scheduler, ruler and store-gateway components. For more information, refer to [Configure Grafana Mimir to allow Vault Agent to inject certificates and keys into Pods](../configure-hashicorp-vault-agent/).

### Mimir's own Vault client

When the Mimir configuration sets `vault.enabled` to `true` with `vault.auth.type` set to `kubernetes`, Mimir reads the token itself. This covers every component that runs the Mimir binary, which additionally includes the dedicated ruler query path, but not the gateway.

The chart does not keep the token in the following three cases, because Mimir does not read the auto-mounted token file:

- `vault.auth.kubernetes.service_account_token` provides the JSON web token inline, so Mimir reads no file at all.
- `vault.auth.kubernetes.service_account_token_path` points to a file outside the `/var/run/secrets/kubernetes.io/serviceaccount/` directory, which means that you project the token yourself.
- `useExternalConfig` is set to `true`. The chart cannot inspect a configuration that it does not generate, so it cannot detect that Vault is in use. Set `automountServiceAccountToken` on the affected components yourself.

The smoke-test and continuous-test components run with `-target=continuous-test`, which never initializes Mimir's Vault client, so the chart opts them out along with everything else.

## Custom containers that need a token

If you add a container through `extraContainers` or `initContainers` that needs to reach the Kubernetes API, then it must project a token itself, because opting the Pod out removes the volume that Kubernetes would otherwise have added. Add the following to the component's `extraVolumes` and `extraVolumeMounts`, and to the `volumeMounts` of your container:

```yaml
volumeMounts:
  - mountPath: /var/run/secrets/kubernetes.io/serviceaccount
    name: kube-api-access

volumes:
  - name: kube-api-access
    projected:
      sources:
        - serviceAccountToken:
            path: token
        - configMap:
            items:
              - key: ca.crt
                path: ca.crt
            name: kube-root-ca.crt
        - downwardAPI:
            items:
              - fieldRef:
                  apiVersion: v1
                  fieldPath: metadata.namespace
                path: namespace
```

Cloud provider identity webhooks, such as IAM roles for service accounts on Amazon EKS and workload identity on Azure Kubernetes Service, inject their own token volume with a non-default audience, so they are unaffected by this setting.

## Limitations

The setting only reaches Pods that this chart templates directly. The following Pods are unaffected:

- The rollout-operator, which the chart deploys as a subchart and which needs Kubernetes API access to coordinate rollouts.
- The minio and grafana-agent-operator subcharts. If you use the bundled minio deployment, note that it is intended for testing rather than production.
- The Grafana Agent Pods for meta-monitoring. The chart creates a `GrafanaAgent` custom resource, and the grafana-agent-operator builds the workloads from it, so there is no Pod specification for the chart to set the field on. The Agent needs Kubernetes API access to discover scrape targets in any case.
