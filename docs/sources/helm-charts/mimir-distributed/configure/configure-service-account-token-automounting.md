---
description: Learn how to control whether Grafana Mimir Pods mount a Kubernetes service account token when you use Helm.
menuTitle: Service account tokens
title: Configure service account token auto-mounting with Helm
weight: 30
---

# Configure service account token auto-mounting with Helm

Kubernetes mounts a service account token into every Pod by default, whether or not the Pod ever calls the Kubernetes API. The CIS Kubernetes Benchmark recommends mounting these tokens only where they are needed, so that a compromised container cannot use the Pod's identity against the Kubernetes API.

Grafana Mimir does not need one. Mimir components find each other over DNS, not through the Kubernetes API. The only role that the chart binds to the Mimir service account grants the use of a PodSecurityPolicy or a SecurityContextConstraints resource, which Kubernetes checks when it admits the Pod, not while the Pod runs.

## Turn off auto-mounting

To opt every Pod that the chart manages out of token auto-mounting, add the following to your values file:

```yaml
global:
  automountServiceAccountToken: false
```

The chart sets the field on the Pod specification, not on the ServiceAccount resource. It therefore applies whether the chart creates the service account or you bring your own with `serviceAccount.create: false`.

Components that authenticate to HashiCorp Vault with the token are an exception. The chart keeps their token. Refer to [Components that authenticate to Vault](#components-that-authenticate-to-vault).

## Choose a value

`automountServiceAccountToken` has three states.

| Value              | Result                                                                                                                   |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| Unset, the default | The chart omits the field. The Kubernetes default applies and a token is mounted. Existing installations are unaffected. |
| `false`            | No token is mounted.                                                                                                     |
| `true`             | A token is mounted, even if the service account itself opts out.                                                         |

Every component that the chart deploys also accepts its own `automountServiceAccountToken`. It takes precedence over `global.automountServiceAccountToken`. For example, to opt out everywhere except the store-gateway:

```yaml
global:
  automountServiceAccountToken: false

store_gateway:
  automountServiceAccountToken: true
```

## Components that authenticate to Vault

Two integrations authenticate to HashiCorp Vault with the Pod's service account token. When you set `global.automountServiceAccountToken` to `false`, the chart determines which components either integration covers, and keeps the token on those Pods.

To override that, set the component's own `automountServiceAccountToken` to `false`. Do this when you mount a token into those Pods yourself.

### Vault Agent injection

When you set `vaultAgent.enabled` to `true`, the chart annotates Pods so that the Vault Agent injector adds a sidecar. The sidecar authenticates with the Pod's token. This covers the alertmanager, compactor, distributor, gateway, ingester, overrides-exporter, querier, query-frontend, query-scheduler, ruler, and store-gateway components. For more information, refer to [Configure Grafana Mimir to allow Vault Agent to inject certificates and keys into Pods](../configure-hashicorp-vault-agent/).

### Mimir's Vault client

When the Mimir configuration sets `vault.enabled` to `true` and `vault.auth.type` to `kubernetes`, Mimir reads the token itself. This covers every component that runs the Mimir binary, including the dedicated ruler query path, but not the gateway or the caches.

There are two exceptions, because Mimir then reads no auto-mounted file:

- `vault.auth.kubernetes.service_account_token` supplies the JSON Web Token inline. Mimir reads no file at all.
- `vault.auth.kubernetes.service_account_token_path` points outside `/var/run/secrets/kubernetes.io/serviceaccount/`. You mount the token yourself.

The smoke-test and continuous-test Pods run with `-target=continuous-test`, which never starts Mimir's Vault client. The chart opts them out along with everything else.

### External configurations

When you set `useExternalConfig` to `true`, the chart cannot inspect a configuration that it does not generate, and cannot tell whether Mimir authenticates to Vault. Declare it with `externalConfigVaultKubernetesAuth`:

```yaml
useExternalConfig: true
externalConfigVaultKubernetesAuth: true

global:
  automountServiceAccountToken: false
```

The chart then applies the same component scope as for a configuration that it generates. Nothing per-component is left for you to maintain, and nothing needs revisiting when a later chart version adds a component.

Leave `externalConfigVaultKubernetesAuth` at its default of `false` if your configuration falls into either of the preceding exceptions, or if it does not use Vault.

## Add a token to your own containers

Opting a Pod out removes the token volume from the whole Pod, not only from the Mimir container. If a container that you add through `extraContainers` or `initContainers` needs the Kubernetes API, mount a token for it. Put the volume in the component's `extraVolumes` and the mount on your container:

```yaml
extraVolumes:
  - name: kube-api-access
    projected:
      sources:
        - serviceAccountToken:
            path: token
        - configMap:
            name: kube-root-ca.crt
            items:
              - key: ca.crt
                path: ca.crt
        - downwardAPI:
            items:
              - fieldRef:
                  apiVersion: v1
                  fieldPath: metadata.namespace
                path: namespace

extraContainers:
  - name: my-container
    image: my-image:latest
    volumeMounts:
      - name: kube-api-access
        mountPath: /var/run/secrets/kubernetes.io/serviceaccount
```

Identity webhooks from cloud providers, such as IAM roles for service accounts on Amazon EKS and workload identity on Azure Kubernetes Service, inject their own token volume with a non-default audience. This setting does not affect them.

## What this setting does not cover

`global.automountServiceAccountToken` reaches only the Pods that this chart templates itself. The following Pods keep the Kubernetes default:

- The rollout-operator, which the chart deploys as a subchart. It needs Kubernetes API access to coordinate rollouts.
- The minio and grafana-agent-operator subcharts. The bundled minio deployment is intended for testing, not production.
- The Grafana Agent Pods for meta-monitoring. The chart creates a `GrafanaAgent` custom resource and the grafana-agent-operator builds the workloads from it, so there is no Pod specification for the chart to set the field on. The Agent needs Kubernetes API access to discover scrape targets in any case.
