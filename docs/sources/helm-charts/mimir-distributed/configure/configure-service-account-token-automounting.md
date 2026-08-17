---
title: "Configure service account token auto-mounting"
menuTitle: "Service account token auto-mounting"
description: "Learn how to stop the mimir-distributed Helm chart from mounting Kubernetes service account tokens into pods that don't need them."
---

# Configure service account token auto-mounting

Kubernetes puts a service account token into every container of every pod it creates, unless you tell it not to. The token is effectively a login for the Kubernetes API, and it appears inside the container at `/var/run/secrets/kubernetes.io/serviceaccount`. The CIS Kubernetes Benchmark asks you to mount tokens only where they are needed, and scanners such as kube-bench and Checkov report pods that don't opt out.

Mimir components don't use the service account under regular operations, as they find each other over DNS rather than through the Kubernetes API, and service accounts get no role bindings by default. A role would be used on OpenShift, for SecurityContextConstraints, but this is checked when Kubernetes admits the pod (not via the token). In case meta-monitoring is enabled, however, read access is granted to the relevant service account for use via Kubernetes API. And even without rights assigned, the token is still a working credential, so anyone who gets into a Mimir container can use it against your API server.

To opt out of auto-mounting, set `global.automountServiceAccountToken: false` for chart-managed pods, or `serviceAccount.automountServiceAccountToken: false` for the chart-managed service account. The PodSpec approach is more direct on workloads, while the ServiceAccount flag signals intent across all workloads. Various scanner tools may behave differently, so you can set both to satisfy either scenario.

Example `values.yaml` file:

```yaml
global:
  automountServiceAccountToken: false

serviceAccount:
  automountServiceAccountToken: false
```

Keep in mind that changing the pod spec forces your pods to be restarted. Plan for such restarts accordingly when updating this configuration.

If you create dedicated service accounts for alertmanager or ruler, they will inherit the auto-mounting policy from the chart-level `serviceAccount.automountServiceAccountToken`. To re-enable auto-mounting for them, you can set `automountServiceAccountToken: true` under `alertmanager.serviceAccount` and `ruler.serviceAccount` respectively.

If you bring your own service account, you can set `serviceAccount.automountServiceAccountToken` so that the chart determines which pods should receive an explicit `automountServiceAccountToken: true`. Your external ServiceAccount object will not be updated by the chart, so set the field on the object yourself, or use `global.automountServiceAccountToken: false`.

Two integrations do log in with the token, and the chart re-enables auto-mounting on the pods that use them so that opting out doesn't break them: Vault Agent, when you set `vaultAgent.enabled`, and Mimir's own Vault client, when your Mimir configuration sets `vault.enabled` with `vault.auth.type: kubernetes`. The chart leaves the Vault client alone if you already supply the token some other way, through `vault.auth.kubernetes.service_account_token` or a `vault.auth.kubernetes.service_account_token_path` outside the standard directory.

Grafana Agent meta-monitoring also needs the token to discover scrape targets using Kubernetes API. If `metaMonitoring.grafanaAgent.enabled: true` is set and the Agent shares the chart's service account, disabling auto-mounting on that account fails the install. Set `metaMonitoring.grafanaAgent.serviceAccount.create: true` to give the Grafana Agent an account of its own, which keeps its token.

Each component also has its own `automountServiceAccountToken` in its values section, for example `ingester.automountServiceAccountToken` or `store_gateway.automountServiceAccountToken`. Refer to `values.yaml` for the exact key of each component. If this value is set, it overrides `global.automountServiceAccountToken` as well as all other detection rules.

{{< admonition type="caution" >}}
Setting `automountServiceAccountToken` to false on a component that needs to log in to Vault (or talk with Kubernetes API) using the token prevents that component from functioning. The component setting overrides the chart's Vault detection, and allows you to override safeguards.
{{< /admonition >}}

For the technical reference about service accounts and their tokens, refer to [Managing Service Accounts](https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/) in the Kubernetes documentation.

## Known limitations

Sub-chart pods (MinIO, Rollout Operator, and Grafana Agent) are currently not covered. The MinIO Community Helm Chart does not expose values for disabling token auto-mounting, and both Rollout Operator and Grafana Agent have legitimate use-cases for token mounting as they utilize the Kubernetes API.

If you set `useExternalConfig: true`, the chart can't read your Mimir configuration. Manually check whether your external configuration enables Vault configuration with Kubernetes authentication, and set `externalConfigVaultKubernetesAuth` to `true` or `false` explicitly to match.

If you are setting `extraArgs` then they are not parsed for Vault safeguard detection. If you manually configure Vault configuration with Kubernetes authentication outside of the safe structured configuration, enable token auto-mounting on individual components manually.
