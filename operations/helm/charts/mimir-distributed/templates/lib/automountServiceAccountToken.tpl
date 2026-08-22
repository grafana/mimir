{{/*
Whether the component runs a Mimir "-target" whose module dependency graph
reaches the "vault" module, and therefore initializes Mimir's own Vault client.
With "vault.auth.type: kubernetes" that client reads the Kubernetes service
account token itself, so such a pod needs the token mounted.

Note this is unrelated to Vault Agent injection, which is a separate integration
covered by "mimir.vaultAgent.isComponentEnabled".

How this list is determined, and how to update it:
  - Each entry corresponds to a "-target=<x>" used by a pod template in this
    chart. To enumerate them:  grep -rn '"-target=' templates/
  - A target belongs here when its dependency closure in the "deps" map of
    pkg/mimir/modules.go reaches the "Vault" module. Every Mimir server target
    names Vault directly. "continuous-test" resolves to {API} -> {Server,
    ActivityTracker} -> ... and never reaches Vault, which is why the
    "continuous-test" and "smoke-test" components are deliberately absent.
  - "gateway", the caches, "kafka" and the minio job are absent for a different
    reason: they run no Mimir binary at all. Note that "gateway" is still
    covered by "mimir.vaultAgent.isComponentEnabled".
  - Revisit when a new -target is deployed by the chart, or when Mimir's module
    graph changes. An out-of-date list only has an effect when
    "global.automountServiceAccountToken" is false and Mimir is configured with
    "vault.auth.type: kubernetes"; the per-component value is the escape hatch.

mimir.lib.isMimirVaultComponent takes 2 arguments
  .ctx = the root context of the chart
  .component = the name of the component
*/}}
{{- define "mimir.lib.isMimirVaultComponent" -}}
{{- $mimirVaultComponents := dict
  "alertmanager" true
  "compactor" true
  "distributor" true
  "ingester" true
  "overrides-exporter" true
  "querier" true
  "query-frontend" true
  "query-scheduler" true
  "ruler" true
  "ruler-querier" true
  "ruler-query-frontend" true
  "ruler-query-scheduler" true
  "store-gateway" true
-}}
{{- if hasKey $mimirVaultComponents .component -}}
true
{{- end -}}
{{- end -}}

{{/*
Return "true" when the component's pod needs a Kubernetes service account token
mounted. Two independent integrations consume it:

  1. Vault Agent injection ("vaultAgent.enabled"). The injected sidecar
     authenticates to Vault with the pod's service account token.
  2. Mimir's own Vault client ("vault.enabled" with "vault.auth.type: kubernetes"
     in the Mimir configuration). Mimir reads the token itself, from
     "vault.auth.kubernetes.service_account_token_path" when that is set, and
     from /var/run/secrets/kubernetes.io/serviceaccount/token otherwise.

The second is skipped in three cases, because the pod then needs no auto-mounted
token:
  - "service_account_token" provides the JWT inline, so no file is read.
  - "service_account_token_path" points outside the auto-mount directory, which
    means the user supplies the volume themselves.
  - "useExternalConfig" is true, because the chart cannot see the configuration
    and so cannot detect Vault. Users on an external configuration must set
    "automountServiceAccountToken" themselves.

mimir.lib.needsServiceAccountToken takes 2 arguments
  .ctx = the root context of the chart
  .component = the name of the component
*/}}
{{- define "mimir.lib.needsServiceAccountToken" -}}
{{- if eq (include "mimir.vaultAgent.isComponentEnabled" .) "true" -}}
true
{{- else if and (eq (include "mimir.lib.isMimirVaultComponent" .) "true") (not .ctx.Values.useExternalConfig) -}}
{{- $config := include "mimir.calculatedConfig" .ctx | fromYaml -}}
{{- $vault := (index $config "vault") | default dict -}}
{{- $auth := (index $vault "auth") | default dict -}}
{{- $kubernetes := (index $auth "kubernetes") | default dict -}}
{{- $tokenPath := (index $kubernetes "service_account_token_path") | default "" | toString -}}
{{- if and (index $vault "enabled")
           (eq ((index $auth "type") | default "" | toString) "kubernetes")
           (not (index $kubernetes "service_account_token"))
           (or (eq $tokenPath "") (hasPrefix "/var/run/secrets/kubernetes.io/serviceaccount/" (clean $tokenPath))) -}}
true
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Render the pod spec "automountServiceAccountToken" field, or nothing.

The value is tri-state, taken from the component's own value first and from
"global.automountServiceAccountToken" otherwise:
  null  - render nothing, so the Kubernetes default applies and the token is
          mounted, inheriting whatever the service account itself specifies
  false - do not mount a service account token
  true  - mount a service account token regardless of what the service account
          itself specifies

Setting this on the pod spec rather than on the ServiceAccount resource means it
works the same for chart-created and bring-your-own service accounts.

When the chart-wide value is false but the component needs a token, "true" is
rendered instead so that Kubernetes authentication keeps working. An explicit
per-component value always wins over that, for users who mount their own token.

mimir.lib.automountServiceAccountToken takes 2 arguments
  .ctx = the root context of the chart
  .component = the name of the component; when omitted, only the chart-wide
    value applies
*/}}
{{- define "mimir.lib.automountServiceAccountToken" -}}
{{- $value := .ctx.Values.global.automountServiceAccountToken -}}
{{- if .component -}}
{{- if and (kindIs "bool" $value) (not $value) (eq (include "mimir.lib.needsServiceAccountToken" .) "true") -}}
{{- $value = true -}}
{{- end -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- if kindIs "bool" $componentSection.automountServiceAccountToken -}}
{{- $value = $componentSection.automountServiceAccountToken -}}
{{- end -}}
{{- end -}}
{{- if kindIs "bool" $value -}}
automountServiceAccountToken: {{ $value }}
{{- end -}}
{{- end -}}
