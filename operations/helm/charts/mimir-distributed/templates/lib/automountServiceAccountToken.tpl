{{/*
Whether the component needs the ServiceAccount token mounted to authenticate to Vault.
Outputs a boolean "true" or "false" of when Vault Agent injection is enabled for the
component, or when the Mimir configuration enables Mimir's native Vault support with the
Kubernetes authentication method, which reads the token from
/var/run/secrets/kubernetes.io/serviceaccount/token by default.
Note: when useExternalConfig is true, we cannot know the final external configuration,
so users of native Vault with Kubernetes authentication must set the per-component
automountServiceAccountToken override themselves.
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.automountServiceAccountToken.vaultNeedsToken" -}}
{{- $needed := false -}}
{{- if and .ctx.Values.vaultAgent.enabled (include "mimir.vaultAgent.isEnabledForComponent" .) -}}
{{- $needed = true -}}
{{- else -}}
{{- /* Components that mount the Mimir configuration from /etc/mimir and can therefore use Mimir's native Vault support. */ -}}
{{- $mimirConfigComponents := dict
  "alertmanager" true
  "compactor" true
  "continuous-test" true
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
{{- if hasKey $mimirConfigComponents .component -}}
{{- $vault := (include "mimir.calculatedConfig" .ctx | fromYaml).vault -}}
{{- if kindIs "map" $vault -}}
{{- if eq ($vault.enabled | toString) "true" -}}
{{- $auth := $vault.auth -}}
{{- if kindIs "map" $auth -}}
{{- if eq ($auth.type | toString) "kubernetes" -}}
{{- $needed = true -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $needed -}}
{{- end -}}

{{/*
Mimir common pod-level automountServiceAccountToken. Outputs a boolean "true" or "false".
This is resolved by checking:
  1. An explicit `<component section>.automountServiceAccountToken` value always wins.
  2. Defaults to "true" when the component needs the ServiceAccount token to authenticate to
     Vault (see "mimir.automountServiceAccountToken.vaultNeedsToken").
  3. Otherwise falls back to `global.automountServiceAccountToken` (missing or null means false).
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.automountServiceAccountToken" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $override := $componentSection.automountServiceAccountToken -}}
{{- if not (kindIs "invalid" $override) -}}
{{- if eq ($override | toString) "true" -}}true{{- else -}}false{{- end -}}
{{- else if eq (include "mimir.automountServiceAccountToken.vaultNeedsToken" .) "true" -}}
true
{{- else if eq (((.ctx.Values.global).automountServiceAccountToken) | toString) "true" -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}
