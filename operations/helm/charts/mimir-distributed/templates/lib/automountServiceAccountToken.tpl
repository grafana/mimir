{{/*
Whether the component runs the Mimir binary. Only those components read the
service account token for themselves, when the configuration sets
"vault.auth.type: kubernetes".

Listed below are the exceptions: the components that run something other than
Mimir, plus the two test components, whose "-target=continuous-test" never
starts the vault module. Anything not listed is assumed to run Mimir, so a
component added to the chart later keeps its token by default.

Params:
  component = name of the component
*/}}
{{- define "mimir.lib.isMimirBinaryComponent" -}}
{{- $nonMimirComponents := dict
  "chunks-cache" true
  "continuous-test" true
  "gateway" true
  "index-cache" true
  "kafka" true
  "metadata-cache" true
  "results-cache" true
  "smoke-test" true
-}}
{{- if not (hasKey $nonMimirComponents .component) -}}
true
{{- end -}}
{{- end -}}

{{/*
Whether the component's pod needs a Kubernetes service account token mounted.
Two integrations authenticate to Vault with it:

  - Vault Agent injection, through "vaultAgent.enabled". The injected sidecar
    uses the pod's token.
  - Mimir's own Vault client, when the configuration sets "vault.enabled" with
    "vault.auth.type: kubernetes". Mimir reads the token from
    "vault.auth.kubernetes.service_account_token_path", or from
    /var/run/secrets/kubernetes.io/serviceaccount/token when that is unset.

The second one does not count when Mimir reads no auto-mounted file. That is
the case when "service_account_token" carries the JWT inline, and when
"service_account_token_path" points outside the auto-mount directory, where the
user mounts the token themselves.

Under "useExternalConfig" the chart never sees the configuration and cannot tell
either way. "externalConfigVaultKubernetesAuth" is how the user declares it. The
component scope below still applies, so nothing per-component is left to
maintain.

Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.needsServiceAccountToken" -}}
{{- if eq (include "mimir.vaultAgent.isComponentEnabled" .) "true" -}}
true
{{- else if eq (include "mimir.lib.isMimirBinaryComponent" .) "true" -}}
{{- if .ctx.Values.externalConfigVaultKubernetesAuth -}}
true
{{- else if not .ctx.Values.useExternalConfig -}}
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
{{- end -}}

{{/*
Render the pod spec "automountServiceAccountToken" field, or nothing at all.

The value comes from the component's own value, and from
"global.automountServiceAccountToken" otherwise. It has three states:
  null  - render nothing, leaving the Kubernetes default, which mounts a token
          unless the service account itself opts out
  false - do not mount a token
  true  - mount a token, whatever the service account says

Setting this on the pod spec instead of on the ServiceAccount resource means it
behaves the same for chart-created and bring-your-own service accounts.

A chart-wide false flips back to true for components that need the token, so
that Vault authentication keeps working. An explicit per-component value still
wins over that, for users who mount a token themselves.

Params:
  ctx = . context
  component = name of the component; omit it to apply only the chart-wide value
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
