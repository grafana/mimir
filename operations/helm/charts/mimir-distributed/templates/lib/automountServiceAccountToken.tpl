{{/*
Returns "true" if the component's pod uses a Kubernetes service account
token. This chart has two integrations that authenticate using it:

- Vault Agent, through "vaultAgent.enabled". The injected sidecar uses
  the service account token automatically.

- Mimir's internal Vault client, through "vault.enabled" with
  "vault.auth.type: kubernetes". Mimir binary uses the service account
  token internally.
  However, "vault.auth.kubernetes.service_account_token_path" may set
  an alternative path, or "vault.auth.kubernetes.service_account_token"
  may provide the token inline. Both cases get checked, and avoid
  triggering token mounting.
  If external configuration is in use, we cannot be sure about these
  values, and we instead depend on "externalConfigVaultKubernetesAuth".

Determination for which components would use Mimir's Vault client is
done based on pre-computed exclusions: the list contains components
that either do not run the Mimir binary or whose target does not reach
the Vault module inside Mimir.

Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.usesServiceAccountToken" -}}
{{- $componentsWithoutMimirKubernetesAuth := dict
  "chunks-cache" true
  "continuous-test" true
  "gateway" true
  "index-cache" true
  "kafka" true
  "memcached" true
  "meta-monitoring" true
  "metadata-cache" true
  "results-cache" true
  "smoke-test" true
-}}
{{- if eq (include "mimir.vaultAgent.isComponentEnabled" .) "true" -}}
true
{{- else if not (hasKey $componentsWithoutMimirKubernetesAuth .component) -}}
{{-   if .ctx.Values.useExternalConfig -}}
{{-     if .ctx.Values.externalConfigVaultKubernetesAuth -}}
true
{{-     end -}}
{{-   else -}}
{{-     $config := include "mimir.calculatedConfig" .ctx | fromYaml -}}
{{-     $vault := (index $config "vault") | default dict -}}
{{-     if index $vault "enabled" -}}
{{-       $auth := (index $vault "auth") | default dict -}}
{{-       if eq ((index $auth "type") | default "" | toString) "kubernetes" -}}
{{-         $kubernetes := (index $auth "kubernetes") | default dict -}}
{{-         if not (index $kubernetes "service_account_token") -}}
{{-           $tokenPath := (index $kubernetes "service_account_token_path") | default "" | toString -}}
{{-           if or (eq $tokenPath "") (hasPrefix "/var/run/secrets/kubernetes.io/serviceaccount/" (clean $tokenPath)) -}}
true
{{-           end -}}
{{-         end -}}
{{-       end -}}
{{-     end -}}
{{-   end -}}
{{- end -}}
{{- end -}}

{{/*
Returns the automountServiceAccountToken value for the service account
block specific to the component. When the chart is instructed not to
create a component-specific account, we fall back to the shared service
account.

Params:
  ctx = . context
  component = name of the component (empty means the shared account itself)
*/}}
{{- define "mimir.lib.saAutomountServiceAccountToken" -}}
{{- $sharedServiceAccount := .ctx.Values.serviceAccount -}}
{{- $serviceAccount := $sharedServiceAccount -}}
{{- if and (eq .component "alertmanager") .ctx.Values.alertmanager.serviceAccount.create -}}
{{-   $serviceAccount = .ctx.Values.alertmanager.serviceAccount -}}
{{- else if and (eq .component "ruler") .ctx.Values.ruler.serviceAccount.create -}}
{{-   $serviceAccount = .ctx.Values.ruler.serviceAccount -}}
{{- end -}}
{{- $value := $serviceAccount.automountServiceAccountToken -}}
{{- if kindIs "invalid" $value -}}
{{-   $value = $sharedServiceAccount.automountServiceAccountToken -}}
{{- end -}}
{{- if not (kindIs "invalid" $value) -}}
{{-   $value -}}
{{- end -}}
{{- end -}}

{{/*
Returns the automountServiceAccountToken value for the pod spec block
specific to the component.

If a value for this field is set in the component-specific block
we render it authoritatively. Otherwise, we evaluate the
"global.automountServiceAccountToken" value.

If it's set to false, or if the service account's automounting is set
to false, a determination is made whether this component would use
the default service account token. If so, we force the auto-mounting
to still happen.

Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.podAutomountServiceAccountToken" -}}
{{- $value := .ctx.Values.global.automountServiceAccountToken -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $componentValue := $componentSection.automountServiceAccountToken -}}
{{- if not (kindIs "invalid" $componentValue) -}}
{{-   $value = $componentValue -}}
{{- else if or (and (not (kindIs "invalid" $value)) (not $value)) (eq (include "mimir.lib.saAutomountServiceAccountToken" .) "false") -}}
{{-   if eq (include "mimir.lib.usesServiceAccountToken" .) "true" -}}
{{-     $value = true -}}
{{-   end -}}
{{- end -}}
{{- if not (kindIs "invalid" $value) -}}
{{-   $value -}}
{{- end -}}
{{- end -}}
