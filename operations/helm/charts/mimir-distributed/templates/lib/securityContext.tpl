{{/*
Mimir common securityContext
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.securityContext" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{- /* Some components have security context in the values.yalm as podSecurityContext and others as securityContext */}}
{{- $podSecurityContextSection := $componentSection.securityContext | default $componentSection.podSecurityContext -}}
{{- if eq .ctx.Values.rbac.type "psp" -}}
{{- $podSecurityContextSection | mustMergeOverwrite .ctx.Values.rbac.fixedIdentities | toYaml -}}
{{- else -}}
{{- $podSecurityContextSection | toYaml -}}
{{- end -}}
{{- end -}}
