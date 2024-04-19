{{/*
Mimir common podSecurityContext
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.podSecurityContext" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{- /* Some components have security context in the values.yalm as podSecurityContext and others as securityContext */}}
{{- $podSecurityContextSection := $componentSection.securityContext | default $componentSection.podSecurityContext -}}
{{- $defaultContext := deepCopy .ctx.Values.rbac.podSecurityContext }}
{{- $podSecurityContextSection | mustMergeOverwrite $defaultContext | toYaml -}}
{{- end -}}
