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
{{- $podSecurityContextSection | mustMergeOverwrite .ctx.Values.rbac.securityContext | toYaml -}}
{{- end -}}
