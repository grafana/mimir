{{/*
Mimir common ServiceMonitor definition
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.securityContext" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{/* Some components have security context in the values.yalm as podSecurityContext and others as securityContext */}}
{{- $podSecurityContextSection := $componentSection.securityContext | default $componentSection.podSecurityContext -}}
{{- if .ne .ctx.rbac.type "scc" -}}
{{- $podSecurityContextSection | mustMergeOverwrite .ctx.rbac.fixedIdentities | toYaml -}}
{{- else -}}
{{- $podSecurityContextSection | toYaml -}}
{{- end -}}
{{- end -}}