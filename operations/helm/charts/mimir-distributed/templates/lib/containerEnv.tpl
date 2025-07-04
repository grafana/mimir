{{/*
Mimir common container environment variables with merge precedence
Params:
  ctx = . context
  component = name of the component
  env = (optional) default environment variables in the Kubernetes EnvVar format
*/}}
{{- define "mimir.lib.containerEnv" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{- $envKV := dict }}
{{- /* Add defaults to the dictionary */}}
{{- range (.env | default list) }}
  {{- $_ := set $envKV .name . }}
{{- end }}
{{- /* Add global extraEnv (overrides defaults) */}}
{{- range (.ctx.Values.global.extraEnv | default list) }}
  {{- $_ := set $envKV .name . }}
{{- end }}
{{- /* Add component-specific env (overrides both defaults and global) */}}
{{- range ($componentSection.env | default list) }}
  {{- $_ := set $envKV .name . }}
{{- end }}
{{- /* Convert back to list format for YAML output */}}
{{- $envList := list }}
{{- range $name, $envVar := $envKV }}
  {{- $envList = append $envList $envVar }}
{{- end }}
{{- toYaml $envList }}
{{- end -}}
