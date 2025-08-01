{{/*
Mimir common container environment variables with merge precedence
Outputs the complete env/envFrom section for a container manifest.
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
{{- range $_, $envVar := $envKV }}
  {{- $envList = append $envList $envVar }}
{{- end }}
{{- /* Build "env" and "envFrom" sections if there is anything to output */}}
{{- if $envList -}}
env:
{{- toYaml $envList | nindent 2 }}
{{- end }}
{{- $globalExtraEnvFrom := .ctx.Values.global.extraEnvFrom | default list }}
{{- $componentExtraEnvFrom := $componentSection.extraEnvFrom | default list }}
{{- if or $globalExtraEnvFrom $componentExtraEnvFrom }}
envFrom:
{{- if $globalExtraEnvFrom }}
{{- toYaml $globalExtraEnvFrom | nindent 2 }}
{{- end }}
{{- if $componentExtraEnvFrom }}
{{- toYaml $componentExtraEnvFrom | nindent 2 }}
{{- end -}}
{{- end -}}
{{- end -}}
