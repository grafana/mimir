{{/*
Mimir common container environment variables with merge precedence.
The order of precedence is as follows (high to low):
1. Component-specific values
2. Global values
3. The provided defaults (if any)
Outputs the complete env/envFrom section for a container manifest.
Params:
  ctx = . context
  component = name of the component
  env = (optional) default environment variables in the Kubernetes EnvVar format
*/}}
{{- define "mimir.lib.containerEnv" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $envKV := dict -}}
{{- range (.env | default list) -}}
  {{- $_ := set $envKV .name . -}}
{{- end -}}
{{- range (.ctx.Values.global.extraEnv | default list) -}}
  {{- $_ := set $envKV .name . -}}
{{- end -}}
{{- range ($componentSection.env | default list) -}}
  {{- $_ := set $envKV .name . -}}
{{- end -}}
{{- $envList := list -}}
{{- range $_, $envVar := $envKV -}}
  {{- $envList = append $envList $envVar -}}
{{- end -}}
env:
  {{- toYaml $envList | nindent 2 }}
{{- if or .ctx.Values.global.extraEnvFrom $componentSection.extraEnvFrom }}
envFrom:
{{- with .ctx.Values.global.extraEnvFrom }}
  {{- toYaml . | nindent 2 -}}
{{- end }}
{{- with $componentSection.extraEnvFrom }}
  {{- toYaml . | nindent 12 }}
{{- end }}
{{- end }}
{{- end -}}
