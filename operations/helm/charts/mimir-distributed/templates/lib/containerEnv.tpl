{{/*
Mimir common container environment variables with merge precedence.
The order of precedence is as follows (high to low):
1. Component-specific values
2. Global values
3. The provided defaults (if any)
The order of the defaults list is preserved; user overrides that match a default
are applied in-place rather than appended.
Outputs the complete env/envFrom section for a container manifest.
Params:
  ctx = . context
  component = name of the component
  env = (optional) default environment variables in the Kubernetes EnvVar format
*/}}
{{- define "mimir.lib.containerEnv" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- /* Build a name→entry lookup from user overrides.
     global.extraEnv is applied first, component.env second so component.env wins (highest precedence). */ -}}
{{- $overridesKV := dict -}}
{{- range (.ctx.Values.global.extraEnv | default list) -}}
  {{- $_ := set $overridesKV .name . -}}
{{- end -}}
{{- range ($componentSection.env | default list) -}}
  {{- $_ := set $overridesKV .name . -}}
{{- end -}}
{{- /* Walk defaults in their original order so the required ordering is preserved.
     Apply the user override for a given name in-place rather than appending at the end. */ -}}
{{- $defaultNames := dict -}}
{{- $envList := list -}}
{{- range (.env | default list) -}}
  {{- $_ := set $defaultNames .name true -}}
  {{- if hasKey $overridesKV .name -}}
    {{- $envList = append $envList (index $overridesKV .name) -}}
  {{- else -}}
    {{- $envList = append $envList . -}}
  {{- end -}}
{{- end -}}
{{- /* Append user-defined vars that are not overrides of an existing default. */ -}}
{{- range $name, $envVar := $overridesKV -}}
  {{- if not (hasKey $defaultNames $name) -}}
    {{- $envList = append $envList $envVar -}}
  {{- end -}}
{{- end -}}
env:
  {{- toYaml $envList | nindent 2 }}
{{- if or .ctx.Values.global.extraEnvFrom $componentSection.extraEnvFrom }}
envFrom:
{{- with .ctx.Values.global.extraEnvFrom }}
  {{- toYaml . | nindent 2 -}}
{{- end }}
{{- with $componentSection.extraEnvFrom }}
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
{{- end -}}
