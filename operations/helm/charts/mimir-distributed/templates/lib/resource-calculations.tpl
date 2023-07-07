{{/*
Mimir calculates the memory size in bytes from a human readable string.
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.memorySiToBytes" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{- $resourceRequestSection := $componentSection.resources.requests -}}
{{- if and (eq (typeOf $resourceRequestSection.memory) "string") (hasSuffix "Gi" $resourceRequestSection.memory) -}}
{{ (trimSuffix "Gi"  $resourceRequestSection.memory |  mulf 1073741824) | floor }}
{{- else if and (eq (typeOf $resourceRequestSection.memory) "string") (hasSuffix "Mi" $resourceRequestSection.memory) -}}
{{ (trimSuffix "Mi"  $resourceRequestSection.memory |  mulf 1048576) | floor }}
{{- else if eq (typeOf $resourceRequestSection.memory) "float64" -}}
{{ $resourceRequestSection.memory }}
{{- else -}}
{{- fail "Memory size must be specified in Gi or Mi" -}}
{{- end -}}
{{- end -}}

{{/*
Mimir calculates the Milli CPU Integer from the requested CPU resources.
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.milliCPUInt" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{- $resourceRequestSection := $componentSection.resources.requests -}}
{{- if and (eq (typeOf $resourceRequestSection.cpu) "string") (hasSuffix "m" $resourceRequestSection.cpu) -}}
{{ trimSuffix "m"  $resourceRequestSection.cpu }}
{{- else -}}
{{ $resourceRequestSection.cpu | mulf 1000 | floor }}
{{- end -}}
{{- end -}}
