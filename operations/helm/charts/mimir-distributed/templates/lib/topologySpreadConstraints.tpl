{{- define "mimir.lib.topologySpreadConstraints" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $topologySpreadConstraintsSection := $componentSection.topologySpreadConstraints -}}
{{- if $topologySpreadConstraintsSection -}}
- maxSkew: {{ $topologySpreadConstraintsSection.maxSkew }}
  topologyKey: {{ $topologySpreadConstraintsSection.topologyKey }}
  whenUnsatisfiable: {{ $topologySpreadConstraintsSection.whenUnsatisfiable }}
  labelSelector:
    matchLabels:
      {{- include "mimir.selectorLabels" . | nindent 6 }}
{{- end -}}
{{- end -}}
