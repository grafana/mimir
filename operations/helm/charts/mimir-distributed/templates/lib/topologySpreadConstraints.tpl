{{- define "mimir.lib.topologySpreadConstraints" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $topologySpreadConstraintsSection := $componentSection.topologySpreadConstraints -}}
{{- $selectorLabels := include "mimir.selectorLabels" . -}}
{{- if $topologySpreadConstraintsSection -}}
{{- $constraints := kindIs "slice" $topologySpreadConstraintsSection | ternary $topologySpreadConstraintsSection (list $topologySpreadConstraintsSection) -}}
topologySpreadConstraints:
{{- range $constraint := $constraints }}
- maxSkew: {{ $constraint.maxSkew }}
  topologyKey: {{ $constraint.topologyKey }}
  whenUnsatisfiable: {{ $constraint.whenUnsatisfiable }}
  labelSelector:
  {{- if $constraint.labelSelector -}}
    {{- $constraint.labelSelector | toYaml | nindent 4 -}}
  {{- else }}
    matchLabels:
      {{- $selectorLabels | nindent 6 }}
  {{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
