{{/*
Params:
  ctx = . context
  component = name of the component
  topologySpreadConstraints = (optional) constraints to render instead of the component section's topologySpreadConstraints
*/}}
{{- define "mimir.lib.topologySpreadConstraints" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $topologySpreadConstraintsSection := .topologySpreadConstraints | default $componentSection.topologySpreadConstraints -}}
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
  {{- end }}
  {{- if $constraint.matchLabelKeys }}
  matchLabelKeys: 
    {{- $constraint.matchLabelKeys | toYaml | nindent 4 }}
  {{- end }}
  {{- if $constraint.nodeAffinityPolicy }}
  nodeAffinityPolicy: {{ $constraint.nodeAffinityPolicy }}
  {{- end }}
  {{- if $constraint.nodeTaintsPolicy }}
  nodeTaintsPolicy: {{ $constraint.nodeTaintsPolicy }}
  {{- end -}}
  {{- if $constraint.minDomains }}
  minDomains: {{ $constraint.minDomains }}
  {{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
