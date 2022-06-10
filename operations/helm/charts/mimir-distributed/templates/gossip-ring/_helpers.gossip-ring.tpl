{{/*
gossip-ring selector labels
*/}}
{{- define "mimir.gossipRingSelectorLabels" -}}
{{ include "mimir.selectorLabels" . }}
{{- if .ctx.Values.enterprise.legacyLabels }}
gossip_ring_member: "true"
{{- else }}
app.kubernetes.io/part-of: memberlist
{{- end }}
{{- end -}}
