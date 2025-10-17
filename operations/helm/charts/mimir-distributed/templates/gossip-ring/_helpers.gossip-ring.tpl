{{/*
gossip-ring selector labels
*/}}
{{- define "mimir.gossipRingSelectorLabels" -}}
{{ include "mimir.selectorLabels" . }}
app.kubernetes.io/part-of: memberlist
{{- end -}}
