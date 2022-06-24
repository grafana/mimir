{{/*
memcached PodDisruptionBudget
*/}}
{{- define "mimir.memcached.podDisruptionBudget" -}}
{{ with (index $.ctx.Values $.component) }}
{{- if .enabled -}}
{{- if .podDisruptionBudget -}}
apiVersion: {{ include "mimir.podDisruptionBudget.apiVersion" $ }}
kind: PodDisruptionBudget
metadata:
  name: {{ include "mimir.resourceName" (dict "ctx" $.ctx "component" $.component) }}
  labels:
    {{- include "mimir.labels" (dict "ctx" $.ctx "component" $.component) | nindent 4 }}
  namespace: {{ $.ctx.Release.Namespace | quote }}
spec:
  selector:
    matchLabels:
      {{- include "mimir.selectorLabels" (dict "ctx" $.ctx "component" $.component) | nindent 6 }}
{{ toYaml .podDisruptionBudget | indent 2 }}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
