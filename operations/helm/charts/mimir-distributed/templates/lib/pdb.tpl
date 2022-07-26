{{/*
Mimir common PodDisruptionBudget definition
Params:
  ctx = . context
  component = name of the component
  memberlist = true/false, whether component is part of memberlist
*/}}
{{- define "mimir.lib.podDisruptionBudget" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . }}
{{ with (index $.ctx.Values $componentSection) }}
{{- if .podDisruptionBudget -}}
apiVersion: {{ include "mimir.podDisruptionBudget.apiVersion" $.ctx }}
kind: PodDisruptionBudget
metadata:
  name: {{ include "mimir.resourceName" (dict "ctx" $.ctx "component" $.component) }}
  labels:
    {{- include "mimir.labels" (dict "ctx" $.ctx "component" $.component "memberlist" $.memberlist) | nindent 4 }}
  namespace: {{ $.ctx.Release.Namespace | quote }}
spec:
  selector:
    matchLabels:
      {{- include "mimir.selectorLabels" (dict "ctx" $.ctx "component" $.component "memberlist" $.memberlist) | nindent 6 }}
{{ toYaml .podDisruptionBudget | indent 2 }}
{{- end -}}
{{- end -}}
{{- end -}}
