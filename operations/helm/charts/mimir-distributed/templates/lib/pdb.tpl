{{/*
Mimir common PodDisruptionBudget definition
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.lib.podDisruptionBudget" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{ with ($componentSection).podDisruptionBudget }}
apiVersion: {{ include "mimir.podDisruptionBudget.apiVersion" $.ctx }}
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
{{ toYaml . | indent 2 }}
{{- end -}}
{{- end -}}
