{{/*
memcached Service
*/}}
{{- define "mimir.memcached.service" -}}
{{ with (index $.ctx.Values $.component) }}
{{- if .enabled -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "mimir.resourceName" (dict "ctx" $.ctx "component" $.component) }}
  labels:
    {{- include "mimir.labels" (dict "ctx" $.ctx "component" $.component) | nindent 4 }}
    {{- with .service.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  annotations:
    {{- toYaml .service.annotations | nindent 4 }}
  namespace: {{ $.ctx.Release.Namespace | quote }}
spec:
  type: ClusterIP
  clusterIP: None
  ports:
    - name: memcached-client
      port: {{ .port }}
      targetPort: {{ .port }}
    {{ if $.ctx.Values.memcachedExporter.enabled }}
    - name: http-metrics
      port: 9150
      targetPort: 9150
    {{ end }}
  selector:
    {{- include "mimir.selectorLabels" (dict "ctx" $.ctx "component" $.component) | nindent 4 }}
{{- end -}}
{{- end -}}
{{- end -}}
