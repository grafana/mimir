{{/*
gateway fullname
*/}}
{{- define "mimir.gatewayFullname" -}}
{{ include "mimir.fullname" . }}-gateway
{{- end }}

{{/*
gateway auth secret name
*/}}
{{- define "mimir.gatewayAuthSecret" -}}
{{ .Values.gateway.basicAuth.existingSecret | default (include "mimir.gatewayFullname" . ) }}
{{- end }}
