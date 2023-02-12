{{/*
nginx auth secret name
*/}}
{{- define "mimir.nginxAuthSecret" -}}
{{ .Values.nginx.basicAuth.existingSecret | default (include "mimir.resourceName" (dict "ctx" . "component" "nginx") ) }}
{{- end }}
