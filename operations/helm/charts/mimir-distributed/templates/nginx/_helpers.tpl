{{/*
nginx auth secret name
*/}}
{{- define "mimir.nginxAuthSecret" -}}
{{ .Values.nginx.basicAuth.existingSecret | default (include "mimir.resourceName" (dict "ctx" . "component" "nginx") ) }}
{{- end }}

{{/*
Returns the HorizontalPodAutoscaler API version for this version of kubernetes.
*/}}
{{- define "mimir.hpa.version" -}}
{{- if semverCompare ">= 1.23-0" (include "mimir.kubeVersion" .) -}}
autoscaling/v2
{{- else -}}
autoscaling/v2beta1
{{- end -}}
{{- end -}}
