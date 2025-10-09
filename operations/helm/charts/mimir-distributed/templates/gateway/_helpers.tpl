{{/*
nginx auth Secret name
*/}}
{{- define "mimir.gateway.nginx.authSecret" -}}
{{ .Values.gateway.nginx.basicAuth.existingSecret | default (include "mimir.resourceName" (dict "ctx" . "component" "gateway-nginx") ) }}
{{- end }}

{{/*
Name of the gateway Service resource
*/}}
{{- define "mimir.gateway.service.name" -}}
{{ .Values.gateway.service.nameOverride | default (include "mimir.resourceName" (dict "ctx" . "component" "gateway") ) }}
{{- end }}


{{/*
Returns "true" or "false" strings if the gateway component (nginx or GEM gateway) should be deployed
*/}}
{{- define "mimir.gateway.isEnabled" -}}
{{- and .Values.gateway.enabled (not .Values.federation_frontend.disableOtherComponents) -}}
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