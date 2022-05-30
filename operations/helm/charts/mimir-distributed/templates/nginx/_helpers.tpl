{{/*
nginx auth secret name
*/}}
{{- define "mimir.nginxAuthSecret" -}}
{{ .Values.nginx.basicAuth.existingSecret | default (include "mimir.resourceName" (dict "ctx" . "component" "nginx") ) }}
{{- end }}

{{/*
Return the appropriate apiVersion for ingress.
*/}}
{{- define "mimir.ingress.apiVersion" -}}
  {{- if and (.Capabilities.APIVersions.Has "networking.k8s.io/v1") (semverCompare ">= 1.19-0" .Capabilities.KubeVersion.Version) -}}
      {{- print "networking.k8s.io/v1" -}}
  {{- else if .Capabilities.APIVersions.Has "networking.k8s.io/v1beta1" -}}
    {{- print "networking.k8s.io/v1beta1" -}}
  {{- else -}}
    {{- print "extensions/v1beta1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return if ingress is stable.
*/}}
{{- define "mimir.ingress.isStable" -}}
  {{- eq (include "mimir.ingress.apiVersion" .) "networking.k8s.io/v1" -}}
{{- end -}}

{{/*
Return if ingress supports ingressClassName.
*/}}
{{- define "mimir.ingress.supportsIngressClassName" -}}
  {{- or (eq (include "mimir.ingress.isStable" .) "true") (and (eq (include "mimir.ingress.apiVersion" .) "networking.k8s.io/v1beta1") (semverCompare ">= 1.18-0" .Capabilities.KubeVersion.Version)) -}}
{{- end -}}

{{/*
Return if ingress supports pathType.
*/}}
{{- define "mimir.ingress.supportsPathType" -}}
  {{- or (eq (include "mimir.ingress.isStable" .) "true") (and (eq (include "mimir.ingress.apiVersion" .) "networking.k8s.io/v1beta1") (semverCompare ">= 1.18-0" .Capabilities.KubeVersion.Version)) -}}
{{- end -}}
