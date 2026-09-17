{{- define "mimir.metaMonitoring.metrics.remoteWrite" -}}
{{- $writeBackToMimir := not .url -}}
{{- $url := .url -}}
{{- if $writeBackToMimir -}}
{{- $url = include "mimir.remoteWriteUrl.inCluster" .ctx }}
{{- end -}}
- url: {{ $url }}
  {{- /* Only emit the basicAuth block if at least one populated field exists; emitting
        `basicAuth:` with no children produces `basicAuth: null` which fails CRD
        validation on ServerSideApply (see grafana/mimir#11200). */ -}}
  {{- if and .auth (or .auth.username (and .auth.passwordSecretKey .auth.passwordSecretName)) }}
  basicAuth:
  {{- if .auth.username }}
    username:
      name: {{ include "mimir.resourceName" (dict "ctx" $.ctx "component" "metrics-instance-usernames") }}
      key: {{ .usernameKey | quote }}
  {{- end }}
  {{- with .auth }}
  {{- if and .passwordSecretKey .passwordSecretName }}
    password:
      name: {{ .passwordSecretName | quote }}
      key: {{ .passwordSecretKey | quote }}
  {{- end }}
  {{- end }}
  {{- else if and .auth (or .auth.passwordSecretKey .auth.passwordSecretName) }}
  {{- required "Set either both passwordSecretKey and passwordSecretName or neither" nil }}
  {{- end }}
  {{- $headers := .headers -}}
  {{- if $writeBackToMimir -}}{{- $_ := set $headers "X-Scope-OrgID" "metamonitoring" -}}{{- end -}}
  {{- with $headers }}
  headers:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .sigv4 }}
  sigv4:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end -}}

{{- define "mimir.metaMonitoring.logs.client" -}}
{{- if .url }}
- url: {{ .url }}
  {{- if and .auth .auth.tenantId }}
  tenantId: {{ .auth.tenantId | quote }}
  {{- end }}
  {{- /* Only emit the basicAuth block if at least one populated field exists; emitting
        `basicAuth:` with no children produces `basicAuth: null` which fails CRD
        validation on ServerSideApply (see grafana/mimir#11200). */ -}}
  {{- if and .auth (or .auth.username (and .auth.passwordSecretKey .auth.passwordSecretName)) }}
  basicAuth:
  {{- if .auth.username }}
    username:
      name: {{ include "mimir.resourceName" (dict "ctx" $.ctx "component" "logs-instance-usernames") }}
      key: {{ .usernameKey | quote }}
  {{- end }}
  {{- with .auth }}
  {{- if and .passwordSecretKey .passwordSecretName }}
    password:
      name: {{ .passwordSecretName | quote }}
      key: {{ .passwordSecretKey | quote }}
  {{- end }}
  {{- end }}
  {{- else if and .auth (or .auth.passwordSecretKey .auth.passwordSecretName) }}
  {{ required "Set either both passwordSecretKey and passwordSecretName or neither" nil }}
  {{- end }}
  externalLabels:
    cluster: {{ include "mimir.clusterName" $.ctx | quote}}
{{- end -}}
{{- end -}}

{{- define "mimir.metaMonitoring.metrics.remoteReadUrl" -}}
{{- with $.ctx.Values.metaMonitoring.grafanaAgent.metrics }}
{{- $writeBackToMimir := not (.remote).url -}}
{{- if $writeBackToMimir -}}
{{- include "mimir.remoteReadUrl.inCluster" $.ctx }}
{{- else -}}
{{- $parsed := urlParse (.remote).url -}}
{{ $parsed.scheme }}://{{ $parsed.host }}/prometheus
{{- end }}
{{- end -}}
{{- end -}}