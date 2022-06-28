{{- define "mimir.metaMonitoring.metrics.remoteWrite" -}}
url: {{ .url }}
{{- if .auth }}
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
{{- else if or .passwordSecretKey .passwordSecretName }}{{ required "Set either both passwordSecretKey and passwordSecretName or neither" nil }}
{{- end }}
{{- end }}
{{- end }}
{{- with .headers }}
headers:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "mimir.metaMonitoring.logs.client" -}}
url: {{ .url }}
{{- if .auth }}
{{- if .auth.tenantId }}
tenantId: {{ .auth.tenantId | quote }}
{{- end }}
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
{{- else if or .passwordSecretKey .passwordSecretName }}
{{ required "Set either both passwordSecretKey and passwordSecretName or neither" nil }}
{{- end }}
{{- end }}
{{- end }}
externalLabels:
  cluster: {{ include "mimir.clusterName" $.ctx | quote}}
{{- end -}}