{{/*
Full name: <release>-<chart>, truncated to 63 chars.
*/}}
{{- define "migrate.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "migrate.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | default .Chart.Version | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}

{{/*
Secret name: explicit .Values.secret.name, defaulting with release prefix when untouched.
Users with ExternalSecrets/Vault set secret.create=false and secret.name to their own Secret.
*/}}
{{- define "migrate.secretName" -}}
{{ .Values.secret.name | default (include "migrate.fullname" .) }}
{{- end -}}
