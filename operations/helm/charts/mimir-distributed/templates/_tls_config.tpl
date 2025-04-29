{{/*
TLS configuration template for internal Mimir components
*/}}
{{- define "mimir.serverTlsConfig" -}}
{{- if .Values.tls.enabled }}
cert_file: {{ .Values.tls.cert.file }}
key_file: {{ .Values.tls.cert.key }}
client_auth_type: {{ .Values.tls.clientAuthType }}
client_ca_file: {{ .Values.tls.cert.ca }}
{{- end }}
{{- end }}

{{- define "mimir.clientTlsConfig" -}}
tls_enabled: {{ .Values.tls.enabled }}
{{- if .Values.tls.enabled }}
tls_cert_path: {{ .Values.tls.cert.file }}
tls_key_path: {{ .Values.tls.cert.key }}
tls_ca_path: {{ .Values.tls.cert.ca }}
tls_insecure_skip_verify: {{ .Values.tls.insecureSkipVerify }}
{{- end }}
{{- end }}

{{/*
TLS protocol template for Mimir components
*/}}
{{ define "mimir.internalProtocol" -}}
http{{- if .Values.tls.enabled -}}s{{- end -}}
{{- end }}


{{/*
TLS volume template for Mimir components
*/}}
{{- define "mimir.tlsVolume" -}}
{{- if .Values.tls.enabled }}
- name: tls-certs
  secret:
    secretName: {{ .Values.tls.secret.name }}
{{- end }}
{{- end }}

{{/*
TLS volume mount template for Mimir components
*/}}
{{- define "mimir.tlsVolumeMount" -}}
{{- if .Values.tls.enabled }}
- name: tls-certs
  mountPath: /etc/mimir/tls
  readOnly: true
{{- end }}
{{- end }} 