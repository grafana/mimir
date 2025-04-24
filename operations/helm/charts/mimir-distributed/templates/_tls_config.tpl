{{/*
TLS configuration template for internal Mimir components
*/}}
{{- define "mimir.tlsConfig" -}}
tls_enabled: {{ .Values.tls.enabled }}
{{- if .Values.tls.enabled }}
tls_cert_path: {{ .Values.tls.cert.file }}
tls_key_path: {{ .Values.tls.cert.key }}
tls_ca_path: {{ .Values.tls.cert.ca }}
tls_insecure_skip_verify: {{ .Values.tls.clientAuth.insecureSkipVerify }}
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