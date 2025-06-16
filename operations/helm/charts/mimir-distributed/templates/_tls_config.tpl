{{/*
TLS configuration template for internal Mimir components
*/}}
{{- define "mimir.serverTlsConfig" -}}
cert_file: {{ .Values.tls.mimir.server.config.cert_file }}
key_file: {{ .Values.tls.mimir.server.config.key_file }}
client_ca_file: {{ .Values.tls.mimir.server.config.client_ca_file }}
client_auth_type: {{ .Values.tls.mimir.server.config.client_auth_type }}
{{- end }}

{{- define "mimir.clientTlsConfig" -}}
tls_enabled: {{ .Values.tls.mimir.enabled }}
{{- if .Values.tls.mimir.enabled }}
tls_cert_path: {{ .Values.tls.mimir.client.tls_cert_path }}
tls_key_path: {{ .Values.tls.mimir.client.tls_key_path }}
tls_ca_path: {{ .Values.tls.mimir.client.tls_ca_path }}
tls_server_name: {{ .Values.tls.mimir.client.tls_server_name }}
tls_insecure_skip_verify: {{ .Values.tls.mimir.client.tls_insecure_skip_verify }}
tls_cipher_suites: {{ .Values.tls.mimir.client.tls_cipher_suites }}
tls_min_version: {{ .Values.tls.mimir.client.tls_min_version }}
{{- end }}
{{- end }}

{{- define "memcached.clientTlsConfig" -}}
tls_enabled: {{ .Values.tls.memcached.enabled }}
{{- if .Values.tls.memcached.enabled }}
tls_cert_path: {{ .Values.tls.memcached.cert_path }}
tls_key_path: {{ .Values.tls.memcached.key_path }}
tls_ca_path: {{ .Values.tls.memcached.ca_path }}
tls_insecure_skip_verify: {{ .Values.tls.memcached.insecureSkipVerify }}
{{- end }}
{{- end }}

{{/*
TLS protocol template for Mimir components
*/}}
{{ define "mimir.internalProtocol" -}}
http{{- if .Values.tls.mimir.enabled -}}s{{- end -}}
{{- end }}
{{ define "memcached.internalProtocol" -}}
http{{- if .Values.tls.memcached.enabled -}}s{{- end -}}
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