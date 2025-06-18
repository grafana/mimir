{{/*
TLS configuration template for internal Mimir components
*/}}
{{- define "mimir.serverTlsConfig" -}}
cert_file: {{ .Values.tls.mimir.server.cert_path }}
key_file: {{ .Values.tls.mimir.server.key_path }}
client_ca_file: {{ .Values.tls.mimir.server.ca_path }}
client_auth_type: {{ .Values.tls.mimir.server.client_auth_type }}
{{- end }}

{{- define "mimir.clientTlsConfig" -}}
tls_enabled: {{ .Values.tls.mimir.enabled }}
{{- if .Values.tls.mimir.enabled }}
tls_cert_path: {{ .Values.tls.mimir.client.cert_path }}
tls_key_path: {{ .Values.tls.mimir.client.key_path }}
tls_ca_path: {{ .Values.tls.mimir.client.ca_path }}
tls_server_name: {{ .Values.tls.mimir.client.server_name }}
tls_insecure_skip_verify: {{ .Values.tls.mimir.client.insecure_skip_verify }}
tls_cipher_suites: {{ .Values.tls.mimir.client.cipher_suites }}
tls_min_version: {{ .Values.tls.mimir.client.min_version }}
{{- end }}
{{- end }}

{{- define "memcached.clientTlsConfig" -}}
tls_enabled: {{ .Values.tls.memcached.enabled }}
{{- if .Values.tls.memcached.enabled }}
tls_cert_path: {{ .Values.tls.memcached.client.cert_path }}
tls_key_path: {{ .Values.tls.memcached.client.key_path }}
tls_ca_path: {{ .Values.tls.memcached.client.ca_path }}
tls_server_name: {{ .Values.tls.memcached.server_name }}
tls_insecure_skip_verify: {{ .Values.tls.memcached.insecure_skip_verify }}
tls_cipher_suites: {{ .Values.tls.memcached.cipher_suits }}
tls_min_version: {{ .Values.tls.memcached.min_version }}
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
