{{/*
Convert labels to string like: key1=value1, key2=value2, ...
Example:
    customLabelFilter:
      cluster: "my-cluster-name"
becomes:
    cluster="my-cluster-name"
*/}}
{{- define "customLabelFilter" -}}
  {{- if . }}
    {{- $labels := "" }}
    {{- range $key, $value := . }}
      {{- $labels = printf "%s%s=\"%s\"," $labels $key $value }}
    {{- end }}
    {{- trimSuffix "," $labels -}}
  {{- end }}
{{- end }}
