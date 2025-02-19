{{/*
Convert a dictionary of labels into a PromQL label selector string.: key1=value1, key2=value2, ...
Example:
    cluster: "my-cluster-name"
becomes:
    cluster="my-cluster-name"
*/}}
{{- define "toPromQLLabelSelector" -}}
  {{- if . }}
    {{- $labels := "" }}
    {{- range $key, $value := . }}
      {{- $labels = printf "%s%s=\"%s\"," $labels $key $value }}
    {{- end }}
    {{- trimSuffix "," $labels -}}
  {{- end }}
{{- end }}
