{{/*
Convert a dictionary of labels and operators into a PromQL label selector string.
Format: key1<operator1>value1, key2<operator2>value2, ...
Supported operators: =, !=, =~, !~
Example with just selector:
  toPromQLLabelSelector:
    cluster: "mimir"
becomes:
  cluster="mimir"
Example with selector and operator:
  toPromQLLabelSelector:
    pod: "^mimir-.*"
  toPromQLLabelOperator:
    pod: "=~"
becomes:
    pod=~"^mimir-.*"
*/}}
{{- define "toPromQLLabelSelector" -}}
  {{- if .selector }}
    {{- $labels := "" }}
    {{- range $key, $value := .selector }}
      {{- $operator := "=" }}
      {{- if $.operator }}
        {{- if index $.operator $key }}
          {{- $operator = index $.operator $key }}
        {{- end }}
      {{- end }}
      {{- $labels = printf "%s%s%s\"%s\"," $labels $key $operator $value }}
    {{- end }}
    {{- trimSuffix "," $labels -}}
  {{- end }}
{{- end }}
