{{/*
Convert labels to string like: key1="value1", key2="value2", ...
Params:
  map = map to convert to csv string
*/}}
{{- define "mimir.lib.mapToCSVString" -}}
{{- $list := list -}}
{{- range $k, $v := $.map -}}
{{- $list = append $list (printf "%s=%s" $k $v) -}}
{{- end -}}
{{ join "," $list }}
{{- end -}}
