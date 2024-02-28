{{/*
Convert labels to string like: key1=value1, key2=value2, ...
Example:
    customHeaders:
      X-Scope-OrgID: tenant-1
becomes:
    customHeaders: "X-Scope-OrgID=tenant-1"
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
