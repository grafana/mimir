{{/*
Convert a list of filter expressions into a PromQL label selector string: expression1, expression2, ...
Example:
    - cluster!="eu-west"
    - app="my-app"
becomes:
    cluster!="eu-west",app="my-app"
*/}}
{{- define "toPromQLLabelSelector" -}}
  {{- if . }}
    {{- $labels := "" }}
    {{- range $expression := . }}
      {{- $labels = printf "%s%s," $labels $expression }}
    {{- end }}
    {{- trimSuffix "," $labels -}}
  {{- end }}
{{- end }}
