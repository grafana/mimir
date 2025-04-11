package templates

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	DefaultMessageTitleEmbed = `{{ template "default.title" . }}`
	DefaultMessageEmbed      = `{{ template "default.message" . }}`
	DefaultMessageColor      = `{{ if eq .Status "firing" }}#D63232{{ else }}#36a64f{{ end }}`
)

var DefaultTemplateString = `
{{ define "__subject" }}[{{ .Status | toUpper }}{{ if eq .Status "firing" }}:{{ .Alerts.Firing | len }}{{ if gt (.Alerts.Resolved | len) 0 }}, RESOLVED:{{ .Alerts.Resolved | len }}{{ end }}{{ end }}] {{ .GroupLabels.SortedPairs.Values | join " " }} {{ if gt (len .CommonLabels) (len .GroupLabels) }}({{ with .CommonLabels.Remove .GroupLabels.Names }}{{ .Values | join " " }}{{ end }}){{ end }}{{ end }}

{{ define "__text_values_list" }}{{ if len .Values }}{{ $first := true }}{{ range $refID, $value := .Values -}}
{{ if $first }}{{ $first = false }}{{ else }}, {{ end }}{{ $refID }}={{ $value }}{{ end -}}
{{ else }}[no value]{{ end }}{{ end }}

{{ define "__text_alert_list" }}{{ range . }}
Value: {{ template "__text_values_list" . }}
Labels:
{{ range .Labels.SortedPairs }} - {{ .Name }} = {{ .Value }}
{{ end }}Annotations:
{{ range .Annotations.SortedPairs }} - {{ .Name }} = {{ .Value }}
{{ end }}{{ if gt (len .GeneratorURL) 0 }}Source: {{ .GeneratorURL }}
{{ end }}{{ if gt (len .SilenceURL) 0 }}Silence: {{ .SilenceURL }}
{{ end }}{{ if gt (len .DashboardURL) 0 }}Dashboard: {{ .DashboardURL }}
{{ end }}{{ if gt (len .PanelURL) 0 }}Panel: {{ .PanelURL }}
{{ end }}{{ end }}{{ end }}

{{ define "default.title" }}{{ template "__subject" . }}{{ end }}

{{ define "default.message" }}{{ if gt (len .Alerts.Firing) 0 }}**Firing**
{{ template "__text_alert_list" .Alerts.Firing }}{{ if gt (len .Alerts.Resolved) 0 }}

{{ end }}{{ end }}{{ if gt (len .Alerts.Resolved) 0 }}**Resolved**
{{ template "__text_alert_list" .Alerts.Resolved }}{{ end }}{{ end }}


{{ define "__teams_text_alert_list" }}{{ range . }}
Value: {{ template "__text_values_list" . }}
Labels:
{{ range .Labels.SortedPairs }} - {{ .Name }} = {{ .Value }}
{{ end }}
Annotations:
{{ range .Annotations.SortedPairs }} - {{ .Name }} = {{ .Value }}
{{ end }}
{{ if gt (len .GeneratorURL) 0 }}Source: [{{ .GeneratorURL }}]({{ .GeneratorURL }})

{{ end }}{{ if gt (len .SilenceURL) 0 }}Silence: [{{ .SilenceURL }}]({{ .SilenceURL }})

{{ end }}{{ if gt (len .DashboardURL) 0 }}Dashboard: [{{ .DashboardURL }}]({{ .DashboardURL }})

{{ end }}{{ if gt (len .PanelURL) 0 }}Panel: [{{ .PanelURL }}]({{ .PanelURL }})

{{ end }}
{{ end }}{{ end }}


{{ define "teams.default.message" }}{{ if gt (len .Alerts.Firing) 0 }}**Firing**
{{ template "__teams_text_alert_list" .Alerts.Firing }}{{ if gt (len .Alerts.Resolved) 0 }}

{{ end }}{{ end }}{{ if gt (len .Alerts.Resolved) 0 }}**Resolved**
{{ template "__teams_text_alert_list" .Alerts.Resolved }}{{ end }}{{ end }}

{{ define "jira.default.summary" }}{{ template "__subject" . }}{{ end }}
{{- define "jira.default.description" -}}
{{- if gt (len .Alerts.Firing) 0 -}}
# Alerts Firing:
{{ template "__text_alert_list_markdown" .Alerts.Firing }}
{{- end -}}
{{- if gt (len .Alerts.Resolved) 0 -}}
# Alerts Resolved:
{{- template "__text_alert_list_markdown" .Alerts.Resolved -}}
{{- end -}}
{{- end -}}

{{- define "jira.default.priority" -}}
{{- $priority := "" }}
{{- range .Alerts.Firing -}}
    {{- $severity := index .Labels "severity" -}}
    {{- if (eq $severity "critical") -}}
        {{- $priority = "High" -}}
    {{- else if (and (eq $severity "warning") (ne $priority "High")) -}}
        {{- $priority = "Medium" -}}
    {{- else if (and (eq $severity "info") (eq $priority "")) -}}
        {{- $priority = "Low" -}}
    {{- end -}}
{{- end -}}
{{- if eq $priority "" -}}
    {{- range .Alerts.Resolved -}}
        {{- $severity := index .Labels "severity" -}}
        {{- if (eq $severity "critical") -}}
            {{- $priority = "High" -}}
        {{- else if (and (eq $severity "warning") (ne $priority "High")) -}}
            {{- $priority = "Medium" -}}
        {{- else if (and (eq $severity "info") (eq $priority "")) -}}
            {{- $priority = "Low" -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- $priority -}}
{{- end -}}

{{ define "webhook.default.payload" -}}
  {{ coll.Dict 
  "receiver" .Receiver
  "status" .Status
  "alerts" .Alerts
  "groupLabels" .GroupLabels
  "commonLabels" .CommonLabels
  "commonAnnotations" .CommonAnnotations
  "externalURL" .ExternalURL
  "version" "1"
  "orgId"  (index .Alerts 0).OrgID
  "truncatedAlerts"  .TruncatedAlerts
  "groupKey" .GroupKey
  "state"  (tmpl.Inline "{{ if eq .Status \"resolved\" }}ok{{ else }}alerting{{ end }}" . )
  "title" (tmpl.Exec "default.title" . )
  "message" (tmpl.Exec "default.message" . )
  | data.ToJSONPretty " "}}
{{- end }}
`

// TemplateForTestsString is the template used for unit tests and integration tests.
// We have it separate from above default template because any tiny change in the template
// will require updating almost all channel tests (15+ files) and it's very time consuming.
const TemplateForTestsString = `
{{ define "__subject" }}[{{ .Status | toUpper }}{{ if eq .Status "firing" }}:{{ .Alerts.Firing | len }}{{ end }}] {{ .GroupLabels.SortedPairs.Values | join " " }} {{ if gt (len .CommonLabels) (len .GroupLabels) }}({{ with .CommonLabels.Remove .GroupLabels.Names }}{{ .Values | join " " }}{{ end }}){{ end }}{{ end }}

{{ define "__text_values_list" }}{{ $len := len .Values }}{{ if $len }}{{ $first := gt $len 1 }}{{ range $refID, $value := .Values -}}
{{ $refID }}={{ $value }}{{ if $first }}, {{ end }}{{ $first = false }}{{ end -}}
{{ else }}[no value]{{ end }}{{ end }}

{{ define "__text_alert_list" }}{{ range . }}
Value: {{ template "__text_values_list" . }}
Labels:
{{ range .Labels.SortedPairs }} - {{ .Name }} = {{ .Value }}
{{ end }}Annotations:
{{ range .Annotations.SortedPairs }} - {{ .Name }} = {{ .Value }}
{{ end }}{{ if gt (len .GeneratorURL) 0 }}Source: {{ .GeneratorURL }}
{{ end }}{{ if gt (len .SilenceURL) 0 }}Silence: {{ .SilenceURL }}
{{ end }}{{ if gt (len .DashboardURL) 0 }}Dashboard: {{ .DashboardURL }}
{{ end }}{{ if gt (len .PanelURL) 0 }}Panel: {{ .PanelURL }}
{{ end }}{{ end }}{{ end }}

{{ define "default.title" }}{{ template "__subject" . }}{{ end }}

{{ define "default.message" }}{{ if gt (len .Alerts.Firing) 0 }}**Firing**
{{ template "__text_alert_list" .Alerts.Firing }}{{ if gt (len .Alerts.Resolved) 0 }}

{{ end }}{{ end }}{{ if gt (len .Alerts.Resolved) 0 }}**Resolved**
{{ template "__text_alert_list" .Alerts.Resolved }}{{ end }}{{ end }}

{{ define "teams.default.message" }}{{ template "default.message" . }}{{ end }}

{{ define "jira.default.summary" }}{{ template "__subject" . }}{{ end }}

{{- define "jira.default.description" -}}
{{- if gt (len .Alerts.Firing) 0 -}}
# Alerts Firing:
{{ template "__text_alert_list_markdown" .Alerts.Firing }}
{{- end -}}
{{- if gt (len .Alerts.Resolved) 0 -}}
# Alerts Resolved:
{{- template "__text_alert_list_markdown" .Alerts.Resolved -}}
{{- end -}}
{{- end -}}

{{- define "jira.default.priority" -}}
{{- $priority := "" }}
{{- range .Alerts.Firing -}}
    {{- $severity := index .Labels "severity" -}}
    {{- if (eq $severity "critical") -}}
        {{- $priority = "High" -}}
    {{- else if (and (eq $severity "warning") (ne $priority "High")) -}}
        {{- $priority = "Medium" -}}
    {{- else if (and (eq $severity "info") (eq $priority "")) -}}
        {{- $priority = "Low" -}}
    {{- end -}}
{{- end -}}
{{- if eq $priority "" -}}
    {{- range .Alerts.Resolved -}}
        {{- $severity := index .Labels "severity" -}}
        {{- if (eq $severity "critical") -}}
            {{- $priority = "High" -}}
        {{- else if (and (eq $severity "warning") (ne $priority "High")) -}}
            {{- $priority = "Medium" -}}
        {{- else if (and (eq $severity "info") (eq $priority "")) -}}
            {{- $priority = "Low" -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- $priority -}}
{{- end -}}
`

func ForTests(t *testing.T) *Template {
	tmpl, err := FromContent([]string{TemplateForTestsString})
	require.NoError(t, err)
	externalURL, err := url.Parse("http://test.com")
	require.NoError(t, err)
	tmpl.ExternalURL = externalURL
	return tmpl
}
