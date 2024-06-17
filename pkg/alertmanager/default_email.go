package alertmanager

const defaultEmail = `
{{ define "ng_alert_notification" }}
Test!
Subject: {{ .Subject }}
AppUrl: {{ .AppUrl }}
BuildVersion: {{ .BuildVersion }}
TemplateData: {{ .TemplateData }}
{{ end }}
`
