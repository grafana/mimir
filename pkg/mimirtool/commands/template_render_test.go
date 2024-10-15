// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Based on https://github.com/grafana/mimir/blob/main/pkg/alertmanager/alertmanager_template_test.go
func TestTemplateRender(t *testing.T) {
	type tc struct {
		name            string
		templateOptions TemplateRenderCmd
		result          string
		expectError     bool
	}
	jsonFilesStr := []string{"testdata/template/alert_data1.json", "testdata/template/alert_data1.json", "testdata/template/alert_data2.json"}
	jsonFiles := make([]*os.File, len(jsonFilesStr))

	for index, jsonFile := range jsonFilesStr {
		file, err := os.OpenFile(jsonFile, os.O_RDONLY, 0)
		assert.NoError(t, err, "Json template data doesn't exist")
		jsonFiles[index] = file

	}
	cases := []tc{
		{
			name: "testing basic message template",
			templateOptions: TemplateRenderCmd{
				TemplateFilesGlobs: []string{"testdata/template/alertmanager_template1.tmpl"},
				TemplateType:       "text",
				TemplateText:       `{{ template "my_message" . }}`,
				TemplateData:       jsonFiles[0],
				TenantID:           "",
			},
			result: `[AlertNameExample | testing_purpose | lab]`,
		},
		{
			name: "testing basic description template",
			templateOptions: TemplateRenderCmd{
				TemplateFilesGlobs: []string{"testdata/template/alertmanager_template1.tmpl"},
				TemplateType:       "text",
				TemplateText:       `{{ template "my_description" . }}`,
				TemplateData:       jsonFiles[1],
				TenantID:           "",
			},
			result: `
Alertname: AlertNameExample
Severity: warning

Details:
• Customer: testing_purpose
• Environment: lab
• Description: blablablabla


`,
		},
		{
			name: "testing custom description template", // Using Specific Mimir Template function
			templateOptions: TemplateRenderCmd{
				TemplateFilesGlobs: []string{"testdata/template/alertmanager_template2.tmpl"},
				TemplateType:       "text",
				TemplateText:       `{{ template "my_description" . }}`,
				TemplateData:       jsonFiles[2],
				TenantID:           "",
			},
			result: `
Alertname: AlertNameExample2
Severity: warning

Details:
• Customer: testing_purpose
• Environment: lab
• Description: blablablabla

<a href="https://example.com/explore?left=%7B%22range%22%3A%7B%22from%22%3A%22now-12h%22%2C%22to%22%3A%22now%22%7D%2C%22queries%22%3A%5B%7B%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22xyz%22%7D%2C%22expr%22%3A%22up%22%2C%22instant%22%3Afalse%2C%22range%22%3Atrue%2C%22refId%22%3A%22A%22%7D%5D%7D">Grafana Explorer URL</a>
`,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			renderedTemplate, err := TemplateRender(&c.templateOptions)
			if c.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, c.result, renderedTemplate)
		})
	}
}
