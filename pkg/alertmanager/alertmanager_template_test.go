// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/util/strutil"
	"github.com/stretchr/testify/assert"
)

func Test_withCustomFunctions(t *testing.T) {
	type tc struct {
		name        string
		template    string
		alerts      template.Alerts
		result      string
		expectError bool
	}
	tmpl, err := template.FromGlobs([]string{}, WithCustomFunctions("test"))
	assert.NoError(t, err)
	cases := []tc{
		{
			name:     "template tenant ID",
			template: "{{ tenantID }}",
			result:   "test",
		},
		{
			name: "parse out query from GeneratorURL",
			alerts: template.Alerts{
				template.Alert{
					GeneratorURL: "http://localhost:9090" + strutil.TableLinkForExpression(`sum by (foo)(rate(bar{foo="bar"}[3m]))`),
				},
			},
			template: `{{ queryFromGeneratorURL (index .Alerts 0).GeneratorURL }}`,
			result:   `sum by (foo)(rate(bar{foo="bar"}[3m]))`,
		},
		{
			name: "error on missing query in GeneratorURL",
			alerts: template.Alerts{
				template.Alert{
					GeneratorURL: "http://localhost:9090?foo=bar",
				},
			},
			template:    `{{ queryFormGeneratorURL (index .Alerts 0).GeneratorURL }}`,
			expectError: true,
		},
		{
			name: "error on URL decoding query in GeneratorURL",
			alerts: template.Alerts{
				template.Alert{
					GeneratorURL: "http://localhost:9090?g0.expr=up{foo=bar}",
				},
			},
			template:    `{{ queryFormGeneratorURL (index .Alerts 0).GeneratorURL }}`,
			expectError: true,
		},
		{
			name:     "generate grafana explore URL",
			template: `{{ grafanaExploreURL "https://foo.bar" "test_datasoruce" "now-12h" "now" "up{foo!=\"bar\"}" }}`,
			result:   `https://foo.bar/explore?left=` + url.QueryEscape(`{"range":{"from":"now-12h","to":"now"},"queries":[{"datasource":{"type":"prometheus","uid":"test_datasoruce"},"expr":"up{foo!=\"bar\"}","instant":false,"range":true,"refId":"A"}]}`),
		},
		{
			name:        "invalid params for grafanaExploreURL",
			template:    `{{ grafanaExploreURL "https://foo.bar" 3 2 1 0 }}`,
			expectError: true,
		},
		{
			name: "Generate Grafana Explore URL from GeneratorURL query",
			alerts: template.Alerts{
				template.Alert{
					GeneratorURL: "http://localhost:9090" + strutil.TableLinkForExpression(`up{foo!="bar"}`),
				},
			},
			template: `{{ grafanaExploreURL "https://foo.bar" "test_datasoruce" "now-12h" "now" (queryFromGeneratorURL (index .Alerts 0).GeneratorURL) }}`,
			result:   `https://foo.bar/explore?left=` + url.QueryEscape(`{"range":{"from":"now-12h","to":"now"},"queries":[{"datasource":{"type":"prometheus","uid":"test_datasoruce"},"expr":"up{foo!=\"bar\"}","instant":false,"range":true,"refId":"A"}]}`),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := tmpl.ExecuteTextString(c.template, template.Data{Alerts: c.alerts})
			if c.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, c.result, res)
		})
	}
}

func Test_loadTemplates(t *testing.T) {
	type tc struct {
		name   string
		loaded []string
		invoke string
		exp    string
		expErr string
	}

	cases := []tc{
		{
			name: "can reference loaded templates",
			loaded: []string{
				`
{{ define "my_tmpl_1" }}My Template 1{{ end }}
`,
			},
			invoke: "my_tmpl_1",
			exp:    "My Template 1",
		},
		{
			name: "fails to reference nonexistant templates",
			loaded: []string{
				`
{{ define "my_tmpl_1" }}My Template 1{{ end }}
`,
			},
			invoke: "does_not_exist",
			expErr: "not defined",
		},
		{
			name:   "can reference default templates without loading them",
			invoke: "discord.default.message",
			exp:    "Alerts Firing:\nLabels:\nAnnotations:\nSource: http://localhost:9090",
		},
		{
			name:   "can reference default email templates without loading them",
			invoke: "email.default.html",
			exp:    "DOCTYPE html",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tmpl, err := loadTemplates(c.loaded, WithCustomFunctions("test"))
			assert.NoError(t, err)

			call := fmt.Sprintf(`{{ template "%s" . }}`, c.invoke)

			data := templateDataForTests(t, tmpl)
			res, err := tmpl.ExecuteTextString(call, data)
			if c.expErr != "" {
				assert.Contains(t, err.Error(), c.expErr)
			} else {
				assert.NoError(t, err)
				assert.Contains(t, res, c.exp)
			}
		})
	}
}

func templateDataForTests(t *testing.T, tmpl *template.Template) *template.Data {
	t.Helper()

	eurl, _ := url.Parse("http://localhost:9090")
	tmpl.ExternalURL = eurl // This is done externally, by the system using the templates.
	return tmpl.Data("receiver", model.LabelSet{}, &types.Alert{
		Alert: model.Alert{
			GeneratorURL: "http://localhost:9090",
		},
	})
}
