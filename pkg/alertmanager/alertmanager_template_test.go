// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"net/url"
	"testing"

	"github.com/prometheus/alertmanager/template"
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
