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
	tmpl, err := template.FromGlobs([]string{}, withCustomFunctions("test"))
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
			template:    `{{ queryFromGeneratorURL (index .Alerts 0).GeneratorURL }}`,
			expectError: true,
		},
		{
			name: "error on URL decoding query in GeneratorURL",
			alerts: template.Alerts{
				template.Alert{
					GeneratorURL: "http://localhost:9090?expr=up{foo=bar}",
				},
			},
			template:    `{{ queryFromGeneratorURL (index .Alerts 0).GeneratorURL }}`,
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
		{
			name: "Generate Grafana Explore URL from GeneratorURL query from loki",
			alerts: template.Alerts{
				template.Alert{
					GeneratorURL: `/explore?left={"queries":[{"datasource":{"type":"loki","uid":"loki"},"expr":"up{foo!=\"bar\"}","queryType":"range"}]}`,
				},
			},
			template: `{{ grafanaExploreURL "http://localhost:9090" "test_datasource" "now-12h" "now" (queryFromGeneratorURL (index .Alerts 0).GeneratorURL) }}`,
			result:   `http://localhost:9090/explore?left=%7B%22range%22%3A%7B%22from%22%3A%22now-12h%22%2C%22to%22%3A%22now%22%7D%2C%22queries%22%3A%5B%7B%22datasource%22%3A%7B%22type%22%3A%22loki%22%2C%22uid%22%3A%22loki%22%7D%2C%22expr%22%3A%22up%7Bfoo%21%3D%5C%22bar%5C%22%7D%22%2C%22instant%22%3Afalse%2C%22range%22%3Atrue%2C%22refId%22%3A%22A%22%7D%5D%7D`,
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
