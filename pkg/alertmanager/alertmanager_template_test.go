// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"net/url"
	"testing"

	"github.com/prometheus/alertmanager/template"
	"github.com/stretchr/testify/assert"
)

func Test_withCustomFunctions(t *testing.T) {
	type tc struct {
		name        string
		template    string
		result      string
		expectError bool
	}
	tmpl, err := template.FromGlobs([]string{}, withCustomFunctions("test"))
	assert.NoError(t, err)
	data := template.Data{}
	cases := []tc{
		{
			name:     "template tenant ID",
			template: "{{ tenantID }}",
			result:   "test",
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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := tmpl.ExecuteTextString(c.template, data)
			if c.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, c.result, res)
		})
	}
}
