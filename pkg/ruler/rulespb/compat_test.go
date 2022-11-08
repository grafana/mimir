// SPDX-License-Identifier: AGPL-3.0-only

package rulespb

import (
	"testing"

	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestRoundtrip(t *testing.T) {
	for name, group := range map[string]string{
		"no eval delay": `
name: testrules
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))

    - alert: ThisIsBad
      expr: sum(rate(test_metric[2m]))
      for: 10m
`,

		"with eval delay": `
name: testrules
evaluation_delay: 3m
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,

		"with eval delay and source tenants": `
name: testrules
evaluation_delay: 3m
source_tenants:
  - a
  - b
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,
	} {
		t.Run(name, func(t *testing.T) {
			rg := rulefmt.RuleGroup{}
			require.NoError(t, yaml.Unmarshal([]byte(group), &rg))

			desc := ToProto("user", "namespace", rg)
			newRg := FromProto(desc)

			newYaml, err := yaml.Marshal(newRg)
			require.NoError(t, err)

			assert.YAMLEq(t, group, string(newYaml))
		})
	}
}

func TestZeroEvalDelayIsIgnored(t *testing.T) {
	const group = `
name: testrules
evaluation_delay: 0s
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`
	rg := rulefmt.RuleGroup{}
	require.NoError(t, yaml.Unmarshal([]byte(group), &rg))

	desc := ToProto("user", "namespace", rg)
	newRg := FromProto(desc)

	assert.Nil(t, newRg.EvaluationDelay)
}
