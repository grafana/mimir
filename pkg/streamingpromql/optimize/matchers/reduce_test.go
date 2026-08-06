// SPDX-License-Identifier: AGPL-3.0-only

package matchers

import (
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestReduce(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedRetained string
		expectedDropped  string
	}{
		{
			name:             "only name matcher",
			input:            `test_series`,
			expectedRetained: `test_series`,
		},
		{
			name:             "deduplicate matchers",
			input:            `test_series{foo="bar",foo="bar",foo="bar",foo=~".*baz.*",foo=~".*baz.*"}`,
			expectedRetained: `test_series{foo="bar",foo=~".*baz.*"}`,
			expectedDropped:  `{foo="bar",foo="bar",foo=~".*baz.*"}`,
		},
		{
			name: "multiple unique equals matchers should only return equals matchers",
			// Even though the regex matcher matches neither equals matcher,
			// a query with multiple equals matchers for the same label name is already guaranteed to return an empty set
			input:            `test_series{foo="bar",foo=~".*bananas.*",foo="baz"}`,
			expectedRetained: `test_series{foo="bar",foo="baz"}`,
			expectedDropped:  `{foo=~".*bananas.*"}`,
		},
		{
			name:             "should remove a regex matcher if it is a superset of an equals matcher",
			input:            `test_series{foo="bar",foo=~".*bar.*"}`,
			expectedRetained: `test_series{foo="bar"}`,
			expectedDropped:  `{foo=~".*bar.*"}`,
		},
		{
			name:             "should preserve a regex matcher if it is not a superset of an equals matcher",
			input:            `test_series{foo="bar",foo="bar",foo=~".*baz.*"}`,
			expectedRetained: `test_series{foo="bar",foo=~".*baz.*"}`,
			expectedDropped:  `{foo="bar"}`,
		},
		{
			name:             "do not drop wildcard negative regex matcher",
			input:            `test_series{foo!~".*"}`,
			expectedRetained: `test_series{foo!~".*"}`,
		},
		{
			name:             "single non-wildcard matcher should not be dropped",
			input:            `test_series{foo!=""}`,
			expectedRetained: `test_series{foo!=""}`,
		},
		{
			name:             "drop all matchers that match supersets of an equals matcher",
			input:            `test_series{foo=~".*bar.*",foo="bar",foo!="",foo!~"",foo!="baz"}`,
			expectedRetained: `test_series{foo="bar"}`,
			expectedDropped:  `{foo!="",foo!="baz",foo!~"",foo=~".*bar.*"}`,
		},
		{
			name:             "keep one matcher of ones that reduce the set size equivalently",
			input:            `test_series{foo=~".*",foo!~"",foo!=""}`,
			expectedRetained: `test_series{foo!~""}`,
			expectedDropped:  `{foo!="",foo=~".*"}`,
		},
		{
			name:             "keep at least one matcher for each label name",
			input:            `test_series{foo=~".*",baz!="",foo!~"",foo!=""}`,
			expectedRetained: `test_series{baz!="",foo!~""}`,
			expectedDropped:  `{foo!="",foo=~".*"}`,
		},
		{
			name:             "keep matcher that excludes empty strings if no other matcher does so",
			input:            `test_series{foo!="",foo!="bar"}`,
			expectedRetained: `test_series{foo!="",foo!="bar"}`,
		},
		{
			name:             "not equals matcher should not be removed if it doesn't match equals matcher value",
			input:            `test_series{foo!="bar",foo="bar"}`,
			expectedRetained: `test_series{foo!="bar",foo="bar"}`,
		},
		{
			name:             "not equals matcher should be removed if it does match equals matcher value",
			input:            `test_series{foo!="bar",foo="baz"}`,
			expectedRetained: `test_series{foo="baz"}`,
			expectedDropped:  `{foo!="bar"}`,
		},
		{
			name:             "not equals empty string matcher should be removed if a positive regex matcher already excludes its value",
			input:            `test_series{foo!="",foo=~".+baz.+"}`,
			expectedRetained: `test_series{foo=~".+baz.+"}`,
			expectedDropped:  `{foo!=""}`,
		},
		{
			name:             "not equals matcher should be removed if a positive regex matcher already excludes its value",
			input:            `test_series{foo!="host99", foo=~"(?i:(host1|host2|host3|host4|host5))"}`,
			expectedRetained: `test_series{foo=~"(?i:(host1|host2|host3|host4|host5))"}`,
			expectedDropped:  `{foo!="host99"}`,
		},
		{
			name:             "not equals matcher should not be removed if a positive regex matcher doesn't exclude its value",
			input:            `test_series{foo!="bad",foo=~"b.+"}`,
			expectedRetained: `test_series{foo!="bad",foo=~"b.+"}`,
		},
	}

	parser := promqlext.NewPromQLParser()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matchers, err := parser.ParseMetricSelector(tt.input)
			require.NoError(t, err)

			retained, dropped := Reduce(matchers, false)
			require.Equal(t, tt.expectedRetained, formatMatchers(retained), "retained matchers do not match expected")
			require.Equal(t, tt.expectedDropped, formatMatchers(dropped), "dropped matchers do not match expected")
		})
	}
}

func formatMatchers(matchers []*labels.Matcher) string {
	var metricName string
	formatted := make([]string, 0, len(matchers))

	for _, m := range matchers {
		if m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual {
			metricName = m.Value
			continue
		}

		formatted = append(formatted, m.String())
	}

	if len(formatted) > 0 {
		slices.Sort(formatted)
		return metricName + "{" + strings.Join(formatted, ",") + "}"
	}

	return metricName
}
