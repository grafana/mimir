// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatcherReducerPlanner_PlanIndexLookup(t *testing.T) {
	ctxPlanningDisabled := ContextWithDisabledPlanning(context.Background())
	tests := []struct {
		name                  string
		ctx                   context.Context
		inIndexMatchers       []string
		inScanMatchers        []string
		expectedIndexMatchers []string
		expectedScanMatchers  []string
	}{
		{
			name:                  "planning disabled should not alter plan",
			ctx:                   ctxPlanningDisabled,
			inIndexMatchers:       []string{`foo="bar"`, `foo=~".*"`, `foo!=""`},
			expectedIndexMatchers: []string{`foo="bar"`, `foo=~".*"`, `foo!=""`},
			inScanMatchers:        []string{`foo="bar"`, `foo!=""`},
			expectedScanMatchers:  []string{`foo="bar"`, `foo!=""`},
		},
		{
			name:                  "planning enabled with scan matchers should not alter plan",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo="bar"`, `foo=~".*"`, `foo!=""`},
			expectedIndexMatchers: []string{`foo="bar"`, `foo=~".*"`, `foo!=""`},
			inScanMatchers:        []string{`foo="bar"`, `foo!=""`},
			expectedScanMatchers:  []string{`foo="bar"`, `foo!=""`},
		},
		{
			name:                  "deduplicate matchers",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo="bar"`, `foo="bar"`, `foo=~".*baz.*"`, `foo=~".*baz.*"`},
			expectedIndexMatchers: []string{`foo="bar"`, `foo=~".*baz.*"`},
		},
		{
			name: "multiple unique equals matchers should only return equals matchers",
			ctx:  context.Background(),
			// Even though the regex matcher matches neither equals matcher,
			// a query with multiple equals matchers for the same label name is already guaranteed to return an empty set
			inIndexMatchers:       []string{`foo="bar"`, `foo=~".*bananas.*"`, `foo="baz"`},
			expectedIndexMatchers: []string{`foo="bar"`, `foo="baz"`},
		},
		{
			name:                  "planning should remove a regex matcher if it is a superset of an equals matcher",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo="bar"`, `foo=~".*bar.*"`},
			expectedIndexMatchers: []string{`foo="bar"`},
		},
		{
			name:                  "planning should preserve a regex matcher if it is not a superset of an equals matcher",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo="bar"`, `foo="bar"`, `foo=~".*baz.*"`},
			expectedIndexMatchers: []string{`foo="bar"`, `foo=~".*baz.*"`},
		},
		{
			name:                  "always drop wildcard matcher, even if it is the only matcher",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo=~".*"`},
			expectedIndexMatchers: []string{},
		},
		{
			name:                  "do not drop wildcard negative regex matcher",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo!~".*"`},
			expectedIndexMatchers: []string{`foo!~".*"`},
		},
		{
			name:                  "single non-wildcard matcher should not be dropped",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo!=""`},
			expectedIndexMatchers: []string{`foo!=""`},
		},
		{
			name:                  "drop all matchers that match supersets of an equals matcher",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo=~".*bar.*"`, `foo="bar"`, `foo!=""`, `foo!~""`, `foo!="baz"`},
			expectedIndexMatchers: []string{`foo="bar"`},
		},
		{
			name:                  "keep one matcher of ones that reduce the set size equivalently",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo=~".*"`, `foo!~""`, `foo!=""`},
			expectedIndexMatchers: []string{`foo!~""`},
		},
		{
			name:                  "keep at least one matcher for each label name",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo=~".*"`, `baz!=""`, `foo!~""`, `foo!=""`},
			expectedIndexMatchers: []string{`baz!=""`, `foo!~""`},
		},
		{
			name:                  "keep matcher that excludes empty strings if no other matcher does so",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo!="bar"`, `foo!=""`},
			expectedIndexMatchers: []string{`foo!="bar"`, `foo!=""`},
		},
		{
			name:                  "not equals matcher should not be removed if it doesn't match equals matcher value",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo!="bar"`, `foo="bar"`},
			expectedIndexMatchers: []string{`foo!="bar"`, `foo="bar"`},
		},
		{
			name:                  "not equals matcher should be removed if it does match equals matcher value",
			ctx:                   context.Background(),
			inIndexMatchers:       []string{`foo!="bar"`, `foo="baz"`},
			expectedIndexMatchers: []string{`foo="baz"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inIndex := parseMatchers(t, tt.inIndexMatchers)
			inScan := parseMatchers(t, tt.inScanMatchers)
			inPlan := concreteLookupPlan{inIndex, inScan}

			planner := MatcherReducerPlanner{}
			outPlan, err := planner.PlanIndexLookup(tt.ctx, inPlan, 0, 0)
			require.NoError(t, err)

			expectedIndex := parseMatchers(t, tt.expectedIndexMatchers)
			expectedScan := parseMatchers(t, tt.expectedScanMatchers)
			expectedPlan := concreteLookupPlan{expectedIndex, expectedScan}
			assertEqualMatchers(t, expectedPlan.IndexMatchers(), outPlan.IndexMatchers())
			assertEqualMatchers(t, expectedPlan.ScanMatchers(), outPlan.ScanMatchers())
		})
	}
}

func assertEqualMatchers(t *testing.T, expected, actual []*labels.Matcher) {
	assert.Equal(t, len(expected), len(actual))
	for i, m := range expected {
		assert.NotNil(t, actual[i])
		switch m.Type {
		case labels.MatchEqual, labels.MatchNotEqual:
			assert.Equal(t, m, actual[i])
		case labels.MatchRegexp, labels.MatchNotRegexp:
			// We can't rely on the FastRegexMatcher pointer to be the same for all regex matchers
			assert.Equal(t, m.Type, actual[i].Type)
			assert.Equal(t, m.Name, actual[i].Name)
			assert.Equal(t, m.Value, actual[i].Value)
		}
	}
}

func parseMatchers(t *testing.T, ms []string) []*labels.Matcher {
	outMatchers := make([]*labels.Matcher, 0, len(ms))
	for _, m := range ms {
		outMatchers = append(outMatchers, parseMatcher(t, m))
	}
	return outMatchers
}