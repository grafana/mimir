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

	equalsMatcher, _ := labels.NewMatcher(labels.MatchEqual, "foo", "bar")
	otherEqualsMatcher, _ := labels.NewMatcher(labels.MatchEqual, "foo", "baz")
	supersetRegexpMatcher, _ := labels.NewMatcher(labels.MatchRegexp, "foo", ".*bar.*")
	nonSupersetRegexpMatcher, _ := labels.NewMatcher(labels.MatchRegexp, "foo", "baz.*")

	wildcardMatcher, _ := labels.NewMatcher(labels.MatchRegexp, "foo", ".*")
	emptyStringNotRegexpMatcher, _ := labels.NewMatcher(labels.MatchNotRegexp, "foo", "")
	emptyStringNotEqualsMatcher, _ := labels.NewMatcher(labels.MatchNotEqual, "foo", "")
	notEqualsMatcher, _ := labels.NewMatcher(labels.MatchNotEqual, "foo", "bar")

	otherLabelEmptyStringNotEqualsMatcher, _ := labels.NewMatcher(labels.MatchNotRegexp, "baz", "")

	tests := []struct {
		name         string
		ctx          context.Context
		inPlan       concreteLookupPlan
		expectedPlan concreteLookupPlan
	}{
		{
			name: "planning disabled should not alter plan",
			ctx:  ctxPlanningDisabled,
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher, wildcardMatcher, emptyStringNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{equalsMatcher, emptyStringNotRegexpMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher, wildcardMatcher, emptyStringNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{equalsMatcher, emptyStringNotRegexpMatcher},
			},
		},
		{
			name: "deduplicate index matchers",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher, equalsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
		},
		{
			name: "deduplicate scan matchers",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{supersetRegexpMatcher, supersetRegexpMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{supersetRegexpMatcher},
			},
		},
		{
			name: "duplicate matchers across index and scan should be deduplicated to only index matcher",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher},
				scanMatchers:  []*labels.Matcher{equalsMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
		},
		{
			name: "multiple equals matchers should only return equals matchers",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher},
				scanMatchers:  []*labels.Matcher{otherEqualsMatcher, supersetRegexpMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher},
				scanMatchers:  []*labels.Matcher{otherEqualsMatcher},
			},
		},
		{
			name: "planning should remove a regex matcher if it is a superset of an equals matcher",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher},
				scanMatchers:  []*labels.Matcher{supersetRegexpMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
		},
		{
			name: "planning should not remove a regex matcher if it is not a superset of an equals matcher",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher, equalsMatcher},
				scanMatchers:  []*labels.Matcher{nonSupersetRegexpMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalsMatcher},
				scanMatchers:  []*labels.Matcher{nonSupersetRegexpMatcher},
			},
		},
		{
			name: "always drop wildcard matcher, even if it is the only matcher",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{wildcardMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{},
			},
		},
		{
			name: "single matcher should not be dropped",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{emptyStringNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{emptyStringNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
		},
		{
			name: "if there is an equals matcher, remove all matchers that match supersets of the equals matcher",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{supersetRegexpMatcher},
				scanMatchers:  []*labels.Matcher{equalsMatcher, emptyStringNotEqualsMatcher, emptyStringNotRegexpMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{equalsMatcher},
			},
		},
		{
			name: "keep a matcher that excludes empty strings if it is the most selective matcher",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{wildcardMatcher},
				scanMatchers:  []*labels.Matcher{emptyStringNotRegexpMatcher, emptyStringNotRegexpMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{emptyStringNotRegexpMatcher},
			},
		},
		{
			name: "keep at least one matcher for each label name",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{wildcardMatcher, otherLabelEmptyStringNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{emptyStringNotRegexpMatcher, emptyStringNotRegexpMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{otherLabelEmptyStringNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{emptyStringNotRegexpMatcher},
			},
		},
		{
			name: "keep matcher that subtracts empty strings if no other matcher does so",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{notEqualsMatcher, emptyStringNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{notEqualsMatcher, emptyStringNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := MatcherReducerPlanner{}
			outPlan, err := planner.PlanIndexLookup(tt.ctx, tt.inPlan, 0, 0)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedPlan.IndexMatchers(), outPlan.IndexMatchers())
			assert.Equal(t, tt.expectedPlan.ScanMatchers(), outPlan.ScanMatchers())

		})
	}
}
