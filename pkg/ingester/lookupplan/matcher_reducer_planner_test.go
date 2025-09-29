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

	equalMatcher, _ := labels.NewMatcher(labels.MatchEqual, "foo", "bar")
	selectiveRegexMatcher, _ := labels.NewMatcher(labels.MatchRegexp, "foo", ".*bar.*")
	nonSelectiveRegexMatcher, _ := labels.NewMatcher(labels.MatchRegexp, "foo", ".*")
	nonSelectiveNotRegexMatcher, _ := labels.NewMatcher(labels.MatchNotRegexp, "foo", "")
	nonSelectiveNotEqualsMatcher, _ := labels.NewMatcher(labels.MatchNotEqual, "foo", "")

	otherLabelNonSelectiveNotRegexMatcher, _ := labels.NewMatcher(labels.MatchNotRegexp, "baz", "")
	otherLabelSelectiveMatcher, _ := labels.NewMatcher(labels.MatchNotEqual, "baz", "bananas")

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
				indexMatchers: []*labels.Matcher{equalMatcher, nonSelectiveRegexMatcher, nonSelectiveNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{equalMatcher, nonSelectiveNotRegexMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalMatcher, nonSelectiveRegexMatcher, nonSelectiveNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{equalMatcher, nonSelectiveNotRegexMatcher},
			},
		},
		{
			name: "planning should not alter plan with multiple selective matchers for label name",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalMatcher},
				scanMatchers:  []*labels.Matcher{selectiveRegexMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalMatcher},
				scanMatchers:  []*labels.Matcher{selectiveRegexMatcher},
			},
		},
		{
			name: "deduplicate index matchers",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalMatcher, equalMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
		},
		{
			name: "deduplicate scan matchers",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{selectiveRegexMatcher, selectiveRegexMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{selectiveRegexMatcher},
			},
		},
		{
			name: "duplicate matchers across index and scan should be deduplicated to only index matcher",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalMatcher},
				scanMatchers:  []*labels.Matcher{equalMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{equalMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
		},
		{
			name: "remove all non-selective matchers if a selective matcher exists",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{nonSelectiveRegexMatcher},
				scanMatchers:  []*labels.Matcher{equalMatcher, nonSelectiveNotEqualsMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{equalMatcher},
			},
		},
		{
			name: "keep one non-selective matcher if no selective matcher exists",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{nonSelectiveRegexMatcher},
				scanMatchers:  []*labels.Matcher{nonSelectiveNotRegexMatcher, nonSelectiveNotRegexMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{nonSelectiveNotRegexMatcher},
			},
		},
		{
			name: "keep at least one matcher for each label name",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{nonSelectiveRegexMatcher, otherLabelNonSelectiveNotRegexMatcher},
				scanMatchers:  []*labels.Matcher{nonSelectiveNotRegexMatcher, nonSelectiveNotRegexMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{otherLabelNonSelectiveNotRegexMatcher},
				scanMatchers:  []*labels.Matcher{nonSelectiveNotRegexMatcher},
			},
		},
		{
			name: "keep at least one matcher for each label name, keep selective matchers over non-selective matchers",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{nonSelectiveRegexMatcher, otherLabelNonSelectiveNotRegexMatcher},
				scanMatchers:  []*labels.Matcher{nonSelectiveNotRegexMatcher, nonSelectiveNotRegexMatcher, otherLabelSelectiveMatcher},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{},
				scanMatchers:  []*labels.Matcher{nonSelectiveNotRegexMatcher, otherLabelSelectiveMatcher},
			},
		},
		{
			name: "nonselective matcher for another label name should not be dropped",
			ctx:  context.Background(),
			inPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{otherLabelSelectiveMatcher, nonSelectiveNotEqualsMatcher},
				scanMatchers:  []*labels.Matcher{},
			},
			expectedPlan: concreteLookupPlan{
				indexMatchers: []*labels.Matcher{otherLabelSelectiveMatcher, nonSelectiveNotEqualsMatcher},
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
