// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"sort"
	"testing"

	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
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
			inIndexMatchers:       []string{`foo="bar"`, `foo="bar"`, `foo="bar"`, `foo=~".*baz.*"`, `foo=~".*baz.*"`},
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

func TestMatcherReducerPlanner_MatchedSeries(t *testing.T) {
	// Create TSDB instance with sample data
	db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	sampleSeries := []labels.Labels{
		labels.FromStrings("__name__", "http_requests_total", "method", "GET", "status", "200", "handler", "/api"),
		labels.FromStrings("__name__", "http_requests_total", "method", "POST", "status", "200", "handler", "/api"),
		labels.FromStrings("__name__", "http_requests_total", "method", "GET", "status", "404", "handler", "/api"),
		labels.FromStrings("__name__", "http_requests_total", "method", "GET", "status", "500", "handler", "/api"),
		labels.FromStrings("__name__", "http_requests_total", "method", "GET", "status", "504", "handler", "/api"),
		labels.FromStrings("__name__", "cpu_usage_percent", "instance", "web-1", "job", "webserver"),
		labels.FromStrings("__name__", "cache_requests_total", "backend", "memcached"),
	}

	// Add sample series to TSDB
	app := db.Appender(context.Background())
	for i, series := range sampleSeries {
		_, err := app.Append(0, series, int64(i*1000), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	tests := []struct {
		name     string
		matchers []string
	}{
		{
			name:     "intersecting (duplicate) equals matcher for the same label name",
			matchers: []string{`__name__="http_requests_total"`, `__name__="http_requests_total"`},
		},
		{
			name:     "non-intersecting equals matchers for the same label name",
			matchers: []string{`__name__="http_requests_total"`, `__name__="cpu_usage_percent"`},
		},
		{
			name:     "multiple equals matchers for different label names",
			matchers: []string{`__name__="http_requests_total"`, `method="GET"`},
		},
		{
			name:     "not equals matcher only",
			matchers: []string{`status!="500"`},
		},
		{
			name:     "not-regex matcher only",
			matchers: []string{`__name__!~".*requests.*"`},
		},
		{
			name:     "wildcard matcher only",
			matchers: []string{`__name__=~".*"`},
		},
		{
			name:     "wildcard matcher and not-equals matcher for different label names",
			matchers: []string{`instance=~".*"`, `__name__!=""`},
		},
		{
			name:     "regex matcher and intersecting equals matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__="http_requests_total"`},
		},
		{
			name:     "regex matcher and non-intersecting equals matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__="cpu_usage_percent"`},
		},
		{
			name:     "regex matcher and non-subtractive not-equals matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__!="cpu_usage_percent"`},
		},
		{
			name:     "regex matcher and fully subtractive not-equals matcher",
			matchers: []string{`__name__=~".*http_requests.*"`, `__name__!="http_requests_total"`},
		},
		{
			name:     "regex matcher and partially subtractive not-equals matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__!="http_requests_total"`},
		},
		{
			name:     "regex matcher and intersecting regex matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__=~".*total.*"`},
		},
		{
			name:     "regex matcher and non-intersecting regex matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__=~".*percent.*"`},
		},
		{
			name:     "regex matcher and partially intersecting regex matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__=~".*http.*"`},
		},
		{
			name:     "regex matcher and intersecting not-regex matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__!~".*usage.*"`},
		},
		{
			name:     "regex matcher and non-intersecting not-regex matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__!~".*"`},
		},
		{
			name:     "regex matcher and partially intersecting not-regex matcher",
			matchers: []string{`__name__=~".*requests.*"`, `__name__!~".*cache.*"`},
		},
		{
			name:     "not-equals matcher and equivalent not-regex matcher",
			matchers: []string{`__name__!=""`, `__name__!~""`},
		},
		{
			name:     "not-equals matcher and subtractive not-regex matcher",
			matchers: []string{`__name__!="http_requests_total"`, `__name__!~".*usage.*"`},
		},
		{
			name:     "not-equals matcher and non-subtractive not-regex matcher",
			matchers: []string{`__name__!="http_requests_total"`, `__name__!~".*bananas.*"`},
		},
		{
			name:     "not-equals matcher and intersecting equals matcher",
			matchers: []string{`__name__!=""`, `__name__="http_requests_total"`},
		},
		{
			name:     "not-equals matcher and non-intersecting equals matcher",
			matchers: []string{`__name__!="http_requests_total"`, `__name__="http_requests_total"`},
		},
		{
			name:     "not-equals matcher and intersecting not-equals matcher",
			matchers: []string{`__name__!=""`, `__name__!="cache_requests_total"`},
		},
		{
			name:     "not-equals matcher and non-intersecting not-equals matcher for different label names",
			matchers: []string{`backend!=""`, `__name__!="cache_requests_total"`},
		},
		{
			name:     "not-regex matcher and not-regex matcher",
			matchers: []string{`__name__!~""`, `__name__!~"http_requests_total"`},
		},
		{
			name:     "not-regex matcher and not-regex matcher, empty set",
			matchers: []string{`__name__!~""`, `__name__!~".*requests.*"`, `__name__!~".*cpu.*"`},
		},
		{
			name:     "not equals matcher with positive regex matcher for the same label name",
			matchers: []string{`status!="500"`, `status=~".04"`},
		},
		{
			name:     "all matcher types",
			matchers: []string{`__name__="http_requests_total"`, `method="GET"`, `status!="500"`, `handler=~"/.*"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matchers := parseMatchers(t, tt.matchers)

			// Query test series with and without matcher reducer planner
			seriesWithMatcherReduction := querySeriesWithPlanner(t, db, matchers, MatcherReducerPlanner{})
			seriesWithoutMatcherReduction := querySeriesWithPlanner(t, db, matchers, NoopPlanner{})
			assert.Equal(t, len(seriesWithoutMatcherReduction), len(seriesWithMatcherReduction))

			// Both approaches should return the same series
			sort.Slice(seriesWithMatcherReduction, func(i, j int) bool {
				return labels.Compare(seriesWithMatcherReduction[i], seriesWithMatcherReduction[j]) < 0
			})
			sort.Slice(seriesWithoutMatcherReduction, func(i, j int) bool {
				return labels.Compare(seriesWithoutMatcherReduction[i], seriesWithoutMatcherReduction[j]) < 0
			})
			assert.Equal(t, seriesWithoutMatcherReduction, seriesWithMatcherReduction)
		})
	}
}

// querySeriesWithMatchers queries series from TSDB using the provided matchers,
// optionally applying MatcherReducerPlanner optimization
func querySeriesWithPlanner(t *testing.T, db *tsdb.DB, matchers []*labels.Matcher, planner index.LookupPlanner) []labels.Labels {
	ctx := context.Background()

	head := db.Head()
	indexReader, err := head.Index()
	require.NoError(t, err)
	defer indexReader.Close()
	inPlan := concreteLookupPlan{indexMatchers: matchers}
	outPlan, err := planner.PlanIndexLookup(ctx, inPlan, 0, 0)
	require.NoError(t, err)

	postings, err := tsdb.PostingsForMatchers(ctx, indexReader, outPlan.IndexMatchers()...)
	require.NoError(t, err)

	// Collect all series from postings
	var outSeries []labels.Labels
	builder := labels.NewScratchBuilder(len(outPlan.IndexMatchers()))
	for postings.Next() {
		var ls labels.Labels
		err = indexReader.Series(postings.At(), &builder, nil)
		require.NoError(t, err)
		ls = builder.Labels()
		outSeries = append(outSeries, ls)
	}
	require.NoError(t, postings.Err())

	return outSeries
}

func assertEqualMatchers(t *testing.T, expected, actual []*labels.Matcher) {
	actualMatchers := make(map[string]*labels.Matcher, len(actual))
	for _, m := range actual {
		actualMatchers[m.String()] = m
	}
	assert.Equal(t, len(expected), len(actual))
	for _, m := range expected {
		actualMatcher := actualMatchers[m.String()]
		assert.NotNil(t, actualMatcher)
		switch m.Type {
		case labels.MatchEqual, labels.MatchNotEqual:
			assert.Equal(t, m, actualMatcher)
		case labels.MatchRegexp, labels.MatchNotRegexp:
			// We can't rely on the FastRegexMatcher pointer to be the same for all regex matchers
			assert.Equal(t, m.Type, actualMatcher.Type)
			assert.Equal(t, m.Name, actualMatcher.Name)
			assert.Equal(t, m.Value, actualMatcher.Value)
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
