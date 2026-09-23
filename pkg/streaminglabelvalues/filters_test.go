// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/e8e25eb09e41bf295e0c9e847cd27cf9016a553a/web/api/v1/search_filters_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaminglabelvalues

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterContains(t *testing.T) {
	// Score for "ber" in "kubernetes": idx=2 ("k-u-ber-..."), maxIdx=10-3=7, score = 1 - 0.9*2/7 = 0.74285...
	const berInKubernetes = 1.0 - 0.9*2.0/7.0
	// Score for "b" in "abc": idx=1, maxIdx=3-1=2, score = 1 - 0.9*1/2 = 0.55.
	const bInAbc = 1.0 - 0.9*1.0/2.0
	// Score for "x" at the very end of "abcdex": idx=5, maxIdx=6-1=5, score = 1 - 0.9 = 0.1.
	const xLastInAbcdex = 1.0 - 0.9*5.0/5.0

	tests := []struct {
		name          string
		term          string
		caseSensitive bool
		value         string
		wantAccepted  bool
		wantScore     float64
	}{
		{name: "prefix scores 1.0", term: "kube", caseSensitive: true, value: "kubernetes", wantAccepted: true, wantScore: 1.0},
		{name: "non-prefix substring scores by position decay", term: "ber", caseSensitive: true, value: "kubernetes", wantAccepted: true, wantScore: berInKubernetes},
		{name: "early non-prefix scores higher than late", term: "b", caseSensitive: true, value: "abc", wantAccepted: true, wantScore: bInAbc},
		{name: "latest possible position scores 0.1", term: "x", caseSensitive: true, value: "abcdex", wantAccepted: true, wantScore: xLastInAbcdex},
		{name: "case-sensitive miss on case difference", term: "Kube", caseSensitive: true, value: "kubernetes", wantAccepted: false, wantScore: 0},
		{name: "case-insensitive matches across cases", term: "Kube", caseSensitive: false, value: "kubernetes", wantAccepted: true, wantScore: 1.0},
		{name: "non-substring rejected", term: "xyz", caseSensitive: true, value: "kubernetes", wantAccepted: false, wantScore: 0},
		{name: "empty value rejected", term: "k", caseSensitive: true, value: "", wantAccepted: false, wantScore: 0},
		{name: "term equals value scores 1.0", term: "metric", caseSensitive: true, value: "metric", wantAccepted: true, wantScore: 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFilterContains(tt.term, tt.caseSensitive, false)
			require.NoError(t, err)
			gotAccepted, gotScore := f.Accept(tt.value)
			assert.Equal(t, tt.wantAccepted, gotAccepted)
			assert.InDelta(t, tt.wantScore, gotScore, 1e-9)
		})
	}
}

func TestFilterContainsRejectsEmptyTerm(t *testing.T) {
	_, err := NewFilterContains("", true, false)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "empty")
}

func TestFilterJaroAcceptsAboveThreshold(t *testing.T) {
	tests := []struct {
		name          string
		term          string
		threshold     float64
		caseSensitive bool
		value         string
		want          bool
	}{
		{name: "exact match", term: "metric", threshold: 0.9, caseSensitive: true, value: "metric", want: true},
		{name: "very close", term: "metric", threshold: 0.8, caseSensitive: true, value: "metricc", want: true},
		{name: "very different", term: "metric", threshold: 0.8, caseSensitive: true, value: "totally_unrelated", want: false},
		{name: "threshold zero accepts almost anything", term: "metric", threshold: 0, caseSensitive: true, value: "x", want: true},
		{name: "case-insensitive matches", term: "Metric", threshold: 0.9, caseSensitive: false, value: "metric", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFilterJaro(tt.term, tt.threshold, tt.caseSensitive)
			require.NoError(t, err)
			gotAccepted, gotScore := f.Accept(tt.value)
			assert.Equal(t, tt.want, gotAccepted, "score=%v", gotScore)
			if gotAccepted {
				assert.GreaterOrEqual(t, gotScore, tt.threshold)
				assert.LessOrEqual(t, gotScore, 1.0)
			}
		})
	}
}

func TestFilterJaroValidates(t *testing.T) {
	_, err := NewFilterJaro("", 0.5, true)
	require.Error(t, err, "empty term should be rejected")
	_, err = NewFilterJaro("metric", -0.1, true)
	require.Error(t, err, "negative threshold should be rejected")
	_, err = NewFilterJaro("metric", 1.5, true)
	require.Error(t, err, "threshold > 1 should be rejected")
}

func TestFilterJaroExactMatchIsOne(t *testing.T) {
	f, err := NewFilterJaro("metric", 0.5, true)
	require.NoError(t, err)
	accepted, score := f.Accept("metric")
	require.True(t, accepted)
	assert.InDelta(t, 1.0, score, 1e-9)
}

func TestFilterSubsequenceAcceptsAboveThreshold(t *testing.T) {
	tests := []struct {
		name          string
		pattern       string
		threshold     float64
		caseSensitive bool
		value         string
		want          bool
		wantScore     float64
	}{
		{name: "prefix scores 1.0 regardless of raw score", pattern: "kub", threshold: 0.5, caseSensitive: true, value: "kubernetes", want: true, wantScore: 1.0},
		{name: "exact match scores 1.0", pattern: "abc", threshold: 0.5, caseSensitive: true, value: "abc", want: true, wantScore: 1.0},
		{name: "non-prefix subseq with low threshold", pattern: "ks", threshold: 0.0, caseSensitive: true, value: "kubernetes", want: true},
		{name: "non-subsequence rejected", pattern: "xyz", threshold: 0.0, caseSensitive: true, value: "kubernetes", want: false},
		{name: "case-insensitive matches", pattern: "KUB", threshold: 0.5, caseSensitive: false, value: "kubernetes", want: true, wantScore: 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFilterSubsequence(tt.pattern, tt.threshold, tt.caseSensitive)
			require.NoError(t, err)
			gotAccepted, gotScore := f.Accept(tt.value)
			assert.Equal(t, tt.want, gotAccepted, "score=%v", gotScore)
			if gotAccepted {
				assert.LessOrEqual(t, gotScore, 1.0)
				assert.GreaterOrEqual(t, gotScore, 0.0)
				if tt.wantScore != 0 {
					assert.InDelta(t, tt.wantScore, gotScore, 1e-9)
				}
			}
		})
	}
}

func TestFilterSubsequenceValidates(t *testing.T) {
	_, err := NewFilterSubsequence("", 0.5, true)
	require.Error(t, err)
	_, err = NewFilterSubsequence("a", -0.1, true)
	require.Error(t, err)
	_, err = NewFilterSubsequence("a", 1.5, true)
	require.Error(t, err)
}

// scoreFilter is a test-only Filter that returns a fixed score.
type scoreFilter struct {
	accepted bool
	score    float64
}

func (s scoreFilter) Accept(string) (bool, float64) { return s.accepted, s.score }

func TestFilterOr(t *testing.T) {
	a := scoreFilter{accepted: false, score: 0}
	b := scoreFilter{accepted: true, score: 0.5}
	c := scoreFilter{accepted: true, score: 0.9}

	or := newFilterOr(a, b, c)
	require.NotNil(t, or)
	accepted, score := or.Accept("anything")
	assert.True(t, accepted)
	assert.InDelta(t, 0.9, score, 1e-9, "filterOr returns max score across accepting children")
}

func TestFilterOrAllReject(t *testing.T) {
	a := scoreFilter{accepted: false, score: 0}
	b := scoreFilter{accepted: false, score: 0}
	or := newFilterOr(a, b)
	accepted, score := or.Accept("anything")
	assert.False(t, accepted)
	assert.Equal(t, 0.0, score)
}

func TestFilterOrShortCircuitsAtPerfectMatch(t *testing.T) {
	called := 0
	tracking := func(score float64) storage.Filter {
		return countingFilter{counter: &called, accepted: true, score: score}
	}
	or := newFilterOr(tracking(1.0), tracking(0.5))
	accepted, score := or.Accept("anything")
	assert.True(t, accepted)
	assert.Equal(t, 1.0, score)
	assert.Equal(t, 1, called, "second filter must not be invoked once a child returns 1.0")
}

func TestBuildFilterExpressionSupportsNestedGroupsWithGCXCorpusCandidates(t *testing.T) {
	for _, test := range []struct {
		name       string
		expression string
		values     map[string]bool
	}{
		{
			name:       "cortex AND grouped alternatives",
			expression: "cortex AND (rule_evaluation_failures OR cache_shard)",
			values: map[string]bool{
				"cortex_prometheus_rule_evaluation_failures_total":  true,
				"cortex_cache_shard_00000_operations_total":         true,
				"loki_prometheus_rule_evaluation_failures_total":    false,
				"envoy_cache_shard_00004_evaluation_failures_total": false,
			},
		},
		{
			name:       "grouped alternatives AND rule evaluation failures",
			expression: "(cortex OR loki) AND rule_evaluation_failures",
			values: map[string]bool{
				"cortex_prometheus_rule_evaluation_failures_total":  true,
				"loki_prometheus_rule_evaluation_failures_total":    true,
				"cortex_cache_shard_00000_operations_total":         false,
				"envoy_cache_shard_00004_evaluation_failures_total": false,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			params, err := NewExpressionParams(test.expression, true, FuzzAlgSubsequence, 0)
			require.NoError(t, err)
			filter, err := BuildFilter(params)
			require.NoError(t, err)
			for value, want := range test.values {
				t.Run(value, func(t *testing.T) {
					accepted, _ := filter.Accept(value)
					assert.Equal(t, want, accepted)
				})
			}
		})
	}
}

// TestExpressionScoring pins the mean-of-leaves relevance scoring for
// expression-based filters against a small realistic metric-name corpus: an
// AND's score is the mean of the scores of the leaves the match actually
// depended on (not just its weakest term), NOT contributes no score, and an
// OR reports whichever branch scored higher.
func TestExpressionScoring(t *testing.T) {
	corpus := []string{
		"cortex_frontend_query_result_cache_attempted_total",
		"cortex_query_frontend_result_cache_requests_total",
		"cortex_frontend_query_result_cache_skipped_total",
		"cortex_frontend_query_result_cache_hits_total",
		"cortex_frontend_query_resul_cache_hits_total",
	}

	runFilter := func(t *testing.T, params *Params, matches map[string]float64) {
		filter, err := BuildFilter(params)
		require.NoError(t, err)
		for _, metric := range corpus {
			accepted, score := filter.Accept(metric)
			if accepted {
				matches[metric] = score
			}
		}
	}

	newExprParams := func(t *testing.T, expression string) *Params {
		t.Helper()
		params, err := NewExpressionParams(expression, false, FuzzAlgJaroWinkler, 70)
		require.NoError(t, err)
		return params
	}

	for _, test := range []struct {
		name       string
		expression string
		want       map[string]float64
	}{
		{
			name:       "cortex",
			expression: "cortex",
			// Both metrics have cortex in the same position.
			want: map[string]float64{
				"cortex_frontend_query_result_cache_attempted_total": 1,
				"cortex_query_frontend_result_cache_requests_total":  1,
			},
		},
		{
			name:       "ctx",
			expression: "ctx",
			// Both metrics have the same spelling mistake in the same position.
			want: map[string]float64{
				"cortex_frontend_query_result_cache_attempted_total": 0.718,
				"cortex_query_frontend_result_cache_requests_total":  0.718,
			},
		},
		{
			name:       "query",
			expression: "query",
			// Both metrics have query, but in different positions so the one
			// with query further to the left scores better.
			want: map[string]float64{
				"cortex_frontend_query_result_cache_attempted_total": 0.680,
				"cortex_query_frontend_result_cache_requests_total":  0.857,
			},
		},
		{
			name:       "cortex and query",
			expression: "cortex and query",
			// Mean of the two single-term cases above: mean(1, 0.680) and
			// mean(1, 0.857).
			want: map[string]float64{
				"cortex_frontend_query_result_cache_attempted_total": 0.840,
				"cortex_query_frontend_result_cache_requests_total":  0.928,
			},
		},
		{
			name:       "cortex and query and not hits",
			expression: "cortex and query and not hits",
			// Adding an unscored NOT branch must not change the mean.
			want: map[string]float64{
				"cortex_frontend_query_result_cache_attempted_total": 0.840,
				"cortex_query_frontend_result_cache_requests_total":  0.928,
			},
		},
		{
			name:       "ctx or hits",
			expression: "ctx or hits",
			// OR reports the higher-scoring branch per metric.
			want: map[string]float64{
				"cortex_frontend_query_result_cache_attempted_total": 0.718,
				"cortex_frontend_query_result_cache_hits_total":      0.720,
				"cortex_frontend_query_resul_cache_hits_total":       0.720,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			matches := map[string]float64{}
			runFilter(t, newExprParams(t, test.expression), matches)
			for metric, score := range test.want {
				assert.Equal(t, fmt.Sprintf("%.3f", score), fmt.Sprintf("%.3f", matches[metric]), metric)
			}
		})
	}
}

type countingFilter struct {
	counter  *int
	accepted bool
	score    float64
}

func (c countingFilter) Accept(string) (bool, float64) {
	*c.counter++
	return c.accepted, c.score
}

func TestBuildFilterEmptyReturnsNil(t *testing.T) {
	f, err := BuildFilter(&Params{})
	require.NoError(t, err)
	assert.Nil(t, f, "empty Terms must yield a nil Filter")

	f, err = BuildFilter(nil)
	require.NoError(t, err)
	assert.Nil(t, f)
}

func TestBuildFilterSubsequenceSingleTermPrefixScoresOne(t *testing.T) {
	// FuzzAlgSubsequence: BuildFilter returns just a FilterSubsequence — no
	// substring fallback. The subsequence filter's prefix override still
	// scores prefix matches at 1.0.
	f, err := BuildFilter(&Params{
		Terms:         []string{"kub"},
		CaseSensitive: true,
		FuzzAlg:       FuzzAlgSubsequence,
		FuzzThreshold: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, f)
	accepted, score := f.Accept("kubernetes")
	assert.True(t, accepted)
	assert.InDelta(t, 1.0, score, 1e-9)
}

func TestBuildFilterJaroWinklerThresholdZeroIsSubstringOnly(t *testing.T) {
	// FuzzAlgJaroWinkler with threshold 0 should yield a substring-only
	// filter (mirrors Prometheus PR #18573 buildSearchFilter, which omits the
	// fuzzy filter when threshold == 0).
	f, err := BuildFilter(&Params{
		Terms:         []string{"metric"},
		CaseSensitive: true,
		FuzzAlg:       FuzzAlgJaroWinkler,
		FuzzThreshold: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, f)
	// Substring match is accepted.
	accepted, _ := f.Accept("metric_a")
	assert.True(t, accepted, "substring should be accepted")
	// Non-substring is rejected because there's no fuzzy fallback.
	accepted, _ = f.Accept("totally_unrelated")
	assert.False(t, accepted, "without fuzzy fallback at threshold 0, non-substring must be rejected")
}

func TestBuildFilterJaroWinklerWithThresholdComposesFallback(t *testing.T) {
	// FuzzAlgJaroWinkler with threshold > 0: substring tried first, fuzzy
	// fallback for non-substring values that score above threshold.
	f, err := BuildFilter(&Params{
		Terms:         []string{"metric"},
		CaseSensitive: true,
		FuzzAlg:       FuzzAlgJaroWinkler,
		FuzzThreshold: 80,
	})
	require.NoError(t, err)
	require.NotNil(t, f)
	// Prefix substring scores 1.0 via the substring path.
	accepted, score := f.Accept("metric")
	assert.True(t, accepted)
	assert.InDelta(t, 1.0, score, 1e-9)
}

func TestBuildFilterMultipleTermsORed(t *testing.T) {
	f, err := BuildFilter(&Params{
		Terms:         []string{"foo", "bar"},
		CaseSensitive: true,
		FuzzAlg:       FuzzAlgSubsequence,
	})
	require.NoError(t, err)
	accepted, _ := f.Accept("foobaz")
	assert.True(t, accepted, "foo term should match")
	accepted, _ = f.Accept("barbaz")
	assert.True(t, accepted, "bar term should match")
	accepted, _ = f.Accept("xyz")
	assert.False(t, accepted)
}

func TestBuildFilterExpressionExecutesNot(t *testing.T) {
	params, err := NewExpressionParams("foo AND NOT old", true, FuzzAlgSubsequence, 0)
	require.NoError(t, err)
	filter, err := BuildFilter(params)
	require.NoError(t, err)
	require.NotNil(t, filter)

	accepted, score := filter.Accept("foo_new")
	assert.True(t, accepted)
	assert.InDelta(t, 1, score, 1e-9)

	accepted, score = filter.Accept("foo_old")
	assert.False(t, accepted)
	assert.Zero(t, score)
}

func TestBuildFilterSharedExpressionParams(t *testing.T) {
	params, err := NewExpressionParams("cortex AND NOT old", true, FuzzAlgSubsequence, 0)
	require.NoError(t, err)

	const workers = 32
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := validateAndBuildSharedExpressionParams(params); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func validateAndBuildSharedExpressionParams(params *Params) error {
	if err := params.validate(); err != nil {
		return err
	}
	filter, err := BuildFilter(params)
	if err != nil {
		return err
	}
	accepted, _ := filter.Accept("cortex_new")
	if !accepted {
		return fmt.Errorf("cortex_new was rejected")
	}
	accepted, _ = filter.Accept("cortex_old")
	if accepted {
		return fmt.Errorf("cortex_old was accepted")
	}
	return nil
}

func TestBuildFilterExpressionNotUsesLiteralSubstringMatching(t *testing.T) {
	params, err := NewExpressionParams("cortex AND NOT test", true, FuzzAlgSubsequence, 0)
	require.NoError(t, err)
	filter, err := BuildFilter(params)
	require.NoError(t, err)

	for _, value := range []string{
		"cortex_request_duration_seconds",
		"cortex_ruler_rule_evaluation_failures_total",
		"cortex_distributor_received_samples_total",
		"cortex_compactor_runs_started_total",
		"cortex_bucket_store_series_hit_count",
	} {
		t.Run(value, func(t *testing.T) {
			accepted, _ := filter.Accept(value)
			assert.True(t, accepted, "NOT must exclude literal substring matches, not fuzzy subsequence matches")
		})
	}

	accepted, score := filter.Accept("cortex_test_requests_total")
	assert.False(t, accepted)
	assert.Zero(t, score)
}

func TestBuildFilterRejectsTermsWithExpression(t *testing.T) {
	params, err := NewExpressionParams("old", true, FuzzAlgSubsequence, 0)
	require.NoError(t, err)
	params.Terms = []string{"foo"}

	filter, err := BuildFilter(params)
	require.EqualError(t, err, "invalid search parameters: search terms and search expression are mutually exclusive")
	require.ErrorIs(t, err, ErrTermsAndExpression)
	assert.Nil(t, filter)
}

func TestBuildFilterDividesThresholdBy100(t *testing.T) {
	// FuzzThreshold=80 should map to internal 0.8.
	// Pick a value that scores > 0.8 against "metric" with Jaro-Winkler.
	f, err := BuildFilter(&Params{
		Terms:         []string{"metric"},
		CaseSensitive: true,
		FuzzAlg:       FuzzAlgJaroWinkler,
		FuzzThreshold: 80,
	})
	require.NoError(t, err)
	require.NotNil(t, f)
	accepted, score := f.Accept("metric")
	assert.True(t, accepted)
	assert.InDelta(t, 1.0, score, 1e-9)
}

func TestBuildFilterCaseInsensitiveWrapsAtORRoot(t *testing.T) {
	for _, test := range []struct {
		name       string
		terms      []string
		expression string
		accepted   string
		rejected   string
	}{
		{
			name:     "legacy terms",
			terms:    []string{"FOO", "Bar"},
			accepted: "FOOBAR",
		},
		{
			name:       "expression with NOT",
			expression: "FOO AND NOT OLD",
			accepted:   "Foo_New",
			rejected:   "FOO_OLD",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				params *Params
				err    error
			)
			if test.expression != "" {
				params, err = NewExpressionParams(test.expression, false, FuzzAlgSubsequence, 0)
			} else {
				params, err = NewParams(test.terms, false, FuzzAlgSubsequence, 0)
			}
			require.NoError(t, err)
			// CaseSensitive=false must wrap the root once so every leaf sees the
			// same pre-lowered candidate, including leaves below NOT.
			filter, err := BuildFilter(params)
			require.NoError(t, err)
			require.NotNil(t, filter)
			_, ok := filter.(*caseFoldingFilter)
			require.True(t, ok)

			accepted, _ := filter.Accept(test.accepted)
			assert.True(t, accepted)
			if test.rejected != "" {
				accepted, score := filter.Accept(test.rejected)
				assert.False(t, accepted)
				assert.Zero(t, score)
			}
		})
	}
}

func TestBuildFilterCaseSensitiveReturnsLeafDirectly(t *testing.T) {
	// CaseSensitive=true must NOT wrap in caseFoldingFilter — the inner filter
	// surfaces directly so we don't pay an unnecessary ToLower per Accept.
	f, err := BuildFilter(&Params{
		Terms:         []string{"foo"},
		CaseSensitive: true,
		FuzzAlg:       FuzzAlgSubsequence,
	})
	require.NoError(t, err)
	require.NotNil(t, f)
	_, ok := f.(*caseFoldingFilter)
	assert.False(t, ok, "case-sensitive BuildFilter must not wrap in caseFoldingFilter")
}

// recordingFilter captures every value passed to Accept so a test can assert
// (a) how many Accept calls reached the wrapped child and (b) what value
// the child saw — proving caseFoldingFilter folds exactly once at the OR
// root and the child sees pre-lowered input. Score is configurable so tests
// can avoid filterOr's score==1.0 short-circuit when they need every leaf
// to be invoked.
type recordingFilter struct {
	score float64
	seen  []string
}

func (r *recordingFilter) Accept(value string) (bool, float64) {
	r.seen = append(r.seen, value)
	return r.score > 0, r.score
}

func TestCaseFoldingFilterFoldsOncePerAcceptAndPassesLoweredValue(t *testing.T) {
	rec := &recordingFilter{score: 1.0}
	f := &caseFoldingFilter{inner: rec}

	for _, v := range []string{"FOO", "Bar", "alreadyLower"} {
		_, _ = f.Accept(v)
	}
	require.Equal(t, []string{"foo", "bar", "alreadylower"}, rec.seen,
		"caseFoldingFilter must hand each value, lowered, to the inner filter exactly once")
}

func TestCaseFoldingFilterFeedsCompositeChildrenLoweredValue(t *testing.T) {
	// Use score 0.5 so OR does not short-circuit before both children receive
	// the candidate.
	rec1 := &recordingFilter{score: 0.5}
	rec2 := &recordingFilter{score: 0.5}
	root := &caseFoldingFilter{inner: newFilterOr(rec1, rec2)}

	accepted, _ := root.Accept("MIXEDCase_Value")
	assert.True(t, accepted)
	assert.Equal(t, []string{"mixedcase_value"}, rec1.seen)
	assert.Equal(t, []string{"mixedcase_value"}, rec2.seen,
		"each child must see the value lowered once at the root")
}

var benchmarkFilterResult struct {
	accepted bool
	score    float64
}

// BenchmarkBuildFilterAccept covers the composition shapes and worst-case
// matcher path used while scanning label values. Non-matching candidates are
// deliberate: most values in a block reach every OR leaf, and Jaro-Winkler's
// fallback cost is hidden when FilterContains accepts first.
func BenchmarkBuildFilterAccept(b *testing.B) {
	terms := make([]string, 32)
	for i := range terms {
		terms[i] = fmt.Sprintf("needle_%02d", i)
	}

	notExpression := "anchor"
	for i := 0; i < 16; i++ {
		notExpression += fmt.Sprintf(" AND NOT obsolete_%02d", i)
	}

	shortNonMatch := strings.Repeat("z", 80)
	longNonMatch := strings.Repeat("z", 2040)
	for _, test := range []struct {
		name          string
		terms         []string
		expression    string
		alg           FuzzAlg
		threshold     int
		caseSensitive bool
		candidate     string
	}{
		{name: "legacy/subsequence/32-term-OR/non-match", terms: terms, alg: FuzzAlgSubsequence, caseSensitive: true, candidate: shortNonMatch},
		{name: "expression/subsequence/32-term-OR/non-match", expression: strings.Join(terms, " OR "), alg: FuzzAlgSubsequence, caseSensitive: true, candidate: shortNonMatch},
		{name: "expression/subsequence/32-term-AND/first-term-rejects", expression: strings.Join(terms, " AND "), alg: FuzzAlgSubsequence, caseSensitive: true, candidate: shortNonMatch},
		{name: "expression/subsequence/NOT/literal-misses", expression: notExpression, alg: FuzzAlgSubsequence, caseSensitive: true, candidate: "anchor_current_metric"},
		{name: "legacy/jaro-winkler/32-term-OR/2KB-non-match", terms: terms, alg: FuzzAlgJaroWinkler, threshold: 80, caseSensitive: true, candidate: longNonMatch},
		{name: "expression/jaro-winkler/32-term-OR/2KB-non-match", expression: strings.Join(terms, " OR "), alg: FuzzAlgJaroWinkler, threshold: 80, caseSensitive: true, candidate: longNonMatch},
		{name: "legacy/subsequence/32-term-OR/case-insensitive-lowercase", terms: terms, alg: FuzzAlgSubsequence, candidate: shortNonMatch},
		{name: "legacy/subsequence/32-term-OR/case-insensitive-mixed-case", terms: terms, alg: FuzzAlgSubsequence, candidate: strings.Repeat("zZ", 40)},
	} {
		b.Run(test.name, func(b *testing.B) {
			var (
				params *Params
				err    error
			)
			if test.expression != "" {
				params, err = NewExpressionParams(test.expression, test.caseSensitive, test.alg, test.threshold)
			} else {
				params, err = NewParams(test.terms, test.caseSensitive, test.alg, test.threshold)
			}
			require.NoError(b, err)
			benchmarkBuildFilterAccept(b, params, test.candidate)
		})
	}
}

func benchmarkBuildFilterAccept(b *testing.B, params *Params, candidate string) {
	filter, err := BuildFilter(params)
	require.NoError(b, err)

	// Warm matchers that initialise term state lazily so benchmark allocations
	// reflect the steady-state per-candidate scan cost.
	_, _ = filter.Accept(candidate)
	b.SetBytes(int64(len(candidate)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkFilterResult.accepted, benchmarkFilterResult.score = filter.Accept(candidate)
	}
}

func TestApplyResumeAfterPassesThroughWhenEmpty(t *testing.T) {
	inner := &scoreFilter{accepted: true, score: 0.5}
	got := ApplyResumeAfter(inner, "", storage.OrderByValueAsc)
	assert.Same(t, storage.Filter(inner), got)
}

func TestApplyResumeAfterAscendingRejectsAtAndBeforeThreshold(t *testing.T) {
	inner := scoreFilter{accepted: true, score: 1.0}
	f := ApplyResumeAfter(inner, "m", storage.OrderByValueAsc)

	accepted, _ := f.Accept("a")
	assert.False(t, accepted, "before threshold is rejected")

	accepted, _ = f.Accept("m")
	assert.False(t, accepted, "exact match on the last-seen value is rejected")

	accepted, score := f.Accept("z")
	assert.True(t, accepted, "past the threshold is accepted")
	assert.Equal(t, 1.0, score)
}

func TestApplyResumeAfterDescendingRejectsAtAndAfterThreshold(t *testing.T) {
	inner := scoreFilter{accepted: true, score: 1.0}
	f := ApplyResumeAfter(inner, "m", storage.OrderByValueDesc)

	accepted, _ := f.Accept("z")
	assert.False(t, accepted, "before threshold (in descending order) is rejected")

	accepted, _ = f.Accept("m")
	assert.False(t, accepted, "exact match on the last-seen value is rejected")

	accepted, score := f.Accept("a")
	assert.True(t, accepted, "past the threshold is accepted")
	assert.Equal(t, 1.0, score)
}

func TestApplyResumeAfterComposesWithInnerRejection(t *testing.T) {
	inner := scoreFilter{accepted: false, score: 0}
	f := ApplyResumeAfter(inner, "a", storage.OrderByValueAsc)
	accepted, score := f.Accept("z")
	assert.False(t, accepted, "past the threshold but the inner filter itself rejects")
	assert.Zero(t, score)
}

func TestApplyResumeAfterHandlesNilInner(t *testing.T) {
	// BuildFilter returns a nil storage.Filter when Params has no terms/expression
	// (accept everything, score 1.0). ApplyResumeAfter must not panic on that nil.
	f := ApplyResumeAfter(nil, "m", storage.OrderByValueAsc)
	accepted, score := f.Accept("a")
	assert.False(t, accepted)
	assert.Zero(t, score)
	accepted, score = f.Accept("z")
	assert.True(t, accepted)
	assert.Equal(t, 1.0, score)
}
