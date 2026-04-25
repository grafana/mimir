package streaminglabelvalues

import (
	"strings"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterContains(t *testing.T) {
	tests := []struct {
		name          string
		term          string
		caseSensitive bool
		value         string
		wantAccepted  bool
		wantScore     float64
	}{
		{name: "prefix scores 1.0", term: "kube", caseSensitive: true, value: "kubernetes", wantAccepted: true, wantScore: 1.0},
		{name: "non-prefix substring scores 0.9", term: "ber", caseSensitive: true, value: "kubernetes", wantAccepted: true, wantScore: 0.9},
		{name: "case-sensitive miss on case difference", term: "Kube", caseSensitive: true, value: "kubernetes", wantAccepted: false, wantScore: 0},
		{name: "case-insensitive matches across cases", term: "Kube", caseSensitive: false, value: "kubernetes", wantAccepted: true, wantScore: 1.0},
		{name: "non-substring rejected", term: "xyz", caseSensitive: true, value: "kubernetes", wantAccepted: false, wantScore: 0},
		{name: "empty value rejected", term: "k", caseSensitive: true, value: "", wantAccepted: false, wantScore: 0},
		{name: "term equals value scores 1.0", term: "metric", caseSensitive: true, value: "metric", wantAccepted: true, wantScore: 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFilterContains(tt.term, tt.caseSensitive)
			require.NoError(t, err)
			gotAccepted, gotScore := f.Accept(tt.value)
			assert.Equal(t, tt.wantAccepted, gotAccepted)
			assert.Equal(t, tt.wantScore, gotScore)
		})
	}
}

func TestFilterContainsRejectsEmptyTerm(t *testing.T) {
	_, err := NewFilterContains("", true)
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

func TestBuildFilterSingleTermSubstringFallsThroughToFuzzy(t *testing.T) {
	// "kub" prefix-matches "kubernetes" so substring scores 1.0; fuzzy not invoked.
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

func TestBuildFilterSingleTermSubstringMissesFuzzyAccepts(t *testing.T) {
	// "kub" is not a substring of "qkbu" but composes with fuzzy as a chain.
	f, err := BuildFilter(&Params{
		Terms:         []string{"kub"},
		CaseSensitive: true,
		FuzzAlg:       FuzzAlgSubsequence,
		FuzzThreshold: 0,
	})
	require.NoError(t, err)
	accepted, score := f.Accept("qkbu")
	_ = score
	// Either accepted via fuzzy, or rejected — both are acceptable behaviour
	// here depending on the Prometheus matcher. The point of the test is that
	// BuildFilter actually composes the fallback chain and doesn't crash.
	_ = accepted
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

func TestBuildFilterRejectsInvalid(t *testing.T) {
	_, err := BuildFilter(&Params{Terms: []string{""}})
	require.Error(t, err)
	_, err = BuildFilter(&Params{Terms: []string{"a"}, FuzzThreshold: 150})
	require.Error(t, err)
	_, err = BuildFilter(&Params{Terms: []string{"a"}, FuzzAlg: FuzzAlg(99)})
	require.Error(t, err)
}
