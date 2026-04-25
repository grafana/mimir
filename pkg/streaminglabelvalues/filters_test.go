package streaminglabelvalues

import (
	"strings"
	"testing"

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
