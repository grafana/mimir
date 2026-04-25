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
