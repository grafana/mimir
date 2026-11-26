// SPDX-License-Identifier: AGPL-3.0-only

package streaminglabelvalues

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParamsAcceptsValid(t *testing.T) {
	tests := []struct {
		name string
		p    *Params
	}{
		{name: "empty terms accepted", p: &Params{}},
		{name: "single term", p: &Params{Terms: []string{"foo"}}},
		{name: "multi term", p: &Params{Terms: []string{"foo", "bar"}}},
		{name: "case sensitive", p: &Params{Terms: []string{"FOO"}, CaseSensitive: true}},
		{name: "jaro winkler", p: &Params{Terms: []string{"foo"}, FuzzAlg: FuzzAlgJaroWinkler}},
		{name: "substring left", p: &Params{Terms: []string{"foo"}, FuzzAlg: FuzzAlgSubstringLeft}},
		{name: "substring", p: &Params{Terms: []string{"foo"}, FuzzAlg: FuzzAlgSubstring}},
		{name: "threshold low edge", p: &Params{Terms: []string{"foo"}, FuzzThreshold: 0}},
		{name: "threshold high edge", p: &Params{Terms: []string{"foo"}, FuzzThreshold: 100}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewParams(tc.p.Terms, tc.p.CaseSensitive, tc.p.FuzzAlg, tc.p.FuzzThreshold)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.p, got)
		})
	}
}

func TestNewParamsRejectsInvalid(t *testing.T) {
	tests := []struct {
		name      string
		terms     []string
		caseSens  bool
		alg       FuzzAlg
		threshold int
		wantMsg   string
	}{
		{name: "empty term in middle", terms: []string{"a", "", "b"}, wantMsg: "search term 1 is empty"},
		{name: "empty single term", terms: []string{""}, wantMsg: "search term 0 is empty"},
		{name: "threshold above 100", terms: []string{"a"}, threshold: 150, wantMsg: "fuzz threshold 150 out of [0,100]"},
		{name: "threshold below 0", terms: []string{"a"}, threshold: -1, wantMsg: "fuzz threshold -1 out of [0,100]"},
		{name: "unknown fuzz alg", terms: []string{"a"}, alg: FuzzAlg(99), wantMsg: "unknown fuzz algorithm 99"},
		// Even with no terms (which would otherwise yield a nil filter at
		// BuildFilter), out-of-range fields are caught here so wire input
		// cannot reach BuildFilter in an invalid state.
		{name: "bad threshold no terms", threshold: 150, wantMsg: "fuzz threshold 150 out of [0,100]"},
		{name: "bad alg no terms", alg: FuzzAlg(99), wantMsg: "unknown fuzz algorithm 99"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewParams(tc.terms, tc.caseSens, tc.alg, tc.threshold)
			require.Error(t, err)
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

func TestNewExpressionParamsAcceptsValid(t *testing.T) {
	tests := []struct {
		name          string
		expression    string
		caseSensitive bool
		alg           FuzzAlg
		threshold     int
	}{
		{name: "single term", expression: "foo"},
		{name: "AND with NOT", expression: "foo AND NOT bar", alg: FuzzAlgSubstring},
		{name: "OR", expression: "foo OR bar", caseSensitive: true, alg: FuzzAlgJaroWinkler, threshold: 80},
		{name: "quoted term", expression: `"foo bar" AND baz`, alg: FuzzAlgSubstringLeft},
		{name: "threshold high edge", expression: "foo", threshold: 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewExpressionParams(tc.expression, tc.caseSensitive, tc.alg, tc.threshold)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Empty(t, got.Terms)
			assert.NotNil(t, got.expressionExpr)
			assert.Equal(t, tc.caseSensitive, got.CaseSensitive)
			assert.Equal(t, tc.alg, got.FuzzAlg)
			assert.Equal(t, tc.threshold, got.FuzzThreshold)
		})
	}
}

func TestNewExpressionParamsRejectsInvalid(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		alg        FuzzAlg
		threshold  int
		wantErr    string
	}{
		{name: "empty", expression: "", wantErr: "search expression is empty"},
		{name: "blank", expression: "   ", wantErr: "search expression is empty"},
		{name: "dangling operator", expression: "foo AND", wantErr: "search expression: expected term or (, got end of expression at position 7"},
		{name: "unclosed parenthesis", expression: "(foo", wantErr: "search expression: expected ), got end of expression at position 4"},
		{name: "unterminated quote", expression: `"foo`, wantErr: "search expression: unterminated quoted term at position 0"},
		{name: "only a negated term", expression: "NOT foo", wantErr: "search expression: every accepting path must require a positive term"},
		{name: "negated alternative", expression: "foo OR NOT bar", wantErr: "search expression: every accepting path must require a positive term"},
		{name: "too long", expression: strings.Repeat("a", 4097), wantErr: "search expression: length 4097 exceeds maximum of 4096 bytes"},
		{name: "too many terms", expression: strings.Repeat("a OR ", 32) + "a", wantErr: "search expression: term count exceeds maximum of 32 at position 160"},
		{name: "too deeply nested", expression: strings.Repeat("(", 17) + "a" + strings.Repeat(")", 17), wantErr: "search expression: nesting depth exceeds maximum of 16 at position 16"},
		{name: "unknown fuzz alg", expression: "foo", alg: FuzzAlg(99), wantErr: "unknown fuzz algorithm 99"},
		{name: "threshold above 100", expression: "foo", threshold: 150, wantErr: "fuzz threshold 150 out of [0,100]"},
		{name: "threshold below 0", expression: "foo", threshold: -1, wantErr: "fuzz threshold -1 out of [0,100]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewExpressionParams(tc.expression, true, tc.alg, tc.threshold)
			require.EqualError(t, err, tc.wantErr)
			assert.Nil(t, got)
		})
	}
}

func TestWithCursor(t *testing.T) {
	p, err := NewParams([]string{"foo"}, true, FuzzAlgJaroWinkler, 50)
	require.NoError(t, err)

	got, err := p.WithCursor("foo_bar", 0.75)
	require.NoError(t, err)
	assert.Equal(t, &Params{Terms: []string{"foo"}, CaseSensitive: true, FuzzAlg: FuzzAlgJaroWinkler, FuzzThreshold: 50, ResumeAfter: "foo_bar", ScoreAfter: 0.75}, got)
	assert.Empty(t, p.ResumeAfter, "WithCursor must not change the receiver")
	assert.Zero(t, p.ScoreAfter, "WithCursor must not change the receiver")

	for _, tc := range []struct {
		name        string
		resumeAfter string
		scoreAfter  float64
	}{
		{name: "no cursor"},
		{name: "lowest score", resumeAfter: "foo", scoreAfter: 0},
		{name: "highest score", resumeAfter: "foo", scoreAfter: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.WithCursor(tc.resumeAfter, tc.scoreAfter)
			require.NoError(t, err)
		})
	}

	t.Run("nil receiver", func(t *testing.T) {
		got, err := (*Params)(nil).WithCursor("foo", 0.5)
		require.NoError(t, err)
		assert.Equal(t, &Params{ResumeAfter: "foo", ScoreAfter: 0.5}, got)
	})
}

func TestWithCursorRejectsInvalid(t *testing.T) {
	p, err := NewParams([]string{"foo"}, true, FuzzAlgSubsequence, 0)
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		resumeAfter string
		scoreAfter  float64
		wantMsg     string
	}{
		{name: "NaN score", resumeAfter: "foo", scoreAfter: math.NaN(), wantMsg: "score after NaN out of [0,1]"},
		{name: "negative score", resumeAfter: "foo", scoreAfter: -0.1, wantMsg: "score after -0.1 out of [0,1]"},
		{name: "score above 1", resumeAfter: "foo", scoreAfter: 1.5, wantMsg: "score after 1.5 out of [0,1]"},
		{name: "infinite score", resumeAfter: "foo", scoreAfter: math.Inf(1), wantMsg: "score after +Inf out of [0,1]"},
		{name: "score without resume after", scoreAfter: 0.7, wantMsg: "score after 0.7 requires resume after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.WithCursor(tc.resumeAfter, tc.scoreAfter)
			require.EqualError(t, err, tc.wantMsg)
			assert.Nil(t, got)
		})
	}
}

func TestNewTermsOrExpressionParams(t *testing.T) {
	t.Run("builds from whichever input is present", func(t *testing.T) {
		tests := []struct {
			name           string
			terms          []string
			expression     string
			wantTerms      []string
			wantExpression bool
		}{
			{name: "terms only", terms: []string{"foo"}, wantTerms: []string{"foo"}},
			{name: "expression only", expression: "foo AND NOT bar", wantExpression: true},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := NewTermsOrExpressionParams(tc.terms, tc.expression, true, FuzzAlgSubstring, 0)
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, tc.wantTerms, got.Terms)
				assert.Equal(t, tc.wantExpression, got.expressionExpr != nil)
			})
		}
	})

	t.Run("terms and expression together are rejected", func(t *testing.T) {
		// A whitespace-only expression is still present, so it conflicts with
		// terms instead of being dropped in favour of them.
		for _, expression := range []string{"bar", "\t"} {
			got, err := NewTermsOrExpressionParams([]string{"foo"}, expression, true, FuzzAlgSubstring, 0)
			require.ErrorIs(t, err, ErrTermsAndExpression, "expression %q", expression)
			require.EqualError(t, err, "invalid search parameters: search terms and search expression are mutually exclusive", "expression %q", expression)
			assert.Nil(t, got, "expression %q", expression)
		}
	})

	t.Run("whitespace-only expression is rejected", func(t *testing.T) {
		// Only "" means no expression. Treating blank input as no filter would
		// return unfiltered results instead of an error.
		got, err := NewTermsOrExpressionParams(nil, "  ", true, FuzzAlgSubstring, 0)
		require.EqualError(t, err, "search expression is empty")
		assert.Nil(t, got)
	})
}
