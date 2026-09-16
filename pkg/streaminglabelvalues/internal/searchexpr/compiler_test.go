// SPDX-License-Identifier: AGPL-3.0-only

package searchexpr

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type containsFilter struct {
	term  string
	score float64
}

func (f containsFilter) Accept(value string) (bool, float64) {
	if !strings.Contains(value, f.term) {
		return false, 0
	}
	return true, f.score
}

func compileContains(t *testing.T, input string, scores map[string]float64) Filter {
	t.Helper()
	expr, err := Parse(input)
	require.NoError(t, err)
	filter, err := Compile(expr, func(term string) (Filter, error) {
		return containsFilter{term: term, score: scores[term]}, nil
	})
	require.NoError(t, err)
	return filter
}

func TestCompileExecutesNotExpressions(t *testing.T) {
	for _, test := range []struct {
		name       string
		expression string
		value      string
		wantAccept bool
		wantScore  float64
	}{
		{name: "negative term accepts a non-match without score", expression: "NOT old", value: "foo_new", wantAccept: true, wantScore: 0},
		{name: "negative term rejects a match", expression: "NOT old", value: "foo_old", wantAccept: false, wantScore: 0},
		{name: "positive AND negative keeps positive score", expression: "foo AND NOT old", value: "foo_new", wantAccept: true, wantScore: 0.8},
		{name: "positive AND negative rejects excluded value", expression: "foo AND NOT old", value: "foo_old", wantAccept: false, wantScore: 0},
		{name: "negative OR positive uses positive score", expression: "NOT old OR foo", value: "foo_new", wantAccept: true, wantScore: 0.8},
		{name: "negative OR branch can accept without score", expression: "foo OR NOT old", value: "brand_new", wantAccept: true, wantScore: 0},
		{name: "double negation preserves positive score", expression: "NOT NOT foo", value: "foo_new", wantAccept: true, wantScore: 0.8},
		{name: "double negation rejects a non-match", expression: "NOT NOT foo", value: "brand_new", wantAccept: false, wantScore: 0},
		{name: "De Morgan over OR accepts only when both terms miss", expression: "NOT (foo OR old)", value: "brand_new", wantAccept: true, wantScore: 0},
		{name: "De Morgan over OR rejects either match", expression: "NOT (foo OR old)", value: "foo_new", wantAccept: false, wantScore: 0},
		{name: "De Morgan over AND accepts when either term misses", expression: "NOT (foo AND old)", value: "foo_new", wantAccept: true, wantScore: 0},
		{name: "De Morgan over AND rejects when both terms match", expression: "NOT (foo AND old)", value: "foo_old", wantAccept: false, wantScore: 0},
		{name: "nested negation restores positive branch score", expression: "NOT (NOT foo OR old)", value: "foo_new", wantAccept: true, wantScore: 0.8},
	} {
		t.Run(test.name, func(t *testing.T) {
			filter := compileContains(t, test.expression, map[string]float64{"foo": 0.8, "old": 0.6})
			accepted, score := filter.Accept(test.value)
			assert.Equal(t, test.wantAccept, accepted)
			assert.InDelta(t, test.wantScore, score, 1e-9)
		})
	}
}

func TestCompileCombinesPositiveScores(t *testing.T) {
	filter := compileContains(t, "foo AND bar OR baz", map[string]float64{
		"foo": 0.8,
		"bar": 0.4,
		"baz": 0.7,
	})

	accepted, score := filter.Accept("foo_bar")
	assert.True(t, accepted)
	assert.InDelta(t, 0.4, score, 1e-9)

	accepted, score = filter.Accept("foo_bar_baz")
	assert.True(t, accepted)
	assert.InDelta(t, 0.7, score, 1e-9)
}

type countingTermFilter struct {
	accepted bool
	score    float64
	calls    map[string]int
	term     string
}

func (f countingTermFilter) Accept(string) (bool, float64) {
	f.calls[f.term]++
	return f.accepted, f.score
}

func TestCompiledOrShortCircuitsOnlyForScoredPerfectMatch(t *testing.T) {
	t.Run("positive perfect match", func(t *testing.T) {
		calls := map[string]int{}
		expr, err := Parse("perfect OR later")
		require.NoError(t, err)
		filter, err := Compile(expr, func(term string) (Filter, error) {
			accepted, score := true, 0.5
			if term == "perfect" {
				score = 1
			}
			return countingTermFilter{accepted: accepted, score: score, calls: calls, term: term}, nil
		})
		require.NoError(t, err)

		accepted, score := filter.Accept("anything")
		assert.True(t, accepted)
		assert.Equal(t, 1.0, score)
		assert.Equal(t, 1, calls["perfect"])
		assert.Zero(t, calls["later"])
	})

	t.Run("unscored negative match", func(t *testing.T) {
		calls := map[string]int{}
		expr, err := Parse("NOT old OR later")
		require.NoError(t, err)
		filter, err := Compile(expr, func(term string) (Filter, error) {
			if term == "old" {
				return containsFilter{term: "never", score: 1}, nil
			}
			return countingTermFilter{accepted: true, score: 0.6, calls: calls, term: term}, nil
		})
		require.NoError(t, err)

		accepted, score := filter.Accept("anything")
		assert.True(t, accepted)
		assert.InDelta(t, 0.6, score, 1e-9)
		assert.Equal(t, 1, calls["later"], "an unscored NOT match must not hide a later positive score")
	})
}

func TestCompileRejectsInvalidInputs(t *testing.T) {
	factoryErr := errors.New("factory failed")
	validFactory := func(term string) (Filter, error) {
		return containsFilter{term: term, score: 1}, nil
	}

	for _, test := range []struct {
		name    string
		expr    Expr
		factory TermFilterFactory
		want    string
	}{
		{name: "nil expression", factory: validFactory, want: "nil expression"},
		{name: "nil factory", expr: Term{Value: "foo"}, want: "factory is nil"},
		{name: "empty term", expr: Term{}, factory: validFactory, want: "empty term"},
		{name: "nil NOT operand", expr: Not{}, factory: validFactory, want: "NOT with a nil operand"},
		{name: "nil AND operand", expr: And{Left: Term{Value: "foo"}}, factory: validFactory, want: "binary expression with a nil operand"},
		{name: "nil OR operand", expr: Or{Right: Term{Value: "foo"}}, factory: validFactory, want: "binary expression with a nil operand"},
		{
			name: "factory error",
			expr: Term{Value: "foo"},
			factory: func(string) (Filter, error) {
				return nil, factoryErr
			},
			want: "compile term \"foo\": factory failed",
		},
		{
			name: "nil term filter",
			expr: Term{Value: "foo"},
			factory: func(string) (Filter, error) {
				return nil, nil
			},
			want: "factory returned nil",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			filter, err := Compile(test.expr, test.factory)
			require.Error(t, err)
			assert.Nil(t, filter)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func FuzzCompile(f *testing.F) {
	for _, expression := range []string{
		"foo",
		"NOT foo",
		"foo AND NOT old",
		"NOT NOT foo",
		"NOT (foo OR old)",
		"NOT (NOT foo OR old)",
		`"AND" OR NOT "old metric"`,
	} {
		f.Add(expression)
	}

	f.Fuzz(func(t *testing.T, expression string) {
		expr, err := Parse(expression)
		if err != nil {
			return
		}
		filter, err := Compile(expr, func(term string) (Filter, error) {
			return containsFilter{term: term, score: 0.5}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, filter)
		_, _ = filter.Accept("foo_new_metric")
	})
}
