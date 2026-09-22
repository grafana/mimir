// SPDX-License-Identifier: AGPL-3.0-only

package searchexpr

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type containsFilter struct {
	term  string
	score float64
}

type unknownExpr struct{}

func (unknownExpr) expr() {}

type staticEvaluator struct {
	result evalResult
}

func (e staticEvaluator) evaluate(string, *[]float64) evalResult {
	return e.result
}

func (f containsFilter) Accept(value string) (bool, float64) {
	if !strings.Contains(value, f.term) {
		return false, 0
	}
	return true, f.score
}

func compileContains(t *testing.T, input string, scores map[string]float64) storage.Filter {
	t.Helper()
	expr, err := Parse(input)
	require.NoError(t, err)
	filter, err := Compile(expr, func(term string, _ bool) (storage.Filter, error) {
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
		{name: "positive AND negative keeps positive score", expression: "foo AND NOT old", value: "foo_new", wantAccept: true, wantScore: 0.8},
		{name: "positive AND negative rejects excluded value", expression: "foo AND NOT old", value: "foo_old", wantAccept: false, wantScore: 0},
		{name: "double negation preserves positive score", expression: "NOT NOT foo", value: "foo_new", wantAccept: true, wantScore: 0.8},
		{name: "double negation rejects a non-match", expression: "NOT NOT foo", value: "brand_new", wantAccept: false, wantScore: 0},
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

// TestCompileCombinesPositiveScores pins the mean-of-leaves scoring design:
// an AND's score is the mean of every scored leaf it depends on (not the
// weakest term), and an OR picks whichever branch's own mean is higher.
func TestCompileCombinesPositiveScores(t *testing.T) {
	filter := compileContains(t, "foo AND bar OR baz", map[string]float64{
		"foo": 0.8,
		"bar": 0.4,
		"baz": 0.7,
	})

	accepted, score := filter.Accept("foo_bar")
	assert.True(t, accepted)
	assert.InDelta(t, 0.6, score, 1e-9, "mean(0.8, 0.4)")

	accepted, score = filter.Accept("foo_bar_baz")
	assert.True(t, accepted)
	assert.InDelta(t, 0.7, score, 1e-9, "OR picks the higher-mean branch: max(mean(0.8,0.4)=0.6, 0.7)")

	accepted, score = filter.Accept("baz")
	assert.True(t, accepted)
	assert.InDelta(t, 0.7, score, 1e-9)

	filter = compileContains(t, "low AND high", map[string]float64{"low": 0.2, "high": 0.8})
	accepted, score = filter.Accept("low_high")
	assert.True(t, accepted)
	assert.InDelta(t, 0.5, score, 1e-9, "mean(0.2, 0.8)")

	filter = compileContains(t, "high OR low", map[string]float64{"low": 0.2, "high": 0.8})
	accepted, score = filter.Accept("high_low")
	assert.True(t, accepted)
	assert.InDelta(t, 0.8, score, 1e-9)
}

// TestCompileReusesScoreBufferAcrossCalls pins that the pooled score buffer
// is correctly reset between Accept calls: a call that claims two buffer
// slots must not leak a stale score into a later call that claims only one.
func TestCompileReusesScoreBufferAcrossCalls(t *testing.T) {
	filter := compileContains(t, "foo OR bar", map[string]float64{"foo": 0.9, "bar": 0.3})

	accepted, score := filter.Accept("foo_bar")
	require.True(t, accepted)
	assert.InDelta(t, 0.9, score, 1e-9)

	accepted, score = filter.Accept("bar_only")
	require.True(t, accepted)
	assert.InDelta(t, 0.3, score, 1e-9)

	accepted, score = filter.Accept("neither")
	assert.False(t, accepted)
	assert.Zero(t, score)
}

// TestCompileAcceptIsSafeForConcurrentUse pins that concurrent Accept calls
// on the same compiled filter each get their own pooled buffer and never
// observe another goroutine's in-flight scores.
func TestCompileAcceptIsSafeForConcurrentUse(t *testing.T) {
	filter := compileContains(t, "foo OR bar", map[string]float64{"foo": 0.9, "bar": 0.3})

	cases := []struct {
		value      string
		wantAccept bool
		wantScore  float64
	}{
		{value: "foo_only", wantAccept: true, wantScore: 0.9},
		{value: "bar_only", wantAccept: true, wantScore: 0.3},
		{value: "foo_bar", wantAccept: true, wantScore: 0.9},
		{value: "neither", wantAccept: false, wantScore: 0},
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		for _, c := range cases {
			wg.Add(1)
			go func(c struct {
				value      string
				wantAccept bool
				wantScore  float64
			}) {
				defer wg.Done()
				accepted, score := filter.Accept(c.value)
				assert.Equal(t, c.wantAccept, accepted, c.value)
				assert.InDelta(t, c.wantScore, score, 1e-9, c.value)
			}(c)
		}
	}
	wg.Wait()
}

func TestCompiledAndShortCircuitsOnRejection(t *testing.T) {
	calls := map[string]int{}
	expr, err := Parse("first AND later")
	require.NoError(t, err)
	filter, err := Compile(expr, func(term string, _ bool) (storage.Filter, error) {
		return countingTermFilter{
			accepted: term != "first",
			score:    0.8,
			calls:    calls,
			term:     term,
		}, nil
	})
	require.NoError(t, err)

	accepted, score := filter.Accept("anything")
	assert.False(t, accepted)
	assert.Zero(t, score)
	assert.Equal(t, 1, calls["first"])
	assert.Zero(t, calls["later"])
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
		filter, err := Compile(expr, func(term string, _ bool) (storage.Filter, error) {
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
		expr, err := Parse("anchor AND (NOT old OR later)")
		require.NoError(t, err)
		filter, err := Compile(expr, func(term string, _ bool) (storage.Filter, error) {
			if term == "anchor" {
				return containsFilter{term: "anything", score: 0.9}, nil
			}
			if term == "old" {
				return containsFilter{term: "never", score: 1}, nil
			}
			return countingTermFilter{accepted: true, score: 0.6, calls: calls, term: term}, nil
		})
		require.NoError(t, err)

		accepted, score := filter.Accept("anything")
		assert.True(t, accepted)
		assert.InDelta(t, 0.75, score, 1e-9, "mean(anchor=0.9, later=0.6); the unscored NOT branch contributes no leaf")
		assert.Equal(t, 1, calls["later"], "an unscored NOT match must not hide a later positive score")
	})
}

func TestCompileRejectsInvalidInputs(t *testing.T) {
	factoryErr := errors.New("factory failed")
	validFactory := func(term string, _ bool) (storage.Filter, error) {
		return containsFilter{term: term, score: 1}, nil
	}

	for _, test := range []struct {
		name    string
		expr    Expr
		factory TermFilterFactory
		want    string
	}{
		{name: "nil expression", factory: validFactory, want: "search expression: cannot compile a nil expression"},
		{name: "nil factory", expr: Term{Value: "foo"}, want: "search expression: term filter factory is nil"},
		{name: "empty term", expr: Term{}, factory: validFactory, want: "search expression: cannot compile an empty term"},
		{name: "nil NOT operand", expr: Not{}, factory: validFactory, want: "search expression: cannot compile NOT with a nil operand"},
		{name: "nil AND operand", expr: And{Left: Term{Value: "foo"}}, factory: validFactory, want: "search expression: cannot compile a binary expression with a nil operand"},
		{name: "nil OR operand", expr: Or{Right: Term{Value: "foo"}}, factory: validFactory, want: "search expression: cannot compile a binary expression with a nil operand"},
		{
			name: "factory error",
			expr: Term{Value: "foo"},
			factory: func(string, bool) (storage.Filter, error) {
				return nil, factoryErr
			},
			want: "search expression: compile term \"foo\": factory failed",
		},
		{
			name: "nil term filter",
			expr: Term{Value: "foo"},
			factory: func(string, bool) (storage.Filter, error) {
				return nil, nil
			},
			want: "search expression: term filter factory returned nil for \"foo\"",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			filter, err := Compile(test.expr, test.factory)
			require.EqualError(t, err, test.want)
			assert.Nil(t, filter)
		})
	}
}

func TestCompileValidatedRejectsInvalidInputs(t *testing.T) {
	validFactory := func(term string, _ bool) (storage.Filter, error) {
		return containsFilter{term: term, score: 1}, nil
	}

	filter, err := CompileValidated(nil, validFactory)
	require.EqualError(t, err, "search expression: cannot compile a nil expression")
	assert.Nil(t, filter)

	filter, err = CompileValidated(Term{Value: "foo"}, nil)
	require.EqualError(t, err, "search expression: term filter factory is nil")
	assert.Nil(t, filter)
}

func TestValidateRejectsNilAndUnknownExpressions(t *testing.T) {
	require.EqualError(t, Validate(nil), "search expression: cannot validate a nil expression")
	require.EqualError(t, Validate(unknownExpr{}), "search expression: cannot compile node searchexpr.unknownExpr")
}

func TestCompilerDefensiveBranches(t *testing.T) {
	validFactory := func(term string, _ bool) (storage.Filter, error) {
		return containsFilter{term: term, score: 1}, nil
	}

	for _, test := range []struct {
		name string
		expr Expr
		want string
	}{
		{name: "empty term", expr: Term{}, want: "search expression: cannot compile an empty term"},
		{name: "nil AND child", expr: And{Left: Term{Value: "foo"}}, want: "search expression: cannot compile a binary expression with a nil operand"},
		{name: "nil OR child", expr: Or{Right: Term{Value: "foo"}}, want: "search expression: cannot compile a binary expression with a nil operand"},
		{name: "nil NOT child", expr: Not{}, want: "search expression: cannot compile NOT with a nil operand"},
		{name: "unknown node", expr: unknownExpr{}, want: "search expression: cannot compile node searchexpr.unknownExpr"},
	} {
		t.Run(test.name, func(t *testing.T) {
			evaluator, err := compileEvaluator(test.expr, false, validFactory)
			require.EqualError(t, err, test.want)
			assert.Nil(t, evaluator)
		})
	}

	evaluator, err := compileEvaluator(
		And{Left: Term{Value: "foo"}, Right: Term{Value: "bar"}},
		true,
		validFactory,
	)
	require.NoError(t, err)
	assert.IsType(t, &orEvaluator{}, evaluator)

	factoryErr := errors.New("left child failed")
	_, _, err = mapChildren(Term{Value: "foo"}, Term{Value: "bar"}, func(Expr) (bool, error) {
		return false, factoryErr
	})
	require.ErrorIs(t, err, factoryErr)
}

func TestCompileReportsEffectiveTermPolarity(t *testing.T) {
	expr, err := Parse("anchor AND NOT old AND NOT NOT restored")
	require.NoError(t, err)

	polarities := map[string]bool{}
	_, err = Compile(expr, func(term string, negated bool) (storage.Filter, error) {
		polarities[term] = negated
		return containsFilter{term: term, score: 1}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{
		"anchor":   false,
		"old":      true,
		"restored": false,
	}, polarities)
}

func TestCompiledFilterAcceptsUnscoredInternalResult(t *testing.T) {
	filter := &compiledFilter{
		root: staticEvaluator{result: evalResult{accepted: true}},
		scorePool: sync.Pool{
			New: func() any {
				buf := make([]float64, 0)
				return &buf
			},
		},
	}
	accepted, score := filter.Accept("anything")
	assert.True(t, accepted)
	assert.Zero(t, score)
}

func TestCombineScoresAcceptsTwoUnscoredPredicates(t *testing.T) {
	result := combineScores(evalResult{accepted: true}, evalResult{accepted: true})
	assert.Equal(t, evalResult{accepted: true}, result)
}

func TestCompileRejectsExpressionsThatCanMatchWithoutPositiveTerm(t *testing.T) {
	for _, expression := range []Expr{
		Not{Expr: Term{Value: "old"}},
		Or{Left: Term{Value: "foo"}, Right: Not{Expr: Term{Value: "old"}}},
		Not{Expr: Or{Left: Term{Value: "foo"}, Right: Term{Value: "old"}}},
		Not{Expr: And{Left: Term{Value: "foo"}, Right: Term{Value: "old"}}},
		Or{
			Left:  And{Left: Term{Value: "foo"}, Right: Not{Expr: Term{Value: "old"}}},
			Right: Not{Expr: Term{Value: "stale"}},
		},
	} {
		filter, err := Compile(expression, func(term string, _ bool) (storage.Filter, error) {
			return containsFilter{term: term, score: 1}, nil
		})
		require.EqualError(t, err, "search expression: every accepting path must require a positive term")
		assert.Nil(t, filter)
	}
}

func TestCompileRejectsASTOutsideResourceLimits(t *testing.T) {
	validFactory := func(term string, _ bool) (storage.Filter, error) {
		return containsFilter{term: term, score: 1}, nil
	}

	tooManyTerms := Expr(Term{Value: "term-0"})
	for i := 1; i <= maxExpressionTerms; i++ {
		tooManyTerms = And{Left: tooManyTerms, Right: Term{Value: "term"}}
	}
	filter, err := Compile(tooManyTerms, validFactory)
	require.EqualError(t, err, "search expression: term count exceeds maximum of 32")
	assert.Nil(t, filter)

	tooDeep := Expr(Term{Value: "foo"})
	for range maxCompiledExpressionDepth {
		tooDeep = Not{Expr: tooDeep}
	}
	filter, err = Compile(tooDeep, validFactory)
	require.EqualError(t, err, "search expression: AST depth exceeds maximum of 48")
	assert.Nil(t, filter)
}

func BenchmarkCompiledFilterAccept(b *testing.B) {
	expr, err := Parse("foo AND bar OR baz")
	require.NoError(b, err)
	filter, err := Compile(expr, func(term string, _ bool) (storage.Filter, error) {
		return containsFilter{term: term, score: 0.7}, nil
	})
	require.NoError(b, err)

	values := []string{"foo_bar", "foo_bar_baz", "baz_only", "no_match"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = filter.Accept(values[i%len(values)])
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
		if Validate(expr) != nil {
			return
		}
		filter, err := Compile(expr, func(term string, _ bool) (storage.Filter, error) {
			return containsFilter{term: term, score: 0.5}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, filter)
		accepted, score := filter.Accept("foo_new_metric")
		if accepted {
			require.Positive(t, score, "an accepted anchored expression must retain a positive-term score")
		}
	})
}
