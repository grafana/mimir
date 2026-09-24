// SPDX-License-Identifier: AGPL-3.0-only

package upstreamtestdata

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
)

func TestCommentOutEvals(t *testing.T) {
	const loadAndTwoEvals = "load 1m\n  metric 1 2 3\n\neval instant at 0m metric\n  metric 1\n\neval instant at 0m metric * 2\n  {} 2\n"

	testCases := map[string]struct {
		content       string
		evalLines     map[int]bool
		expected      string
		expectedLines []int
	}{
		"space-indented block, other blocks untouched": {
			content:   loadAndTwoEvals,
			evalLines: map[int]bool{7: true},
			expected:  "load 1m\n  metric 1 2 3\n\neval instant at 0m metric\n  metric 1\n\n# Unsupported by streaming engine.\n# eval instant at 0m metric * 2\n#   {} 2\n",
		},
		"tab-indented block uses the '#\\t' form our copies use": {
			content:   "eval instant at 0m metric\n\tmetric 1\n",
			evalLines: map[int]bool{1: true},
			expected:  "# Unsupported by streaming engine.\n# eval instant at 0m metric\n#\tmetric 1\n",
		},
		"expect lines are part of the block": {
			content:   "eval instant at 0m foo\n  expect fail msg: bad\n",
			evalLines: map[int]bool{1: true},
			expected:  "# Unsupported by streaming engine.\n# eval instant at 0m foo\n#   expect fail msg: bad\n",
		},
		"lines that don't start an eval block are reported and left alone": {
			content:       loadAndTwoEvals,
			evalLines:     map[int]bool{1: true, 5: true},
			expected:      loadAndTwoEvals,
			expectedLines: []int{1, 5},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, unmatched := CommentOutEvals(tc.content, tc.evalLines)
			require.Equal(t, tc.expected, actual)
			require.Equal(t, tc.expectedLines, unmatched)

			// The in-sync test relies on this reversing exactly.
			require.Equal(t, tc.content, RestoreUnsupportedTestCases(actual))
		})
	}
}

func TestParseEval(t *testing.T) {
	testCases := map[string]struct {
		line              string
		expectedExpr      string
		expectedIsInstant bool
		expectedOK        bool
	}{
		"instant":           {line: "eval instant at 50m sum(x)", expectedExpr: "sum(x)", expectedIsInstant: true, expectedOK: true},
		"instant with fail": {line: "eval_fail instant at 0m foo", expectedExpr: "foo", expectedIsInstant: true, expectedOK: true},
		"instant ordered":   {line: "eval_ordered instant at 0m sort(x)", expectedExpr: "sort(x)", expectedIsInstant: true, expectedOK: true},
		"range":             {line: "eval range from 0 to 2m step 1m rate(x[1m])", expectedExpr: "rate(x[1m])", expectedOK: true},
		"not an eval":       {line: "load 1m"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			expr, isInstant, ok := ParseEval(tc.line)
			require.Equal(t, tc.expectedExpr, expr)
			require.Equal(t, tc.expectedIsInstant, isInstant)
			require.Equal(t, tc.expectedOK, ok)
			require.Equal(t, tc.expectedOK, IsEval(tc.line))
		})
	}
}

func TestClassifyEval(t *testing.T) {
	notSupported := compat.NewNotSupportedError("some feature")
	otherErr := errors.New("boom")

	testCases := map[string]struct {
		line           string
		engineErr      error
		expectedResult BuildResult
		expectedErr    error
		expectedExpr   string
		expectedRange  bool
	}{
		"instant query builds":         {line: "eval instant at 0m sum(x)", expectedResult: BuildOK, expectedExpr: "sum(x)"},
		"range query builds":           {line: "eval range from 0 to 1m step 1m sum(x)", expectedResult: BuildOK, expectedExpr: "sum(x)", expectedRange: true},
		"not supported":                {line: "eval instant at 0m sum(x)", engineErr: notSupported, expectedResult: BuildUnsupported, expectedErr: notSupported, expectedExpr: "sum(x)"},
		"wrapped not supported":        {line: "eval instant at 0m sum(x)", engineErr: fmt.Errorf("planning: %w", notSupported), expectedResult: BuildUnsupported, expectedErr: notSupported, expectedExpr: "sum(x)"},
		"other error":                  {line: "eval instant at 0m sum(x)", engineErr: otherErr, expectedResult: BuildError, expectedErr: otherErr, expectedExpr: "sum(x)"},
		"not an eval, engine not used": {line: "load 1m", expectedResult: BuildNotEval},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			engine := &fakeEngine{err: tc.engineErr}
			result, err := ClassifyEval(t.Context(), engine, tc.line)

			require.Equal(t, tc.expectedResult, result)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedExpr, engine.expr)
			require.Equal(t, tc.expectedRange, engine.isRange)
			// A query that was built must be closed.
			require.Equal(t, tc.expectedResult == BuildOK, engine.closed)
		})
	}
}

// fakeEngine records which query it was asked to build and fails with err, if set.
type fakeEngine struct {
	err     error
	expr    string
	isRange bool
	closed  bool
}

func (e *fakeEngine) NewInstantQuery(_ context.Context, _ storage.Queryable, _ promql.QueryOpts, qs string, _ time.Time) (promql.Query, error) {
	e.expr = qs
	return e.query()
}

func (e *fakeEngine) NewRangeQuery(_ context.Context, _ storage.Queryable, _ promql.QueryOpts, qs string, _, _ time.Time, _ time.Duration) (promql.Query, error) {
	e.expr, e.isRange = qs, true
	return e.query()
}

func (e *fakeEngine) query() (promql.Query, error) {
	if e.err != nil {
		return nil, e.err
	}
	return &fakeQuery{closed: &e.closed}, nil
}

type fakeQuery struct {
	promql.Query
	closed *bool
}

func (q *fakeQuery) Close() { *q.closed = true }
