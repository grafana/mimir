// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestDurationMiddleware(t *testing.T) {
	testCases := map[string]struct {
		query         string
		expectInstant string
		expectRange   string
	}{
		"no duration expression stays the same": {
			query:         "rate(http_requests_total[5m])",
			expectInstant: "rate(http_requests_total[5m])",
			expectRange:   "rate(http_requests_total[5m])",
		},
		"valid duration expression should be rewritten": {
			query:         "rate(http_requests_total[5m + 10s] offset (2*2))",
			expectInstant: "rate(http_requests_total[5m10s] offset 4s)",
			expectRange:   "rate(http_requests_total[5m10s] offset 4s)",
		},
		"valid duration expression with start() should be rewritten": {
			query:         "http_requests_total @ start()",
			expectInstant: "http_requests_total @ 1.000",
			expectRange:   "http_requests_total @ 2.000",
		},
		"valid duration expression with end() should be rewritten": {
			query:         "http_requests_total @ end()",
			expectInstant: "http_requests_total @ 1.000",
			expectRange:   "http_requests_total @ 3.000",
		},
		"valid duration expression with step() should be rewritten": {
			query:         "rate(http_requests_total[5m + step()])",
			expectInstant: "rate(http_requests_total[5m])",
			expectRange:   "rate(http_requests_total[6m])",
		},
		"valid duration expression with range() should be rewritten": {
			query:         "rate(http_requests_total[range() + 30s])",
			expectInstant: "rate(http_requests_total[30s])",
			expectRange:   "rate(http_requests_total[31s])",
		},
	}
	p := parser.NewParser(parser.Options{
		ExperimentalDurationExpr: true,
	})
	for name, tc := range testCases {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("name=%s instant=%v", name, instant), func(t *testing.T) {
				var req MetricsQueryRequest
				expr, err := p.ParseExpr(tc.query)
				require.NoError(t, err)
				if instant {
					req = NewPrometheusInstantQueryRequest(
						"",
						nil,
						1000,
						0,
						expr,
						Options{},
						nil,
						"",
					)
				} else {
					req = NewPrometheusRangeQueryRequest(
						"",
						nil,
						2000,
						3000,
						60000,
						0,
						expr,
						Options{},
						nil,
						"",
					)
				}

				capture := &captureMiddleware{}
				logCapture := bytes.Buffer{}
				middleware := newDurationsMiddleware(log.NewLogfmtLogger(&logCapture))
				chain := middleware.Wrap(capture)

				_, err = chain.Do(context.Background(), req)
				require.NoError(t, err)
				if instant {
					require.Equal(t, tc.expectInstant, capture.query)
				} else {
					require.Equal(t, tc.expectRange, capture.query)
				}
				if tc.expectInstant != tc.query {
					require.Contains(t, logCapture.String(), "rewritten")
				} else {
					require.NotContains(t, logCapture.String(), "rewritten")
				}
			})
		}
	}
}

func TestDurationVisitorRejectInvalid(t *testing.T) {
	testCases := map[string]struct {
		expr        parser.Expr
		expectError string
	}{
		"using start() operator": {
			expr: &parser.VectorSelector{
				OriginalOffsetExpr: &parser.DurationExpr{
					Op: parser.START,
				},
			},
			expectError: "unexpected duration expression operator \"start\" in query-frontend",
		},
		"vector selector in duration": {
			expr: &parser.VectorSelector{
				OriginalOffsetExpr: &parser.DurationExpr{
					LHS: &parser.VectorSelector{},
					Op:  parser.ADD,
					RHS: &parser.NumberLiteral{Val: 5},
				},
			},
			expectError: "unexpected duration expression type *parser.VectorSelector in query-frontend",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := inspect(tc.expr, durationVisitor)
			require.Error(t, err)
			require.EqualError(t, err, tc.expectError)
		})
	}
}

type captureMiddleware struct {
	query string
}

func (c *captureMiddleware) Do(_ context.Context, req MetricsQueryRequest) (Response, error) {
	c.query = req.GetQuery()
	return &PrometheusResponse{}, nil
}

func TestDurationsMiddleware_ShouldNotPanicOnNilQueryExpression(t *testing.T) {
	capture := &captureMiddleware{}
	middleware := newDurationsMiddleware(log.NewNopLogger())
	handler := middleware.Wrap(capture)

	// Create a request with a nil queryExpr to simulate a failed parse.
	req := NewPrometheusInstantQueryRequest("", nil, 1000, 0, nil, Options{}, nil, "")

	// This should not panic, should pass through to the next handler.
	require.NotPanics(t, func() {
		resp, err := handler.Do(context.Background(), req)
		// With nil expr, the middleware falls through to the next handler.
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}
