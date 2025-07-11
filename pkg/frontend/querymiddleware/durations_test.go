// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func init() {
	parser.ExperimentalDurationExpr = true
}

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
		"valid duration expression with step() should be rewritten": {
			query:         "rate(http_requests_total[5m + step()])",
			expectInstant: "rate(http_requests_total[5m])",
			expectRange:   "rate(http_requests_total[6m])",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			capture := &captureMiddleware{}
			logCapture := bytes.Buffer{}
			middleware := newDurationsMiddleware(log.NewLogfmtLogger(&logCapture))
			chain := middleware.Wrap(capture)

			expr, err := parser.ParseExpr(tc.query)
			require.NoError(t, err)

			{
				// Do instant query first.
				req := NewPrometheusInstantQueryRequest(
					"",
					nil,
					0,
					0,
					expr,
					Options{},
					nil,
					"",
				)
				logCapture.Reset()
				_, err = chain.Do(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, tc.expectInstant, capture.query)
				if tc.expectInstant != tc.query {
					require.Contains(t, logCapture.String(), "rewritten")
				} else {
					require.NotContains(t, logCapture.String(), "rewritten")
				}
			}

			{
				// Do range query next.
				req := NewPrometheusRangeQueryRequest(
					"",
					nil,
					0,
					0,
					60,
					0,
					expr,
					Options{},
					nil,
					"",
				)
				logCapture.Reset()
				_, err = chain.Do(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, tc.expectRange, capture.query)
				if tc.expectInstant != tc.query {
					require.Contains(t, logCapture.String(), "rewritten")
				} else {
					require.NotContains(t, logCapture.String(), "rewritten")
				}
			}
		})
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
			v := &durationVisitor{}
			err := parser.Walk(v, tc.expr, nil)
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
