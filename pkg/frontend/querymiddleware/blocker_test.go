// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

func Test_queryBlocker_Do(t *testing.T) {
	tests := []struct {
		name           string
		request        Request
		shouldContinue bool
		limits         mockLimits
	}{
		{
			name:           "doesn't block queries due to empty limits",
			limits:         mockLimits{},
			shouldContinue: true,
			request: Request(&PrometheusRangeQueryRequest{
				Query: "rate(metric_counter[5m])",
			}),
		},
		{
			name: "blocks single line query non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "rate(metric_counter[5m])", Regex: false},
				},
			},
			request: Request(&PrometheusRangeQueryRequest{
				Query: "rate(metric_counter[5m])",
			}),
		},
		{
			name: "not blocks single line query non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "rate(metric_counter[5m])", Regex: false},
				},
			},
			shouldContinue: true,

			request: Request(&PrometheusRangeQueryRequest{
				Query: "rate(metric_counter[15m])",
			}),
		},
		{
			name: "blocks multiple line query non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: `rate(metric_counter[5m])/
rate(other_counter[5m])`, Regex: false},
				},
			},
			request: Request(&PrometheusRangeQueryRequest{
				Query: `rate(metric_counter[5m])/
rate(other_counter[5m])`,
			}),
		},
		{
			name: "not blocks multiple line query non regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "rate(metric_counter[5m])", Regex: false},
				},
			},
			shouldContinue: true,

			request: Request(&PrometheusRangeQueryRequest{
				Query: `rate(metric_counter[15m])/
rate(other_counter[15m])`,
			}),
		},
		{
			name: "blocks single line query regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: ".*metric_counter.*", Regex: true},
				},
			},
			request: Request(&PrometheusRangeQueryRequest{
				Query: "rate(metric_counter[5m])",
			}),
		},
		{
			name: "blocks multiple line query regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					// We need to turn on the s flag to allow dot matches newlines.
					{Pattern: "(?s).*metric_counter.*", Regex: true},
				},
			},
			request: Request(&PrometheusRangeQueryRequest{
				Query: `rate(other_counter[15m])/
		rate(metric_counter[15m])`,
			}),
		},
		{
			name: "blocks single line query invalid regex pattern",
			limits: mockLimits{
				blockedQueries: []*validation.BlockedQuery{
					{Pattern: "[a-9}", Regex: true},
				},
			},
			shouldContinue: true,

			request: Request(&PrometheusRangeQueryRequest{
				Query: "rate(metric_counter[5m])",
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			mw := newQueryBlockerMiddleware(tt.limits, logger, reg)
			_, err := mw.Wrap(&mockNextHandler{t: t, shouldContinue: tt.shouldContinue}).Do(user.InjectOrgID(context.Background(), "test"), tt.request)
			if tt.shouldContinue {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			if err != nil {
				assert.Contains(t, err.Error(), globalerror.QueryBlocked)
				assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
                        # HELP cortex_query_frontend_rejected_queries_total Number of queries that were rejected by the cluster administrator.
                        # TYPE cortex_query_frontend_rejected_queries_total counter
						cortex_query_frontend_rejected_queries_total{reason="blocked", user="test"} 1
					`),
				))
			}
		})
	}
}

type mockNextHandler struct {
	t              *testing.T
	shouldContinue bool
}

func (h *mockNextHandler) Do(_ context.Context, _ Request) (Response, error) {
	if !h.shouldContinue {
		h.t.Error("The next middleware should not be called.")
	}
	return nil, nil
}
