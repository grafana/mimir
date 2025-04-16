// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/querier/stats"
)

func Test_dynamicStep_getNewStep(t *testing.T) {
	type args struct {
		cardinality int
		step        int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "cardinality too low to change the query step",
			args: args{
				cardinality: 900,
				step:        2000,
			},
			want: 2000,
		},
		{
			name: "complex query detected",
			args: args{
				cardinality: 2000,
				step:        500,
			},
			want: 5200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dynamicStep{
				logger:              log.NewNopLogger(),
				complexityThreshold: complexityThreshold,
			}
			assert.Equal(t, int64(tt.want), d.getNewStep(uint64(tt.args.cardinality), int64(tt.args.step)))
		})
	}
}

func Test_dynamicStep_Do(t *testing.T) {
	tests := []struct {
		name        string
		oldStep     int
		cardinality int
		handler     MetricsQueryHandler
		wantBigger  bool
	}{
		{
			name: "complexity is less than the threshold",
			handler: &dynamicStep{
				logger: log.NewNopLogger(),
			},
			wantBigger:  false,
			oldStep:     1000,
			cardinality: 900,
		},
		{
			name:       "complexity is greater than the threshold",
			wantBigger: true,
			handler: &dynamicStep{
				logger: log.NewNopLogger(),
			},
			oldStep:     500,
			cardinality: 1900,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDynamicStepMiddleware(complexityThreshold, log.NewNopLogger(), prometheus.NewRegistry())
			_, ctx := stats.ContextWithEmptyStats(context.Background())
			handle := d.Wrap(&mockNextHandler{t, true})

			req := PrometheusRangeQueryRequest{
				start:     parseTimeRFC3339(t, "2023-01-31T09:00:00Z").Unix() * 1000,
				end:       parseTimeRFC3339(t, "2023-01-31T10:00:00Z").Unix() * 1000,
				queryExpr: parseQuery(t, "up"),
				step:      int64(tt.oldStep),
				hints: &Hints{
					CardinalityEstimate: &EstimatedSeriesCount{
						EstimatedSeriesCount: uint64(tt.cardinality),
					},
				},
			}
			_, err := handle.Do(ctx, &req)
			assert.NoError(t, err)

			if tt.wantBigger {
				assert.Greater(t, req.step, int64(tt.oldStep))
			} else {
				assert.Equal(t, int64(tt.oldStep), req.step)
			}
		})
	}
}
