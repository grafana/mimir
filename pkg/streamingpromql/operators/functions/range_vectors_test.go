// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func Test_runDoubleExponentialSmoothing(t *testing.T) {
	type args struct {
		fHead           []promql.FPoint
		fTail           []promql.FPoint
		smoothingFactor float64
		trendFactor     float64
	}
	tests := []struct {
		name         string
		args         args
		wantFloat    float64
		wantHasFloat bool
	}{
		{
			name: "empty head and tail",
			args: args{
				fHead:           nil,
				fTail:           nil,
				smoothingFactor: 0.1,
				trendFactor:     0.1,
			},
			wantHasFloat: false,
		},
		{
			name: "6 points in head, 0 points in tail",
			args: args{
				fHead:           []promql.FPoint{{F: 1}, {F: 1}, {F: 1}, {F: 1}, {F: 1}, {F: 1}},
				smoothingFactor: 0.1,
				trendFactor:     0.1,
			},
			wantFloat:    1,
			wantHasFloat: true,
		},
		{
			name: "3 points in head, 3 points in tail",
			args: args{
				fHead:           []promql.FPoint{{F: 1}, {F: 1}, {F: 1}},
				fTail:           []promql.FPoint{{F: 1}, {F: 1}, {F: 1}},
				smoothingFactor: 0.1,
				trendFactor:     0.1,
			},
			wantFloat:    1,
			wantHasFloat: true,
		},
		{
			name: "4 points in head, 2 points in tail",
			args: args{
				fHead:           []promql.FPoint{{F: 1}, {F: 1}, {F: 1}, {F: 1}},
				fTail:           []promql.FPoint{{F: 1}, {F: 1}},
				smoothingFactor: 0.1,
				trendFactor:     0.1,
			},
			wantFloat:    1,
			wantHasFloat: true,
		},
		{
			name: "2 points in head, 4 points in tail",
			args: args{
				fHead:           []promql.FPoint{{F: 1}, {F: 1}},
				fTail:           []promql.FPoint{{F: 1}, {F: 1}, {F: 1}, {F: 1}},
				smoothingFactor: 0.1,
				trendFactor:     0.1,
			},
			wantFloat:    1,
			wantHasFloat: true,
		},
		{
			name: "1 points in head, 5 points in tail",
			args: args{
				fHead:           []promql.FPoint{{F: 1}},
				fTail:           []promql.FPoint{{F: 1}, {F: 1}, {F: 1}, {F: 1}, {F: 1}},
				smoothingFactor: 0.1,
				trendFactor:     0.1,
			},
			wantFloat:    1,
			wantHasFloat: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFloat, gotHasFloat, _, err := calculateDoubleExponentialSmoothing(tt.args.fHead, tt.args.fTail, tt.args.smoothingFactor, tt.args.trendFactor)
			require.NoError(t, err)
			require.Equal(t, tt.wantFloat, gotFloat)
			require.Equal(t, tt.wantHasFloat, gotHasFloat)
		})
	}
}
