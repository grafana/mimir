// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
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
		wantErr      bool
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
			name: "total 6 points",
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
			name: "two points in head",
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
			name: "one point in head",
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
			gotFloat, gotHasFloat, _, err := runDoubleExponentialSmoothing(tt.args.fHead, tt.args.fTail, tt.args.smoothingFactor, tt.args.trendFactor)
			if (err != nil) != tt.wantErr {
				t.Errorf("runDoubleExponentialSmoothing() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFloat != tt.wantFloat {
				t.Errorf("runDoubleExponentialSmoothing() gotFloat = %v, wantFloat %v", gotFloat, tt.wantFloat)
			}
			if gotHasFloat != tt.wantHasFloat {
				t.Errorf("runDoubleExponentialSmoothing() gotHasFloat = %v, wantHasFloat %v", gotHasFloat, tt.wantHasFloat)
			}
		})
	}
}
