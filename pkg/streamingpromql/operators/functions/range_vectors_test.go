// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestDoubleExponentialSmoothing_Validation(t *testing.T) {
	tests := []struct {
		name            string
		points          []promql.FPoint
		smoothingFactor float64
		trendFactor     float64
		wantErr         bool
		wantOk          bool
		wantValue       float64
		stepT           int64
	}{
		{
			name: "insufficient points",
			points: []promql.FPoint{
				{T: 1000, F: 1},
			},
			smoothingFactor: 0.5,
			trendFactor:     0.5,
			wantValue:       0,
			wantOk:          false,
			wantErr:         false,
			stepT:           1000,
		},
		{
			name:            "invalid smoothing factor (0)",
			smoothingFactor: 0,
			trendFactor:     0.5,
			wantValue:       0,
			wantOk:          false,
			wantErr:         true,
			stepT:           2000,
		},
		{
			name:            "invalid smoothing factor (1)",
			smoothingFactor: 1,
			trendFactor:     0.5,
			wantValue:       0,
			wantOk:          false,
			wantErr:         true,
			stepT:           2000,
		},
		{
			name:            "invalid trend factor (0)",
			smoothingFactor: 0.5,
			trendFactor:     0,
			wantValue:       0,
			wantOk:          false,
			wantErr:         true,
			stepT:           2000,
		},
		{
			name:            "invalid trend factor (1)",
			smoothingFactor: 0.5,
			trendFactor:     1,
			wantValue:       0,
			wantOk:          false,
			wantErr:         true,
			stepT:           2000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := types.NewFPointRingBuffer(limiting.NewMemoryConsumptionTracker(0, nil))
			for _, p := range tt.points {
				if err := buffer.Append(p); err != nil {
					t.Fatalf("Failed to append point: %v", err)
				}
			}

			// Create an empty histogram buffer for the step data
			histBuffer := types.NewHPointRingBuffer(limiting.NewMemoryConsumptionTracker(0, nil))

			step := &types.RangeVectorStepData{
				StepT:      tt.stepT,
				Floats:     buffer.ViewUntilSearchingForwards(tt.stepT, nil),
				Histograms: histBuffer.ViewUntilSearchingForwards(tt.stepT, nil),
			}

			// Create args with smoothing factor and trend factor
			// Create multiple samples to match the time range
			smoothingSamples := make([]promql.FPoint, 5) // 5 steps from 0 to 4000ms
			trendSamples := make([]promql.FPoint, 5)
			for i := 0; i < 5; i++ {
				t := int64(i * 1000)
				smoothingSamples[i] = promql.FPoint{T: t, F: tt.smoothingFactor}
				trendSamples[i] = promql.FPoint{T: t, F: tt.trendFactor}
			}

			args := []types.ScalarData{
				{
					Samples: smoothingSamples,
				},
				{
					Samples: trendSamples,
				},
			}

			// Create time range with proper steps
			timeRange := types.QueryTimeRange{
				StartT:               0,
				EndT:                 4000,
				IntervalMilliseconds: 1000,
				StepCount:            5,
			}

			gotValue, gotOk, gotHist, err := doubleExponentialSmoothing(step, 0, args, timeRange, nil, nil)

			// Check error
			if (err != nil) != tt.wantErr {
				t.Errorf("doubleExponentialSmoothing() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// If we expect an error, don't check other values
			if tt.wantErr {
				return
			}

			// Check ok value
			if gotOk != tt.wantOk {
				t.Errorf("doubleExponentialSmoothing() ok = %v, want %v", gotOk, tt.wantOk)
			}

			// Check histogram (should always be nil for this function)
			if gotHist != nil {
				t.Errorf("doubleExponentialSmoothing() histogram = %v, want nil", gotHist)
			}

			// If we don't expect ok, don't check the value
			if !tt.wantOk {
				return
			}

			// Check value with small epsilon for floating point comparison
			if math.Abs(gotValue-tt.wantValue) > 0.0001 {
				t.Errorf("doubleExponentialSmoothing() value = %v, want %v", gotValue, tt.wantValue)
			}
		})
	}
}
