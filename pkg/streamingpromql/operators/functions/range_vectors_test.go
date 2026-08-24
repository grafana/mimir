// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
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

func Test_pickAnchorStartIndices(t *testing.T) {
	// The picker only consults timestamps, so the loaders return zero-valued samples carrying the
	// timestamp at each index. nFloats/nHists are derived from the timestamp slices.
	floatLoader := func(ts []int64) func(int) (float64, int64, bool, int) {
		return func(idx int) (float64, int64, bool, int) {
			if idx < len(ts) {
				return 0, ts[idx], true, idx + 1
			}
			return 0, 0, false, idx
		}
	}
	histLoader := func(ts []int64) func(int) (*histogram.FloatHistogram, int64, bool, int) {
		return func(idx int) (*histogram.FloatHistogram, int64, bool, int) {
			if idx < len(ts) {
				return nil, ts[idx], true, idx + 1
			}
			return nil, 0, false, idx
		}
	}

	tests := []struct {
		name           string
		floatTimes     []int64
		histTimes      []int64
		rangeStart     int64
		wantFIdx       int
		wantHIdx       int
		wantHasInRange bool
	}{
		{
			name:       "empty window: every sample at or before rangeStart",
			floatTimes: []int64{0, 60, 120},
			histTimes:  []int64{0, 60},
			rangeStart: 120,
			// No sample lies strictly after rangeStart.
			wantHasInRange: false,
		},
		{
			name:           "no anchor: every sample after rangeStart",
			floatTimes:     []int64{60, 120},
			histTimes:      []int64{90},
			rangeStart:     0,
			wantFIdx:       0,
			wantHIdx:       0,
			wantHasInRange: true,
		},
		{
			name:           "float-only anchor",
			floatTimes:     []int64{60, 120, 180},
			histTimes:      nil,
			rangeStart:     120,
			wantFIdx:       1, // anchor float at t=120 seeds prev
			wantHIdx:       0,
			wantHasInRange: true,
		},
		{
			name:           "histogram-only anchor",
			floatTimes:     nil,
			histTimes:      []int64{60, 120, 180},
			rangeStart:     120,
			wantFIdx:       0,
			wantHIdx:       1, // anchor histogram at t=120 seeds prev
			wantHasInRange: true,
		},
		{
			name:           "mixed: float is the later anchor, skip pre-anchor histograms",
			floatTimes:     []int64{120, 180, 240}, // hhffmixed-shaped: float anchor at t=120
			histTimes:      []int64{0, 60},
			rangeStart:     120,
			wantFIdx:       0, // float anchor at t=120 seeds prev
			wantHIdx:       2, // both pre-anchor histograms skipped
			wantHasInRange: true,
		},
		{
			name:           "mixed: histogram is the later anchor, skip pre-anchor float",
			floatTimes:     []int64{60},
			histTimes:      []int64{120, 180},
			rangeStart:     120,
			wantFIdx:       1, // pre-anchor float skipped
			wantHIdx:       0, // histogram anchor at t=120 seeds prev
			wantHasInRange: true,
		},
		{
			name:           "mixed: float and histogram anchors tie at rangeStart, float wins",
			floatTimes:     []int64{120, 180},
			histTimes:      []int64{120, 180},
			rangeStart:     120,
			wantFIdx:       0, // float anchor seeds prev
			wantHIdx:       1, // pre/at-anchor histogram skipped
			wantHasInRange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fIdx, hIdx, hasInRange := pickAnchorStartIndices(
				floatLoader(tt.floatTimes),
				histLoader(tt.histTimes),
				len(tt.floatTimes),
				len(tt.histTimes),
				tt.rangeStart,
			)
			require.Equal(t, tt.wantHasInRange, hasInRange)
			if !hasInRange {
				return
			}
			require.Equal(t, tt.wantFIdx, fIdx)
			require.Equal(t, tt.wantHIdx, hIdx)
		})
	}
}
