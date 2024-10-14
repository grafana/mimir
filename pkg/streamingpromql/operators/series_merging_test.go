// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestMergeSeries(t *testing.T) {
	testCases := map[string]struct {
		input               []types.InstantVectorSeriesData
		sourceSeriesIndices []int

		expectedOutput                 types.InstantVectorSeriesData
		expectedConflict               *MergeConflict
		expectInputHPointSlicesCleared bool
	}{
		"no input series": {
			input:          []types.InstantVectorSeriesData{},
			expectedOutput: types.InstantVectorSeriesData{},
		},
		"single float only input series": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 3, F: 30},
					},
				},
			},
			sourceSeriesIndices: []int{0},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
				},
			},
		},
		"single histogram only input series": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					},
				},
			},
			sourceSeriesIndices: []int{0},
			expectedOutput: types.InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
					{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
				},
			},
		},
		"two float only input series with no overlap, series in time order": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 3, F: 30},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 4, F: 40},
						{T: 5, F: 50},
						{T: 6, F: 60},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
					{T: 4, F: 40},
					{T: 5, F: 50},
					{T: 6, F: 60},
				},
			},
		},
		"two histogram only input series with no overlap, series in time order": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
						{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
						{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
					{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
					{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
		"two float only input series with no overlap, series not in time order": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 4, F: 40},
						{T: 5, F: 50},
						{T: 6, F: 60},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 3, F: 30},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
					{T: 4, F: 40},
					{T: 5, F: 50},
					{T: 6, F: 60},
				},
			},
		},
		"two histogram only input series with no overlap, series not in time order": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
						{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
						{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
					{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
					{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
		"three float only input series with no overlap": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 3, F: 30},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 4, F: 40},
						{T: 5, F: 50},
						{T: 6, F: 60},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 7, F: 70},
						{T: 8, F: 80},
						{T: 9, F: 90},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1, 2},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
					{T: 4, F: 40},
					{T: 5, F: 50},
					{T: 6, F: 60},
					{T: 7, F: 70},
					{T: 8, F: 80},
					{T: 9, F: 90},
				},
			},
		},
		"three histogram only input series with no overlap": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
						{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
						{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 7, H: &histogram.FloatHistogram{Count: 70, Sum: 700}},
						{T: 8, H: &histogram.FloatHistogram{Count: 80, Sum: 800}},
						{T: 9, H: &histogram.FloatHistogram{Count: 90, Sum: 900}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1, 2},
			expectedOutput: types.InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
					{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
					{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
					{T: 7, H: &histogram.FloatHistogram{Count: 70, Sum: 700}},
					{T: 8, H: &histogram.FloatHistogram{Count: 80, Sum: 800}},
					{T: 9, H: &histogram.FloatHistogram{Count: 90, Sum: 900}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
		"two float only input series with overlap": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 3, F: 30},
						{T: 5, F: 50},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
						{T: 4, F: 40},
						{T: 6, F: 60},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
					{T: 4, F: 40},
					{T: 5, F: 50},
					{T: 6, F: 60},
				},
			},
		},
		"two histogram only input series with overlap": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
						{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
						{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
					{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
					{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
		"three float only input series with overlap": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 4, F: 40},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
						{T: 5, F: 50},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 3, F: 30},
						{T: 6, F: 60},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1, 2},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
					{T: 4, F: 40},
					{T: 5, F: 50},
					{T: 6, F: 60},
				},
			},
		},
		"three histogram only input series with overlap": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
						{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
						{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1, 2},
			expectedOutput: types.InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
					{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
					{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
		"float only input series with conflict": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
						{T: 3, F: 30},
						{T: 5, F: 50},
					},
				},
			},
			sourceSeriesIndices: []int{3, 2},
			expectedConflict: &MergeConflict{
				firstConflictingSeriesIndex:  3,
				secondConflictingSeriesIndex: 2,
				description:                  "duplicate series",
				timestamp:                    2,
			},
		},
		"histogram only input series with conflict": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
						{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					},
				},
			},
			sourceSeriesIndices: []int{3, 2},
			expectedConflict: &MergeConflict{
				firstConflictingSeriesIndex:  3,
				secondConflictingSeriesIndex: 2,
				description:                  "duplicate series",
				timestamp:                    2,
			},
		},
		"float only input series with conflict after resorting": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 5, F: 50},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 6, F: 60},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
						{T: 4, F: 40},
					},
				},
			},
			sourceSeriesIndices: []int{6, 9, 4},
			expectedConflict: &MergeConflict{
				firstConflictingSeriesIndex:  6,
				secondConflictingSeriesIndex: 4,
				description:                  "duplicate series",
				timestamp:                    2,
			},
		},
		"histogram only input series with conflict after resorting": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 6, H: &histogram.FloatHistogram{Count: 60, Sum: 600}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 4, H: &histogram.FloatHistogram{Count: 40, Sum: 400}},
					},
				},
			},
			sourceSeriesIndices: []int{6, 9, 4},
			expectedConflict: &MergeConflict{
				firstConflictingSeriesIndex:  6,
				secondConflictingSeriesIndex: 4,
				description:                  "duplicate series",
				timestamp:                    2,
			},
		},
		"float and histogram input series with no conflict": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 3, F: 30},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 2, Sum: 2}},
						{T: 4, H: &histogram.FloatHistogram{Count: 4, Sum: 4}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 3, F: 30},
				},
				Histograms: []promql.HPoint{
					{T: 2, H: &histogram.FloatHistogram{Count: 2, Sum: 2}},
					{T: 4, H: &histogram.FloatHistogram{Count: 4, Sum: 4}},
				},
			},
		},
		"mixed float and histogram input series, series in time order": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
					},
					Histograms: []promql.HPoint{
						{T: 3, H: &histogram.FloatHistogram{Count: 3, Sum: 3}},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 4, F: 40},
						{T: 5, F: 50},
					},
					Histograms: []promql.HPoint{
						{T: 6, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 4, F: 40},
					{T: 5, F: 50},
				},
				Histograms: []promql.HPoint{
					{T: 3, H: &histogram.FloatHistogram{Count: 3, Sum: 3}},
					{T: 6, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
		"mixed float and histogram input series, series not in time order": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 4, F: 40},
						{T: 5, F: 50},
					},
					Histograms: []promql.HPoint{
						{T: 6, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
					},
					Histograms: []promql.HPoint{
						{T: 3, H: &histogram.FloatHistogram{Count: 3, Sum: 3}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 4, F: 40},
					{T: 5, F: 50},
				},
				Histograms: []promql.HPoint{
					{T: 3, H: &histogram.FloatHistogram{Count: 3, Sum: 3}},
					{T: 6, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
		"mixed float and histogram input series, series in conflict on different types": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 2, Sum: 2}},
					},
				},
			},
			sourceSeriesIndices: []int{5, 1},
			expectedConflict: &MergeConflict{
				firstConflictingSeriesIndex:  1,
				secondConflictingSeriesIndex: -1,
				description:                  "both float and histogram samples",
				timestamp:                    2,
			},
		},
		"mixed float and histogram input series, series in conflict on different type and not in time order": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 3, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 3, F: 30},
					},
				},
			},
			sourceSeriesIndices: []int{5, 1},
			expectedConflict: &MergeConflict{
				firstConflictingSeriesIndex:  5,
				secondConflictingSeriesIndex: -1,
				description:                  "both float and histogram samples",
				timestamp:                    3,
			},
		},
		"input series have no points": {
			input: []types.InstantVectorSeriesData{
				{
					Floats:     []promql.FPoint{},
					Histograms: []promql.HPoint{},
				},
				{
					Floats:     []promql.FPoint{},
					Histograms: []promql.HPoint{},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			expectedOutput: types.InstantVectorSeriesData{
				Floats:     nil,
				Histograms: nil,
			},
		},
		"mixed float and histogram input series overlap in different ways": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 3, F: 30},
					},
					Histograms: []promql.HPoint{
						{T: 4, H: &histogram.FloatHistogram{Count: 4, Sum: 4}},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 7, F: 70},
						{T: 8, F: 80},
					},
					Histograms: []promql.HPoint{
						{T: 9, H: &histogram.FloatHistogram{Count: 9, Sum: 9}},
						{T: 10, H: &histogram.FloatHistogram{Count: 10, Sum: 10}},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 5, F: 50},
					},
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 2, Sum: 2}},
						{T: 6, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1, 2},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 3, F: 30},
					{T: 5, F: 50},
					{T: 7, F: 70},
					{T: 8, F: 80},
				},
				Histograms: []promql.HPoint{
					{T: 2, H: &histogram.FloatHistogram{Count: 2, Sum: 2}},
					{T: 4, H: &histogram.FloatHistogram{Count: 4, Sum: 4}},
					{T: 6, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
					{T: 9, H: &histogram.FloatHistogram{Count: 9, Sum: 9}},
					{T: 10, H: &histogram.FloatHistogram{Count: 10, Sum: 10}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
		"input series exclusively with floats, histograms, or mixed, all overlap": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 8, F: 80},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 3, H: &histogram.FloatHistogram{Count: 3, Sum: 3}},
						{T: 10, H: &histogram.FloatHistogram{Count: 10, Sum: 10}},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 7, F: 70},
						{T: 9, F: 90},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 2, Sum: 2}},
						{T: 6, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 5, F: 50},
					},
					Histograms: []promql.HPoint{
						{T: 4, H: &histogram.FloatHistogram{Count: 4, Sum: 4}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1, 2, 3, 4},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 5, F: 50},
					{T: 7, F: 70},
					{T: 8, F: 80},
					{T: 9, F: 90},
				},
				Histograms: []promql.HPoint{
					{T: 2, H: &histogram.FloatHistogram{Count: 2, Sum: 2}},
					{T: 3, H: &histogram.FloatHistogram{Count: 3, Sum: 3}},
					{T: 4, H: &histogram.FloatHistogram{Count: 4, Sum: 4}},
					{T: 6, H: &histogram.FloatHistogram{Count: 6, Sum: 6}},
					{T: 10, H: &histogram.FloatHistogram{Count: 10, Sum: 10}},
				},
			},
			expectInputHPointSlicesCleared: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result, conflict, err := MergeSeries(testCase.input, testCase.sourceSeriesIndices, limiting.NewMemoryConsumptionTracker(0, nil))

			require.NoError(t, err)

			if testCase.expectedConflict != nil {
				require.Equal(t, testCase.expectedConflict, conflict)
			} else {
				require.Nil(t, conflict)
				require.Equal(t, testCase.expectedOutput, result)
			}

			if testCase.expectInputHPointSlicesCleared {
				for sliceIdx, slice := range testCase.input {
					for pointIdx, point := range slice.Histograms {
						require.Nilf(t, point.H, "expected point at index %v of HPoint slice at index %v to have been cleared, but it was not", pointIdx, sliceIdx)
					}
				}
			}
		})
	}
}
