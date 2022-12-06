package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func TestCompareSeries(t *testing.T) {
	tests := map[string]struct {
		first       []*storepb.Series
		second      []*storepb.Series
		expectedErr string
	}{
		"should pass if all series and chunks match": {
			first: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 25, MaxTime: 35}},
				},
			},
			second: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 25, MaxTime: 35}},
				},
			},
		},
		"should fail the number of series doesn't match": {
			first: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				},
			},
			second: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 25, MaxTime: 35}},
				},
			},
			expectedErr: "the number of series don't match",
		},
		"should fail the number of series matches but the actual series don't": {
			first: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_XXX"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 25, MaxTime: 35}},
				},
			},
			second: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 25, MaxTime: 35}},
				},
			},
			expectedErr: "the series labels don't match at position 1",
		},
		"should fail the series match but the number of chunks don't": {
			first: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}},
				},
			},
			second: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 25, MaxTime: 35}},
				},
			},
			expectedErr: "the number of chunks don't match for series at position 1",
		},
		"should fail the series match, the number of chunks match but the actual chunks don't": {
			first: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 0, MaxTime: 35}},
				},
			},
			second: []*storepb.Series{
				{
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
					Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
				}, {
					Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
					Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 25, MaxTime: 35}},
				},
			},
			expectedErr: "the chunks don't match for series at position 1",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			err := compareSeries(testData.first, testData.second, "zone-a", "zone-b")

			if testData.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, testData.expectedErr)
			}
		})
	}
}

func TestComparePerZoneSeries(t *testing.T) {
	perZoneSeries := map[string][]*storepb.Series{
		"zone-a": {
			{
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
				Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
			}, {
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
				Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 0, MaxTime: 35}},
			},
		},
		"zone-b": {
			{
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
				Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
			}, {
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
				Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}},
			},
		},
		"zone-c": {
			{
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_1"}},
				Chunks: []storepb.AggrChunk{{MinTime: 10, MaxTime: 20}, {MinTime: 20, MaxTime: 30}},
			}, {
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric_2"}},
				Chunks: []storepb.AggrChunk{{MinTime: 15, MaxTime: 25}, {MinTime: 0, MaxTime: 35}},
			},
		},
	}

	errs := comparePerZoneSeries(perZoneSeries, "zone-c")
	require.Len(t, errs, 1)
	assert.ErrorContains(t, errs[0], "the number of chunks don't match for series at position 1")
}
