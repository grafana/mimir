package distributor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestCountHistogramBuckets(t *testing.T) {
	tests := []struct {
		name      string
		histogram *mimirpb.Histogram
		expected  int
	}{
		{
			name: "empty histogram",
			histogram: &mimirpb.Histogram{
				PositiveSpans: []mimirpb.BucketSpan{},
				NegativeSpans: []mimirpb.BucketSpan{},
			},
			expected: 0,
		},
		{
			name: "positive buckets only",
			histogram: &mimirpb.Histogram{
				PositiveSpans: []mimirpb.BucketSpan{
					{Offset: 0, Length: 5},
					{Offset: 2, Length: 3},
				},
				NegativeSpans: []mimirpb.BucketSpan{},
			},
			expected: 8, // 5 + 3
		},
		{
			name: "negative buckets only",
			histogram: &mimirpb.Histogram{
				PositiveSpans: []mimirpb.BucketSpan{},
				NegativeSpans: []mimirpb.BucketSpan{
					{Offset: 0, Length: 4},
					{Offset: 1, Length: 2},
				},
			},
			expected: 6, // 4 + 2
		},
		{
			name: "both positive and negative buckets",
			histogram: &mimirpb.Histogram{
				PositiveSpans: []mimirpb.BucketSpan{
					{Offset: 0, Length: 3},
				},
				NegativeSpans: []mimirpb.BucketSpan{
					{Offset: 0, Length: 2},
					{Offset: 1, Length: 4},
				},
			},
			expected: 9, // 3 + 2 + 4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := countHistogramBuckets(tt.histogram)
			assert.Equal(t, tt.expected, actual)
		})
	}
}