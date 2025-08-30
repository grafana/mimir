// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDropExactDuplicates(t *testing.T) {
	tests := []struct {
		name     string
		input    []*mimirpb.MetricMetadata
		expected []*mimirpb.MetricMetadata
	}{
		{
			name:     "empty input",
			input:    []*mimirpb.MetricMetadata{},
			expected: []*mimirpb.MetricMetadata{},
		},
		{
			name:     "nil input",
			input:    nil,
			expected: []*mimirpb.MetricMetadata{},
		},
		{
			name: "single item",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "test_metric",
					Help:             "Test help text",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "test_metric",
					Help:             "Test help text",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
			},
		},
		{
			name: "no duplicates",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
		},
		{
			name: "exact duplicates",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
		},
		{
			name: "partial duplicates - different help",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1 different",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1 different",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
			},
		},
		{
			name: "partial duplicates - different type",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.GAUGE,
					Unit:             "seconds",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.GAUGE,
					Unit:             "seconds",
				},
			},
		},
		{
			name: "partial duplicates - different unit",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "bytes",
				},
			},
		},
		{
			name: "multiple exact duplicates",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
		},
		{
			name: "complex case with mixed duplicates",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric3",
					Help:             "Help 3",
					Type:             mimirpb.HISTOGRAM,
					Unit:             "count",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
				{
					MetricFamilyName: "metric3",
					Help:             "Help 3",
					Type:             mimirpb.HISTOGRAM,
					Unit:             "count",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dropExactDuplicates(tt.input)

			// Check that the result has the expected length
			assert.Equal(t, len(tt.expected), len(result), "Result length mismatch")

			// Check that all expected items are present in the result
			for i, expected := range tt.expected {
				require.Less(t, i, len(result), "Result has fewer items than expected")
				assert.Equal(t, expected.MetricFamilyName, result[i].MetricFamilyName, "MetricFamilyName mismatch at index %d", i)
				assert.Equal(t, expected.Help, result[i].Help, "Help mismatch at index %d", i)
				assert.Equal(t, expected.Type, result[i].Type, "Type mismatch at index %d", i)
				assert.Equal(t, expected.Unit, result[i].Unit, "Unit mismatch at index %d", i)
			}

			// Check that the result doesn't contain any exact duplicates
			seen := make(map[string]bool)
			for _, item := range result {
				key := item.MetricFamilyName + "|" + item.Help + "|" + item.Type.String() + "|" + item.Unit
				assert.False(t, seen[key], "Duplicate found: %s", key)
				seen[key] = true
			}
		})
	}
}

func TestDropExactDuplicatesPreservesOrder(t *testing.T) {
	input := []*mimirpb.MetricMetadata{
		{
			MetricFamilyName: "metric1",
			Help:             "Help 1",
			Type:             mimirpb.COUNTER,
			Unit:             "seconds",
		},
		{
			MetricFamilyName: "metric2",
			Help:             "Help 2",
			Type:             mimirpb.GAUGE,
			Unit:             "bytes",
		},
		{
			MetricFamilyName: "metric1", // Duplicate of first item
			Help:             "Help 1",
			Type:             mimirpb.COUNTER,
			Unit:             "seconds",
		},
		{
			MetricFamilyName: "metric3",
			Help:             "Help 3",
			Type:             mimirpb.HISTOGRAM,
			Unit:             "count",
		},
	}

	result := dropExactDuplicates(input)

	// The first occurrence of metric1 should be preserved
	assert.Equal(t, "metric1", result[0].MetricFamilyName)
	assert.Equal(t, "metric2", result[1].MetricFamilyName)
	assert.Equal(t, "metric3", result[2].MetricFamilyName)
	assert.Equal(t, 3, len(result))
}
