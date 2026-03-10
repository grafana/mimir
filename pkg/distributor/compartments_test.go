// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"slices"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func TestGetCompartmentTokensForWriteRequest(t *testing.T) {
	userID := "user-1"

	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "metric_a"}, makeSamples(1000, 1), nil, nil),
			makeTimeseries([]string{model.MetricNameLabel, "metric_b"}, makeSamples(1000, 2), nil, nil),
			makeTimeseries([]string{model.MetricNameLabel, "metric_c"}, makeSamples(1000, 3), nil, nil),
		},
		Metadata: []*mimirpb.MetricMetadata{
			{MetricFamilyName: "metric_a", Type: mimirpb.COUNTER},
			{MetricFamilyName: "metric_b", Type: mimirpb.GAUGE},
		},
	}

	t.Run("no router returns single compartmentTokens with default topic", func(t *testing.T) {
		cts, initialMetadataIndex := getCompartmentTokensForWriteRequest(nil, "default-topic", userID, req)

		assert.Equal(t, 3, initialMetadataIndex)
		require.Len(t, cts, 1)
		assert.Equal(t, "default-topic", cts[0].topic)

		// All 5 items (3 series + 2 metadata) should be in the single entry.
		assert.Len(t, cts[0].indexes, 5)
		assert.Len(t, cts[0].tokens, 5)

		// Indexes should be 0..4.
		for i := 0; i < 5; i++ {
			assert.Equal(t, i, cts[0].indexes[i])
		}

		// Tokens should match what getSeriesAndMetadataTokens returns.
		expectedKeys, _ := getSeriesAndMetadataTokens(userID, req)
		assert.Equal(t, expectedKeys, cts[0].tokens)
	})

	t.Run("with router groups by compartment", func(t *testing.T) {
		router := ingest.NewCompartmentRouter(ingest.CompartmentsConfig{
			Enabled:         true,
			NumCompartments: 3,
			TopicFormat:     "comp-<compartment-id>",
		})

		cts, initialMetadataIndex := getCompartmentTokensForWriteRequest(router, "unused", userID, req)
		assert.Equal(t, 3, initialMetadataIndex)

		// Verify each returned compartmentTokens has the correct topic and non-empty items.
		allIndexes := map[int]bool{}
		for _, ct := range cts {
			assert.Contains(t, []string{"comp-0", "comp-1", "comp-2"}, ct.topic)
			assert.Equal(t, len(ct.indexes), len(ct.tokens))
			for _, idx := range ct.indexes {
				assert.False(t, allIndexes[idx], "index %d appears in multiple compartments", idx)
				allIndexes[idx] = true
			}
		}

		// All 5 items should be covered.
		assert.Len(t, allIndexes, 5)

		// Verify series and metadata for the same metric end up in the same compartment.
		// metric_a is at series index 0 and metadata index 3 (initialMetadataIndex + 0).
		// metric_b is at series index 1 and metadata index 4 (initialMetadataIndex + 1).
		for _, ct := range cts {
			hasSeriesA := slices.Contains(ct.indexes, 0)
			hasMetadataA := slices.Contains(ct.indexes, 3)
			assert.Equal(t, hasSeriesA, hasMetadataA, "series and metadata for metric_a should be in the same compartment")

			hasSeriesB := slices.Contains(ct.indexes, 1)
			hasMetadataB := slices.Contains(ct.indexes, 4)
			assert.Equal(t, hasSeriesB, hasMetadataB, "series and metadata for metric_b should be in the same compartment")
		}
	})

	t.Run("series without __name__ get a deterministic compartment", func(t *testing.T) {
		noNameReq := &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{"foo", "bar"}, makeSamples(1000, 1), nil, nil),
			},
		}

		router := ingest.NewCompartmentRouter(ingest.CompartmentsConfig{
			Enabled:         true,
			NumCompartments: 3,
			TopicFormat:     "comp-<compartment-id>",
		})

		cts, _ := getCompartmentTokensForWriteRequest(router, "unused", userID, noNameReq)

		totalItems := 0
		for _, ct := range cts {
			totalItems += len(ct.indexes)
		}
		assert.Equal(t, 1, totalItems)
	})
}
