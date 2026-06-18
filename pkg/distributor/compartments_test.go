// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"slices"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestCompartmentTokens_WriteRequestIndexes(t *testing.T) {
	ct := &compartmentTokens{
		indexes: []int{2, 5, 8, 11},
		tokens:  []uint32{100, 200, 300, 400},
	}

	t.Run("remaps token-level indexes to WriteRequest indexes", func(t *testing.T) {
		assert.Equal(t, []int{2, 8}, ct.writeRequestIndexes([]int{0, 2}))
	})

	t.Run("single index", func(t *testing.T) {
		assert.Equal(t, []int{11}, ct.writeRequestIndexes([]int{3}))
	})

	t.Run("all indexes", func(t *testing.T) {
		assert.Equal(t, []int{2, 5, 8, 11}, ct.writeRequestIndexes([]int{0, 1, 2, 3}))
	})

	t.Run("empty indexes", func(t *testing.T) {
		assert.Empty(t, ct.writeRequestIndexes([]int{}))
	})
}

func TestGetCompartmentTokensForWriteRequest(t *testing.T) {
	userID := "user-1"

	newTestCompartmentRouter := func(numCompartments int) *compartments.Router {
		return compartments.NewRouter(numCompartments, "comp-<read-compartment-id>")
	}

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

	t.Run("groups items by compartment, keeping series+metadata of a metric together", func(t *testing.T) {
		router := newTestCompartmentRouter(3)

		cts, initialMetadataIndex := getCompartmentTokensForWriteRequest(router, userID, req)
		assert.Equal(t, 3, initialMetadataIndex)

		seenIndexes := map[int]bool{}
		for _, ct := range cts {
			assert.NotEmpty(t, ct.indexes, "empty compartments must be filtered out")
			assert.Equal(t, router.TopicForCompartment(ct.compartmentID), ct.topic)
			assert.Len(t, ct.tokens, len(ct.indexes))
			for _, idx := range ct.indexes {
				assert.False(t, seenIndexes[idx], "index %d appears in multiple compartments", idx)
				seenIndexes[idx] = true
			}
		}

		// All 5 items are covered exactly once.
		assert.Len(t, seenIndexes, 5)

		// Series and metadata of the same metric land in the same compartment.
		// metric_a: series index 0, metadata index 3. metric_b: series index 1, metadata index 4.
		for _, ct := range cts {
			assert.Equal(t, slices.Contains(ct.indexes, 0), slices.Contains(ct.indexes, 3), "metric_a series and metadata must share a compartment")
			assert.Equal(t, slices.Contains(ct.indexes, 1), slices.Contains(ct.indexes, 4), "metric_b series and metadata must share a compartment")
		}
	})

	t.Run("series without __name__ are assigned a deterministic compartment", func(t *testing.T) {
		noNameReq := &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{"foo", "bar"}, makeSamples(1000, 1), nil, nil),
			},
		}

		cts, _ := getCompartmentTokensForWriteRequest(newTestCompartmentRouter(3), userID, noNameReq)

		total := 0
		for _, ct := range cts {
			total += len(ct.indexes)
		}
		assert.Equal(t, 1, total)
	})
}
