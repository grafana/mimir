// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"slices"
	"strconv"
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	newTestCompartmentRouter := func(numCompartments int) *compartments.TopicRouter {
		return compartments.NewTopicRouter(numCompartments, "comp-<read-compartment-id>")
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

func TestGroupSeriesHashesByReadCompartment(t *testing.T) {
	const (
		userID          = "user-1"
		numCompartments = 5
	)
	metadata := tenant.NewMetadata(":source=test")
	router := compartments.NewTopicRouter(numCompartments, "comp-<read-compartment-id>")

	metricNames := []string{"metric_a", "metric_b", "metric_a", "metric_c", "metric_d", "metric_b"}
	timeseries := make([]mimirpb.PreallocTimeseries, 0, len(metricNames))
	seriesHashes := make([]uint64, 0, len(metricNames))
	expectedHashes := map[int][]uint64{}
	for i, metricName := range metricNames {
		timeseries = append(timeseries, makeTimeseries([]string{model.MetricNameLabel, metricName}, makeSamples(1000, 1), nil, nil))
		hash := uint64(100 + i)
		seriesHashes = append(seriesHashes, hash)
		compartment := router.CompartmentForMetric(userID, metricName)
		expectedHashes[compartment] = append(expectedHashes[compartment], hash)
	}

	var expected []readCompartmentSeries
	for compartment := range numCompartments {
		if hashes, ok := expectedHashes[compartment]; ok {
			expected = append(expected, readCompartmentSeries{
				compartment:        int32(compartment),
				usageTrackerUserID: userID + "/rc/" + strconv.Itoa(compartment) + ":source=test",
				seriesHashes:       hashes,
			})
		}
	}
	// The test needs more than one group and at least one empty compartment.
	require.Greater(t, len(expected), 1)
	require.Less(t, len(expected), numCompartments)

	assert.Equal(t, expected, groupSeriesHashesByReadCompartment(router, userID, metadata, timeseries, seriesHashes))
}
