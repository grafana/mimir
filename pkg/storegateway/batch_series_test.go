// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestPreloadingBatchSet_Concurrency(t *testing.T) {
	const (
		numRuns     = 100
		numBatches  = 100
		preloadSize = 10
	)

	// Create some batches.
	batches := make([]seriesChunksSet, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		batches = append(batches, seriesChunksSet{
			series: []seriesEntry{{
				lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
			}},
		})
	}

	// Run many times to increase the likelihood to find a race (if any).
	for i := 0; i < numRuns; i++ {
		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(batches), batches...)
		preloading := newPreloadingSeriesChunkSetIterator(context.Background(), preloadSize, source)

		for preloading.Next() {
			require.NoError(t, preloading.Err())
			require.NotZero(t, preloading.At())
		}
		require.Error(t, preloading.Err())
	}

}
