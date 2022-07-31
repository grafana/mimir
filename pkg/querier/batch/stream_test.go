// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/stream_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestStream(t *testing.T) {
	for i, tc := range []struct {
		input1, input2 []chunk.Batch
		output         batchStream
	}{
		{
			input1: []chunk.Batch{mkBatch(0)},
			output: []chunk.Batch{mkBatch(0)},
		},

		{
			input1: []chunk.Batch{mkBatch(0)},
			input2: []chunk.Batch{mkBatch(0)},
			output: []chunk.Batch{mkBatch(0)},
		},

		{
			input1: []chunk.Batch{mkBatch(0)},
			input2: []chunk.Batch{mkBatch(chunk.BatchSize)},
			output: []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize)},
		},

		{
			input1: []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize)},
			input2: []chunk.Batch{mkBatch(chunk.BatchSize / 2), mkBatch(2 * chunk.BatchSize)},
			output: []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize), mkBatch(2 * chunk.BatchSize)},
		},

		{
			input1: []chunk.Batch{mkBatch(chunk.BatchSize / 2), mkBatch(3 * chunk.BatchSize / 2), mkBatch(5 * chunk.BatchSize / 2)},
			input2: []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize), mkBatch(3 * chunk.BatchSize)},
			output: []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize), mkBatch(2 * chunk.BatchSize), mkBatch(3 * chunk.BatchSize)},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			result := make(batchStream, len(tc.input1)+len(tc.input2))
			result = mergeStreams(tc.input1, tc.input2, result, chunk.BatchSize)
			require.Equal(t, batchStream(tc.output), result)
		})
	}
}

func mkBatch(from int64) chunk.Batch {
	var result chunk.Batch
	for i := int64(0); i < chunk.BatchSize; i++ {
		result.Timestamps[i] = from + i
		result.SampleValues[i] = float64(from + i)
	}
	result.Length = chunk.BatchSize
	return result
}
