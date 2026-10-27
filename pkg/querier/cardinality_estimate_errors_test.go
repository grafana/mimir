// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
)

// A source's refusal is classified by the limit its status detail names,
// not by the words of its message: each limit maps to a 422 with its own
// refusals label, the message reaches the caller as it was, and a
// ResourceExhausted status without the detail, such as gRPC's own size
// limit, stays a failure.
func TestSourceRefusalLimit(t *testing.T) {
	for _, limit := range []string{refusedByMaxIndexBytes, refusedByMaxHashes, refusedByMaxGroups} {
		t.Run(limit, func(t *testing.T) {
			// The message does not name the limit.
			refusal := cardinality.Refusalf(limit, "the request is over the limit")
			for name, classify := range map[string]func(error) error{
				"store-gateway": func(err error) error { return classifyStoreGatewayError("1.1.1.1", err) },
				"ingester":      classifyIngesterError,
			} {
				err := classify(refusal)
				assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err), name)
				assert.Equal(t, limit, estimateRefusalLimit(err), name)
				assert.Contains(t, err.Error(), "the request is over the limit", name)
				assert.NotContains(t, err.Error(), "rpc error", "the status wrapper is not in the caller's text")
				// A wrapped status, as the distributor passes one on, classifies the same.
				assert.Equal(t, limit, estimateRefusalLimit(classify(fmt.Errorf("partition 1: %w", refusal))), name)
			}
		})
	}

	t.Run("a max_hashes refusal says how many series the walk needed", func(t *testing.T) {
		err := classifyStoreGatewayError("1.1.1.1", cardinality.MaxHashesRefusal(1234, 1000))
		assert.Equal(t, refusedByMaxHashes, estimateRefusalLimit(err))
		n, ok := estimateHashesNeeded(CardinalityEstimateResult{}, refusedByLimit(refusedByMaxHashes, "split: %w", err))
		require.True(t, ok)
		assert.Equal(t, int64(1234), n)
	})

	t.Run("ResourceExhausted without the detail is a failure, whatever the words", func(t *testing.T) {
		for _, msg := range []string{"grpc: received message larger than max (200000000 vs. 104857600)", "more than max_index_bytes 1000"} {
			plain := status.Error(codes.ResourceExhausted, msg)
			assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(classifyStoreGatewayError("1.1.1.1", plain)), msg)
			assert.Empty(t, estimateRefusalLimit(classifyStoreGatewayError("1.1.1.1", plain)))
			assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(classifyIngesterError(plain)), msg)
		}
	})

	t.Run("a streamed name-table refusal on max_index_bytes says how the budget was split", func(t *testing.T) {
		err := nameTableCallError("1.1.1.1", cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "over the budget"), 1000, 500, 2)
		assert.Equal(t, refusedByMaxIndexBytes, estimateRefusalLimit(err))
		assert.Contains(t, err.Error(), "max_index_bytes 1000, split into 500 bytes for each of 2 store-gateway calls")
		err = nameTableCallError("1.1.1.1", cardinality.Refusalf(cardinality.LimitMaxGroups, "over the groups"), 1000, 500, 2)
		assert.Equal(t, refusedByMaxGroups, estimateRefusalLimit(err))
		assert.NotContains(t, err.Error(), "split into")
	})
}
