// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
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
	for _, limit := range []string{refusedByMaxGroups, refusedByMemory} {
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

	t.Run("ResourceExhausted without the detail is a failure, whatever the words", func(t *testing.T) {
		for _, msg := range []string{"grpc: received message larger than max (200000000 vs. 104857600)", "more than max_groups 1000"} {
			plain := status.Error(codes.ResourceExhausted, msg)
			assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(classifyStoreGatewayError("1.1.1.1", plain)), msg)
			assert.Empty(t, estimateRefusalLimit(classifyStoreGatewayError("1.1.1.1", plain)))
			assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(classifyIngesterError(plain)), msg)
		}
	})

}

// A source at its limit of concurrent counts is a 503 the caller may retry,
// told by its status detail; any other Unavailable stays a redacted failure.
func TestBusySourceIsUnavailable(t *testing.T) {
	busy := cardinality.Busyf("too many cardinality estimate count walks running, more than 8")
	unreachable := status.Error(codes.Unavailable, "too many cardinality estimate count walks running: dial tcp 10.9.9.9:9095")
	for name, classify := range map[string]func(error) error{
		"store-gateway": func(err error) error { return classifyStoreGatewayError("1.1.1.1", err) },
		"ingester":      classifyIngesterError,
	} {
		for _, in := range []error{busy, fmt.Errorf("partition 1: %w", busy)} {
			err := classify(in)
			assert.Equal(t, http.StatusServiceUnavailable, estimateHTTPStatus(err), name)
			assert.Contains(t, err.Error(), "too many cardinality estimate count walks running, more than 8", name)
			assert.NotContains(t, err.Error(), "rpc error", name)
			assert.Empty(t, estimateRefusalLimit(err), name)
			assert.Equal(t, "failed", estimateRequestStatus(err), name)
		}
		err := classify(unreachable)
		assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(err), name)
		assert.NotContains(t, err.Error(), "10.9.9.9", name)
	}

	// Busy store-gateways are retried like unreachable ones, and when none counted a block the request is a 503.
	err := classifyStoreGatewayError("1.1.1.1", busy)
	assert.True(t, shouldRetrySeriesCounts(context.Background(), err))
	assert.Equal(t, http.StatusServiceUnavailable, estimateHTTPStatus(errNothingCounted(1, estimateCoverage{Failure: err})))
}
