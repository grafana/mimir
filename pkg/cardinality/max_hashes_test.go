// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHashesNeeded(t *testing.T) {
	n, ok := HashesNeeded(fmt.Errorf("store-gateway: %w", MaxHashesRefusal(1234, 1000)))
	assert.True(t, ok)
	assert.Equal(t, int64(1234), n)
	_, ok = HashesNeeded(Refusalf(LimitMaxIndexBytes, "the request would read about 5000 index bytes, more than max_index_bytes 1000"))
	assert.False(t, ok)
	_, ok = HashesNeeded(status.Error(codes.ResourceExhausted, "the request would hash 5 matching series, more than max_hashes 1"))
	assert.False(t, ok, "the series come from the status detail, not the text")
}

// A refusal names its limit in a status detail, which survives wrapping and
// does not depend on the message's words.
func TestRefusedBy(t *testing.T) {
	for _, limit := range []string{LimitMaxIndexBytes, LimitMaxHashes, LimitMaxGroups} {
		err := Refusalf(limit, "some message")
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Equal(t, "some message", status.Convert(err).Message(), "the caller-visible message is unchanged")
		assert.Equal(t, limit, RefusedBy(err))
		assert.Equal(t, limit, RefusedBy(fmt.Errorf("wrapped: %w", err)))
	}
	assert.Equal(t, LimitMaxHashes, RefusedBy(MaxHashesRefusal(5, 1)))
	assert.Empty(t, RefusedBy(status.Error(codes.ResourceExhausted, "grpc: received message larger than max (200000000 vs. 104857600)")), "gRPC's own ResourceExhausted is not a refusal")
	assert.Empty(t, RefusedBy(status.Error(codes.ResourceExhausted, "more than max_index_bytes 1000")), "the words alone are not a refusal")
	assert.Empty(t, RefusedBy(errors.New("max_hashes")))
	assert.Empty(t, RefusedBy(nil))
}

func TestHashBudget(t *testing.T) {
	assert.Equal(t, int64(256<<20/HashBytes), HashBudget(256<<20))
	assert.Equal(t, int64(1), HashBudget(1), "a set limit allows at least one hash")
	assert.Zero(t, HashBudget(0), "no limit")
}

func TestBoundMaxHashes(t *testing.T) {
	assert.Equal(t, int64(0), BoundMaxHashes(0, 0), "no budget keeps an unbounded request")
	assert.Equal(t, int64(10), BoundMaxHashes(10, 0), "no budget keeps the request's bound")
	assert.Equal(t, int64(100), BoundMaxHashes(0, 100), "an unbounded request gets the budget")
	assert.Equal(t, int64(10), BoundMaxHashes(10, 100), "a smaller request is kept")
	assert.Equal(t, int64(100), BoundMaxHashes(1000, 100), "a larger request is cut")
}
