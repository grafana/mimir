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

func TestMaxGroupsRefusal(t *testing.T) {
	err := MaxGroupsRefusal("the blocks hold", 12, "pod", 10)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Equal(t, "the blocks hold 12 distinct values of pod, more than max_groups 10", status.Convert(err).Message())
	assert.Equal(t, LimitMaxGroups, RefusedBy(err))
}
