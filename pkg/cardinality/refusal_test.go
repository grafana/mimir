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

// A refusal names its limit in a status detail, which survives wrapping and
// does not depend on the message's words.
func TestRefusedBy(t *testing.T) {
	for _, limit := range []string{LimitMaxGroups, LimitMemory} {
		err := Refusalf(limit, "some message")
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Equal(t, "some message", status.Convert(err).Message(), "the caller-visible message is unchanged")
		assert.Equal(t, limit, RefusedBy(err))
		assert.Equal(t, limit, RefusedBy(fmt.Errorf("wrapped: %w", err)))
	}
	assert.Empty(t, RefusedBy(status.Error(codes.ResourceExhausted, "grpc: received message larger than max (200000000 vs. 104857600)")), "gRPC's own ResourceExhausted is not a refusal")
	assert.Empty(t, RefusedBy(status.Error(codes.ResourceExhausted, "more than max_groups 1000")), "the words alone are not a refusal")
	assert.Empty(t, RefusedBy(errors.New("max_groups")))
	assert.Empty(t, RefusedBy(nil))
}

func TestMaxGroupsRefusal(t *testing.T) {
	err := MaxGroupsRefusal("the blocks hold", 12, "pod", 10)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Equal(t, "the blocks hold 12 distinct values of pod, more than max_groups 10", status.Convert(err).Message())
	assert.Equal(t, LimitMaxGroups, RefusedBy(err))
}

// A busy source is told apart from an unreachable one by a status detail,
// and is not a limit's refusal.
func TestIsBusy(t *testing.T) {
	err := Busyf("too many counts running, more than %d", 8)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, "too many counts running, more than 8", status.Convert(err).Message())
	assert.True(t, IsBusy(err))
	assert.True(t, IsBusy(fmt.Errorf("wrapped: %w", err)))
	assert.Empty(t, RefusedBy(err))
	assert.False(t, IsBusy(status.Error(codes.Unavailable, "too many counts running")), "the words alone are not busy")
	assert.False(t, IsBusy(Refusalf(LimitMemory, "some message")))
	assert.False(t, IsBusy(errors.New("busy")))
	assert.False(t, IsBusy(nil))
}
