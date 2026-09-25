// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var noopParser = supplierFunc(func() (*mimirpb.WriteRequest, func(), int, error) {
	return &mimirpb.WriteRequest{}, nil, 0, nil
})

// TestRequest_CleanUpOrder tests that the semantics of cleanups is similar to stacking defer statements:
// last one is the first to be executed.
func TestRequest_CleanUpOrder(t *testing.T) {
	var cleanupOrder []int
	cleanupOne := func() {
		cleanupOrder = append(cleanupOrder, 1)
	}
	cleanupTwo := func() {
		cleanupOrder = append(cleanupOrder, 2)
	}

	r := newRequest(noopParser)

	r.AddCleanup(cleanupOne)
	r.AddCleanup(cleanupTwo)
	r.CleanUp()

	assert.Equal(t, []int{2, 1}, cleanupOrder)
}

func TestRequest_ReleaseWriteRequestPreservesCompletionCleanup(t *testing.T) {
	parsed, released, completed := 0, 0, 0
	r := newRequest(func() (*mimirpb.WriteRequest, func(), int, error) {
		parsed++
		return &mimirpb.WriteRequest{}, func() { released++ }, 123, nil
	})
	r.AddCleanup(func() { completed++ })
	_, err := r.WriteRequest()
	require.NoError(t, err)
	r.releaseWriteRequest()
	r.releaseWriteRequest()
	require.Equal(t, 1, released)
	require.Zero(t, completed)
	require.Nil(t, r.request)
	require.Nil(t, r.getRequest)
	require.Nil(t, r.requestCleanup)
	require.Equal(t, 123, r.UncompressedBodySize())
	_, err = r.WriteRequest()
	require.EqualError(t, err, "decoded write request has been released")
	require.Equal(t, 1, parsed)
	r.CleanUp()
	r.CleanUp()
	require.Equal(t, 1, released)
	require.Equal(t, 1, completed)
	for _, cleanup := range r.cleanupsArr {
		require.Nil(t, cleanup)
	}
}

// TestRequest_CleanUpDoubleCalling tests that calling CleanUp twice doesn't invoke the functions again.
func TestRequest_CleanUpDoubleCalling(t *testing.T) {
	invocations := 0

	r := newRequest(noopParser)

	r.AddCleanup(func() { invocations++ })
	r.CleanUp()
	r.CleanUp()

	assert.Equal(t, 1, invocations)
}

func TestRequest_WriteRequestIsParsedOnlyOnce(t *testing.T) {
	parseCount := 0
	p := supplierFunc(func() (*mimirpb.WriteRequest, func(), int, error) {
		parseCount++
		return &mimirpb.WriteRequest{}, nil, 0, nil
	})

	r := newRequest(p)
	_, _ = r.WriteRequest()
	_, _ = r.WriteRequest()
	assert.Equal(t, 1, parseCount)
}
