// SPDX-License-Identifier: AGPL-3.0-only

package push

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var noopParser = supplierFunc(func() (*mimirpb.WriteRequest, func(), error) {
	return &mimirpb.WriteRequest{}, nil, nil
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
	p := supplierFunc(func() (*mimirpb.WriteRequest, func(), error) {
		parseCount++
		return &mimirpb.WriteRequest{}, nil, nil
	})

	r := newRequest(p)
	_, _ = r.WriteRequest()
	_, _ = r.WriteRequest()
	assert.Equal(t, 1, parseCount)
}
