// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/limiter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
)

func TestLimiter(t *testing.T) {
	c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	l := NewLimiter(10, c, "limit of %v exceeded")

	assert.NoError(t, l.Reserve(5))
	assert.Equal(t, float64(0), prom_testutil.ToFloat64(c))

	assert.NoError(t, l.Reserve(5))
	assert.Equal(t, float64(0), prom_testutil.ToFloat64(c))

	err := l.Reserve(1)
	assert.ErrorContains(t, err, "limit of 10 exceeded")
	assert.Equal(t, float64(1), prom_testutil.ToFloat64(c))
	checkErrorStatusCode(t, err)

	err = l.Reserve(2)
	assert.ErrorContains(t, err, "limit of 10 exceeded")
	assert.Equal(t, float64(1), prom_testutil.ToFloat64(c))
	checkErrorStatusCode(t, err)
}

func checkErrorStatusCode(t *testing.T, err error) {
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, uint32(http.StatusUnprocessableEntity), uint32(st.Code()))
}

// newStaticChunksLimiterFactory makes a new ChunksLimiterFactory with a static limit.
func newStaticChunksLimiterFactory(limit uint64) ChunksLimiterFactory {
	return NewChunksLimiterFactory(func() uint64 {
		return limit
	})
}

// newStaticSeriesLimiterFactory makes a new ChunksLimiterFactory with a static limit.
func newStaticSeriesLimiterFactory(limit uint64) SeriesLimiterFactory {
	return NewSeriesLimiterFactory(func() uint64 {
		return limit
	})
}
