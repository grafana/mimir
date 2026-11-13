// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

func TestCacheKeyGenerator_CardinalityEstimate(t *testing.T) {
	request := func(ctx context.Context) *http.Request {
		u, err := url.Parse("/prometheus/api/v1/cardinality/estimate?start=0&end=86400")
		require.NoError(t, err)
		return (&http.Request{Method: http.MethodGet, URL: u, Header: http.Header{}}).WithContext(ctx)
	}
	generator := NewCacheSplitter(querymiddleware.DefaultCacheKeyGenerator{})

	t.Run("a request under a label policy is not cached", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), contextKeyLabelPolicyHash, "policy-hash")
		key, err := generator.CardinalityEstimate(request(ctx))
		require.ErrorIs(t, err, querymiddleware.ErrUnsupportedRequest)
		assert.Nil(t, key)
	})

	t.Run("a request with no label policy gets the delegate's key", func(t *testing.T) {
		key, err := generator.CardinalityEstimate(request(context.Background()))
		require.NoError(t, err)
		require.NotNil(t, key)
		assert.NotEmpty(t, key.CacheKey)
	})
}
