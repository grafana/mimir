// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/cardinality"
)

const (
	cardinalityLabelNamesQueryCachePrefix  = "cn:"
	cardinalityLabelValuesQueryCachePrefix = "cv:"
)

func newCardinalityQueryCacheRoundTripper(cache cache.Cache, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper {
	delegate := &cardinalityQueryCache{
		limits: limits,
	}

	return newGenericQueryCacheRoundTripper(cache, delegate, next, logger, newResultsCacheMetrics("cardinality", reg))
}

type cardinalityQueryCache struct {
	limits Limits
}

func (c *cardinalityQueryCache) getTTL(userID string) time.Duration {
	return c.limits.ResultsCacheTTLForCardinalityQuery(userID)
}

func (c *cardinalityQueryCache) parseRequest(req *http.Request) (*genericQueryRequest, error) {
	switch {
	case strings.HasSuffix(req.URL.Path, cardinalityLabelNamesPathSuffix):
		parsed, err := cardinality.DecodeLabelNamesRequest(req)
		if err != nil {
			return nil, err
		}

		return &genericQueryRequest{
			cacheKey:       parsed.String(),
			cacheKeyPrefix: cardinalityLabelNamesQueryCachePrefix,
		}, nil
	case strings.HasSuffix(req.URL.Path, cardinalityLabelValuesPathSuffix):
		parsed, err := cardinality.DecodeLabelValuesRequest(req)
		if err != nil {
			return nil, err
		}

		return &genericQueryRequest{
			cacheKey:       parsed.String(),
			cacheKeyPrefix: cardinalityLabelValuesQueryCachePrefix,
		}, nil
	default:
		return nil, errors.New("unknown cardinality API endpoint")
	}
}
