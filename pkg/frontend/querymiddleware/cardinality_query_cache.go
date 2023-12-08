// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/cardinality"
)

const (
	cardinalityLabelNamesQueryCachePrefix   = "cn:"
	cardinalityLabelValuesQueryCachePrefix  = "cv:"
	cardinalityActiveSeriesQueryCachePrefix = "ca:"
)

func newCardinalityQueryCacheRoundTripper(cache cache.Cache, generator CacheKeyGenerator, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper {
	ttl := &cardinalityQueryTTL{
		limits: limits,
	}

	return newGenericQueryCacheRoundTripper(cache, generator.LabelValuesCardinality, ttl, next, logger, newResultsCacheMetrics("cardinality", reg))
}

type cardinalityQueryTTL struct {
	limits Limits
}

func (c *cardinalityQueryTTL) ttl(userID string) time.Duration {
	return c.limits.ResultsCacheTTLForCardinalityQuery(userID)
}

func (DefaultCacheKeyGenerator) LabelValuesCardinality(_ context.Context, path string, values url.Values) (*GenericQueryCacheKey, error) {
	switch {
	case strings.HasSuffix(path, cardinalityLabelNamesPathSuffix):
		parsed, err := cardinality.DecodeLabelNamesRequestFromValues(values)
		if err != nil {
			return nil, err
		}

		return &GenericQueryCacheKey{
			CacheKey:       parsed.String(),
			CacheKeyPrefix: cardinalityLabelNamesQueryCachePrefix,
		}, nil
	case strings.HasSuffix(path, cardinalityLabelValuesPathSuffix):
		parsed, err := cardinality.DecodeLabelValuesRequestFromValues(values)
		if err != nil {
			return nil, err
		}

		return &GenericQueryCacheKey{
			CacheKey:       parsed.String(),
			CacheKeyPrefix: cardinalityLabelValuesQueryCachePrefix,
		}, nil
	case strings.HasSuffix(path, cardinalityActiveSeriesPathSuffix):
		parsed, err := cardinality.DecodeActiveSeriesRequestFromValues(values)
		if err != nil {
			return nil, err
		}

		return &GenericQueryCacheKey{
			CacheKey:       parsed.String(),
			CacheKeyPrefix: cardinalityActiveSeriesQueryCachePrefix,
		}, nil
	default:
		return nil, errors.New("unknown cardinality API endpoint")
	}
}
