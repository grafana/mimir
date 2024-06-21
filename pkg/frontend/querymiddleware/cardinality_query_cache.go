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

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util"
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

	return newGenericQueryCacheRoundTripper(cache, generator.LabelValuesCardinality, ttl, next, logger, newResultsCacheMetrics(queryTypeCardinality, reg))
}

type cardinalityQueryTTL struct {
	limits Limits
}

func (c *cardinalityQueryTTL) ttl(userID string) time.Duration {
	return c.limits.ResultsCacheTTLForCardinalityQuery(userID)
}

func (DefaultCacheKeyGenerator) LabelValuesCardinality(r *http.Request) (*GenericQueryCacheKey, error) {
	reqValues, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	switch {
	case strings.HasSuffix(r.URL.Path, cardinalityLabelNamesPathSuffix):
		parsed, err := cardinality.DecodeLabelNamesRequestFromValues(reqValues)
		if err != nil {
			return nil, err
		}

		return &GenericQueryCacheKey{
			CacheKey:       parsed.String(),
			CacheKeyPrefix: cardinalityLabelNamesQueryCachePrefix,
		}, nil
	case strings.HasSuffix(r.URL.Path, cardinalityLabelValuesPathSuffix):
		parsed, err := cardinality.DecodeLabelValuesRequestFromValues(reqValues)
		if err != nil {
			return nil, err
		}

		return &GenericQueryCacheKey{
			CacheKey:       parsed.String(),
			CacheKeyPrefix: cardinalityLabelValuesQueryCachePrefix,
		}, nil
	case strings.HasSuffix(r.URL.Path, cardinalityActiveSeriesPathSuffix):
		parsed, err := cardinality.DecodeActiveSeriesRequestFromValues(reqValues)
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
