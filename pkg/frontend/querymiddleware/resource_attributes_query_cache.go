// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util"
)

const (
	resourceAttributesQueryCachePrefix       = "ra:"
	resourceAttributesSeriesQueryCachePrefix = "rs:"
)

func newResourceAttributesQueryCacheRoundTripper(cache cache.Cache, generator CacheKeyGenerator, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper {
	ttl := &resourceAttributesQueryTTL{
		limits: limits,
	}

	return newGenericQueryCacheRoundTripper(cache, generator.ResourceAttributes, ttl, next, logger, newResultsCacheMetrics(queryTypeResourceAttributes, reg))
}

type resourceAttributesQueryTTL struct {
	limits Limits
}

func (c *resourceAttributesQueryTTL) ttl(userID string) time.Duration {
	return c.limits.ResultsCacheTTLForResourceAttributesQuery(userID)
}

func (DefaultCacheKeyGenerator) ResourceAttributes(r *http.Request) (*GenericQueryCacheKey, error) {
	reqValues, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	var cacheKeyPrefix string
	switch {
	case IsResourceAttributesSeriesQuery(r.URL.Path):
		cacheKeyPrefix = resourceAttributesSeriesQueryCachePrefix
	case IsResourceAttributesQuery(r.URL.Path):
		cacheKeyPrefix = resourceAttributesQueryCachePrefix
	default:
		return nil, fmt.Errorf("unknown resource attributes API endpoint: %s", r.URL.Path)
	}

	cacheKey, err := generateResourceAttributesQueryCacheKey(r.URL.Path, reqValues)
	if err != nil {
		return nil, err
	}

	return &GenericQueryCacheKey{
		CacheKey:       cacheKey,
		CacheKeyPrefix: cacheKeyPrefix,
	}, nil
}

func generateResourceAttributesQueryCacheKey(path string, values url.Values) (string, error) {
	var (
		twoHoursMillis = (2 * time.Hour).Milliseconds()
		b              = strings.Builder{}
	)

	// Parse start/end times. When missing, default to MinTime / now.
	// Unlike the labels cache (which uses MaxTime for open-ended queries),
	// the resource attributes handler defaults end=now, so we must use
	// time.Now() here to produce a cache key that changes over time.
	startTime, _ := util.ParseTime(values.Get("start"))
	if startTime == 0 {
		startTime = v1.MinTime.UnixMilli()
	}
	endTime, _ := util.ParseTime(values.Get("end"))
	if endTime == 0 {
		endTime = time.Now().UnixMilli()
	}

	// Align to 2-hour block boundaries (same as labels cache).
	if startTime != v1.MinTime.UnixMilli() {
		if remainder := startTime % twoHoursMillis; remainder != 0 {
			startTime -= remainder
		}
	}
	if remainder := endTime % twoHoursMillis; remainder != 0 {
		endTime += twoHoursMillis - remainder
	}

	b.WriteString(strconv.FormatInt(startTime, 10))
	b.WriteRune(stringParamSeparator)
	b.WriteString(strconv.FormatInt(endTime, 10))

	// Add sorted matchers or resource.attr filters depending on the endpoint.
	if strings.HasSuffix(path, resourceAttributesSeriesPathSuffix) {
		// /api/v1/resources/series uses resource.attr=key:value filters.
		filters := slices.Clone(values["resource.attr"])
		slices.Sort(filters)
		for _, f := range filters {
			b.WriteRune(stringParamSeparator)
			b.WriteString(f)
		}
	} else {
		// /api/v1/resources uses match[] matchers.
		matcherSets, err := parseRequestMatchersParam(values, "match[]")
		if err != nil {
			return "", err
		}
		b.WriteRune(stringParamSeparator)
		b.WriteString(util.MultiMatchersStringer(matcherSets).String())
	}

	// Add limit if present.
	if limitStr := values.Get("limit"); limitStr != "" {
		if limit, err := strconv.ParseInt(limitStr, 10, 64); err == nil && limit > 0 {
			b.WriteRune(stringParamSeparator)
			b.WriteString(strconv.FormatInt(limit, 10))
		}
	}

	return b.String(), nil
}
