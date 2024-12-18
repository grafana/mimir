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
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/grafana/mimir/pkg/util"
)

const (
	labelNamesQueryCachePrefix  = "ln:"
	labelValuesQueryCachePrefix = "lv:"

	stringParamSeparator = rune(0)
)

func newLabelsQueryCacheRoundTripper(
	cache cache.Cache,
	generator CacheKeyGenerator,
	limits Limits,
	next http.RoundTripper,
	logger log.Logger,
	reg prometheus.Registerer,
) http.RoundTripper {
	ttl := &labelsQueryTTL{
		limits: limits,
	}

	return newGenericQueryCacheRoundTripper(cache, generator.LabelValues, ttl, next, logger, newResultsCacheMetrics(queryTypeLabels, reg))
}

type labelsQueryTTL struct {
	limits Limits
}

func (c *labelsQueryTTL) ttl(userID string) time.Duration {
	return c.limits.ResultsCacheTTLForLabelsQuery(userID)
}

func (g DefaultCacheKeyGenerator) LabelValues(r *http.Request) (*GenericQueryCacheKey, error) {
	labelValuesReq, err := g.codec.DecodeLabelsSeriesQueryRequest(r.Context(), r)
	if err != nil {
		return nil, err
	}

	var cacheKeyPrefix string
	switch labelValuesReq.(type) {
	case *PrometheusLabelNamesQueryRequest:
		cacheKeyPrefix = labelNamesQueryCachePrefix
	case *PrometheusLabelValuesQueryRequest:
		cacheKeyPrefix = labelValuesQueryCachePrefix
	}

	labelMatcherSets, err := parseRequestMatchersParam(
		map[string][]string{"match[]": labelValuesReq.GetLabelMatcherSets()},
		"match[]",
	)
	if err != nil {
		return nil, err
	}

	cacheKey := generateLabelsQueryRequestCacheKey(
		labelValuesReq.GetStartOrDefault(),
		labelValuesReq.GetEndOrDefault(),
		labelValuesReq.GetLabelName(),
		labelMatcherSets,
		labelValuesReq.GetLimit(),
	)

	return &GenericQueryCacheKey{
		CacheKey:       cacheKey,
		CacheKeyPrefix: cacheKeyPrefix,
	}, nil
}

func generateLabelsQueryRequestCacheKey(startTime, endTime int64, labelName string, matcherSets [][]*labels.Matcher, limit uint64) string {
	var (
		twoHoursMillis = (2 * time.Hour).Milliseconds()
		b              = strings.Builder{}
	)

	// Align start and end times to default block boundaries. The reason is that both TSDB (so the Mimir ingester)
	// and Mimir store-gateway query the label names and values out of blocks overlapping within the start and end
	// time. This means that for maximum granularity is the block.
	if startTime != v1.MinTime.UnixMilli() {
		if reminder := startTime % twoHoursMillis; reminder != 0 {
			startTime -= reminder
		}
	}
	if endTime != v1.MaxTime.UnixMilli() {
		if reminder := endTime % twoHoursMillis; reminder != 0 {
			endTime += twoHoursMillis - reminder
		}
	}

	// Add start and end time.
	b.WriteString(fmt.Sprintf("%d", startTime))
	b.WriteRune(stringParamSeparator)
	b.WriteString(fmt.Sprintf("%d", endTime))

	// Add label name (if any).
	if labelName != "" {
		b.WriteRune(stringParamSeparator)
		b.WriteString(labelName)
	}

	// Add matcher sets.
	b.WriteRune(stringParamSeparator)
	b.WriteString(util.MultiMatchersStringer(matcherSets).String())

	// if limit is set, then it will be a positive number
	if limit > 0 {
		b.WriteRune(stringParamSeparator)
		b.WriteString(strconv.Itoa(int(limit)))
	}

	return b.String()
}

func parseRequestMatchersParam(values url.Values, paramName string) ([][]*labels.Matcher, error) {
	matcherSets := make([][]*labels.Matcher, 0, len(values[paramName]))

	for _, value := range values[paramName] {

		matchers, err := parser.ParseMetricSelector(value)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid '%s' parameter", paramName)
		}
		matcherSets = append(matcherSets, matchers)
	}

	// Ensure stable sorting (improves query results cache hit ratio).
	for _, set := range matcherSets {
		slices.SortFunc(set, func(a, b *labels.Matcher) int {
			return compareLabelMatchers(a, b)
		})
	}

	slices.SortFunc(matcherSets, func(a, b []*labels.Matcher) int {
		idx := 0

		for ; idx < len(a) && idx < len(b); idx++ {
			if c := compareLabelMatchers(a[idx], b[idx]); c != 0 {
				return c
			}
		}

		// All label matchers are equal so far. Check which one has fewer matchers.
		if idx < len(b) {
			return -1
		}
		if idx < len(a) {
			return 1
		}
		return 0
	})

	return matcherSets, nil
}

func compareLabelMatchers(a, b *labels.Matcher) int {
	if a.Name != b.Name {
		return strings.Compare(a.Name, b.Name)
	}
	if a.Type != b.Type {
		return int(b.Type) - int(a.Type)
	}
	return strings.Compare(a.Value, b.Value)
}
