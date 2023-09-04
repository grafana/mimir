// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/util"
)

const (
	labelNamesQueryCachePrefix  = "ln:"
	labelValuesQueryCachePrefix = "lv:"

	stringParamSeparator = rune(0)
)

func newLabelsQueryCacheRoundTripper(cache cache.Cache, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper {
	delegate := &labelsQueryCache{
		limits: limits,
	}

	return newGenericQueryCacheRoundTripper(cache, delegate, next, logger, newResultsCacheMetrics("label_names_and_values", reg))
}

type labelsQueryCache struct {
	limits Limits
}

func (c *labelsQueryCache) getTTL(userID string) time.Duration {
	return c.limits.ResultsCacheTTLForLabelsQuery(userID)
}

func (c *labelsQueryCache) parseRequest(path string, values url.Values) (*genericQueryRequest, error) {
	var (
		cacheKeyPrefix string
		labelName      string
	)

	// Detect the request type
	switch {
	case strings.HasSuffix(path, labelNamesPathSuffix):
		cacheKeyPrefix = labelNamesQueryCachePrefix
	case labelValuesPathSuffix.MatchString(path):
		cacheKeyPrefix = labelValuesQueryCachePrefix
		labelName = labelValuesPathSuffix.FindStringSubmatch(path)[1]
	default:
		return nil, errors.New("unknown labels API endpoint")
	}

	// Both the label names and label values API endpoints support the same exact parameters (with the same defaults),
	// so in this function there's no distinction between the two.
	startTime, err := parseRequestTimeParam(values, "start", v1.MinTime.UnixMilli())
	if err != nil {
		return nil, err
	}

	endTime, err := parseRequestTimeParam(values, "end", v1.MaxTime.UnixMilli())
	if err != nil {
		return nil, err
	}

	matcherSets, err := parseRequestMatchersParam(values, "match[]")
	if err != nil {
		return nil, err
	}

	return &genericQueryRequest{
		cacheKey:       generateLabelsQueryRequestCacheKey(startTime, endTime, labelName, matcherSets),
		cacheKeyPrefix: cacheKeyPrefix,
	}, nil
}

func generateLabelsQueryRequestCacheKey(startTime, endTime int64, labelName string, matcherSets [][]*labels.Matcher) string {
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

	return b.String()
}

func parseRequestTimeParam(values url.Values, paramName string, defaultValue int64) (int64, error) {
	var value string
	if len(values[paramName]) > 0 {
		value = values[paramName][0]
	}

	if value == "" {
		return defaultValue, nil
	}

	parsed, err := util.ParseTime(value)
	if err != nil {
		return 0, errors.Wrapf(err, "invalid '%s' parameter", paramName)
	}

	return parsed, nil
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
		slices.SortFunc(set, func(a, b *labels.Matcher) bool {
			return compareLabelMatchers(a, b) < 0
		})
	}

	slices.SortFunc(matcherSets, func(a, b []*labels.Matcher) bool {
		idx := 0

		for ; idx < len(a) && idx < len(b); idx++ {
			if c := compareLabelMatchers(a[idx], b[idx]); c != 0 {
				return c < 0
			}
		}

		// All label matchers are equal so far. Check which one has fewer matchers.
		return idx < len(b)
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
