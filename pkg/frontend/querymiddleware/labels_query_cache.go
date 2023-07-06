// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
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

func (c *labelsQueryCache) parseRequest(req *http.Request) (*genericQueryRequest, error) {
	if err := req.ParseForm(); err != nil {
		return nil, err
	}

	var (
		cacheKeyPrefix string
		labelName      string
	)

	// Detect the request type
	switch {
	case strings.HasSuffix(req.URL.Path, labelNamesPathSuffix):
		cacheKeyPrefix = labelNamesQueryCachePrefix
	case labelValuesPathSuffix.MatchString(req.URL.Path):
		cacheKeyPrefix = labelValuesQueryCachePrefix
		labelName = labelValuesPathSuffix.FindStringSubmatch(req.URL.Path)[1]
	default:
		return nil, errors.New("unknown labels API endpoint")
	}

	// Both the label names and label values API endpoints support the same exact parameters (with the same defaults),
	// so in this function there's no distinction between the two.
	startTime, err := parseRequestTimeParam(req, "start", util.PrometheusMinTime.UnixMilli())
	if err != nil {
		return nil, err
	}

	endTime, err := parseRequestTimeParam(req, "end", util.PrometheusMaxTime.UnixMilli())
	if err != nil {
		return nil, err
	}

	matcherSets, err := parseRequestMatchersParam(req, "match[]")
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
	if startTime != util.PrometheusMinTime.UnixMilli() {
		if reminder := startTime % twoHoursMillis; reminder != 0 {
			startTime -= reminder
		}
	}
	if endTime != util.PrometheusMaxTime.UnixMilli() {
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

func parseRequestTimeParam(req *http.Request, paramName string, defaultValue int64) (int64, error) {
	value := req.FormValue(paramName)
	if value == "" {
		return defaultValue, nil
	}

	parsed, err := util.ParseTime(value)
	if err != nil {
		return 0, errors.Wrapf(err, "invalid '%s' parameter", paramName)
	}

	return parsed, nil
}

func parseRequestMatchersParam(req *http.Request, paramName string) ([][]*labels.Matcher, error) {
	matcherSets := make([][]*labels.Matcher, 0, len(req.Form[paramName]))

	for _, value := range req.Form[paramName] {

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
