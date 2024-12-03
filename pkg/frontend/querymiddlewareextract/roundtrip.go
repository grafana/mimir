package querymiddlewareextract

import (
	"regexp"
	"strings"
	"time"
)

const (
	day                                               = 24 * time.Hour
	queryRangePathSuffix                              = "/api/v1/query_range"
	instantQueryPathSuffix                            = "/api/v1/query"
	cardinalityLabelNamesPathSuffix                   = "/api/v1/cardinality/label_names"
	cardinalityLabelValuesPathSuffix                  = "/api/v1/cardinality/label_values"
	cardinalityActiveSeriesPathSuffix                 = "/api/v1/cardinality/active_series"
	cardinalityActiveNativeHistogramMetricsPathSuffix = "/api/v1/cardinality/active_native_histogram_metrics"
	labelNamesPathSuffix                              = "/api/v1/labels"
	remoteReadPathSuffix                              = "/api/v1/read"
	seriesPathSuffix                                  = "/api/v1/series"

	queryTypeInstant                      = "query"
	queryTypeRange                        = "query_range"
	queryTypeRemoteRead                   = "remote_read"
	queryTypeCardinality                  = "cardinality"
	queryTypeLabels                       = "label_names_and_values"
	queryTypeActiveSeries                 = "active_series"
	queryTypeActiveNativeHistogramMetrics = "active_native_histogram_metrics"
	queryTypeOther                        = "other"
)

const (
	// resultsCacheVersion should be increased every time cache should be invalidated (after a bugfix or cache format change).
	resultsCacheVersion = 1

	// cacheControlHeader is the name of the cache control header.
	cacheControlHeader = "Cache-Control"

	// noStoreValue is the value that cacheControlHeader has if the response indicates that the results should not be cached.
	noStoreValue = "no-store"
)

var (
	labelValuesPathSuffix = regexp.MustCompile(`\/api\/v1\/label\/([^\/]+)\/values$`)
)

func IsRangeQuery(path string) bool {
	return strings.HasSuffix(path, queryRangePathSuffix)
}

func IsInstantQuery(path string) bool {
	return strings.HasSuffix(path, instantQueryPathSuffix)
}

func IsCardinalityQuery(path string) bool {
	return strings.HasSuffix(path, cardinalityLabelNamesPathSuffix) ||
		strings.HasSuffix(path, cardinalityLabelValuesPathSuffix)
}

func IsLabelNamesQuery(path string) bool {
	return strings.HasSuffix(path, labelNamesPathSuffix)
}

func IsLabelValuesQuery(path string) bool {
	return labelValuesPathSuffix.MatchString(path)
}

func IsLabelsQuery(path string) bool {
	return IsLabelNamesQuery(path) || IsLabelValuesQuery(path)
}

func IsSeriesQuery(path string) bool {
	return strings.HasSuffix(path, seriesPathSuffix)
}

func IsActiveSeriesQuery(path string) bool {
	return strings.HasSuffix(path, cardinalityActiveSeriesPathSuffix)
}

func IsActiveNativeHistogramMetricsQuery(path string) bool {
	return strings.HasSuffix(path, cardinalityActiveNativeHistogramMetricsPathSuffix)
}

func IsRemoteReadQuery(path string) bool {
	return strings.HasSuffix(path, remoteReadPathSuffix)
}
