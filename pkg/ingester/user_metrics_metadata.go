// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/user_metrics_metadata.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"slices"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// userMetricsMetadata allows metric metadata of a tenant to be held by the ingester.
// Metadata is kept as a set as it can come from multiple targets that Prometheus scrapes
// with the same metric name.
type userMetricsMetadata struct {
	limiter *Limiter
	metrics *ingesterMetrics
	userID  string

	mtx              sync.RWMutex
	metricToMetadata map[string]metricMetadataSet

	errorSamplers ingesterErrSamplers
}

func newMetadataMap(l *Limiter, m *ingesterMetrics, errorSamplers ingesterErrSamplers, userID string) *userMetricsMetadata {
	return &userMetricsMetadata{
		metricToMetadata: map[string]metricMetadataSet{},
		limiter:          l,
		metrics:          m,
		errorSamplers:    errorSamplers,
		userID:           userID,
	}
}

func (mm *userMetricsMetadata) add(metric string, metadata *mimirpb.MetricMetadata) error {
	mm.mtx.Lock()
	defer mm.mtx.Unlock()

	// As we get the set, we also validate two things:
	// 1. The user is allowed to create new metrics to add metadata to.
	// 2. If the metadata set is already present, it hasn't reached the limit of metadata we can append.
	set, ok := mm.metricToMetadata[metric]
	if !ok {
		// Verify that the user can create more metric metadata given we don't have a set for that metric name.
		if !mm.limiter.IsWithinMaxMetricsWithMetadataPerUser(mm.userID, len(mm.metricToMetadata)) {
			mm.metrics.discardedMetadataPerUserMetadataLimit.WithLabelValues(mm.userID).Inc()
			return mm.errorSamplers.maxMetadataPerUserLimitExceeded.WrapError(newPerUserMetadataLimitReachedError(mm.limiter.limits.MaxGlobalMetricsWithMetadataPerUser(mm.userID)))
		}
		set = metricMetadataSet{}
		mm.metricToMetadata[metric] = set
	}

	if !mm.limiter.IsWithinMaxMetadataPerMetric(mm.userID, len(set)) {
		mm.metrics.discardedMetadataPerMetricMetadataLimit.WithLabelValues(mm.userID).Inc()
		return mm.errorSamplers.maxMetadataPerMetricLimitExceeded.WrapError(newPerMetricMetadataLimitReachedError(mm.limiter.limits.MaxGlobalMetadataPerMetric(mm.userID), metric))
	}

	// if we have seen this metadata before, it is a no-op and we don't need to change our metrics.
	_, ok = set[*metadata]
	if !ok {
		mm.metrics.memMetadata.Inc()
		mm.metrics.memMetadataCreatedTotal.WithLabelValues(mm.userID).Inc()
	}

	mm.metricToMetadata[metric][*metadata] = time.Now()
	return nil
}

// If deadline is zero, all metadata is purged.
func (mm *userMetricsMetadata) purge(deadline time.Time) {
	mm.mtx.Lock()
	defer mm.mtx.Unlock()
	var deleted int
	for m, s := range mm.metricToMetadata {
		deleted += s.purge(deadline)

		if len(s) <= 0 {
			delete(mm.metricToMetadata, m)
		}
	}

	mm.metrics.memMetadata.Sub(float64(deleted))
	mm.metrics.memMetadataRemovedTotal.WithLabelValues(mm.userID).Add(float64(deleted))
}

func (mm *userMetricsMetadata) toClientMetadata(req *client.MetricsMetadataRequest) []*mimirpb.MetricMetadata {
	if req.Limit == 0 {
		return nil
	}
	mm.mtx.RLock()
	defer mm.mtx.RUnlock()

	rCap := int32(len(mm.metricToMetadata))
	if len(req.MetricNames) > 0 {
		// A MetricNames request returns only the requested subset, so size to it.
		// We assume one metadata record per metric (the common case), so we don't
		// take in account LimitPerMetric. Moreover, sizing by LimitPerMetric would
		// badly over-allocate, since that cap rarely reflects the actual number of
		// per-metric metadata.
		rCap = int32(len(req.MetricNames))
	}
	if req.Limit >= 0 && req.Limit < rCap {
		rCap = req.Limit
	}

	r := make([]*mimirpb.MetricMetadata, 0, rCap)
	addMetricMetadataSet := func(set metricMetadataSet) {
		var lengthPerMetric int32
		for m := range set {
			if req.LimitPerMetric > 0 && lengthPerMetric >= req.LimitPerMetric {
				break
			}
			r = append(r, &m)
			lengthPerMetric++
		}
	}

	// MetricNames takes precedence over the single-name Metric filter: return
	// metadata for each requested name we hold.
	if len(req.MetricNames) > 0 {
		var numMetrics int32
		for _, name := range req.MetricNames {
			if req.Limit > 0 && numMetrics >= req.Limit {
				break
			}
			set, ok := mm.metricToMetadata[name]
			if !ok {
				continue
			}
			addMetricMetadataSet(set)
			numMetrics++
		}
		return r
	}

	// Fallback to the deprecated single-name Metric filter, still honored for the
	// classic /api/v1/metadata?metric= endpoint.
	if metric := req.Metric; metric != "" { //nolint:staticcheck // req.Metric is deprecated but still supported here.
		set := mm.metricToMetadata[metric]
		addMetricMetadataSet(set)
		return r
	}

	// Otherwise return all metric names, up to the limit.
	var numMetrics int32
	for _, set := range mm.metricToMetadata {
		if req.Limit > 0 && numMetrics >= req.Limit {
			break
		}
		addMetricMetadataSet(set)
		numMetrics++
	}
	return r
}

// search returns the names of the metrics whose text matches filter, sorted
// by order, without the names at or before resumeAfter, and at most limit
// names. A limit of 0 means no limit. The text of a metric is its name and
// every HELP text of the metric, separated by newlines, so a NOT term rejects
// a metric when any of its HELP texts holds the term. order must be
// OrderByValueAsc or OrderByValueDesc.
//
// The scan holds the read lock and copies no names or HELP texts. It keeps
// only the first limit matches in order, and it skips the filter for a name
// that cannot get into them.
//
// When caseSensitive is false, search writes the text in lower case, byte for
// byte as strings.ToLower does. The case-folding root of filter then finds
// nothing to change, so it does not allocate a lower-case copy per metric.
func (mm *userMetricsMetadata) search(ctx context.Context, filter storage.Filter, caseSensitive bool, order storage.Ordering, resumeAfter string, limit int) ([]storage.SearchResult, error) {
	descending := order == storage.OrderByValueDesc
	// before reports if name a sorts before name b in the results.
	before := func(a, b string) bool {
		if descending {
			return a > b
		}
		return a < b
	}
	top := searchResultHeap{before: before}
	var text []byte

	mm.mtx.RLock()
	defer mm.mtx.RUnlock()

	if limit > 0 {
		top.results = make([]storage.SearchResult, 0, min(limit, len(mm.metricToMetadata)))
	}
	scanned := 0
	for name, set := range mm.metricToMetadata {
		scanned++
		// A check per name would add a ctx.Err call per name. After a cancel, the
		// interval limits the extra work to searchBatchSize names.
		if scanned%searchBatchSize == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if resumeAfter != "" && !before(resumeAfter, name) {
			continue
		}
		if limit > 0 && len(top.results) == limit && !before(name, top.results[0].Value) {
			continue
		}
		text = appendText(text[:0], name, caseSensitive)
		for metadata := range set {
			text = append(text, '\n')
			text = appendText(text, metadata.Help, caseSensitive)
		}
		// The filter does not keep the value, so a view of the reused buffer is
		// safe.
		accepted, score := filter.Accept(unsafe.String(unsafe.SliceData(text), len(text)))
		if !accepted {
			continue
		}
		result := storage.SearchResult{Value: name, Score: score}
		if limit > 0 && len(top.results) == limit {
			top.replaceLast(result)
		} else {
			top.push(result, limit > 0)
		}
	}
	slices.SortFunc(top.results, func(a, b storage.SearchResult) int {
		if before(a.Value, b.Value) {
			return -1
		}
		return 1
	})
	return top.results, nil
}

// appendText appends s to dst, in lower case when caseSensitive is false. The
// lower case is byte for byte the output of strings.ToLower, which writes an
// invalid UTF-8 byte as utf8.RuneError. Lowering the name and each HELP text
// apart gives the same bytes as lowering the joined text, because no UTF-8
// sequence can hold the '\n' separator.
func appendText(dst []byte, s string, caseSensitive bool) []byte {
	if caseSensitive {
		return append(dst, s...)
	}
	// start is the first byte not yet appended. Bytes that lowering does not
	// change are appended in bulk.
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf && (c < 'A' || c > 'Z') {
			i++
			continue
		}
		dst = append(dst, s[start:i]...)
		if c < utf8.RuneSelf {
			dst = append(dst, c+'a'-'A')
			i++
		} else {
			r, size := utf8.DecodeRuneInString(s[i:])
			dst = utf8.AppendRune(dst, unicode.ToLower(r))
			i += size
		}
		start = i
	}
	return append(dst, s[start:]...)
}

// searchResultHeap holds search results with the result that sorts last at
// index 0, so a better result can replace it in O(log n).
type searchResultHeap struct {
	results []storage.SearchResult
	before  func(a, b string) bool
}

// push adds r. Without heap order, push only appends, because the caller
// sorts all results at the end.
func (h *searchResultHeap) push(r storage.SearchResult, heapOrder bool) {
	h.results = append(h.results, r)
	if !heapOrder {
		return
	}
	for i := len(h.results) - 1; i > 0; {
		parent := (i - 1) / 2
		if !h.before(h.results[parent].Value, h.results[i].Value) {
			break
		}
		h.results[parent], h.results[i] = h.results[i], h.results[parent]
		i = parent
	}
}

// replaceLast replaces the result that sorts last with r.
func (h *searchResultHeap) replaceLast(r storage.SearchResult) {
	h.results[0] = r
	n := len(h.results)
	for i := 0; ; {
		last := i
		if left := 2*i + 1; left < n && h.before(h.results[last].Value, h.results[left].Value) {
			last = left
		}
		if right := 2*i + 2; right < n && h.before(h.results[last].Value, h.results[right].Value) {
			last = right
		}
		if last == i {
			return
		}
		h.results[i], h.results[last] = h.results[last], h.results[i]
		i = last
	}
}

type metricMetadataSet map[mimirpb.MetricMetadata]time.Time

// If deadline is zero time, all metrics are purged.
func (mms metricMetadataSet) purge(deadline time.Time) int {
	var deleted int
	for metadata, t := range mms {
		if deadline.IsZero() || deadline.After(t) {
			delete(mms, metadata)
			deleted++
		}
	}

	return deleted
}
