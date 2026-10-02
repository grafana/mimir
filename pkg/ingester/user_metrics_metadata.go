// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/user_metrics_metadata.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"slices"
	"sync"
	"time"

	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
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

// searchHelp matches filter against the HELP text of every metadata record
// held for each metric name, and returns the matching names as search
// results, honoring order, resumeAfter and limit.
//
// A metric name matches if ANY of its (possibly several, conflicting) HELP
// records match filter: HELP text is informational only, so a tenant
// searching for a term in HELP expects to find the metric name regardless of
// which scrape target's record happened to hold the matching text.
func (mm *userMetricsMetadata) searchHelp(filter storage.Filter, order storage.Ordering, resumeAfter string, limit int) storage.SearchResultSet {
	// Copy out the names and HELP strings under RLock, then release the lock
	// before running the (possibly O(N*M)) filter evaluation. add() takes
	// mtx.Lock() on every Push-touching-metadata write, so holding RLock
	// across the scan would stall writes for this tenant.
	mm.mtx.RLock()
	helpsByName := make(map[string][]string, len(mm.metricToMetadata))
	names := make([]string, 0, len(mm.metricToMetadata))
	for name, set := range mm.metricToMetadata {
		helps := make([]string, 0, len(set))
		for metadata := range set {
			helps = append(helps, metadata.Help)
		}
		helpsByName[name] = helps
		names = append(names, name)
	}
	mm.mtx.RUnlock()

	slices.Sort(names)

	wrapped := streaminglabelvalues.ApplyResumeAfter(&helpMatchFilter{helpsByName: helpsByName, inner: filter}, resumeAfter, order)
	results := storage.ApplySearchHints(names, &storage.SearchHints{Filter: wrapped, OrderBy: order, Limit: limit})
	return storage.NewSearchResultSetFromSlice(results, nil)
}

// helpMatchFilter adapts a HELP-text storage.Filter into a Filter over metric
// names: Accept(name) matches if inner matches any of name's HELP records.
// A nil inner accepts every name, matching BuildFilter's own nil-filter
// convention.
type helpMatchFilter struct {
	helpsByName map[string][]string
	inner       storage.Filter
}

func (f *helpMatchFilter) Accept(name string) (bool, float64) {
	if f.inner == nil {
		return true, 1.0
	}
	for _, help := range f.helpsByName[name] {
		if accepted, score := f.inner.Accept(help); accepted {
			return true, score
		}
	}
	return false, 0
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
