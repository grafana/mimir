// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/user_metrics_metadata.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"sync"
	"time"

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
	mm.mtx.RLock()
	defer mm.mtx.RUnlock()
	rCap := int32(len(mm.metricToMetadata))
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
	if req.Limit == 0 {
		return r
	}
	if req.Metric != "" {
		set := mm.metricToMetadata[req.Metric]
		addMetricMetadataSet(set)
		return r
	}
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
