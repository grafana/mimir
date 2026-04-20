// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// perRequestDedupe decides whether to accept or dedupe an entire write request
// based on the HA labels of the first series only. This is the legacy behavior
// and assumes all series in a request share the same cluster/replica labels.
type perRequestDedupe struct {
	limits                            *validation.Overrides
	haTracker                         haTracker
	dedupedSamples                    *prometheus.CounterVec
	nonHASamples                      *prometheus.CounterVec
	discardedSamplesTooManyHaClusters *prometheus.CounterVec
	costAttributionMgr                *costattribution.Manager
}

func (p *perRequestDedupe) dedupe(ctx context.Context, pushReq *Request, req *mimirpb.WriteRequest, userID, haReplicaLabel, haClusterLabel, group string, now time.Time, next PushFunc) error {
	cluster, replica := findHALabels(haReplicaLabel, haClusterLabel, req.Timeseries[0].Labels)
	// Make a copy of these, since they may be retained as labels on our metrics, e.g. dedupedSamples.
	cluster, replica = strings.Clone(cluster), strings.Clone(replica)

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("cluster", cluster),
		attribute.String("replica", replica),
	)

	sampleTimestamp := timestamp.FromTime(now)
	if p.limits.HATrackerUseSampleTimeForFailover(userID) {
		sampleTimestamp = getEarliestSampleTimestamp(req, sampleTimestamp)
	}

	numSamples := 0
	for _, ts := range req.Timeseries {
		numSamples += len(ts.Samples) + len(ts.Histograms)
	}

	removeReplica, err := checkSample(ctx, userID, cluster, replica, sampleTimestamp, p.haTracker, p.limits)
	if err != nil {
		if errors.As(err, &replicasDidNotMatchError{}) {
			// These samples have been deduped.
			p.dedupedSamples.WithLabelValues(userID, cluster).Add(float64(numSamples))
		}

		if errors.As(err, &tooManyClustersError{}) {
			p.discardedSamplesTooManyHaClusters.WithLabelValues(userID, group).Add(float64(numSamples))
			p.costAttributionMgr.SampleTracker(userID).IncrementDiscardedSamples(req.Timeseries[0].Labels, float64(numSamples), reasonTooManyHAClusters, now)
		}

		return err
	}

	if removeReplica {
		// If we found both the cluster and replica labels, we only want to include the cluster label when
		// storing series in Mimir. If we kept the replica label we would end up with another series for the same
		// series we're trying to dedupe when HA tracking moves over to a different replica.
		for ix := range req.Timeseries {
			req.Timeseries[ix].RemoveLabel(haReplicaLabel)
		}
	} else {
		// If there wasn't an error but removeReplica is false that means we didn't find both HA labels.
		p.nonHASamples.WithLabelValues(userID).Add(float64(numSamples))
	}

	return next(ctx, pushReq)
}

// getEarliestSampleTimestamp returns the earliest sample or histogram timestamp
// across all timeseries in the request, or defaultTimestamp if no sample predates it.
func getEarliestSampleTimestamp(req *mimirpb.WriteRequest, defaultTimestamp int64) int64 {
	earliestSampleTimestamp := defaultTimestamp
	for _, ts := range req.Timeseries {
		if len(ts.Samples) > 0 {
			if tsms := ts.Samples[0].TimestampMs; tsms < earliestSampleTimestamp {
				earliestSampleTimestamp = tsms
			}
		}
		if len(ts.Histograms) > 0 {
			if tsms := ts.Histograms[0].Timestamp; tsms < earliestSampleTimestamp {
				earliestSampleTimestamp = tsms
			}
		}
	}
	return earliestSampleTimestamp
}
