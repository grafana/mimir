// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// perSampleDedupe evaluates HA deduplication per timeseries within a write
// request, so series with different cluster/replica labels (or non-HA series)
// mixed in a single request are handled correctly. This is the experimental
// strategy enabled by the HATrackerPerSampleDedupe limit.
type perSampleDedupe struct {
	limits                            *validation.Overrides
	haTracker                         haTracker
	dedupedSamples                    *prometheus.CounterVec
	nonHASamples                      *prometheus.CounterVec
	discardedSamplesTooManyHaClusters *prometheus.CounterVec
	costAttributionMgr                *costattribution.Manager
}

type replicaState int

const (
	// replicaRejectedUnknown sample is rejected due to an unknown error.
	replicaRejectedUnknown replicaState = 0
	// replicaIsPrimary sample is from the elected primary replica and should be accepted.
	replicaIsPrimary replicaState = 1 << iota
	// replicaNotHA sample doesn't have both HA labels and should be accepted.
	replicaNotHA
	// replicaDeduped sample is from a non-primary replica and should be deduplicated.
	replicaDeduped
	// replicaRejectedTooManyClusters sample is rejected because the tenant has too many HA clusters.
	replicaRejectedTooManyClusters

	replicaAccepted = replicaIsPrimary | replicaNotHA
)

func (r replicaState) equals(other replicaState) bool {
	if other == replicaRejectedUnknown {
		return r == replicaRejectedUnknown
	}
	return r&other != 0
}

type haReplica struct {
	cluster, replica string
}

type replicaInfo struct {
	state replicaState
	// sampleCount counts the number of samples + histograms across all timeseries
	// that map to this replica in the request.
	sampleCount int
	// earliestSampleTimestamp is the earliest sample or histogram timestamp across
	// the timeseries that map to this replica. Used by the HA tracker's
	// failover-sample-timeout check when HATrackerUseSampleTimeForFailover is on,
	// so each cluster's failover decision uses its own samples, not a stale
	// request-wide minimum dominated by a different cluster.
	earliestSampleTimestamp int64
}

var haReplicaSlicePool = sync.Pool{
	New: func() interface{} {
		s := make([]haReplica, 0, 2500)
		return &s
	},
}

func (p *perSampleDedupe) dedupe(ctx context.Context, pushReq *Request, req *mimirpb.WriteRequest, userID, haReplicaLabel, haClusterLabel, group string, now time.Time, next PushFunc) error {
	// Each replicaInfo's earliestSampleTimestamp is seeded with `now` (the
	// per-request ceiling) and then min'd down against each series' first
	// sample/histogram time when HATrackerUseSampleTimeForFailover is on, so
	// every cluster's failover-sample-timeout check uses its own samples
	// instead of a request-wide minimum dominated by another cluster.
	defaultSampleTimestamp := timestamp.FromTime(now)
	useSampleTime := p.limits.HATrackerUseSampleTimeForFailover(userID)
	replicasPtr, replicaInfos := getReplicasAndInfos(req, haReplicaLabel, haClusterLabel, defaultSampleTimestamp, useSampleTime)
	replicas := *replicasPtr
	defer func() {
		var zero haReplica
		for i := range replicas {
			replicas[i] = zero
		}
		haReplicaSlicePool.Put(replicasPtr)
	}()

	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		var clustersAsStrings, replicasAsStrings strings.Builder
		isFirst := true
		for replicaKey := range replicaInfos {
			if !isFirst {
				clustersAsStrings.WriteString(", ")
				replicasAsStrings.WriteString(", ")
			}
			clustersAsStrings.WriteString(replicaKey.cluster)
			replicasAsStrings.WriteString(replicaKey.replica)
			isFirst = false
		}
		span.SetAttributes(
			attribute.String("clusters", clustersAsStrings.String()),
			attribute.String("replicas", replicasAsStrings.String()),
		)
	}

	var errs multierror.MultiError
	samplesPerState, processErr := p.processHaReplicas(ctx, userID, replicaInfos)
	if processErr != nil {
		errs.Add(processErr)
	}

	// Capture labels before sortByAccepted reorders timeseries and mixes rejection types.
	var tooManyClustersLabels []mimirpb.LabelAdapter
	if samplesPerState[replicaRejectedTooManyClusters] > 0 {
		tooManyClustersLabels = findLabelsForRejectedTooManyClusters(replicaInfos, replicas, req)
	}

	lastAccepted := sortByAccepted(req, replicaInfos, replicas)
	removeHAReplicaLabels(req, lastAccepted, replicas, replicaInfos, haReplicaLabel)

	p.updateHADedupeMetrics(userID, group, replicaInfos, samplesPerState, tooManyClustersLabels, now)

	// Free the unaccepted (deduplicated/rejected) timeseries immediately and truncate
	// the slice so downstream middleware only sees accepted timeseries. We must NOT
	// defer restoration of the original slice header because downstream middleware
	// (e.g., validation, relabeling) may compact req.Timeseries via RemoveSliceIndexes,
	// which shifts elements in the backing array. Restoring the original header would
	// then expose duplicate references, causing double-frees when ReuseSlice runs.
	for i := lastAccepted + 1; i < len(req.Timeseries); i++ {
		mimirpb.ReusePreallocTimeseries(&req.Timeseries[i])
	}
	req.Timeseries = req.Timeseries[:lastAccepted+1]

	if len(req.Timeseries) > 0 {
		if pushErr := next(ctx, pushReq); pushErr != nil {
			// Return only the push error: combining it with dedupe errors in a multierror
			// would let errors.As find replicasDidNotMatchError first, masking 5xx with 202.
			return pushErr
		}
	}

	return errs.Err()
}

// replicaObserved consults the HA tracker for the given replica and classifies
// the outcome into a replicaState.
func (p *perSampleDedupe) replicaObserved(ctx context.Context, userID string, replica haReplica, ts int64) (replicaState, error) {
	isAccepted, err := checkSample(ctx, userID, replica.cluster, replica.replica, ts, p.haTracker, p.limits)
	if err != nil {
		var replicasDidNotMatch replicasDidNotMatchError
		var tooManyClusters tooManyClustersError
		switch {
		case errors.As(err, &replicasDidNotMatch):
			return replicaDeduped, err
		case errors.As(err, &tooManyClusters):
			return replicaRejectedTooManyClusters, err
		default:
			return replicaRejectedUnknown, err
		}
	}

	if isAccepted {
		return replicaIsPrimary, nil
	}
	// If there wasn't an error but isAccepted is false that means we didn't find both HA labels.
	return replicaNotHA, nil
}

// processHaReplicas iterates over every unique replica observed in the request,
// consults the HA tracker, and returns the per-state sample counts together
// with a multierror that orders rejection errors before dedupe errors so
// toErrorWithGRPCStatus deterministically picks the higher-severity gRPC status.
// The per-replica earliestSampleTimestamp captured in getReplicasAndInfos is
// used as the sample time for the failover-sample-timeout check, so mixed
// clusters don't share a stale request-wide minimum.
func (p *perSampleDedupe) processHaReplicas(ctx context.Context, userID string, replicaInfos map[haReplica]*replicaInfo) (map[replicaState]int, error) {
	var rejectionErrs, dedupErrs multierror.MultiError
	samplesPerState := make(map[replicaState]int)
	for replicaKey, info := range replicaInfos {
		if info.state.equals(replicaRejectedUnknown) {
			state, replicaErr := p.replicaObserved(ctx, userID, replicaKey, info.earliestSampleTimestamp)
			if replicaErr != nil {
				if state == replicaDeduped {
					dedupErrs.Add(replicaErr)
				} else {
					rejectionErrs.Add(replicaErr)
				}
			}
			info.state = state
		}
		samplesPerState[info.state] += info.sampleCount
	}

	var errs multierror.MultiError
	for _, err := range rejectionErrs {
		errs.Add(err)
	}
	for _, err := range dedupErrs {
		errs.Add(err)
	}
	return samplesPerState, errs.Err()
}

// updateHADedupeMetrics emits per-state metrics after all replicas have been
// classified.
func (p *perSampleDedupe) updateHADedupeMetrics(userID, group string, replicaInfos map[haReplica]*replicaInfo, samplesPerState map[replicaState]int, tooManyClustersLabels []mimirpb.LabelAdapter, now time.Time) {
	for replica, info := range replicaInfos {
		if info.state.equals(replicaDeduped) && info.sampleCount > 0 {
			cluster := strings.Clone(replica.cluster) // Make a copy of this, since it may be retained as labels on our metrics
			p.dedupedSamples.WithLabelValues(userID, cluster).Add(float64(info.sampleCount))
		}
	}
	if samplesPerState[replicaNotHA] > 0 {
		p.nonHASamples.WithLabelValues(userID).Add(float64(samplesPerState[replicaNotHA]))
	}
	if samplesPerState[replicaRejectedTooManyClusters] > 0 {
		p.costAttributionMgr.SampleTracker(userID).IncrementDiscardedSamples(tooManyClustersLabels, float64(samplesPerState[replicaRejectedTooManyClusters]), reasonTooManyHAClusters, now)
		p.discardedSamplesTooManyHaClusters.WithLabelValues(userID, group).Add(float64(samplesPerState[replicaRejectedTooManyClusters]))
	}
}

// getReplicasAndInfos extracts the HA replica for every timeseries and builds
// a map of unique replicas along with a parallel slice giving each timeseries
// its replica. The replica slice is borrowed from haReplicaSlicePool and must
// be returned by the caller.
//
// defaultSampleTimestamp seeds each replicaInfo's earliestSampleTimestamp; when
// trackEarliestPerReplica is true, it is min'd against every timeseries'
// earliest sample/histogram timestamp for that replica so the failover-sample
// -timeout check uses per-cluster samples instead of a request-wide minimum.
func getReplicasAndInfos(req *mimirpb.WriteRequest, haReplicaLabel, haClusterLabel string, defaultSampleTimestamp int64, trackEarliestPerReplica bool) (*[]haReplica, map[haReplica]*replicaInfo) {
	count := len(req.Timeseries)
	replicasPtr := haReplicaSlicePool.Get().(*[]haReplica)
	if cap(*replicasPtr) < count {
		*replicasPtr = make([]haReplica, count)
	} else {
		*replicasPtr = (*replicasPtr)[:count]
	}

	replicas := *replicasPtr
	replicaInfos := make(map[haReplica]*replicaInfo)

	var previousReplica haReplica
	var previousInfo *replicaInfo

	for i, ts := range req.Timeseries {
		cluster, replica := findHALabels(haReplicaLabel, haClusterLabel, ts.Labels)
		currentReplica := haReplica{cluster: cluster, replica: replica}
		replicas[i] = currentReplica

		// If the current replica is the same as the previous one we skip the map
		// lookup and update the counters directly.
		if i > 0 && currentReplica == previousReplica {
			previousInfo.sampleCount += len(ts.Samples) + len(ts.Histograms)
			if trackEarliestPerReplica {
				updateEarliestSampleTimestamp(previousInfo, ts)
			}
			continue
		}

		info, found := replicaInfos[currentReplica]
		if !found {
			// The replica info is stored in a map where the key is the replica itself.
			// The replica labels are references to the request buffer, which will be reused.
			// To safely use the replica as map key, we need to clone its labels.
			currentReplica.cluster = strings.Clone(currentReplica.cluster)
			currentReplica.replica = strings.Clone(currentReplica.replica)
			info = &replicaInfo{earliestSampleTimestamp: defaultSampleTimestamp}
			replicaInfos[currentReplica] = info
		}
		info.sampleCount += len(ts.Samples) + len(ts.Histograms)
		if trackEarliestPerReplica {
			updateEarliestSampleTimestamp(info, ts)
		}
		previousReplica = currentReplica
		previousInfo = info
	}
	return replicasPtr, replicaInfos
}

// updateEarliestSampleTimestamp min's info.earliestSampleTimestamp against the
// first sample and histogram timestamps of ts (matching getEarliestSampleTimestamp).
func updateEarliestSampleTimestamp(info *replicaInfo, ts mimirpb.PreallocTimeseries) {
	if len(ts.Samples) > 0 {
		if t := ts.Samples[0].TimestampMs; t < info.earliestSampleTimestamp {
			info.earliestSampleTimestamp = t
		}
	}
	if len(ts.Histograms) > 0 {
		if t := ts.Histograms[0].Timestamp; t < info.earliestSampleTimestamp {
			info.earliestSampleTimestamp = t
		}
	}
}

// removeHAReplicaLabels strips the replica label from accepted primary series
// (but not from non-HA series) so they dedupe cleanly in storage across
// replica failovers.
func removeHAReplicaLabels(req *mimirpb.WriteRequest, lastAccepted int, replicas []haReplica, replicaInfos map[haReplica]*replicaInfo, haReplicaLabel string) {
	for i := 0; i <= lastAccepted; i++ {
		r := replicas[i]
		if !replicaInfos[r].state.equals(replicaIsPrimary) {
			continue
		}
		req.Timeseries[i].RemoveLabel(haReplicaLabel)
	}
}

// findLabelsForRejectedTooManyClusters returns the labels of the first series
// in the request whose replica was rejected for exceeding the HA-clusters
// limit. Must be called before sortByAccepted reshuffles the request.
func findLabelsForRejectedTooManyClusters(replicaInfos map[haReplica]*replicaInfo, replicas []haReplica, req *mimirpb.WriteRequest) []mimirpb.LabelAdapter {
	var rejectedReplica haReplica
	for replica, info := range replicaInfos {
		if info.state.equals(replicaRejectedTooManyClusters) {
			rejectedReplica = replica
			break
		}
	}
	for i, r := range replicas {
		if r == rejectedReplica {
			return req.Timeseries[i].Labels
		}
	}
	return nil
}

// sortByAccepted partitions the request in-place so accepted timeseries are at
// the front of the slice, and returns the index of the last accepted entry
// (or -1 if the request contains no accepted series).
func sortByAccepted(req *mimirpb.WriteRequest, replicaInfos map[haReplica]*replicaInfo, replicas []haReplica) int {
	left := 0
	right := len(replicas) - 1
	for left < right {
		for left < right && replicaInfos[replicas[left]].state.equals(replicaAccepted) {
			left++
		}
		for right > left && !replicaInfos[replicas[right]].state.equals(replicaAccepted) {
			right--
		}
		if left == right {
			break
		}
		req.Timeseries[left], req.Timeseries[right] = req.Timeseries[right], req.Timeseries[left]
		replicas[left], replicas[right] = replicas[right], replicas[left]
		left++
		right--
	}

	if left == right {
		if replicaInfos[replicas[left]].state.equals(replicaAccepted) {
			return left
		}
		return left - 1
	}

	// left > right, so right is the index of the last accepted element.
	return right
}
