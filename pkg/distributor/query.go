// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/query.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/grafana/loki/blob/main/pkg/util/loser/tree.go
// Provenance-includes-location: https://github.com/grafana/dskit/blob/main/loser/loser.go

package distributor

import (
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/instrument"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/readcache"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	// readNoExtend is a ring.Operation that only selects instances marked as ring.ACTIVE.
	// This should mirror the operation used when choosing ingesters to write series to (ring.WriteNoExtend).
	// We include ring.PENDING instances as well to ensure we don't miss any instances that have
	// recently started and we may not have observed in the ring.ACTIVE state yet.
	// In the case where an ingester has just started, queriers may have only observed the ingester in the PENDING state,
	// but distributors may have observed the ingester in the ACTIVE state and started sending samples.
	readNoExtend = ring.NewOp([]ring.InstanceState{ring.ACTIVE, ring.PENDING}, nil)

	errStreamClosed = cancellation.NewErrorf("stream closed")
)

// QueryExemplars returns exemplars with timestamp between from and to, for the series matching the input series
// label matchers. The exemplars in the response are sorted by series labels.
func (d *Distributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*ingester_client.ExemplarQueryResponse, error) {
	var result *ingester_client.ExemplarQueryResponse
	err := instrument.CollectedRequest(ctx, "Distributor.QueryExemplars", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToExemplarQueryRequest(from, to, matchers...)
		if err != nil {
			return err
		}

		replicationSets, _, err := d.getIngesterReplicationSetsForQuery(ctx, nil)
		if err != nil {
			return err
		}

		results, err := forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, client ingester_client.IngesterClient) (*ingester_client.ExemplarQueryResponse, error) {
			return client.QueryExemplars(ctx, req)
		})
		if err != nil {
			return err
		}
		defer func() {
			for _, r := range results {
				r.FreeBuffer()
			}
		}()

		result = mergeExemplarQueryResponses(results)

		s := trace.SpanFromContext(ctx)
		s.SetAttributes(attribute.Int("series", len(result.Timeseries)))
		return nil
	})
	return result, err
}

// QueryStream queries multiple ingesters via the streaming interface and returns a big ol' set of chunks.
func (d *Distributor) QueryStream(ctx context.Context, queryMetrics *stats.QueryMetrics, from, to model.Time, projectionInclude bool, projectionLabels []string, matchers ...*labels.Matcher) (ingester_client.CombinedQueryStreamResponse, error) {
	var result ingester_client.CombinedQueryStreamResponse
	// Allocate the per-query readcache hit tracker outside the
	// instrument.CollectedRequest closure so the histogram is
	// always observed exactly once per call — including the error
	// paths below where the closure returns early. Zero observations
	// (errored or all-ingester queries) are intentional: they form
	// the baseline that the readcache routing migration drifts
	// upward from.
	hits := newReadcacheHitTracker()
	defer func() {
		d.queryReadcacheInstancesHit.Observe(float64(hits.count()))
	}()

	err := instrument.CollectedRequest(ctx, "Distributor.QueryStream", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToQueryRequest(from, to, projectionInclude, projectionLabels, matchers)
		if err != nil {
			return err
		}

		req.StreamingChunksBatchSize = d.cfg.StreamingChunksPerIngesterSeriesBufferSize

		var (
			replicationSets     []ring.ReplicationSet
			partitionByInstance map[string]int32
		)
		if d.shouldRouteReadToReadcache(ctx) {
			// Nautilus-only tenant: resolve partitions from the
			// rebalancer assignment log and route exclusively to
			// readcache. No ingester fallback.
			userID, err := tenant.TenantID(ctx)
			if err != nil {
				return err
			}
			replicationSets, partitionByInstance, err = d.getReadcacheReplicationSetsForQuery(userID, from, to, matchers)
			if err != nil {
				return err
			}
		} else {
			replicationSets, partitionByInstance, err = d.getIngesterReplicationSetsForQuery(ctx, matchers)
			if err != nil {
				return err
			}
		}

		result, err = d.queryIngesterStream(ctx, replicationSets, partitionByInstance, hits, req, queryMetrics)
		if err != nil {
			return err
		}

		s := trace.SpanFromContext(ctx)
		s.SetAttributes(attribute.Int("streaming-series", len(result.StreamingSeries)))
		s.SetAttributes(attribute.Int("readcache-instances-hit", hits.count()))
		return nil
	})

	return result, err
}

// getIngesterReplicationSetsForQuery returns a list of ring.ReplicationSet, containing ingester instances,
// that must be queried for a read operation.
//
// If multiple ring.ReplicationSets are returned, each must be queried separately, and results merged.
//
// When ingest storage is enabled and matchers contain an exact __name__ match, only the partitions
// that serve the metric name's hash range are returned (query locality optimization).
//
// The second return value is a map from ingester instance ID to the
// partition ID that the metric-name resolution mapped that instance
// to. It is non-nil only on the named path; on the full-fanout path
// it is nil. The QueryStream caller uses it to set
// QueryRequest.QueryAttributionHint for query-load attribution on the
// ingester.
func (d *Distributor) getIngesterReplicationSetsForQuery(ctx context.Context, matchers []*labels.Matcher) ([]ring.ReplicationSet, map[string]int32, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	if d.cfg.IngestStorageConfig.Enabled {
		r := d.partitionsRing

		// Build a subring to query. We use ShuffleShardWithLookback() to limit the partitions to query
		// to the tenant's shard (when shuffle sharding is enabled) and to filter out inactive partitions
		// that have been inactive for longer than the lookback period.
		shardSize := 0
		if d.cfg.ShuffleShardingEnabled {
			shardSize = d.limits.IngestionPartitionsTenantShardSize(userID)
		}
		r, err = r.ShuffleShardWithLookback(userID, shardSize, d.cfg.IngestersLookbackPeriod, time.Now())
		if err != nil {
			return nil, nil, err
		}

		if metricName, ok := extractExactMetricName(matchers); ok {
			sets, partitionByInstance, err := d.getReplicationSetsForMetricName(r, userID, metricName)
			if err != nil {
				return nil, nil, err
			}
			return sets, partitionByInstance, nil
		}

		sets, err := r.GetReplicationSetsForOperation(readNoExtend)
		if err != nil {
			return nil, nil, err
		}
		return sets, nil, nil
	}

	// Lookup ingesters ring because ingest storage is disabled.
	shardSize := d.limits.IngestionTenantShardSize(userID)
	r := d.ingestersRing

	// If tenant uses shuffle sharding, we should only query ingesters which are part of the tenant's subring.
	if lookbackPeriod := d.cfg.IngestersLookbackPeriod; d.cfg.ShuffleShardingEnabled && lookbackPeriod > 0 {
		r = r.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, time.Now())
	}

	replicationSet, err := r.GetReplicationSetForOperation(readNoExtend)
	if err != nil {
		return nil, nil, err
	}

	return []ring.ReplicationSet{replicationSet}, nil, nil
}

// extractExactMetricName returns the metric name from an exact __name__="..." matcher.
// Returns ("", false) if no such matcher exists.
func extractExactMetricName(matchers []*labels.Matcher) (string, bool) {
	for _, m := range matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual && m.Value != "" {
			return m.Value, true
		}
	}
	return "", false
}

// getReplicationSetsForMetricName returns ReplicationSets for only the partitions that
// serve the hash range of the given metric name. This is the query locality optimization:
// instead of fanning out to all partitions, we only query the ones that can contain
// series for this metric.
//
// The second return value maps each owner instance ID (across all
// returned replication sets) to the partition ID it was selected for.
// Used by QueryStream to populate QueryRequest.QueryAttributionHint
// so the ingester can bucket samples-scanned per partition. Each
// instance in the returned sets appears at most once because the
// resolved partitions are disjoint.
func (d *Distributor) getReplicationSetsForMetricName(r *ring.PartitionInstanceRing, userID string, metricName string) ([]ring.ReplicationSet, map[string]int32, error) {
	partRing := r.PartitionRing()
	lo, hi := mimirpb.MetricNameHashRange(userID, metricName)

	// Find all partition IDs that own tokens in the hash range [lo, hi].
	// We sample keys across the range to find partition boundaries.
	partitionIDs := make(map[int32]struct{})
	const sampleCount = 64
	step := uint32(1)
	rangeSize := hi - lo
	if rangeSize > sampleCount {
		step = rangeSize / sampleCount
	}
	for key := lo; key <= hi; key += step {
		partID, err := partRing.ActivePartitionForKey(key)
		if err != nil {
			if errors.Is(err, ring.ErrNoActivePartitionFound) {
				continue
			}
			return nil, nil, err
		}
		partitionIDs[partID] = struct{}{}
	}
	// Always check the endpoint to avoid missing a partition at the boundary.
	if partID, err := partRing.ActivePartitionForKey(hi); err == nil {
		partitionIDs[partID] = struct{}{}
	}

	if len(partitionIDs) == 0 {
		return nil, nil, ring.ErrNoActivePartitionFound
	}

	// Build ReplicationSets for the filtered partitions.
	instRing := r.InstanceRing()
	result := make([]ring.ReplicationSet, 0, len(partitionIDs))
	partitionByInstance := make(map[string]int32, len(partitionIDs))

	for partID := range partitionIDs {
		ownerIDs := partRing.PartitionOwnerIDs(partID)
		instances := make([]ring.InstanceDesc, 0, len(ownerIDs))

		for _, instanceID := range ownerIDs {
			instance, err := instRing.GetInstance(instanceID)
			if err != nil {
				continue
			}
			instances = append(instances, instance)
			partitionByInstance[instance.Id] = partID
		}

		if len(instances) == 0 {
			return nil, nil, fmt.Errorf("partition %d: %w", partID, ring.ErrTooManyUnhealthyInstances)
		}

		zonesBuffer := make([]string, 0, 3)
		for _, inst := range instances {
			found := false
			for _, z := range zonesBuffer {
				if z == inst.Zone {
					found = true
					break
				}
			}
			if !found {
				zonesBuffer = append(zonesBuffer, inst.Zone)
			}
		}

		result = append(result, ring.ReplicationSet{
			Instances:            instances,
			ZoneAwarenessEnabled: true,
			MaxUnavailableZones:  len(zonesBuffer) - 1,
		})
	}
	return result, partitionByInstance, nil
}

// getReadcacheReplicationSetsForQuery builds the replication sets for
// a read on a nautilus-only tenant. It is the read-path analogue of
// the write path's getKeysByAssignment: partitions are resolved from
// the rebalancer's range->partition assignment log (the same log the
// write path consults), not the production partition ring, because a
// nautilus tenant's data lives on the nautilus_ingest topic whose
// partition universe is defined by that log.
//
// Resolution is interval-aware. The query's sample-time range
// [from, to] is padded into the wall-clock window during which those
// samples could have been written, so the read picks up every
// partition that owned the hashrange across any range->partition move
// inside the query interval (not just the partition that owns it at
// the current instant):
//   - An exact __name__ matcher narrows the query to the metric
//     name's hash range [lo, hi] (mimirpb.MetricNameHashRange) and
//     only the partitions whose tiles overlapped that range during the
//     window are queried (PartitionsOverlappingInterval).
//   - Otherwise the query fans out to every partition that owned any
//     part of the keyspace during the window (AllPartitionsDuring).
//
// The partition->readcache instance dimension is resolved over the
// SAME window via readcacheassignment.Log.OwnersDuring: every readcache
// that owned the partition at any point during [w0,w1) is queried,
// because after a partition->readcache move each owner holds only the
// slice it ingested while it owned the partition (the new owner starts
// at the Kafka live edge; the previous owner keeps its frozen slice).
// The per-instance merge dedups the safety-window overlap band.
//
// Each (owner, partition) pair becomes its own single-instance
// ring.ReplicationSet. InstanceDesc.Id is the synthetic
// "owner/p<partition>" key (unique per pair, since a readcache owns
// many partitions and a partition may have several owners across the
// window); InstanceDesc.Addr carries the real readcache instance to
// dial, so queryClientForInstance dials that specific owner rather
// than re-resolving to a single current owner. partitionByInstance
// maps the synthetic Id to the partition for the QueryAttributionHint.
//
// Any inability to resolve (no live assignment log, no live readcache
// log, an uncovered partition, or a partition with no owner during the
// window) returns errReadcacheRoutingUnavailable. There is no ingester
// fallback for nautilus-only tenants.
func (d *Distributor) getReadcacheReplicationSetsForQuery(userID string, from, to model.Time, matchers []*labels.Matcher) ([]ring.ReplicationSet, map[string]int32, error) {
	now := d.now()

	log := d.GetNautilusLog()
	if log == nil {
		return nil, nil, newReadcacheRoutingUnavailableError("no live assignment log snapshot is available")
	}
	rcLog := d.GetReadcacheLog()
	if rcLog == nil {
		return nil, nil, newReadcacheRoutingUnavailableError("no live readcache assignment log snapshot is available")
	}

	// Pad the sample-time range into the wall-clock window during
	// which those samples could have been written. A sample stamped s
	// is accepted at wallclock w in [s-creationGrace, s+oooWindow], so
	// over s in [from,to] the union is [from-creationGrace,
	// to+oooWindow]. Clamp the lower bound to now-queryIngestersWithin:
	// readcache holds nothing older, and the querier already clamps the
	// request range to the same horizon.
	// The log lookups use half-open intersection (lease matches iff
	// From < w1), while w1 is meant to be the last wallclock instant a
	// relevant write could land, inclusive. Add one millisecond (model.
	// Time resolution) so a lease starting exactly at that instant is
	// matched.
	w0 := from.Time().Add(-d.limits.CreationGracePeriod(userID))
	w1 := to.Time().Add(d.limits.OutOfOrderTimeWindow(userID)).Add(time.Millisecond)
	if qiw := d.limits.QueryIngestersWithin(userID); qiw > 0 {
		if floor := now.Add(-qiw); w0.Before(floor) {
			w0 = floor
		}
	}
	// Clamp the upper bound to the present: samples written after this
	// instant cannot be visible to this query, so ownership beyond the
	// present is irrelevant to routing. Without the clamp, a large OOO
	// window pushes w1 far into the future and picks up the successor
	// leases the rebalancer pre-issues LeaseLookahead before each
	// rotation; when such a lease moves a range to a partition whose
	// readcache assignment isn't live yet, OwnersDuring comes up empty
	// and the query hard-fails for the length of the lookahead, every
	// round.
	if ceil := now.Add(time.Millisecond); w1.After(ceil) {
		w1 = ceil
	}

	var partitionIDs []int32
	if metricName, ok := extractExactMetricName(matchers); ok {
		lo, hi := mimirpb.MetricNameHashRange(userID, metricName)
		partitionIDs = log.PartitionsOverlappingInterval(w0, w1, lo, hi)
	} else {
		partitionIDs = log.AllPartitionsDuring(w0, w1)
	}
	if len(partitionIDs) == 0 {
		return nil, nil, newReadcacheRoutingUnavailableError("assignment log resolved no partitions for the query")
	}

	sets := make([]ring.ReplicationSet, 0, len(partitionIDs))
	partitionByInstance := make(map[string]int32, len(partitionIDs))
	for _, partID := range partitionIDs {
		// Every readcache that owned partID during the window, not
		// just the owner at `now`: a query spanning a partition move
		// must reach both the previous owner (frozen slice) and the
		// current owner (live slice).
		owners := rcLog.OwnersDuring(partID, w0, w1)
		if len(owners) == 0 {
			return nil, nil, newReadcacheRoutingUnavailableError(fmt.Sprintf("partition %d had no readcache owner during the query window", partID))
		}
		for _, owner := range owners {
			// Synthetic, (owner, partition)-unique instance ID: a
			// readcache owns multiple partitions and a partition may
			// have several owners across the window, so the key must
			// combine both. Addr carries the real instance to dial.
			instanceID := fmt.Sprintf("%s/p%d", owner, partID)
			sets = append(sets, ring.ReplicationSet{
				Instances: []ring.InstanceDesc{{Id: instanceID, Addr: owner}},
			})
			partitionByInstance[instanceID] = partID
		}
	}
	return sets, partitionByInstance, nil
}

// mergeExemplarSets merges and dedupes two sets of already sorted exemplar pairs.
// Both a and b should be lists of exemplars from the same series.
// Defined here instead of pkg/util to avoid a import cycle.
func mergeExemplarSets(a, b []mimirpb.Exemplar) []mimirpb.Exemplar {
	result := make([]mimirpb.Exemplar, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].TimestampMs < b[j].TimestampMs {
			result = append(result, a[i])
			i++
		} else if a[i].TimestampMs > b[j].TimestampMs {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	// Add the rest of a or b. One of them is empty now.
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

func mergeExemplarQueryResponses(results []*ingester_client.ExemplarQueryResponse) *ingester_client.ExemplarQueryResponse {
	var keys []string
	exemplarResults := make(map[string]mimirpb.TimeSeries)
	for _, r := range results {
		for _, ts := range r.Timeseries {
			lbls := mimirpb.FromLabelAdaptersToKeyString(ts.Labels)
			e, ok := exemplarResults[lbls]
			if !ok {
				exemplarResults[lbls] = ts
				keys = append(keys, lbls)
			} else {
				// Merge in any missing values from another ingesters exemplars for this series.
				ts.Exemplars = mergeExemplarSets(e.Exemplars, ts.Exemplars)
				exemplarResults[lbls] = ts
			}
		}
	}

	// Query results from each ingester were sorted, but are not necessarily still sorted after merging.
	slices.Sort(keys)

	result := make([]mimirpb.TimeSeries, len(exemplarResults))
	for i, k := range keys {
		result[i] = exemplarResults[k]
		result[i].MakeReferencesSafeToRetain()
	}

	return &ingester_client.ExemplarQueryResponse{Timeseries: result}
}

type ingesterQueryResult struct {
	streamingSeries seriesChunksStream
}

// queryIngesterStream queries the ingesters using the gRPC streaming API.
//
// When partitionByInstance is non-nil and a readcache pool is
// configured, each ingester instance is opportunistically remapped
// to the readcache pod currently owning its partition (per the log
// streamed from the rebalancer); on lookup miss, expired lease, or
// transport error during dial, the call falls back transparently to
// the ingester. The tenant runtime knob ReadcacheReadRouting gates
// the swap (see shouldRouteReadToReadcache).
//
// Each readcache instance committed to (including any warmup-fallback
// to a previous lease owner) is recorded in hits so QueryStream can
// emit the per-query histogram observation. hits is per-call and
// must not be nil; record() is a no-op when no readcache is dialed,
// which is the desired behaviour for the all-ingester baseline.
func (d *Distributor) queryIngesterStream(ctx context.Context, replicationSets []ring.ReplicationSet, partitionByInstance map[string]int32, hits *readcacheHitTracker, req *ingester_client.QueryRequest, queryMetrics *stats.QueryMetrics) (ingester_client.CombinedQueryStreamResponse, error) {
	queryLimiter := limiter.QueryLimiterFromContextWithFallback(ctx)
	memoryTracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
	if err != nil {
		return ingester_client.CombinedQueryStreamResponse{}, err
	}
	reqStats := stats.FromContext(ctx)

	// queryIngester MUST call cancelContext once processing is completed in order to release resources. It's required
	// by ring.DoMultiUntilQuorumWithoutSuccessfulContextCancellation() to properly release resources.
	queryIngester := func(ctx context.Context, ing *ring.InstanceDesc, cancelContext context.CancelCauseFunc) (ingesterQueryResult, error) {
		log, ctx := spanlogger.New(ctx, d.log, tracer, "Distributor.queryIngesterStream")
		cleanup := func() {
			log.Finish()
			cancelContext(errStreamClosed)
		}

		var stream ingester_client.Ingester_QueryStreamClient
		closeStream := true
		defer func() {
			if closeStream {
				if stream != nil {
					if err := util.CloseAndExhaust[*ingester_client.QueryStreamResponse](stream); err != nil {
						level.Warn(log).Log("msg", "closing ingester client stream failed", "err", err)
					}
				}

				cleanup()
			}
		}()

		log.SetTag("ingester_address", ing.Addr)
		log.SetTag("ingester_zone", ing.Zone)

		var result ingesterQueryResult

		partID, hasPart := partitionByInstance[ing.Id]

		queryClient, viaReadcache, err := d.queryClientForInstance(ctx, *ing, partitionByInstance, hits, log)
		if err != nil {
			return result, err
		}

		// When the call is routed to a readcache pod, stamp the
		// resolved partition into the request so the pod scopes its
		// read to exactly that partition's TSDB and attributes the
		// scanned-samples query load to it. The request is shared
		// across all per-instance goroutines, so copy it before
		// mutating the hint. Ingester calls keep req untouched (the
		// ingester ignores the hint after the nautilus revert).
		streamReq := req
		if viaReadcache && hasPart {
			r := *req
			r.QueryAttributionHint = &ingester_client.QueryAttributionHint{PartitionId: partID}
			streamReq = &r
		}

		// Surface readcache fan-out in the per-query stats (and the
		// query-frontend's "query stats" log line): one count per
		// QueryStream RPC issued to a readcache instance, including
		// the warming fallback below.
		queryStats := stats.FromContext(ctx)
		if viaReadcache {
			queryStats.AddReadcacheQueryStreamCalls(1)
		}

		stream, err = queryClient.QueryStream(ctx, streamReq)
		if err != nil {
			// If readcache says it's still warming, try the
			// partition's previous lease owner before giving up.
			// For experimental tenants there is no ingester
			// fallback (see plan section 2C.4); a failure here
			// surfaces as 503 to the caller, matching the
			// failure semantics of a full ingester outage. The
			// previous owner is also a readcache pod, so it
			// receives the same partition-hinted request.
			if readcache.IsStillWarming(err) && hasPart {
				if prev, prevID, ok := d.previousReadcacheClientForPartition(ctx, partID); ok {
					level.Info(log).Log("msg", "readcache still warming; falling back to previous lease owner", "partition", partID)
					hits.record(prevID)
					queryStats.AddReadcacheQueryStreamCalls(1)
					stream, err = prev.QueryStream(ctx, streamReq)
				}
			}
			if err != nil {
				return result, err
			}
		}

		// Why retain the batches rather than iteratively build a single slice?
		// If we iteratively build a single slice, we'll spend a lot of time copying elements as the slice grows beyond its capacity.
		// So instead, we build the slice in one go once we know how many series we have.
		var streamingSeriesBatches [][]labels.Labels
		streamingSeriesCount := 0

		memoryConsumptionTracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
		if err != nil {
			return result, err
		}

		deduplicator, err := limiter.SeriesLabelsDeduplicatorFromContext(ctx)
		if err != nil {
			return result, err
		}

		for {
			labelsBatch, isEOS, err := result.receiveResponse(stream, queryLimiter, memoryConsumptionTracker, deduplicator)
			if errors.Is(err, io.EOF) {
				// We will never get an EOF here from an ingester that is streaming chunks, so we don't need to do anything to set up streaming here.
				return result, nil
			} else if err != nil {
				return result, err
			}
			if labelsBatch != nil {
				streamingSeriesCount += len(labelsBatch)
				streamingSeriesBatches = append(streamingSeriesBatches, labelsBatch)
			}
			if isEOS {
				if streamingSeriesCount > 0 {
					result.streamingSeries.Series = make([]labels.Labels, 0, streamingSeriesCount)
					for _, batch := range streamingSeriesBatches {
						result.streamingSeries.Series = append(result.streamingSeries.Series, batch...)
					}

					streamReader := ingester_client.NewSeriesChunksStreamReader(ctx, stream, ing.Id, streamingSeriesCount, queryLimiter, memoryTracker, cleanup, d.log)
					closeStream = false
					result.streamingSeries.StreamReader = streamReader
				}

				return result, nil
			}
		}
	}

	cleanup := func(result ingesterQueryResult) {
		if result.streamingSeries.StreamReader != nil {
			result.streamingSeries.StreamReader.Close()
		}
	}

	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	quorumConfig.IsTerminalError = validation.IsLimitError

	results, err := ring.DoMultiUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSets, quorumConfig, queryIngester, cleanup)
	if err != nil {
		return ingester_client.CombinedQueryStreamResponse{}, err
	}

	streamReaderCount := 0
	for _, res := range results {
		// Start buffering chunks for streaming series
		if res.streamingSeries.StreamReader != nil {
			res.streamingSeries.StreamReader.StartBuffering()
			streamReaderCount++
		}
	}

	// Now turn the accumulated maps into slices.
	resp := ingester_client.CombinedQueryStreamResponse{
		StreamingSeries: mergeSeriesChunkStreams(results, d.estimatedIngestersPerSeries(replicationSets)),
		StreamReaders:   make([]*ingester_client.SeriesChunksStreamReader, 0, streamReaderCount),
	}

	for _, res := range results {
		if res.streamingSeries.StreamReader != nil {
			resp.StreamReaders = append(resp.StreamReaders, res.streamingSeries.StreamReader)
		}
	}

	reqStats.AddFetchedSeries(uint64(len(resp.StreamingSeries)))
	// Stats for streaming series chunks and bytes are handled in streamingChunkSeries.
	return resp, nil
}

// receiveResponse receives a response from stream returns the label sets of each series.
// A bool is also returned to indicate whether the end of the stream has been reached.
func (r *ingesterQueryResult) receiveResponse(stream ingester_client.Ingester_QueryStreamClient, queryLimiter *limiter.QueryLimiter, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, deduplicator limiter.SeriesLabelsDeduplicator) ([]labels.Labels, bool, error) {
	resp, err := stream.Recv()
	if err != nil {
		return nil, false, err
	}
	defer resp.FreeBuffer()

	if len(resp.StreamingSeries) > 0 {
		labelsBatch := make([]labels.Labels, 0, len(resp.StreamingSeries))
		for _, s := range resp.StreamingSeries {
			l := mimirpb.FromLabelAdaptersToLabelsWithCopy(s.Labels)

			uniqueSeriesLabels, err := deduplicator.Deduplicate(l, memoryConsumptionTracker)
			if err != nil {
				return nil, false, err
			}

			if err := queryLimiter.AddSeries(uniqueSeriesLabels); err != nil {
				return nil, false, err
			}

			// We enforce the chunk count limit here, but enforce the chunk bytes limit while streaming the chunks themselves.
			if err := queryLimiter.AddChunks(int(s.ChunkCount)); err != nil {
				return nil, false, err
			}

			if err := queryLimiter.AddEstimatedChunks(int(s.ChunkCount)); err != nil {
				return nil, false, err
			}

			labelsBatch = append(labelsBatch, uniqueSeriesLabels)
		}

		return labelsBatch, resp.IsEndOfSeriesStream, nil
	}

	return nil, resp.IsEndOfSeriesStream, nil
}

// estimatedIngestersPerSeries estimates the number of ingesters that will have chunks for each streaming series.
func (d *Distributor) estimatedIngestersPerSeries(replicationSets []ring.ReplicationSet) int {
	if d.cfg.IngestStorageConfig.Enabled {
		// When the ingest storage is enabled, quorum is reached as soon as 1 series is queried
		// from 1 ingester.
		return 1
	}

	// When ingest storage is disabled we expect only 1 replication set. We check it anyway to
	// avoid any issue in the future.
	if len(replicationSets) != 1 {
		return d.ingestersRing.ReplicationFactor()
	}

	replicationSet := replicationSets[0]

	// Under normal circumstances, a quorum of ingesters will have chunks for each series, so here
	// we return the number of ingesters required for quorum.
	if replicationSet.MaxUnavailableZones > 0 {
		// Zone-aware: quorum is replication factor less allowable unavailable zones.
		return d.ingestersRing.ReplicationFactor() - replicationSet.MaxUnavailableZones
	}

	// Not zone-aware: quorum is replication factor less allowable unavailable ingesters.
	return d.ingestersRing.ReplicationFactor() - replicationSet.MaxErrors
}

type seriesChunksStream struct {
	StreamReader *ingester_client.SeriesChunksStreamReader
	Series       []labels.Labels
}

func mergeSeriesChunkStreams(results []ingesterQueryResult, estimatedIngestersPerSeries int) []ingester_client.StreamingSeries {
	tree := newSeriesChunkStreamsTree(results)
	if tree == nil {
		return nil
	}

	var allSeries []ingester_client.StreamingSeries

	for tree.Next() {
		nextIngester, nextSeriesFromIngester, nextSeriesIndex := tree.Winner()
		lastSeriesIndex := len(allSeries) - 1

		if len(allSeries) == 0 || labels.Compare(allSeries[lastSeriesIndex].Labels, nextSeriesFromIngester) != 0 {
			// First time we've seen this series.
			series := ingester_client.StreamingSeries{
				Labels:  nextSeriesFromIngester,
				Sources: make([]ingester_client.StreamingSeriesSource, 1, estimatedIngestersPerSeries),
			}

			series.Sources[0] = ingester_client.StreamingSeriesSource{
				StreamReader: nextIngester.StreamReader,
				SeriesIndex:  nextSeriesIndex,
			}

			allSeries = append(allSeries, series)
		} else {
			// We've seen this series before.
			allSeries[lastSeriesIndex].Sources = append(allSeries[lastSeriesIndex].Sources, ingester_client.StreamingSeriesSource{
				StreamReader: nextIngester.StreamReader,
				SeriesIndex:  nextSeriesIndex,
			})
		}
	}

	return allSeries
}

func newSeriesChunkStreamsTree(results []ingesterQueryResult) *seriesChunkStreamsTree {
	nIngesters := 0

	for _, r := range results {
		if r.streamingSeries.StreamReader != nil {
			nIngesters++
		}
	}

	if nIngesters == 0 {
		return nil
	}

	t := seriesChunkStreamsTree{
		nodes: make([]seriesChunkStreamsTreeNode, nIngesters*2),
	}

	i := 0

	for _, r := range results {
		if r.streamingSeries.StreamReader != nil {
			t.nodes[i+nIngesters].ingester = r.streamingSeries
			t.moveNext(i + nIngesters) // Must call Next on each item so that At() has a value.
			i++
		}
	}
	if nIngesters > 0 {
		t.nodes[0].index = -1 // flag to be initialized on first call to Next().
	}
	return &t
}

// seriesChunkStreamsTree is a loser tree used to merge sets of series from different ingesters.
// This implementation is based on https://github.com/grafana/dskit/blob/main/loser/loser.go, but
// adapted to return the index of each series within its corresponding ingester stream.
type seriesChunkStreamsTree struct {
	nodes []seriesChunkStreamsTreeNode
}

type seriesChunkStreamsTreeNode struct {
	index           int                // This is the loser for all nodes except the 0th, where it is the winner.
	value           labels.Labels      // Value copied from the loser node, or winner for node 0.
	ingester        seriesChunksStream // Only populated for leaf nodes.
	nextSeriesIndex uint64             // Only populated for leaf nodes.
}

func (t *seriesChunkStreamsTree) moveNext(index int) bool {
	n := &t.nodes[index]
	n.nextSeriesIndex++
	if int(n.nextSeriesIndex) > len(n.ingester.Series) {
		n.value = labels.EmptyLabels()
		n.index = -1
		return false
	}
	n.value = n.ingester.Series[n.nextSeriesIndex-1]
	return true
}

func (t *seriesChunkStreamsTree) Winner() (seriesChunksStream, labels.Labels, uint64) {
	n := t.nodes[t.nodes[0].index]
	return n.ingester, n.value, n.nextSeriesIndex - 1
}

func (t *seriesChunkStreamsTree) Next() bool {
	if len(t.nodes) == 0 {
		return false
	}
	if t.nodes[0].index == -1 { // If tree has not been initialized yet, do that.
		t.initialize()
		return t.nodes[t.nodes[0].index].index != -1
	}
	if t.nodes[t.nodes[0].index].index == -1 { // already exhausted
		return false
	}
	t.moveNext(t.nodes[0].index)
	t.replayGames(t.nodes[0].index)
	return t.nodes[t.nodes[0].index].index != -1
}

func (t *seriesChunkStreamsTree) initialize() {
	winners := make([]int, len(t.nodes))
	// Initialize leaf nodes as winners to start.
	for i := len(t.nodes) / 2; i < len(t.nodes); i++ {
		winners[i] = i
	}
	for i := len(t.nodes) - 2; i > 0; i -= 2 {
		// At each stage the winners play each other, and we record the loser in the node.
		loser, winner := t.playGame(winners[i], winners[i+1])
		p := parent(i)
		t.nodes[p].index = loser
		t.nodes[p].value = t.nodes[loser].value
		winners[p] = winner
	}
	t.nodes[0].index = winners[1]
	t.nodes[0].value = t.nodes[winners[1]].value
}

// Starting at pos, re-consider all values up to the root.
func (t *seriesChunkStreamsTree) replayGames(pos int) {
	// At the start, pos is a leaf node, and is the winner at that level.
	n := parent(pos)
	for n != 0 {
		if t.less(t.nodes[n].value, t.nodes[pos].value) {
			loser := pos
			// Record pos as the loser here, and the old loser is the new winner.
			pos = t.nodes[n].index
			t.nodes[n].index = loser
			t.nodes[n].value = t.nodes[loser].value
		}
		n = parent(n)
	}
	// pos is now the winner; store it in node 0.
	t.nodes[0].index = pos
	t.nodes[0].value = t.nodes[pos].value
}

func (t *seriesChunkStreamsTree) playGame(a, b int) (loser, winner int) {
	if t.less(t.nodes[a].value, t.nodes[b].value) {
		return b, a
	}
	return a, b
}

func (t *seriesChunkStreamsTree) less(a, b labels.Labels) bool {
	if a.IsEmpty() {
		return false
	}

	if b.IsEmpty() {
		return true
	}

	return labels.Compare(a, b) < 0
}

func parent(i int) int { return i / 2 }
