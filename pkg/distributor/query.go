// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/query.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/grafana/loki/blob/main/pkg/util/loser/tree.go
// Provenance-includes-location: https://github.com/grafana/dskit/blob/main/loser/loser.go

package distributor

import (
	"context"
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

		replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
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
	err := instrument.CollectedRequest(ctx, "Distributor.QueryStream", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToQueryRequest(from, to, projectionInclude, projectionLabels, matchers)
		if err != nil {
			return err
		}

		req.StreamingChunksBatchSize = d.cfg.StreamingChunksPerIngesterSeriesBufferSize

		replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
		if err != nil {
			return err
		}

		result, err = d.queryIngesterStream(ctx, replicationSets, req, queryMetrics)
		if err != nil {
			return err
		}

		s := trace.SpanFromContext(ctx)
		s.SetAttributes(attribute.Int("streaming-series", len(result.StreamingSeries)))
		return nil
	})

	return result, err
}

// getIngesterReplicationSetsForQuery returns a list of ring.ReplicationSet, containing ingester instances,
// that must be queried for a read operation.
//
// If multiple ring.ReplicationSets are returned, each must be queried separately, and results merged.
func (d *Distributor) getIngesterReplicationSetsForQuery(ctx context.Context) ([]ring.ReplicationSet, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	if d.cfg.IngestStorageConfig.Enabled {
		shardSize := d.limits.IngestionPartitionsTenantShardSize(userID)
		r := d.partitionsRing

		// If tenant uses shuffle sharding, we should only query partitions which are part of the tenant's subring.
		if lookbackPeriod := d.cfg.ShuffleShardingLookbackPeriod; shardSize > 0 && lookbackPeriod > 0 {
			r, err = r.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, time.Now())
			if err != nil {
				return nil, err
			}
		}

		return r.GetReplicationSetsForOperation(readNoExtend)
	}

	// Lookup ingesters ring because ingest storage is disabled.
	shardSize := d.limits.IngestionTenantShardSize(userID)
	r := d.ingestersRing

	// If tenant uses shuffle sharding, we should only query ingesters which are part of the tenant's subring.
	if lookbackPeriod := d.cfg.ShuffleShardingLookbackPeriod; lookbackPeriod > 0 {
		r = r.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, time.Now())
	}

	replicationSet, err := r.GetReplicationSetForOperation(readNoExtend)
	if err != nil {
		return nil, err
	}

	return []ring.ReplicationSet{replicationSet}, nil
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
func (d *Distributor) queryIngesterStream(ctx context.Context, replicationSets []ring.ReplicationSet, req *ingester_client.QueryRequest, queryMetrics *stats.QueryMetrics) (ingester_client.CombinedQueryStreamResponse, error) {
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

		client, err := d.ingesterPool.GetClientForInstance(*ing)
		if err != nil {
			return result, err
		}

		stream, err = client.(ingester_client.IngesterClient).QueryStream(ctx, req)
		if err != nil {
			return result, err
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
