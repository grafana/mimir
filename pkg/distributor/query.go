// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/query.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"io"
	"sort"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/ring"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/instrument"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
)

func (d *Distributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*ingester_client.ExemplarQueryResponse, error) {
	var result *ingester_client.ExemplarQueryResponse
	err := instrument.CollectedRequest(ctx, "Distributor.QueryExemplars", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToExemplarQueryRequest(from, to, matchers...)
		if err != nil {
			return err
		}

		// We ask for all ingesters without passing matchers because exemplar queries take in an array of array of label matchers.
		replicationSet, err := d.GetIngestersForQuery(ctx, nil)
		if err != nil {
			return err
		}

		result, err = d.queryIngestersExemplars(ctx, replicationSet, req)
		if err != nil {
			return err
		}

		if s := opentracing.SpanFromContext(ctx); s != nil {
			s.LogKV("series", len(result.Timeseries))
		}
		return nil
	})
	return result, err
}

// QueryStream multiple ingesters via the streaming interface and returns big ol' set of chunks.
func (d *Distributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*ingester_client.QueryStreamResponse, error) {
	var result *ingester_client.QueryStreamResponse
	err := instrument.CollectedRequest(ctx, "Distributor.QueryStream", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToQueryRequest(from, to, matchers)
		if err != nil {
			return err
		}

		replicationSet, err := d.GetIngestersForQuery(ctx, matchers...)
		if err != nil {
			return err
		}

		result, err = d.queryIngesterStream(ctx, replicationSet, req)
		if err != nil {
			return err
		}

		if s := opentracing.SpanFromContext(ctx); s != nil {
			s.LogKV("chunk-series", len(result.GetChunkseries()), "time-series", len(result.GetTimeseries()))
		}
		return nil
	})
	return result, err
}

// GetIngestersForQuery returns a replication set including all ingesters that should be queried
// to fetch series matching input label matchers.
func (d *Distributor) GetIngestersForQuery(ctx context.Context, matchers ...*labels.Matcher) (ring.ReplicationSet, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return ring.ReplicationSet{}, err
	}

	// If shuffle sharding is enabled we should only query ingesters which are
	// part of the tenant's subring.
	if d.cfg.ShardingStrategy == util.ShardingStrategyShuffle {
		shardSize := d.limits.IngestionTenantShardSize(userID)
		lookbackPeriod := d.cfg.ShuffleShardingLookbackPeriod

		if shardSize > 0 && lookbackPeriod > 0 {
			return d.ingestersRing.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, time.Now()).GetReplicationSetForOperation(ring.Read)
		}
	}

	// If "shard by all labels" is disabled, we can get ingesters by metricName if exists.
	if !d.cfg.ShardByAllLabels && len(matchers) > 0 {
		metricNameMatcher, _, ok := extract.MetricNameMatcherFromMatchers(matchers)

		if ok && metricNameMatcher.Type == labels.MatchEqual {
			return d.ingestersRing.Get(shardByMetricName(userID, metricNameMatcher.Value), ring.Read, nil, nil, nil)
		}
	}

	return d.ingestersRing.GetReplicationSetForOperation(ring.Read)
}

// GetIngestersForMetadata returns a replication set including all ingesters that should be queried
// to fetch metadata (eg. label names/values or series).
func (d *Distributor) GetIngestersForMetadata(ctx context.Context) (ring.ReplicationSet, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return ring.ReplicationSet{}, err
	}

	// If shuffle sharding is enabled we should only query ingesters which are
	// part of the tenant's subring.
	if d.cfg.ShardingStrategy == util.ShardingStrategyShuffle {
		shardSize := d.limits.IngestionTenantShardSize(userID)
		lookbackPeriod := d.cfg.ShuffleShardingLookbackPeriod

		if shardSize > 0 && lookbackPeriod > 0 {
			return d.ingestersRing.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, time.Now()).GetReplicationSetForOperation(ring.Read)
		}
	}

	return d.ingestersRing.GetReplicationSetForOperation(ring.Read)
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

// queryIngestersExemplars queries the ingesters for exemplars.
func (d *Distributor) queryIngestersExemplars(ctx context.Context, replicationSet ring.ReplicationSet, req *ingester_client.ExemplarQueryRequest) (*ingester_client.ExemplarQueryResponse, error) {
	// Fetch exemplars from multiple ingesters in parallel, using the replicationSet
	// to deal with consistency.
	results, err := replicationSet.Do(ctx, d.cfg.ExtraQueryDelay, func(ctx context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}

		resp, err := client.(ingester_client.IngesterClient).QueryExemplars(ctx, req)
		d.ingesterQueries.WithLabelValues(ing.Addr).Inc()
		if err != nil {
			d.ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
			return nil, err
		}

		return resp, nil
	})
	if err != nil {
		return nil, err
	}

	return mergeExemplarQueryResponses(results), nil
}

func mergeExemplarQueryResponses(results []interface{}) *ingester_client.ExemplarQueryResponse {
	var keys []string
	exemplarResults := make(map[string]mimirpb.TimeSeries)
	for _, result := range results {
		r := result.(*ingester_client.ExemplarQueryResponse)
		for _, ts := range r.Timeseries {
			lbls := ingester_client.LabelsToKeyString(mimirpb.FromLabelAdaptersToLabels(ts.Labels))
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
	sort.Strings(keys)

	result := make([]mimirpb.TimeSeries, len(exemplarResults))
	for i, k := range keys {
		result[i] = exemplarResults[k]
	}

	return &ingester_client.ExemplarQueryResponse{Timeseries: result}
}

// queryIngesterStream queries the ingesters using the new streaming API.
func (d *Distributor) queryIngesterStream(ctx context.Context, replicationSet ring.ReplicationSet, req *ingester_client.QueryRequest) (*ingester_client.QueryStreamResponse, error) {
	var (
		queryLimiter = limiter.QueryLimiterFromContextWithFallback(ctx)
		reqStats     = stats.FromContext(ctx)
		results      = make(chan *ingester_client.QueryStreamResponse)
		// Note we can't signal goroutines to stop by closing 'results', because it has multiple concurrent senders.
		stop        = make(chan struct{}) // Signal all background goroutines to stop.
		doneReading = make(chan struct{}) // Signal that the reader has stopped.
	)

	hashToChunkseries := map[string]ingester_client.TimeSeriesChunk{}
	hashToTimeSeries := map[string]mimirpb.TimeSeries{}

	// Start reading and accumulating responses. stopReading chan will
	// be closed when all calls to ingesters have finished.
	go func() {
		defer close(doneReading)
		for {
			select {
			case <-stop:
				return
			case response := <-results:
				// Accumulate any chunk series
				for _, series := range response.Chunkseries {
					key := ingester_client.LabelsToKeyString(mimirpb.FromLabelAdaptersToLabels(series.Labels))
					existing := hashToChunkseries[key]
					existing.Labels = series.Labels
					existing.Chunks = accumulateChunks(existing.Chunks, series.Chunks)
					hashToChunkseries[key] = existing
				}

				// Accumulate any time series
				for _, series := range response.Timeseries {
					key := ingester_client.LabelsToKeyString(mimirpb.FromLabelAdaptersToLabels(series.Labels))
					existing := hashToTimeSeries[key]
					existing.Labels = series.Labels
					if existing.Samples == nil {
						existing.Samples = series.Samples
					} else {
						existing.Samples = mergeSamples(existing.Samples, series.Samples)
					}
					hashToTimeSeries[key] = existing
				}
			}
		}
	}()

	// Fetch samples from multiple ingesters, and send them to the results chan
	_, err := replicationSet.Do(ctx, d.cfg.ExtraQueryDelay, func(ctx context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}
		d.ingesterQueries.WithLabelValues(ing.Addr).Inc()

		stream, err := client.(ingester_client.IngesterClient).QueryStream(ctx, req)
		if err != nil {
			d.ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
			return nil, err
		}
		defer stream.CloseSend() //nolint:errcheck

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				// Do not track a failure if the context was canceled.
				if !grpcutil.IsGRPCContextCanceled(err) {
					d.ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
				}

				return nil, err
			}

			// Enforce the max chunks limits.
			if chunkLimitErr := queryLimiter.AddChunks(resp.ChunksCount()); chunkLimitErr != nil {
				return nil, validation.LimitError(chunkLimitErr.Error())
			}

			for _, series := range resp.Chunkseries {
				if limitErr := queryLimiter.AddSeries(series.Labels); limitErr != nil {
					return nil, validation.LimitError(limitErr.Error())
				}
			}

			if chunkBytesLimitErr := queryLimiter.AddChunkBytes(resp.ChunksSize()); chunkBytesLimitErr != nil {
				return nil, validation.LimitError(chunkBytesLimitErr.Error())
			}

			for _, series := range resp.Timeseries {
				if limitErr := queryLimiter.AddSeries(series.Labels); limitErr != nil {
					return nil, validation.LimitError(limitErr.Error())
				}
			}

			// This goroutine could be left running after replicationSet.Do() returns,
			// so check before writing to the results chan.
			select {
			case <-stop:
				return nil, nil
			case results <- resp:
			}
		}
		return nil, nil
	})
	close(stop)
	if err != nil {
		return nil, err
	}

	// Wait for reading loop to finish.
	<-doneReading
	// Now turn the accumulated maps into slices.
	resp := &ingester_client.QueryStreamResponse{
		Chunkseries: make([]ingester_client.TimeSeriesChunk, 0, len(hashToChunkseries)),
		Timeseries:  make([]mimirpb.TimeSeries, 0, len(hashToTimeSeries)),
	}
	for _, series := range hashToChunkseries {
		resp.Chunkseries = append(resp.Chunkseries, series)
	}
	for _, series := range hashToTimeSeries {
		resp.Timeseries = append(resp.Timeseries, series)
	}

	reqStats.AddFetchedSeries(uint64(len(resp.Chunkseries) + len(resp.Timeseries)))
	reqStats.AddFetchedChunkBytes(uint64(resp.ChunksSize()))
	reqStats.AddFetchedChunks(uint64(resp.ChunksCount()))

	return resp, nil
}

// Merges and dedupes two sorted slices with samples together.
func mergeSamples(a, b []mimirpb.Sample) []mimirpb.Sample {
	if sameSamples(a, b) {
		return a
	}

	result := make([]mimirpb.Sample, 0, len(a)+len(b))
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

func sameSamples(a, b []mimirpb.Sample) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Build a slice of chunks, eliminating duplicates.
// This is O(N^2) but most of the time N is small.
func accumulateChunks(a, b []ingester_client.Chunk) []ingester_client.Chunk {
	ret := a
	for j := range b {
		if !containsChunk(a, b[j]) {
			ret = append(ret, b[j])
		}
	}
	return ret
}

func containsChunk(a []ingester_client.Chunk, b ingester_client.Chunk) bool {
	for i := range a {
		if a[i].Equal(b) {
			return true
		}
	}
	return false
}
