// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/query.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"io"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/exp/slices"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/stats"
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

		// We ask for all ingesters without passing matchers because exemplar queries take in an array of label matchers.
		replicationSet, err := d.GetIngesters(ctx)
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

// QueryStream queries multiple ingesters via the streaming interface and returns a big ol' set of chunks.
func (d *Distributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (querier.DistributorQueryStreamResponse, error) {
	var result querier.DistributorQueryStreamResponse
	err := instrument.CollectedRequest(ctx, "Distributor.QueryStream", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToQueryRequest(from, to, matchers)
		if err != nil {
			return err
		}

		req.PreferStreamingChunks = d.cfg.PreferStreamingChunks

		replicationSet, err := d.GetIngesters(ctx)
		if err != nil {
			return err
		}

		result, err = d.queryIngesterStream(ctx, replicationSet, req)
		if err != nil {
			return err
		}

		if s := opentracing.SpanFromContext(ctx); s != nil {
			s.LogKV(
				"chunk-series", len(result.Chunkseries),
				"time-series", len(result.Timeseries),
				"streaming-series", len(result.StreamingSeries),
			)
		}
		return nil
	})

	return result, err
}

// GetIngesters returns a replication set including all ingesters.
func (d *Distributor) GetIngesters(ctx context.Context) (ring.ReplicationSet, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return ring.ReplicationSet{}, err
	}

	// If tenant uses shuffle sharding, we should only query ingesters which are
	// part of the tenant's subring.
	shardSize := d.limits.IngestionTenantShardSize(userID)
	lookbackPeriod := d.cfg.ShuffleShardingLookbackPeriod

	if shardSize > 0 && lookbackPeriod > 0 {
		return d.ingestersRing.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, time.Now()).GetReplicationSetForOperation(ring.Read)
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
	results, err := replicationSet.Do(ctx, 0, func(ctx context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}

		resp, err := client.(ingester_client.IngesterClient).QueryExemplars(ctx, req)
		if err != nil {
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
	slices.Sort(keys)

	result := make([]mimirpb.TimeSeries, len(exemplarResults))
	for i, k := range keys {
		result[i] = exemplarResults[k]
	}

	return &ingester_client.ExemplarQueryResponse{Timeseries: result}
}

type streamerWithSeriesLabels struct {
	streamer     *querier.SeriesChunksStreamReader
	seriesLabels []labels.Labels
}

// queryIngesterStream queries the ingesters using the new streaming API.
// TODO: break this method into smaller methods, it's enormous
func (d *Distributor) queryIngesterStream(ctx context.Context, replicationSet ring.ReplicationSet, req *ingester_client.QueryRequest) (querier.DistributorQueryStreamResponse, error) {
	var (
		queryLimiter  = limiter.QueryLimiterFromContextWithFallback(ctx)
		reqStats      = stats.FromContext(ctx)
		results       = make(chan *ingester_client.QueryStreamResponse)
		streamersChan = make(chan streamerWithSeriesLabels)
		// Note we can't signal goroutines to stop by closing 'results', because it has multiple concurrent senders.
		stop        = make(chan struct{}) // Signal all background goroutines to stop.
		doneReading = make(chan struct{}) // Signal that the reader has stopped.
	)

	hashToChunkseries := map[string]ingester_client.TimeSeriesChunk{}
	hashToTimeSeries := map[string]mimirpb.TimeSeries{}
	hashToStreamingSeries := map[string]querier.StreamingSeries{}

	// Start reading and accumulating responses. stopReading chan will
	// be closed when all calls to ingesters have finished.
	go func() {
		// We keep track of the number of chunks that were able to be deduplicated entirely
		// via the AccumulateChunks function (fast) instead of needing to merge samples one
		// by one (slow). Useful to verify the performance impact of things that potentially
		// result in different samples being written to each ingester.
		var numDeduplicatedChunks int
		var numTotalChunks int

		defer func() {
			close(doneReading)
			// TODO: do something similar for streaming
			d.ingesterChunksDeduplicated.Add(float64(numDeduplicatedChunks))
			d.ingesterChunksTotal.Add(float64(numTotalChunks))
		}()

		for {
			select {
			case <-stop:
				// TODO: will we will leak any streamers already sent to streamersChan?
				// Or will this be handled by the context passed to the gRPC runtime being cancelled eventually?

				return
			case response := <-results:
				// Accumulate any chunk series
				for _, series := range response.Chunkseries {
					key := ingester_client.LabelsToKeyString(mimirpb.FromLabelAdaptersToLabels(series.Labels))
					existing := hashToChunkseries[key]
					existing.Labels = series.Labels

					numPotentialChunks := len(existing.Chunks) + len(series.Chunks)
					existing.Chunks = ingester_client.AccumulateChunks(existing.Chunks, series.Chunks)

					numDeduplicatedChunks += numPotentialChunks - len(existing.Chunks)
					numTotalChunks += len(series.Chunks)
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
			case s := <-streamersChan:
				for seriesIndex, seriesLabels := range s.seriesLabels {
					key := ingester_client.LabelsToKeyString(seriesLabels)
					series, exists := hashToStreamingSeries[key]

					if !exists {
						series = querier.StreamingSeries{
							Labels:  seriesLabels,
							Sources: make([]querier.StreamingSeriesSource, 0, 3), // TODO: take capacity from number of zones
						}
					}

					series.Sources = append(series.Sources, querier.StreamingSeriesSource{
						SeriesIndex:  seriesIndex,
						StreamReader: s.streamer,
					})

					hashToStreamingSeries[key] = series
				}
			}
		}
	}()

	// Fetch samples from multiple ingesters, and send them to the results chan
	_, err := replicationSet.Do(ctx, 0, func(_ context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}

		// FIXME: using parent context here (rather than context passed to this function by replicationSet.Do()) to avoid cancelling streaming requests
		// when replicationSet.Do() returns, but this breaks aborting requests early if too many zones have failed or if we've received enough responses
		// from other zones
		stream, err := client.(ingester_client.IngesterClient).QueryStream(ctx, req)
		if err != nil {
			return nil, err
		}
		closeStream := true
		defer func() {
			if closeStream {
				stream.CloseSend() //nolint:errcheck
			}
		}()

		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}

			if len(resp.Timeseries) > 0 || len(resp.Chunkseries) > 0 {
				// Enforce the max chunks limits.
				// TODO: enforce chunk limit for streaming series
				if chunkLimitErr := queryLimiter.AddChunks(ingester_client.ChunksCount(resp.Chunkseries)); chunkLimitErr != nil {
					return nil, validation.LimitError(chunkLimitErr.Error())
				}

				for _, series := range resp.Chunkseries {
					if limitErr := queryLimiter.AddSeries(series.Labels); limitErr != nil {
						return nil, validation.LimitError(limitErr.Error())
					}
				}

				if chunkBytesLimitErr := queryLimiter.AddChunkBytes(ingester_client.ChunksSize(resp.Chunkseries)); chunkBytesLimitErr != nil {
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
			} else if len(resp.Series) > 0 {
				seriesLabels := make([]labels.Labels, 0, len(resp.Series))

				for {
					for _, s := range resp.Series {
						if limitErr := queryLimiter.AddSeries(s.Labels); limitErr != nil {
							return nil, validation.LimitError(limitErr.Error())
						}

						seriesLabels = append(seriesLabels, mimirpb.FromLabelAdaptersToLabels(s.Labels))
					}

					if resp.IsEndOfSeriesStream {
						break
					}

					resp, err = stream.Recv()
					if err != nil {
						return nil, err
					}
				}

				// TODO: make buffer size smarter, for example:
				// - buffer fewer series when selecting large time range (and therefore more chunks per series)
				// - buffer fewer series per ingester when querying many ingesters (to control overall memory consumption)
				// Note that this value does not control the size of messages sent by ingesters, which may be larger or smaller than this buffer size.
				streamer := querier.NewSeriesStreamer(stream, len(seriesLabels), d.cfg.StreamingChunksPerIngesterSeriesBufferSize)

				// This goroutine could be left running after replicationSet.Do() returns,
				// so check before writing to the results chan.
				select {
				case <-stop:
					return nil, nil
				case streamersChan <- streamerWithSeriesLabels{streamer, seriesLabels}:
					streamer.StartBuffering()
					closeStream = false // The SeriesChunksStreamReader is responsible for closing the stream now.
					return nil, nil
				}
			} else if resp.IsEndOfSeriesStream {
				// No series matching query on ingester - nothing to do.
				return nil, nil
			}
		}
		return nil, nil
	})
	close(stop)
	if err != nil {
		return querier.DistributorQueryStreamResponse{}, err
	}

	// Wait for reading loop to finish.
	<-doneReading
	// Now turn the accumulated maps into slices.
	resp := querier.DistributorQueryStreamResponse{
		Chunkseries:     make([]ingester_client.TimeSeriesChunk, 0, len(hashToChunkseries)),
		Timeseries:      make([]mimirpb.TimeSeries, 0, len(hashToTimeSeries)),
		StreamingSeries: make([]querier.StreamingSeries, 0, len(hashToStreamingSeries)),
	}
	for _, series := range hashToChunkseries {
		resp.Chunkseries = append(resp.Chunkseries, series)
	}
	for _, series := range hashToTimeSeries {
		resp.Timeseries = append(resp.Timeseries, series)
	}
	for _, series := range hashToStreamingSeries {
		resp.StreamingSeries = append(resp.StreamingSeries, series)
	}

	reqStats.AddFetchedSeries(uint64(len(resp.Chunkseries) + len(resp.Timeseries) + len(resp.StreamingSeries)))
	reqStats.AddFetchedChunkBytes(uint64(ingester_client.ChunksSize(resp.Chunkseries))) // TODO: accumulate this while streaming (this includes the size of labels - do we want to include those here too?)
	reqStats.AddFetchedChunks(uint64(ingester_client.ChunksCount(resp.Chunkseries)))    // TODO: accumulate this while streaming

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
