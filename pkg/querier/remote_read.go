// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/remote_read.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	prom_remote "github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	// Queries are a set of matchers with time ranges - should not get into megabytes
	maxRemoteReadQuerySize = 1024 * 1024

	// Maximum number of bytes in frame when using streaming remote read.
	// Google's recommendation is to keep protobuf message not larger than 1MB.
	// https://developers.google.com/protocol-buffers/docs/techniques#large-data
	maxRemoteReadFrameBytes = 1024 * 1024

	statusClientClosedRequest = 499
)

// RemoteReadHandler handles Prometheus remote read requests.
func RemoteReadHandler(q storage.SampleAndChunkQueryable, logger log.Logger) http.Handler {
	return remoteReadHandler(q, maxRemoteReadFrameBytes, logger)
}

func remoteReadHandler(q storage.SampleAndChunkQueryable, maxBytesInFrame int, lg log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req client.ReadRequest
		logger := util_log.WithContext(r.Context(), lg)
		if err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRemoteReadQuerySize, nil, &req, util.RawSnappy); err != nil {
			level.Error(logger).Log("msg", "failed to parse proto", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		respType, err := negotiateResponseType(req.AcceptedResponseTypes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		switch respType {
		case client.STREAMED_XOR_CHUNKS:
			remoteReadStreamedXORChunks(ctx, q, w, &req, maxBytesInFrame, logger)
		default:
			remoteReadSamples(ctx, q, w, &req, logger)
		}
	})
}

func remoteReadSamples(
	ctx context.Context,
	q storage.Queryable,
	w http.ResponseWriter,
	req *client.ReadRequest,
	logger log.Logger,
) {
	resp := client.ReadResponse{
		Results: make([]*client.QueryResponse, len(req.Queries)),
	}
	// Fetch samples for all queries in parallel.
	errCh := make(chan error)

	for i, qr := range req.Queries {
		go func(i int, qr *client.QueryRequest) {
			from, to, matchers, err := client.FromQueryRequest(qr)
			if err != nil {
				errCh <- err
				return
			}

			querier, err := q.Querier(int64(from), int64(to))
			if err != nil {
				errCh <- err
				return
			}

			params := &storage.SelectHints{
				Start: int64(from),
				End:   int64(to),
			}
			seriesSet := querier.Select(ctx, false, params, matchers...)
			resp.Results[i], err = seriesSetToQueryResponse(seriesSet)
			errCh <- err
		}(i, qr)
	}

	var lastErr error
	for range req.Queries {
		err := <-errCh
		if err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		http.Error(w, lastErr.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Add("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	if err := util.SerializeProtoResponse(w, &resp, util.RawSnappy); err != nil {
		level.Error(logger).Log("msg", "error sending remote read response", "err", err)
	}
}

func remoteReadStreamedXORChunks(
	ctx context.Context,
	q storage.ChunkQueryable,
	w http.ResponseWriter,
	req *client.ReadRequest,
	maxBytesInFrame int,
	logger log.Logger,
) {
	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "internal http.ResponseWriter does not implement http.Flusher interface", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")

	for i, qr := range req.Queries {
		if err := processReadStreamedQueryRequest(ctx, i, qr, q, w, f, maxBytesInFrame); err != nil {
			if errors.Is(err, context.Canceled) {
				// We're intentionally not logging in this case to reduce noise about an unactionable condition.
				http.Error(w, err.Error(), statusClientClosedRequest)
				return
			}

			level.Error(logger).Log("msg", "error while processing remote read request", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func processReadStreamedQueryRequest(
	ctx context.Context,
	idx int,
	queryReq *client.QueryRequest,
	q storage.ChunkQueryable,
	w http.ResponseWriter,
	f http.Flusher,
	maxBytesInFrame int,
) error {
	from, to, matchers, err := client.FromQueryRequest(queryReq)
	if err != nil {
		return err
	}

	querier, err := q.ChunkQuerier(int64(from), int64(to))
	if err != nil {
		return err
	}

	params := &storage.SelectHints{
		Start: int64(from),
		End:   int64(to),
	}

	return streamChunkedReadResponses(
		prom_remote.NewChunkedWriter(w, f),
		// The streaming API has to provide the series sorted.
		querier.Select(ctx, true, params, matchers...),
		idx,
		maxBytesInFrame,
	)
}

func seriesSetToQueryResponse(s storage.SeriesSet) (*client.QueryResponse, error) {
	result := &client.QueryResponse{}

	var it chunkenc.Iterator
	for s.Next() {
		series := s.At()
		samples := []mimirpb.Sample{}
		histograms := []mimirpb.Histogram{}
		it = series.Iterator(it)
		for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
			switch valType {
			case chunkenc.ValFloat:
				t, v := it.At()
				samples = append(samples, mimirpb.Sample{
					TimestampMs: t,
					Value:       v,
				})
			case chunkenc.ValHistogram:
				t, h := it.AtHistogram(nil) // Nil argument as we pass the data to the protobuf as-is without copy.
				histograms = append(histograms, mimirpb.FromHistogramToHistogramProto(t, h))
			case chunkenc.ValFloatHistogram:
				t, h := it.AtFloatHistogram(nil) // Nil argument as we pass the data to the protobuf as-is without copy.
				histograms = append(histograms, mimirpb.FromFloatHistogramToHistogramProto(t, h))
			default:
				return nil, fmt.Errorf("unsupported value type: %v", valType)
			}
		}

		if err := it.Err(); err != nil {
			return nil, err
		}

		ts := mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(series.Labels()),
			Samples:    samples,
			Histograms: histograms,
		}

		result.Timeseries = append(result.Timeseries, ts)
	}

	return result, s.Err()
}

func negotiateResponseType(accepted []client.ReadRequest_ResponseType) (client.ReadRequest_ResponseType, error) {
	if len(accepted) == 0 {
		return client.SAMPLES, nil
	}

	supported := map[client.ReadRequest_ResponseType]struct{}{
		client.SAMPLES:             {},
		client.STREAMED_XOR_CHUNKS: {},
	}

	for _, resType := range accepted {
		if _, ok := supported[resType]; ok {
			return resType, nil
		}
	}
	return 0, errors.Errorf("server does not support any of the requested response types: %v; supported: %v", accepted, supported)
}

func streamChunkedReadResponses(stream io.Writer, ss storage.ChunkSeriesSet, queryIndex, maxBytesInFrame int) error {
	var (
		chks []client.StreamChunk
		lbls []mimirpb.LabelAdapter
	)

	var iter chunks.Iterator
	for ss.Next() {
		series := ss.At()
		iter = series.Iterator(iter)
		lbls = mimirpb.FromLabelsToLabelAdapters(series.Labels())

		frameBytesRemaining := initializedFrameBytesRemaining(maxBytesInFrame, lbls)
		isNext := iter.Next()

		for isNext {
			chk := iter.At()

			if chk.Chunk == nil {
				return errors.Errorf("found not populated chunk returned by SeriesSet at ref: %v", chk.Ref)
			}

			// Cut the chunk.
			chks = append(chks, client.StreamChunk{
				MinTimeMs: chk.MinTime,
				MaxTimeMs: chk.MaxTime,
				Type:      client.StreamChunk_Encoding(chk.Chunk.Encoding()),
				Data:      chk.Chunk.Bytes(),
			})
			frameBytesRemaining -= chks[len(chks)-1].Size()

			// We are fine with minor inaccuracy of max bytes per frame. The inaccuracy will be max of full chunk size.
			isNext = iter.Next()
			if frameBytesRemaining > 0 && isNext {
				continue
			}

			b, err := proto.Marshal(&client.StreamReadResponse{
				ChunkedSeries: []*client.StreamChunkedSeries{
					{
						Labels: lbls,
						Chunks: chks,
					},
				},
				QueryIndex: int64(queryIndex),
			})
			if err != nil {
				return errors.Wrap(err, "marshal client.StreamReadResponse")
			}

			if _, err := stream.Write(b); err != nil {
				return errors.Wrap(err, "write to stream")
			}
			chks = chks[:0]
			frameBytesRemaining = initializedFrameBytesRemaining(maxBytesInFrame, lbls)
		}
		if err := iter.Err(); err != nil {
			return err
		}
	}
	return ss.Err()
}

func initializedFrameBytesRemaining(maxBytesInFrame int, lbls []mimirpb.LabelAdapter) int {
	frameBytesLeft := maxBytesInFrame
	for _, lbl := range lbls {
		frameBytesLeft -= lbl.Size()
	}
	return frameBytesLeft
}
