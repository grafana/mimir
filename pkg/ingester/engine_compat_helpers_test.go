// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// compatProducer writes requests to the ingest topic, as the distributors do, and returns the offset of the last.
func compatProducer(tb testing.TB, cfg ingest.KafkaConfig) (write func(tenant string, request *mimirpb.WriteRequest), last func() int64) {
	producer, err := ingest.NewKafkaWriterClient(cfg, 20, log.NewNopLogger(), nil)
	require.NoError(tb, err)
	tb.Cleanup(producer.Close)
	var lastOffset int64 = -1
	return func(tenant string, request *mimirpb.WriteRequest) {
			records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords(cfg.Topic, 0, tenant, request, 1<<20)
			require.NoError(tb, err)
			require.NoError(tb, producer.ProduceSync(context.Background(), records...).FirstErr())
			lastOffset = records[len(records)-1].Offset
		}, func() int64 {
			return lastOffset
		}
}

func compatSeries(name string, samples ...mimirpb.Sample) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
		Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: name}},
		Samples: samples,
	}}
}

func parityContext(tenant string, offset int64) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(),
		"x-scope-orgid", tenant,
		"__consistency_level__", "strong",
		"__consistency_offsets__", fmt.Sprintf("v1=0:%d", offset))
}

func parityMatcher(kind client.MatchType, name, value string) *client.LabelMatcher {
	return &client.LabelMatcher{Type: kind, Name: name, Value: value}
}

func parityRequest(start, end int64, matchers ...*client.LabelMatcher) *client.QueryRequest {
	return &client.QueryRequest{StartTimestampMs: start, EndTimestampMs: end, Matchers: matchers, StreamingChunksBatchSize: 64}
}

func paritySeries(tb testing.TB, api client.IngesterClient, ctx context.Context, request *client.QueryRequest) map[string][]string {
	tb.Helper()
	stream, err := api.QueryStream(ctx, request)
	require.NoError(tb, err)
	var series []client.QueryStreamSeries
	var chunks []client.QueryStreamSeriesChunks
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(tb, err)
		copy := proto.Clone(response).(*client.QueryStreamResponse)
		series = append(series, copy.StreamingSeries...)
		chunks = append(chunks, copy.StreamingSeriesChunks...)
	}
	actual := map[string][]string{}
	for _, s := range series {
		key := mimirpb.FromLabelAdaptersToLabels(s.Labels).String()
		_, exists := actual[key]
		require.False(tb, exists, "duplicate series %s", key)
		actual[key] = []string{}
	}
	for _, group := range chunks {
		require.Less(tb, group.SeriesIndex, uint64(len(series)))
		key := mimirpb.FromLabelAdaptersToLabels(series[group.SeriesIndex].Labels).String()
		for _, wireChunk := range group.Chunks {
			var encoding chunkenc.Encoding
			switch chunk.Encoding(wireChunk.Encoding) {
			case chunk.PrometheusXorChunk:
				encoding = chunkenc.EncXOR
			case chunk.PrometheusXor2Chunk:
				encoding = chunkenc.EncXOR2
			case chunk.PrometheusHistogramChunk:
				encoding = chunkenc.EncHistogram
			case chunk.PrometheusFloatHistogramChunk:
				encoding = chunkenc.EncFloatHistogram
			default:
				tb.Fatalf("unknown wire chunk encoding %d", wireChunk.Encoding)
			}
			decoded, err := chunkenc.FromData(encoding, wireChunk.Data)
			require.NoError(tb, err)
			it := decoded.Iterator(nil)
			for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
				var timestamp int64
				var value string
				switch typ {
				case chunkenc.ValFloat:
					var sample float64
					timestamp, sample = it.At()
					value = fmt.Sprintf("f:%016x", math.Float64bits(sample))
				case chunkenc.ValHistogram:
					var h *histogram.Histogram
					timestamp, h = it.AtHistogram(nil)
					h.CounterResetHint = histogram.UnknownCounterReset
					value = fmt.Sprintf("h:%#v", *h)
				case chunkenc.ValFloatHistogram:
					var h *histogram.FloatHistogram
					timestamp, h = it.AtFloatHistogram(nil)
					h.CounterResetHint = histogram.UnknownCounterReset
					value = fmt.Sprintf("fh:%#v", *h)
				default:
					tb.Fatalf("unexpected chunk value type %v", typ)
				}
				if timestamp >= request.StartTimestampMs && timestamp <= request.EndTimestampMs {
					actual[key] = append(actual[key], fmt.Sprintf("%d:%s", timestamp, value))
				}
			}
			require.NoError(tb, it.Err(), "series=%s encoding=%d bytes=%d", key, wireChunk.Encoding, len(wireChunk.Data))
		}
	}
	for key := range actual {
		sort.Strings(actual[key])
	}
	return actual
}
