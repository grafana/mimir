// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// Records whose series carry metadata of their own, with stand-ins for the metric families' metadata, build the same
// series as their request pushed directly: the stand-ins add none.
func TestTSDBBuilderBuildsTheSameSeriesFromRecordsWithSeriesMetadata(t *testing.T) {
	config, overrides := blockBuilderConfig(t, "kafka:9092", nil)
	logger := log.NewNopLogger()
	builder := NewTSDBBuilder(0, config, overrides, logger, newTSDBBuilderMetrics(prometheus.NewPedanticRegistry()), mimir_tsdb.NewTSDBMetrics(prometheus.NewPedanticRegistry(), logger))
	t.Cleanup(func() { require.NoError(t, builder.Close()) })

	request := func() *mimirpb.WriteRequest {
		requests := mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"}
		series := func(metadata mimirpb.SeriesMetadata, lbls ...string) *mimirpb.TimeSeries {
			return &mimirpb.TimeSeries{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(lbls...)), SeriesMetadata: metadata}
		}
		first, second := series(requests, model.MetricNameLabel, "requests_total", "pod", "pod-0"), series(requests, model.MetricNameLabel, "requests_total", "pod", "pod-1")
		first.Samples = []mimirpb.Sample{{TimestampMs: 1000, Value: 1}, {TimestampMs: 2000, Value: 2}}
		second.Samples = []mimirpb.Sample{{TimestampMs: 1000, Value: 3}}
		latency := series(mimirpb.SeriesMetadata{Type: mimirpb.HISTOGRAM, Help: "Latency.", Unit: "seconds"}, model.MetricNameLabel, "latency_seconds")
		latency.Histograms = []mimirpb.Histogram{{Timestamp: 1000, Count: &mimirpb.Histogram_CountInt{CountInt: 1}, ZeroCount: &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 1}, ZeroThreshold: 1e-128}}
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: first}, {TimeSeries: second}, {TimeSeries: latency}},
			Metadata: []*mimirpb.MetricMetadata{
				{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"},
				{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests served.", Unit: "requests"},
				{MetricFamilyName: "latency_seconds", Type: mimirpb.HISTOGRAM, Help: "Latency.", Unit: "seconds"},
				{MetricFamilyName: "no_series", Type: mimirpb.GAUGE, Help: "No series of its own."},
			},
		}
	}

	records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords(testTopic, 0, "records", request(), math.MaxInt)
	require.NoError(t, err)
	for _, record := range records {
		// The consumer takes each record's context, which a fetched record has.
		record.Context = context.Background()
	}
	consumer := ingest.NewPusherConsumer(builder, config.Kafka, ingest.NewPusherConsumerMetrics(prometheus.NewPedanticRegistry()), logger)
	require.NoError(t, consumer.Consume(context.Background(), slices.Values(records)))
	require.NoError(t, builder.PushToStorageAndReleaseRequest(user.InjectOrgID(context.Background(), "direct"), request()))

	built := map[string][]string{}
	for _, tenantID := range []string{"records", "direct"} {
		db, err := builder.getOrCreateTSDB(tsdbTenant{partitionID: 0, tenantID: tenantID})
		require.NoError(t, err)
		querier, err := db.Querier(math.MinInt64, math.MaxInt64)
		require.NoError(t, err)
		set := querier.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"))
		for set.Next() {
			series := set.At()
			var samples []string
			it := series.Iterator(nil)
			for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
				switch typ {
				case chunkenc.ValFloat:
					ts, v := it.At()
					samples = append(samples, fmt.Sprintf("%d:%g", ts, v))
				case chunkenc.ValHistogram:
					ts, h := it.AtHistogram(nil)
					samples = append(samples, fmt.Sprintf("%d:%s", ts, h))
				default:
					t.Fatalf("unexpected sample type %v", typ)
				}
			}
			require.NoError(t, it.Err())
			built[tenantID] = append(built[tenantID], fmt.Sprintf("%s %v", series.Labels(), samples))
		}
		require.NoError(t, set.Err())
		require.NoError(t, querier.Close())
	}
	require.Len(t, built["direct"], 3)
	require.Equal(t, built["direct"], built["records"])
}
