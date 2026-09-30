// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/chunks"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/metrics"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func init() {
	countersEnabled = true
}

const hour = int64(3_600_000)

type sample struct {
	t int64
	v float64
}

func floatRequest(samples ...sample) record.DecodedRequest {
	series := record.DecodedSeries{Labels: [][2]string{{"__name__", "metric"}}}
	for _, s := range samples {
		series.Samples = append(series.Samples, mimirpb.Sample{TimestampMs: s.t, Value: s.v})
	}
	return record.DecodedRequest{Series: []record.DecodedSeries{series}}
}

func seriesRequest(name string, samples ...sample) record.DecodedRequest {
	request := floatRequest(samples...)
	request.Series[0].Labels[0][1] = name
	return request
}

func seriesOf(name string, samples ...sample) record.DecodedSeries {
	return seriesRequest(name, samples...).Series[0]
}

func samplesRange(from, to int64, at func(int64) sample) []sample {
	var out []sample
	for index := from; index < to; index++ {
		out = append(out, at(index))
	}
	return out
}

func histogramAt(timestamp int64, buckets uint32) mimirpb.Histogram {
	deltas := make([]int64, buckets)
	// One observation per bucket, so the count matches like `Histogram.Validate` wants.
	if buckets > 0 {
		deltas[0] = 1
	}
	return mimirpb.Histogram{
		Timestamp:      timestamp,
		Count:          &mimirpb.Histogram_CountInt{CountInt: uint64(buckets)},
		PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: buckets}},
		PositiveDeltas: deltas,
	}
}

func storeWith(t testing.TB, l limits.Limits) *Store {
	t.Helper()
	overrides := limits.NewOverrides(l)
	overrides.SetActivePartitions(1)
	return Default().WithOverrides(overrides)
}

func outOfOrderLimits() limits.Limits {
	l := limits.DefaultLimits()
	l.OutOfOrderTimeWindowMs = 2 * hour
	return l
}

func discarded(reason DiscardReason, tenant string) uint64 {
	return uint64(testutil.ToFloat64(metrics.DiscardedSamples.WithLabelValues(reason.Label(), tenant, "")))
}

func decodeChunks(t testing.TB, views []QuerySeriesView) []client.Chunk {
	t.Helper()
	var out []client.Chunk
	for _, view := range views {
		for _, chunk := range view.Chunks {
			var decoded client.Chunk
			require.NoError(t, decoded.Unmarshal(chunk.Wire))
			decoded.Data = slices.Clone(decoded.Data)
			out = append(out, decoded)
		}
	}
	return out
}

func query(t testing.TB, s *Store, start, end int64) []client.Chunk {
	t.Helper()
	views, err := s.SelectChunks("tenant", start, end, nil)
	require.NoError(t, err)
	return decodeChunks(t, views)
}

func floatSamples(t testing.TB, s *Store, start, end int64) []sample {
	t.Helper()
	var out []sample
	for _, chunk := range query(t, s, start, end) {
		if chunk.Encoding != chunks.EncodingXOR {
			continue
		}
		decoded, err := chunks.DecodeXOR(chunk.Data)
		require.NoError(t, err)
		for _, s := range decoded {
			out = append(out, sample{s.T, s.V})
		}
	}
	slices.SortStableFunc(out, func(a, b sample) int {
		switch {
		case a.t < b.t:
			return -1
		case a.t > b.t:
			return 1
		}
		return 0
	})
	return out
}

func withSeries(t testing.TB, s *Store, check func(*Series)) {
	t.Helper()
	for _, shard := range s.shards {
		shard.RLock()
		tenant, ok := shard.tenants["tenant"]
		if ok && tenant.series.len > 0 {
			var found *Series
			tenant.series.forEach(func(entry *seriesEntry) {
				if found == nil {
					found = &entry.series
				}
			})
			check(found)
			shard.RUnlock()
			return
		}
		shard.RUnlock()
	}
	t.Fatal("tenant has no series")
}

func matcher(kind int32, name, value string) LabelMatcher {
	return LabelMatcher{Type: kind, Name: name, Value: value}
}

// everySeries returns every series' labels, from a query without matchers.
func everySeries(t testing.TB, s *Store, tenant string) [][][2]string {
	t.Helper()
	views, err := s.SelectChunks(tenant, math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	out := make([][][2]string, len(views))
	for index, view := range views {
		out[index] = decodeLabels(t, view.EncodedLabels)
	}
	return out
}

func decodeLabels(t testing.TB, encoded []byte) [][2]string {
	t.Helper()
	var series client.QueryStreamSeries
	require.NoError(t, series.Unmarshal(encoded))
	pairs := make([][2]string, len(series.Labels))
	for index, label := range series.Labels {
		pairs[index] = [2]string{string([]byte(label.Name)), string([]byte(label.Value))}
	}
	return pairs
}

func comparePairLists(a, b [][2]string) int {
	return slices.CompareFunc(a, b, comparePairs)
}

func samplesOf(t testing.TB, s *Store, tenant, name string) []sample {
	t.Helper()
	views, err := s.SelectChunks(tenant, math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(MatchEqual, "__name__", name)})
	require.NoError(t, err)
	byTime := map[int64]float64{}
	for _, chunk := range decodeChunks(t, views) {
		decoded, err := chunks.DecodeXOR(chunk.Data)
		require.NoError(t, err)
		for _, s := range decoded {
			byTime[s.T] = s.V
		}
	}
	out := make([]sample, 0, len(byTime))
	for t, v := range byTime {
		out = append(out, sample{t, v})
	}
	slices.SortFunc(out, func(a, b sample) int {
		switch {
		case a.t < b.t:
			return -1
		case a.t > b.t:
			return 1
		}
		return 0
	})
	return out
}
