// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// rw2Labels is a series like a Kubernetes scrape's: a dozen labels, most of them of few values.
func rw2Labels(n int) promlabels.Labels {
	return promlabels.FromStrings(
		"__name__", fmt.Sprintf("container_metric_%d", n%300),
		"cluster", "dev-us-central-0",
		"container", fmt.Sprintf("container-%d", n%40),
		"endpoint", "http-metrics",
		"image", fmt.Sprintf("us-docker.pkg.dev/project/image-%d:v1.2.%d", n%40, n%7),
		"instance", fmt.Sprintf("10.132.%d.%d:9100", (n/256)%256, n%256),
		"job", fmt.Sprintf("namespace-%d/job-%d", n%30, n%60),
		"namespace", fmt.Sprintf("namespace-%d", n%30),
		"node", fmt.Sprintf("gke-dev-us-central-0-pool-%d", n%80),
		"pod", fmt.Sprintf("pod-%d-%x", n%2000, n*2654435761%65536),
		"service", fmt.Sprintf("service-%d", n%60),
	)
}

// rw2Records serializes series [from, from+count) of rw2Labels with one sample at ts each into version 2 records of
// batch series each, the way distributors write them.
func rw2Records(t testing.TB, from, count, batch int, ts int64) [][]byte {
	var records [][]byte
	serializer := ingest.RecordSerializerFromVersion(2)
	for at := from; at < from+count; at += batch {
		req := &mimirpb.WriteRequest{}
		for n := at; n < min(at+batch, from+count); n++ {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:  mimirpb.FromLabelsToLabelAdapters(rw2Labels(n)),
				Samples: []mimirpb.Sample{{TimestampMs: ts, Value: float64(n)}},
			}})
		}
		recs, _, err := serializer.ToRecords("ingest", 0, "tenant", req, 64<<20)
		require.NoError(t, err)
		for _, rec := range recs {
			records = append(records, rec.Value)
		}
	}
	return records
}

func rw2Engine(t testing.TB, series int) *Engine {
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	app := engine.Appender(context.Background())
	for n := range series {
		_, err := app.Append(0, rw2Labels(n), 0, 0)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	return engine
}

type rw2Sink struct{ ingested, errors int }

func (s *rw2Sink) Error(int, error, int64) bool                       { s.errors++; return true }
func (s *rw2Sink) Ingested(int, promlabels.Labels, storage.SeriesRef) { s.ingested++ }
func (s *rw2Sink) NeedsLabels() bool                                  { return false }

// appendDecoded is what the ingester does with a record today: decode it into a request, and append its floats.
func appendDecoded(t testing.TB, engine *Engine, content []byte, sink FloatSink) ([]int, int) {
	req := &mimirpb.PreallocWriteRequest{}
	require.NoError(t, ingest.DeserializeRecordContent(content, req, 2))
	indices := make([]int, len(req.Timeseries))
	for i := range indices {
		indices[i] = i
	}
	leftover, ingested, err := engine.AppendFloats(req.Timeseries, indices, 0, 1<<62, 0, false, sink)
	require.NoError(t, err)
	mimirpb.ReuseSlice(req.Timeseries)
	return leftover, ingested
}

func appendRW2(t testing.TB, engine *Engine, content []byte, sink FloatSink) ([]int, int) {
	record, err := ReadRW2(content, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
	require.NoError(t, err)
	defer record.Release()
	leftover, ingested, err := engine.AppendRW2(record, 0, 1<<62, 0, false, sink)
	require.NoError(t, err)
	return leftover, ingested
}

func seriesSamples(t *testing.T, engine *Engine) map[string][]float64 {
	q, err := engine.Querier(0, 1<<40)
	require.NoError(t, err)
	defer q.Close()
	out := map[string][]float64{}
	set := q.Select(context.Background(), false, nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+"))
	for set.Next() {
		it := set.At().Iterator(nil)
		for it.Next() != 0 {
			_, v := it.At()
			out[set.At().Labels().String()] = append(out[set.At().Labels().String()], v)
		}
	}
	require.NoError(t, set.Err())
	return out
}

// Appending a record without decoding it into a request appends what decoding it does, and leaves the same series
// to the caller: the ones the engine doesn't have.
func TestAppendRW2AppendsWhatDecodingDoes(t *testing.T) {
	const existing, total = 3000, 3500
	decoded, direct := rw2Engine(t, existing), rw2Engine(t, existing)
	for step, ts := range []int64{1000, 2000} {
		records := rw2Records(t, 0, total, 1000, ts)
		for _, content := range records {
			wantSink, gotSink := &rw2Sink{}, &rw2Sink{}
			wantLeft, want := appendDecoded(t, decoded, content, wantSink)
			gotLeft, got := appendRW2(t, direct, content, gotSink)
			require.Equal(t, wantLeft, gotLeft, "step %d", step)
			require.Equal(t, want, got)
			require.Equal(t, *wantSink, *gotSink)
		}
	}
	require.Equal(t, seriesSamples(t, decoded), seriesSamples(t, direct))
	// The series the engine didn't have are left over, a record's last 500 of 1000.
	_, ingested := appendRW2(t, direct, rw2Records(t, existing-500, 1000, 1000, 3000)[0], &rw2Sink{})
	require.Equal(t, 500, ingested)
}

// ownedHashSink is told the owned series tokens of the series that got samples.
type ownedHashSink struct {
	rw2Sink
	hashes map[int]uint32
}

func (s *ownedHashSink) IngestedOwnedHash(series int, ownedHash uint32) { s.hashes[series] = ownedHash }

// A sink that wants the owned series tokens of the series that got samples is told them, both for a decoded request
// and for a record: what hashing the series' labels gives.
func TestAppendFloatsReportsOwnedHashes(t *testing.T) {
	const existing, total = 300, 400
	engine := rw2Engine(t, existing)
	content := rw2Records(t, 0, total, total, 1000)[0]

	sink := &ownedHashSink{hashes: map[int]uint32{}}
	leftover, _ := appendRW2(t, engine, content, sink)
	require.Len(t, leftover, total-existing)
	require.Len(t, sink.hashes, existing)
	for series, hash := range sink.hashes {
		require.Equal(t, secondaryHash(rw2Labels(series)), hash, "series %d", series)
	}

	sink = &ownedHashSink{hashes: map[int]uint32{}}
	appendDecoded(t, engine, rw2Records(t, 0, total, total, 2000)[0], sink)
	require.Len(t, sink.hashes, existing)
	for series, hash := range sink.hashes {
		require.Equal(t, secondaryHash(rw2Labels(series)), hash, "series %d", series)
	}
}

func TestRW2KeyIsTheSeriesEncodedLabels(t *testing.T) {
	known := rw2Labels(1)
	unseen := promlabels.NewBuilder(known).Set("rw2_key_test_unseen_name", "value").Labels()
	req := &mimirpb.WriteRequest{}
	for _, series := range []promlabels.Labels{known, unseen} {
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(series),
			Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 1}},
		}})
	}
	recs, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 64<<20)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	read := func() *RW2Record {
		record, err := ReadRW2(recs[0].Value, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
		require.NoError(t, err)
		t.Cleanup(record.Release)
		return record
	}

	record := read()
	pairs, _ := record.source.pairs(0, nil)
	want := labels.FromSorted(pairs)
	key, ok := record.source.key(0, nil)
	require.True(t, ok)
	require.Equal(t, string(want), string(key))
	// No series has a label of a name the engine never saw.
	_, ok = record.source.key(1, nil)
	require.False(t, ok)

	// A record looks its names up again: one may have been added since.
	pairs, _ = record.source.pairs(1, nil)
	want = labels.FromSorted(pairs)
	key, ok = read().source.key(1, nil)
	require.True(t, ok)
	require.Equal(t, string(want), string(key))
}

func TestAppendRW2RejectsMalformedRecords(t *testing.T) {
	content := rw2Records(t, 0, 10, 10, 1000)[0]
	for _, cut := range []int{1, len(content) / 2, len(content) - 1} {
		_, err := ReadRW2(content[:cut], ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
		require.ErrorIs(t, err, errMalformedRW2, "cut at %d", cut)
	}
}

// BenchmarkIngestRecord appends records of series the engine has, one sample each: decoded into a request, as the
// ingester does, or read directly.
func BenchmarkIngestRecord(b *testing.B) {
	const series, batch = 200_000, 2000
	for _, path := range []string{"decoded", "direct"} {
		b.Run(path, func(b *testing.B) {
			withoutRefLeaksInstrumentation(b)
			engine := rw2Engine(b, series)
			var records [][][]byte
			for step := range 4 {
				records = append(records, rw2Records(b, 0, series, batch, int64(step+1)*15_000))
			}
			sink := &rw2Sink{}
			b.ReportAllocs()
			b.ResetTimer()
			step, at := 0, 0
			for range b.N {
				content := records[step%len(records)][at]
				if path == "decoded" {
					appendDecoded(b, engine, content, sink)
				} else {
					appendRW2(b, engine, content, sink)
				}
				if at++; at == len(records[0]) {
					// The next scrape: a timestamp that comes after, once the series have each got one.
					at, step = 0, step+1
					if step%len(records) == 0 {
						b.StopTimer()
						records = records[:0]
						for next := range 4 {
							records = append(records, rw2Records(b, 0, series, batch, int64(step+next+1)*15_000))
						}
						b.StartTimer()
					}
				}
			}
			b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "series/s")
		})
	}
}

// withoutRefLeaksInstrumentation decodes as production does: tests otherwise copy every buffer they decode into its own
// mapping, to find references to it that outlive it.
func withoutRefLeaksInstrumentation(b *testing.B) {
	mimirpb.CustomCodecConfig{}.RegisterGlobally(nil)
	b.Cleanup(func() {
		mimirpb.CustomCodecConfig{InstrumentRefLeaksConfig: mimirpb.InstrumentRefLeaksConfig{Percentage: 100}}.RegisterGlobally(prometheus.NewRegistry())
	})
}
