// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"runtime"
	"runtime/metrics"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// The engine's benchmarks of the series' own metric metadata mirror #19563's of the TSDB's (head_native_metric_metadata
// _bench_test.go), with the engine's routes in place of the TSDB's appender.

type metadataBenchFixture struct {
	series []promlabels.Labels
	// Each series' metadata in each variant, made up front so that making them isn't timed.
	values [][]mimirpb.SeriesMetadata
}

// newMetadataBenchFixture has series of families metrics, each series with its family's metadata in each variant,
// the variant in its help after the first.
func newMetadataBenchFixture(series, families, variants int) metadataBenchFixture {
	f := metadataBenchFixture{values: make([][]mimirpb.SeriesMetadata, variants)}
	for n := range series {
		f.series = append(f.series, promlabels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", n%families), "instance", fmt.Sprintf("instance-%d", n)))
	}
	for variant := range variants {
		perFamily := make([]mimirpb.SeriesMetadata, families)
		for family := range families {
			help := fmt.Sprintf("Help of metric_%d.", family)
			if variant > 0 {
				help += fmt.Sprintf(" (%d)", variant)
			}
			perFamily[family] = mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: help, Unit: "seconds"}
		}
		for n := range series {
			f.values[variant] = append(f.values[variant], perFamily[n%families])
		}
	}
	return f
}

func (f metadataBenchFixture) value(series, variant int) mimirpb.SeriesMetadata {
	return f.values[variant][series]
}

// records are the fixture's series at at, in records of 2000 series, with the variant's metadata: the engine reads
// the same records whether it keeps their metadata or not, as both arms of a pair do.
func (f metadataBenchFixture) records(b *testing.B, at int64, variant int) [][]byte {
	var out [][]byte
	for start := 0; start < len(f.series); start += 2_000 {
		req := &mimirpb.WriteRequest{}
		families := map[string]bool{}
		for n := start; n < min(start+2_000, len(f.series)); n++ {
			ts := &mimirpb.TimeSeries{Labels: mimirpb.FromLabelsToLabelAdapters(f.series[n]), Samples: []mimirpb.Sample{{TimestampMs: at, Value: float64(n)}}, SeriesMetadata: f.value(n, variant)}
			if name := f.series[n].Get(model.MetricNameLabel); !families[name] {
				families[name] = true
				req.Metadata = append(req.Metadata, &mimirpb.MetricMetadata{MetricFamilyName: name, Type: ts.SeriesMetadata.Type, Help: ts.SeriesMetadata.Help, Unit: ts.SeriesMetadata.Unit})
			}
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: ts})
		}
		records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 64<<20)
		require.NoError(b, err)
		for _, r := range records {
			out = append(out, r.Value)
		}
	}
	return out
}

func newMetadataBenchEngine(b *testing.B, f metadataBenchFixture, withMetadata bool) *Engine {
	e, err := OpenEngine("", "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = e.Close() })
	e.SetNativeMetricMetadata(withMetadata)
	app := e.AppenderV2(context.Background())
	for n, l := range f.series {
		var opts storage.AppendV2Options
		if withMetadata {
			opts.Metadata = appendedMetadata(f.value(n, 0))
		}
		_, err := app.Append(0, l, 0, 1_000, 1, nil, nil, opts)
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())
	return e
}

func appendRecords(b *testing.B, e *Engine, contents [][]byte) {
	for _, content := range contents {
		record, err := ReadRW2(content, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
		require.NoError(b, err)
		_, _, err = e.AppendRW2(record, 0, 1<<62, 0, false, &rw2Sink{})
		require.NoError(b, err)
		record.Release()
	}
}

// BenchmarkEngineMetricMetadataAppend is the steady-state cost of appending a round of samples to every series, with
// their metadata as it was, as it was in fresh strings, unique to each series, or changing every round.
func BenchmarkEngineMetricMetadataAppend(b *testing.B) {
	const numSeries = 1_000
	for _, c := range []struct {
		name         string
		families     int
		changing     bool
		freshStrings bool
	}{
		{name: "stable", families: 100},
		{name: "stable-fresh-strings", families: 100, freshStrings: true},
		{name: "stable-unique", families: numSeries},
		{name: "changing", families: 100, changing: true},
	} {
		for _, route := range []string{"record", "appender"} {
			for _, withMetadata := range []bool{false, true} {
				if !withMetadata && (c.freshStrings || c.changing) {
					continue
				}
				b.Run(fmt.Sprintf("case=%s/route=%s/metadata=%t", c.name, route, withMetadata), func(b *testing.B) {
					variants := 1
					if c.changing {
						variants = 3
					}
					f := newMetadataBenchFixture(numSeries, c.families, variants)
					e := newMetadataBenchEngine(b, f, withMetadata)
					b.ReportAllocs()
					b.ResetTimer()
					for i := range b.N {
						at := int64(2_000 + i*1_000)
						variant := 0
						if c.changing {
							variant = i%2 + 1
						}
						switch route {
						case "record":
							b.StopTimer()
							contents := f.records(b, at, variant)
							b.StartTimer()
							appendRecords(b, e, contents)
						case "appender":
							app := e.AppenderV2(context.Background())
							for n, l := range f.series {
								var opts storage.AppendV2Options
								if withMetadata {
									opts.Metadata = appendedMetadata(f.value(n, variant))
									if c.freshStrings {
										opts.Metadata.Help = strings.Clone(opts.Metadata.Help)
									}
								}
								if _, err := app.Append(0, l, 0, at, 1, nil, nil, opts); err != nil {
									b.Fatal(err)
								}
							}
							if err := app.Commit(); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		}
	}
}

// liveHeapAfterTwoCollections is the live heap once two collections dropped what pools keep.
func liveHeapAfterTwoCollections() uint64 {
	runtime.GC()
	runtime.GC()
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// BenchmarkEngineMetricMetadataRetainedHeap is the live heap an engine's series and their metadata take, per series:
// stable values shared by a family, values unique to each series, and five versions of each.
func BenchmarkEngineMetricMetadataRetainedHeap(b *testing.B) {
	for _, s := range []struct {
		name               string
		series, families   int
		versions, variants int
	}{
		{name: "stable", series: 100_000, families: 100, versions: 1, variants: 1},
		{name: "stable-unique", series: 25_000, families: 25_000, versions: 1, variants: 1},
		{name: "observed-versions=5", series: 25_000, families: 100, versions: 5, variants: 5},
	} {
		for _, withMetadata := range []bool{false, true} {
			b.Run(fmt.Sprintf("scenario=%s/metadata=%t", s.name, withMetadata), func(b *testing.B) {
				f := newMetadataBenchFixture(s.series, s.families, s.variants)
				var rounds [][][]byte
				for version := 1; version < s.versions; version++ {
					rounds = append(rounds, f.records(b, int64(2_000+version*1_000), version%s.variants))
				}
				var growth uint64
				b.ResetTimer()
				for range b.N {
					b.StopTimer()
					before := liveHeapAfterTwoCollections()
					b.StartTimer()
					e := newMetadataBenchEngine(b, f, withMetadata)
					for _, contents := range rounds {
						appendRecords(b, e, contents)
					}
					b.StopTimer()
					growth += liveHeapAfterTwoCollections() - before
					runtime.KeepAlive(e)
					_ = e.Close()
					b.StartTimer()
				}
				b.ReportMetric(float64(growth)/float64(b.N*s.series), "B/series")
			})
		}
	}
}

// BenchmarkEngineNativeMetricMetadataLookup mirrors the TSDB's lookup benchmark: 10k series, metadata on all of them
// or 1%, one version or five, and a limit of 10 or none.
func BenchmarkEngineNativeMetricMetadataLookup(b *testing.B) {
	const numSeries = 10_000
	for _, coverage := range []int{1, 100} {
		for _, versions := range []int{1, 5} {
			for _, limit := range []int{10, 0} {
				b.Run(fmt.Sprintf("coverage=%d%%/versions=%d/limit=%d", coverage, versions, limit), func(b *testing.B) {
					f := newMetadataBenchFixture(numSeries, 100, versions)
					e, err := OpenEngine("", "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
					require.NoError(b, err)
					b.Cleanup(func() { _ = e.Close() })
					e.SetNativeMetricMetadata(true)
					for version := range versions {
						app := e.AppenderV2(context.Background())
						for n, l := range f.series {
							var opts storage.AppendV2Options
							if n%100 < coverage {
								opts.Metadata = appendedMetadata(f.value(n, version))
							}
							_, err := app.Append(0, l, 0, int64(1_000+version*1_000), 1, nil, nil, opts)
							require.NoError(b, err)
						}
						require.NoError(b, app.Commit())
					}
					matchers := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchRegexp, model.MetricNameLabel, "metric_.*")}}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, _, err := e.NativeMetricMetadata(context.Background(), matchers, limit); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
