// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/record"
)

// These are the Rust store's memory benchmarks, run with GO_INGESTER_BENCH=1: RSS varies by host,
// and the fixtures take substantial memory.

func liveHeapBytes() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func TestHighCardinalityMultiTenantMemory(t *testing.T) {
	requireBench(t)
	const (
		tenants          = 3
		seriesPerTenant  = 25_000
		samplesPerSeries = 200
		retentionMs      = 2 * 60 * 60 * 1000
	)
	histogramEvery := envInt("RUST_INGESTER_BENCH_HISTOGRAM_EVERY", 10)
	isHistogram := func(id int) bool { return histogramEvery != 0 && id%histogramEvery == 0 }
	chunkDir := t.TempDir()
	s, err := New(20*60*1000, Retention{Ms: retentionMs, Set: true}, chunkDir)
	require.NoError(t, err)
	baseline := rssBytes()
	started := time.Now()
	end := nowMs()
	fiftyBuckets := func(timestamp int64) mimirpb.Histogram {
		deltas := make([]int64, 50)
		// One observation in each bucket, matching the count.
		deltas[0] = 1
		return mimirpb.Histogram{Timestamp: timestamp, Count: &mimirpb.Histogram_CountInt{CountInt: 50}, PositiveSpans: []mimirpb.BucketSpan{{Offset: 0, Length: 50}}, PositiveDeltas: deltas}
	}
	seriesLabels := func(tenant, id int) [][2]string {
		pairs := make([][2]string, 0, 19)
		for label := range 19 {
			switch {
			case label == 0:
				pairs = append(pairs, [2]string{"__name__", fmt.Sprintf("metric_%04d", id%100)})
			case label < 10:
				pairs = append(pairs, [2]string{fmt.Sprintf("label_%04d", label), fmt.Sprintf("shared_value_%05d", (id+label*17)%1000)})
			default:
				pairs = append(pairs, [2]string{fmt.Sprintf("label_%04d", label), fmt.Sprintf("unique_value_%02d_%02d_%08d", tenant, label, id)})
			}
		}
		return pairs
	}
	ingest := func(samples func(id int) ([]mimirpb.Sample, []mimirpb.Histogram)) {
		for tenant := range tenants {
			for batchStart := 0; batchStart < seriesPerTenant; batchStart += 350 {
				request := record.DecodedRequest{}
				for id := batchStart; id < min(batchStart+350, seriesPerTenant); id++ {
					floats, histograms := samples(id)
					request.Series = append(request.Series, record.DecodedSeries{Labels: seriesLabels(tenant, id), Samples: floats, Histograms: histograms})
				}
				require.NoError(t, s.IngestRecovered(fmt.Sprintf("tenant-%d", tenant), request, end))
			}
		}
	}
	ingest(func(id int) ([]mimirpb.Sample, []mimirpb.Histogram) {
		var floats []mimirpb.Sample
		var histograms []mimirpb.Histogram
		for sample := range samplesPerSeries {
			timestamp := end - retentionMs + 1 + int64(sample)*(retentionMs-1)/samplesPerSeries
			if isHistogram(id) {
				histograms = append(histograms, fiftyBuckets(timestamp))
			} else {
				floats = append(floats, mimirpb.Sample{TimestampMs: timestamp, Value: float64(id + sample)})
			}
		}
		return floats, histograms
	})
	ingestSeconds := time.Since(started).Seconds()
	rest := rssBytes()
	var stopped atomic.Bool
	var peak atomic.Uint64
	peak.Store(rest)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stopped.Load() {
			if rss := rssBytes(); rss > peak.Load() {
				peak.Store(rss)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	queryAll := func() float64 {
		started := time.Now()
		for tenant := range tenants {
			views, err := s.SelectChunks(fmt.Sprintf("tenant-%d", tenant), end-retentionMs, end, nil)
			require.NoError(t, err)
			require.Len(t, views, seriesPerTenant)
			runtime.KeepAlive(views)
		}
		return time.Since(started).Seconds()
	}
	querySeconds := queryAll()
	started = time.Now()
	ingest(func(id int) ([]mimirpb.Sample, []mimirpb.Histogram) {
		if isHistogram(id) {
			return nil, []mimirpb.Histogram{fiftyBuckets(end)}
		}
		return []mimirpb.Sample{{TimestampMs: end, Value: float64(id + samplesPerSeries)}}, nil
	})
	updateSeconds := time.Since(started).Seconds()
	queryAfterUpdateSeconds := queryAll()
	stopped.Store(true)
	<-done
	after := rssBytes()
	fmt.Printf("series=%d samples=%d baseline_rss=%d rest_rss=%d query_peak_rss=%d after_query_rss=%d ingest_wall_s=%.3f query_wall_s=%.3f update_wall_s=%.3f query_after_update_wall_s=%.3f\n",
		tenants*seriesPerTenant, tenants*seriesPerTenant*samplesPerSeries, baseline, rest, max(peak.Load(), after), after,
		ingestSeconds, querySeconds, updateSeconds, queryAfterUpdateSeconds)
	started = time.Now()
	require.NoError(t, s.WriteSnapshot(nil))
	snapshotWriteSeconds := time.Since(started).Seconds()
	info, err := os.Stat(filepath.Join(chunkDir, snapshotFileName))
	require.NoError(t, err)
	require.NoError(t, s.Close())
	started = time.Now()
	restored, err := Restore(20*60*1000, Retention{Ms: retentionMs, Set: true}, chunkDir, 2)
	require.NoError(t, err)
	require.NotNil(t, restored, "snapshot restored")
	snapshotRestoreSeconds := time.Since(started).Seconds()
	views, err := restored.Store.SelectChunks("tenant-0", end-retentionMs, end, nil)
	require.NoError(t, err)
	require.Len(t, views, seriesPerTenant)
	fmt.Printf("snapshot_bytes=%d snapshot_write_wall_s=%.3f snapshot_restore_wall_s=%.3f\n", info.Size(), snapshotWriteSeconds, snapshotRestoreSeconds)
	require.NoError(t, restored.Store.Close())
}

// TestParallelIngestThroughput compares ingest throughput for several store thread counts.
func TestParallelIngestThroughput(t *testing.T) {
	requireBench(t)
	records := func(start, count int) []IngestRecord {
		out := make([]IngestRecord, 0, count)
		for rec := start; rec < start+count; rec++ {
			request := record.DecodedRequest{}
			for series := range 350 {
				pairs := make([][2]string, 0, 13)
				for label := range 12 {
					pairs = append(pairs, [2]string{fmt.Sprintf("label_%d", label), fmt.Sprintf("value_%d_%d", label%4, series)})
				}
				pairs = append(pairs, [2]string{"__name__", fmt.Sprintf("metric_%d", series%40)})
				request.Series = append(request.Series, record.DecodedSeries{Labels: pairs, Samples: []mimirpb.Sample{{TimestampMs: int64(rec) * 1000, Value: float64(rec)}}})
			}
			out = append(out, IngestRecord{Tenant: "tenant", Request: request})
		}
		return out
	}
	for _, threads := range []int{1, 2, 4, 8} {
		s, err := NewWithShards(20*60*1000, Retention{}, "", 16, threads)
		require.NoError(t, err)
		batches := make([][]IngestRecord, 100)
		for batch := range batches {
			batches[batch] = records(batch*64, 64)
		}
		started := time.Now()
		for _, batch := range batches {
			require.NoError(t, s.IngestBatch(batch))
		}
		elapsed := time.Since(started).Seconds()
		fmt.Printf("threads=%d records_per_second=%.0f series_samples_per_second=%.0f\n", threads, 6400/elapsed, 6400*350/elapsed)
	}
}

// TestRetainedSeriesHeapBytes is like a partition of mimir-dev-15: series with 19 labels, 15s
// samples over 13h of retention, of which only the newest part is in the emulated Go head. The
// live heap per series is what the ingester pays for every stored series.
func TestRetainedSeriesHeapBytes(t *testing.T) {
	requireBench(t)
	const (
		series      = 5_000
		retentionMs = 13 * 60 * 60 * 1000
		intervalMs  = int64(15_000)
	)
	directory := t.TempDir()
	s, err := New(20*60*1000, Retention{Ms: retentionMs, Set: true}, directory)
	require.NoError(t, err)
	end := nowMs()
	before := liveHeapBytes()
	labelsOf := func(id int) [][2]string {
		pairs := make([][2]string, 0, 19)
		for label := range 19 {
			switch {
			case label == 0:
				pairs = append(pairs, [2]string{"__name__", fmt.Sprintf("metric_%04d", id%200)})
			case label < 12:
				pairs = append(pairs, [2]string{fmt.Sprintf("label_%02d", label), fmt.Sprintf("shared_%04d", (id+label)%50)})
			default:
				pairs = append(pairs, [2]string{fmt.Sprintf("label_%02d", label), fmt.Sprintf("unique_%02d_%08d", label, id)})
			}
		}
		slices.SortFunc(pairs, comparePairs)
		return pairs
	}
	// Written in time order across all series, like Kafka records.
	for timestamp := end - retentionMs; timestamp <= end; timestamp += intervalMs {
		for first := 0; first < series; first += 1_000 {
			request := record.DecodedRequest{}
			for id := first; id < min(first+1_000, series); id++ {
				request.Series = append(request.Series, record.DecodedSeries{Labels: labelsOf(id), Samples: []mimirpb.Sample{{TimestampMs: timestamp, Value: float64(id) + float64(timestamp/intervalMs)}}})
			}
			require.NoError(t, s.IngestRecovered("tenant", request, end))
		}
	}
	s.HeadTick(true, false)
	ingested := liveHeapBytes() - before
	require.NoError(t, s.WriteSnapshot([]SnapshotOffset{{Offset: 1, HasOffset: true, TimestampMs: 1}}))
	require.NoError(t, s.Close())
	s = nil
	before = liveHeapBytes()
	restored, err := Restore(20*60*1000, Retention{Ms: retentionMs, Set: true}, directory, 4)
	require.NoError(t, err)
	restoredBytes := liveHeapBytes() - before
	// Where the restored bytes are, from the structures' capacities.
	var table, labelBytes, chunkBytes, heads, postings int
	for _, shard := range restored.Store.shards {
		for _, tn := range shard.tenants {
			for _, g := range tn.series.groups {
				table += cap(g.entries)*int(unsafe.Sizeof(seriesEntry{})) + len(g.first.slots)*int(unsafe.Sizeof(hashSlot{}))
			}
			tn.series.forEach(func(entry *seriesEntry) {
				labelBytes += entry.labels.HeapSize()
				chunkBytes += len(entry.series.chunks)
				if entry.series.floatHead != nil {
					heads += entry.series.floatHead.appender.ByteCapacity()
				}
			})
			postings += cap(tn.series.refs) * int(unsafe.Sizeof(seriesRef{}))
			for _, values := range tn.series.postings {
				postings += len(values) * 8
			}
			for _, list := range tn.series.sharedPostings {
				postings += 24 + cap(list)*4
			}
		}
	}
	fmt.Printf("per_series table=%d labels=%d chunks=%d float_heads=%d postings=%d\n", table/series, labelBytes/series, chunkBytes/series, heads/series, postings/series)
	fmt.Printf("retained_series_heap_bytes series=%d ingested_per_series=%d restored_per_series=%d\n", series, int(ingested)/series, int(restoredBytes)/series)
	require.NoError(t, restored.Store.Close())
}
