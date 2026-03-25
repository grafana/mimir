// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

// BenchmarkMemPostingsMemory measures heap memory consumed by MemPostings
// at various scales. It reports bytes-per-series for the inverted index
// structure alone (not counting memSeries or chunk data).
//
// Run with: go test -tags stringlabels -run=^$ -bench=BenchmarkMemPostingsMemory -benchmem -count=1 ./pkg/ingester/
func BenchmarkMemPostingsMemory(b *testing.B) {
	for _, tc := range []struct {
		name             string
		numSeries        int
		labelsPerSeries  int
		uniqueNames      int
		valuesPerName    int // approximate unique values per label name
	}{
		{
			name:            "100k_series_typical",
			numSeries:       100_000,
			labelsPerSeries: 10,
			uniqueNames:     20,
			valuesPerName:   5_000,
		},
		{
			name:            "500k_series_typical",
			numSeries:       500_000,
			labelsPerSeries: 10,
			uniqueNames:     20,
			valuesPerName:   25_000,
		},
		{
			name:            "1M_series_typical",
			numSeries:       1_000_000,
			labelsPerSeries: 10,
			uniqueNames:     20,
			valuesPerName:   50_000,
		},
		{
			name:            "1M_series_high_cardinality",
			numSeries:       1_000_000,
			labelsPerSeries: 15,
			uniqueNames:     30,
			valuesPerName:   100_000,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkMemPostingsMemory(b, tc.numSeries, tc.labelsPerSeries, tc.uniqueNames, tc.valuesPerName)
		})
	}
}

func benchmarkMemPostingsMemory(b *testing.B, numSeries, labelsPerSeries, uniqueNames, valuesPerName int) {
	b.Helper()

	labelNames := make([]string, uniqueNames)
	for i := range labelNames {
		labelNames[i] = fmt.Sprintf("label_name_%04d", i)
	}

	labelValues := make([][]string, uniqueNames)
	for i := range labelValues {
		labelValues[i] = make([]string, valuesPerName)
		for j := range labelValues[i] {
			labelValues[i][j] = fmt.Sprintf("value_%s_%06d", labelNames[i], j)
		}
	}

	// Pre-build label sets so their allocation doesn't pollute the measurement.
	type seriesLabels struct {
		lset labels.Labels
	}
	allSeries := make([]seriesLabels, numSeries)
	for i := range allSeries {
		builder := labels.NewBuilder(labels.EmptyLabels())
		for j := 0; j < labelsPerSeries && j < uniqueNames; j++ {
			valIdx := i % len(labelValues[j])
			builder.Set(labelNames[j], labelValues[j][valIdx])
		}
		allSeries[i].lset = builder.Labels()
	}

	// Force GC and get baseline.
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		mp := index.NewMemPostings()
		runtime.GC()
		runtime.GC()
		var afterEmpty runtime.MemStats
		runtime.ReadMemStats(&afterEmpty)
		b.StartTimer()

		for i, s := range allSeries {
			mp.Add(storage.SeriesRef(i+1), s.lset)
		}

		b.StopTimer()

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		memPostingsBytes := after.HeapAlloc - afterEmpty.HeapAlloc
		bytesPerSeries := memPostingsBytes / uint64(numSeries)

		b.ReportMetric(float64(memPostingsBytes), "mempostings_bytes")
		b.ReportMetric(float64(bytesPerSeries), "bytes/series")
		b.ReportMetric(float64(memPostingsBytes)/(1024*1024), "mempostings_MiB")

		// Count unique label pairs for context.
		uniquePairs := 0
		for j := 0; j < labelsPerSeries && j < uniqueNames; j++ {
			seen := map[string]struct{}{}
			for i := range allSeries {
				valIdx := i % len(labelValues[j])
				seen[labelValues[j][valIdx]] = struct{}{}
			}
			uniquePairs += len(seen)
		}
		b.ReportMetric(float64(uniquePairs), "unique_label_pairs")
		b.ReportMetric(float64(memPostingsBytes)/float64(uniquePairs), "bytes/label_pair")

		// Keep mp alive past the measurement.
		runtime.KeepAlive(mp)
		b.StartTimer()
	}
}
