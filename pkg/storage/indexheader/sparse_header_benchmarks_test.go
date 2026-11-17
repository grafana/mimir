// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

// BenchmarkLoadSparseHeader isolates the sparse index-header load path:
// gzipped proto bytes -> in-memory representation used by index-header readers.
// It also reports the retained (live heap) bytes of the loaded representation as "live-B/op".
func BenchmarkLoadSparseHeader(b *testing.B) {
	const sparseSampleFactor = 32

	for _, shape := range []struct {
		name   string
		labels []benchmarkLabelClass
	}{
		{"few-large-labels", []benchmarkLabelClass{{"high_cardinality", 5, 20_000}, {"medium_cardinality", 45, 500}, {"low_cardinality", 100, 3}}},
		{"many-small-labels", []benchmarkLabelClass{{"high_cardinality", 5, 2_000}, {"medium_cardinality", 45, 50}, {"low_cardinality", 1_700, 3}}},
	} {
		b.Run(shape.name, func(b *testing.B) {
			gzBytes := buildBenchmarkGzippedSparseHeader(b, sparseSampleFactor, shape.labels)
			logger := log.NewNopLogger()

			liveBytes, liveObjects := measureLoadedSparseHeaderLiveMemory(b, gzBytes, sparseSampleFactor, logger)

			b.ReportAllocs()
			for b.Loop() {
				_, _, err := loadSparseHeader(gzBytes, sparseSampleFactor, logger)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(liveBytes, "live-B/op")
			b.ReportMetric(liveObjects, "live-objects/op")
		})
	}
}

type benchmarkLabelClass struct {
	namePrefix     string
	count          int
	sampledOffsets int
}

type loadedSparseHeader struct {
	sparseSymbols         streamindex.SparseSymbols
	sparsePostingsOffsets map[string]*streamindex.SparseTableOffsetsForLabel
}

func measureLoadedSparseHeaderLiveMemory(b *testing.B, gzBytes []byte, sparseSampleFactor int, logger log.Logger) (liveBytes, liveObjects float64) {
	const replicas = 4

	retained := make([]loadedSparseHeader, 0, replicas)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < replicas; i++ {
		sparseSymbols, sparsePostingsOffsets, err := loadSparseHeader(gzBytes, sparseSampleFactor, logger)
		require.NoError(b, err)
		retained = append(retained, loadedSparseHeader{sparseSymbols, sparsePostingsOffsets})
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(retained)

	return float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / replicas,
		float64(int64(after.HeapObjects)-int64(before.HeapObjects)) / replicas
}

// buildBenchmarkGzippedSparseHeader builds a sparse index-header proto with the given label classes,
// with value strings resembling pod/instance names, and serializes it the same way it is stored on disk.
func buildBenchmarkGzippedSparseHeader(b *testing.B, sparseSampleFactor int, labels []benchmarkLabelClass) []byte {
	random := rand.New(rand.NewSource(42))
	const suffixAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	randomSuffix := func(n int) string {
		s := make([]byte, n)
		for i := range s {
			s[i] = suffixAlphabet[random.Intn(len(suffixAlphabet))]
		}
		return string(s)
	}

	postings := make(map[string]*indexheaderpb.PostingValueOffsets)
	tableOff := int64(8)
	totalSampledEntries := 0
	addLabel := func(name string, sampledOffsets int) {
		offsets := make([]*indexheaderpb.PostingOffset, sampledOffsets)
		for i := range offsets {
			// Fixed-width counter keeps values sorted, like in the real postings offset table.
			value := fmt.Sprintf("%s-%07d-%s", name, i, randomSuffix(8+random.Intn(16)))
			offsets[i] = &indexheaderpb.PostingOffset{Value: value, TableOff: tableOff}
			tableOff += int64(sparseSampleFactor) * int64(len(name)+len(value)+12)
		}
		postings[name] = &indexheaderpb.PostingValueOffsets{Offsets: offsets, LastValOffset: tableOff}
		totalSampledEntries += sampledOffsets
	}

	for _, class := range labels {
		for i := 0; i < class.count; i++ {
			addLabel(fmt.Sprintf("%s_%04d", class.namePrefix, i), class.sampledOffsets)
		}
	}

	symbolsCount := totalSampledEntries * sparseSampleFactor
	numSymbolsOffsets := streamindex.NumSampledSymbols(symbolsCount)
	// Space the offsets like a table of ~30-byte symbols.
	offsetStride := int64(symbolsCount/numSymbolsOffsets) * 30
	symbolsOffsets := make([]int64, 0, numSymbolsOffsets)
	for i := 0; i < numSymbolsOffsets; i++ {
		symbolsOffsets = append(symbolsOffsets, 8+int64(i)*offsetStride)
	}

	sparseHeaderProto := &indexheaderpb.Sparse{
		Symbols: &indexheaderpb.Symbols{
			Offsets:      symbolsOffsets,
			SymbolsCount: int64(symbolsCount),
		},
		PostingsOffsetTable: &indexheaderpb.PostingOffsetTable{
			Postings:                      postings,
			PostingOffsetInMemorySampling: int64(sparseSampleFactor),
		},
	}

	gzipped, err := gzipSparseHeaderProto(sparseHeaderProto)
	require.NoError(b, err)

	b.Logf("sparse header fixture: %d labels, %d sampled entries, gzipped %d bytes",
		len(postings), totalSampledEntries, gzipped.Len())

	return gzipped.Bytes()
}
