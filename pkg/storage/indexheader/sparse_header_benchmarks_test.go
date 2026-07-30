// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"bytes"
	"compress/gzip"
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

	gzBytes := buildBenchmarkGzippedSparseHeader(b, sparseSampleFactor)
	logger := log.NewNopLogger()

	liveBytes, liveObjects := measureLoadedSparseHeaderLiveMemory(b, gzBytes, sparseSampleFactor, logger)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := loadSparseHeader(gzBytes, sparseSampleFactor, logger)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(liveBytes, "live-B/op")
	b.ReportMetric(liveObjects, "live-objects/op")
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

	return float64(after.HeapAlloc-before.HeapAlloc) / replicas,
		float64(after.HeapObjects-before.HeapObjects) / replicas
}

// buildBenchmarkGzippedSparseHeader builds a sparse index-header proto with a realistic shape
// (a few high-cardinality labels dominating the total number of sampled entries,
// value strings resembling pod/instance names) and serializes it the same way it is stored on disk.
func buildBenchmarkGzippedSparseHeader(b *testing.B, sparseSampleFactor int) []byte {
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

	for i := 0; i < 5; i++ {
		addLabel(fmt.Sprintf("high_cardinality_%02d", i), 20_000)
	}
	for i := 0; i < 45; i++ {
		addLabel(fmt.Sprintf("medium_cardinality_%02d", i), 500)
	}
	for i := 0; i < 100; i++ {
		addLabel(fmt.Sprintf("low_cardinality_%03d", i), 3)
	}

	// symbolFactor mirrors the constant of the same name in the index package.
	const symbolFactor = 32
	symbolsCount := totalSampledEntries * sparseSampleFactor
	symbolsOffsets := make([]int64, 0, symbolsCount/symbolFactor+1)
	for off := int64(8); len(symbolsOffsets)*symbolFactor < symbolsCount; off += int64(symbolFactor) * 30 {
		symbolsOffsets = append(symbolsOffsets, off)
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

	marshalled, err := sparseHeaderProto.Marshal()
	require.NoError(b, err)

	gzipped := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(gzipped)
	_, err = gzipWriter.Write(marshalled)
	require.NoError(b, err)
	require.NoError(b, gzipWriter.Close())

	b.Logf("sparse header fixture: %d labels, %d sampled entries, proto %d bytes, gzipped %d bytes",
		len(postings), totalSampledEntries, len(marshalled), gzipped.Len())

	return gzipped.Bytes()
}
