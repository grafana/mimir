// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/header_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/testhelper"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestReaders(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
	})

	// Create block index version 2.
	idIndexV2, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("a", "5"),
		labels.FromStrings("a", "6"),
		labels.FromStrings("a", "7"),
		labels.FromStrings("a", "8"),
		labels.FromStrings("a", "9"),
		// Missing 10 on purpose.
		labels.FromStrings("a", "11"),
		labels.FromStrings("a", "12"),
		labels.FromStrings("a", "13"),
		labels.FromStrings("a", "1", "longer-string", "1"),
		labels.FromStrings("a", "1", "longer-string", "2"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, idIndexV2.String()), metadata.NoneFunc))

	metaIndexV1, err := metadata.ReadFromDir("./testdata/index_format_v1")
	require.NoError(t, err)
	test.Copy(t, "./testdata/index_format_v1", filepath.Join(tmpDir, metaIndexV1.ULID.String()))

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, metaIndexV1.ULID.String()), metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, &metaIndexV1.BlockMeta)

	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, metaIndexV1.ULID.String()), metadata.NoneFunc))

	for _, id := range []ulid.ULID{idIndexV2, metaIndexV1.ULID} {
		t.Run(id.String(), func(t *testing.T) {
			indexName := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
			require.NoError(t, WriteBinary(ctx, bkt, id, indexName))

			indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, id.String(), block.IndexFilename))
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, indexFile.Close())
			})

			b := realByteSlice(indexFile.Bytes())

			t.Run("binary reader", func(t *testing.T) {
				br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, Config{})
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, br.Close())
				})

				compareIndexToHeader(t, b, br)
			})

			t.Run("binary reader with map populate", func(t *testing.T) {
				br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, Config{MapPopulateEnabled: true})
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, br.Close())
				})

				compareIndexToHeader(t, b, br)
			})

			t.Run("lazy binary reader", func(t *testing.T) {
				factory := func() (Reader, error) {
					return NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, Config{})
				}

				br, err := NewLazyBinaryReader(ctx, factory, log.NewNopLogger(), nil, tmpDir, id, NewLazyBinaryReaderMetrics(nil), nil)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, br.Close())
				})

				compareIndexToHeader(t, b, br)
			})

			t.Run("stream binary reader", func(t *testing.T) {
				br, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, Config{})
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, br.Close())
				})

				compareIndexToHeader(t, b, br)
			})

		})
	}

}

func compareIndexToHeader(t *testing.T, indexByteSlice index.ByteSlice, headerReader Reader) {
	indexReader, err := index.NewReader(indexByteSlice)
	require.NoError(t, err)
	defer func() { _ = indexReader.Close() }()

	actVersion, err := headerReader.IndexVersion()
	require.NoError(t, err)
	require.Equal(t, indexReader.Version(), actVersion)

	if indexReader.Version() == index.FormatV2 {
		// For v2 symbols ref sequential integers 0, 1, 2 etc.
		iter := indexReader.Symbols()
		i := 0
		for iter.Next() {
			r, err := headerReader.LookupSymbol(uint32(i))
			require.NoError(t, err)
			require.Equal(t, iter.At(), r)

			i++
		}
		require.NoError(t, iter.Err())
		_, err := headerReader.LookupSymbol(uint32(i))
		require.Error(t, err)

	} else {
		// For v1 symbols refs are actual offsets in the index.
		symbols, err := getSymbolTable(indexByteSlice)
		require.NoError(t, err)

		for refs, sym := range symbols {
			r, err := headerReader.LookupSymbol(refs)
			require.NoError(t, err)
			require.Equal(t, sym, r)
		}
		_, err = headerReader.LookupSymbol(200000)
		require.Error(t, err)
	}

	expLabelNames, err := indexReader.LabelNames()
	require.NoError(t, err)
	actualLabelNames, err := headerReader.LabelNames()
	require.NoError(t, err)
	require.Equal(t, expLabelNames, actualLabelNames)

	expRanges, err := indexReader.PostingsRanges()
	require.NoError(t, err)

	minStart := int64(math.MaxInt64)
	maxEnd := int64(math.MinInt64)
	for il, lname := range expLabelNames {
		expectedLabelVals, err := indexReader.SortedLabelValues(lname)
		require.NoError(t, err)

		vals, err := headerReader.LabelValues(lname, nil)
		require.NoError(t, err)
		require.Equal(t, expectedLabelVals, vals)

		for iv, v := range vals {
			if minStart > expRanges[labels.Label{Name: lname, Value: v}].Start {
				minStart = expRanges[labels.Label{Name: lname, Value: v}].Start
			}
			if maxEnd < expRanges[labels.Label{Name: lname, Value: v}].End {
				maxEnd = expRanges[labels.Label{Name: lname, Value: v}].End
			}

			ptr, err := headerReader.PostingsOffset(lname, v)
			require.NoError(t, err)

			// For index-cache those values are exact.
			//
			// For binary they are exact except last item posting offset. It's good enough if the value is larger than exact posting ending.
			if indexReader.Version() == index.FormatV2 {
				if iv == len(vals)-1 && il == len(expLabelNames)-1 {
					require.Equal(t, expRanges[labels.Label{Name: lname, Value: v}].Start, ptr.Start)
					require.Truef(t, expRanges[labels.Label{Name: lname, Value: v}].End <= ptr.End, "got offset %v earlier than actual posting end %v ", ptr.End, expRanges[labels.Label{Name: lname, Value: v}].End)
					continue
				}
			} else {
				// For index formatV1 the last one does not mean literally last value, as postings were not sorted.
				// Account for that. We know it's 40 label value.
				if v == "40" {
					require.Equal(t, expRanges[labels.Label{Name: lname, Value: v}].Start, ptr.Start)
					require.Truef(t, expRanges[labels.Label{Name: lname, Value: v}].End <= ptr.End, "got offset %v earlier than actual posting end %v ", ptr.End, expRanges[labels.Label{Name: lname, Value: v}].End)
					continue
				}
			}
			require.Equal(t, expRanges[labels.Label{Name: lname, Value: v}], ptr)
		}
	}

	ptr, err := headerReader.PostingsOffset(index.AllPostingsKey())
	require.NoError(t, err)
	require.Equal(t, expRanges[labels.Label{Name: "", Value: ""}].Start, ptr.Start)
	require.Equal(t, expRanges[labels.Label{Name: "", Value: ""}].End, ptr.End)
}

func prepareIndexV2Block(t testing.TB, tmpDir string, bkt objstore.Bucket) *metadata.Meta {
	/* Copy index 6MB block index version 2. It was generated via thanosbench. Meta.json:
		{
		"ulid": "01DRBP4RNVZ94135ZA6B10EMRR",
		"minTime": 1570766415000,
		"maxTime": 1570939215001,
		"stats": {
			"numSamples": 115210000,
			"numSeries": 10000,
			"numChunks": 990000
		},
		"compaction": {
			"level": 1,
			"sources": [
				"01DRBP4RNVZ94135ZA6B10EMRR"
			]
		},
		"version": 1,
		"thanos": {
			"labels": {
				"cluster": "one",
				"dataset": "continuous"
			},
			"downsample": {
				"resolution": 0
			},
			"source": "blockgen"
		}
	}
	*/

	m, err := metadata.ReadFromDir("./testdata/index_format_v2")
	require.NoError(t, err)
	test.Copy(t, "./testdata/index_format_v2", filepath.Join(tmpDir, m.ULID.String()))

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, m.ULID.String()), metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, &m.BlockMeta)
	require.NoError(t, err)
	require.NoError(t, block.Upload(context.Background(), log.NewNopLogger(), bkt, filepath.Join(tmpDir, m.ULID.String()), metadata.NoneFunc))

	return m
}

func BenchmarkBinaryWrite(t *testing.B) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	defer func() { require.NoError(t, bkt.Close()) }()

	m := prepareIndexV2Block(t, tmpDir, bkt)
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexHeaderFilename)

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		require.NoError(t, WriteBinary(ctx, bkt, m.ULID, fn))
	}
}

func BenchmarkBinaryReader_ThanosbenchBlock(t *testing.B) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)

	m := prepareIndexV2Block(t, tmpDir, bkt)
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexHeaderFilename)
	require.NoError(t, WriteBinary(ctx, bkt, m.ULID, fn))

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		br, err := newFileBinaryReader(fn, 32, Config{})
		require.NoError(t, err)
		require.NoError(t, br.Close())
	}
}

func BenchmarkBinaryReader_LargerBlock(b *testing.B) {
	const (
		// labelLongSuffix is a label with ~50B in size, to emulate real-world high cardinality.
		labelLongSuffix = "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
		series          = 1e6
	)

	ctx := context.Background()

	tmpDir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(b, err)

	seriesLabels := make([]labels.Labels, 0, series)
	for n := 0; n < 10; n++ {
		for i := 0; i < series/10/5; i++ {
			seriesLabels = append(seriesLabels, labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", strconv.Itoa(n)+labelLongSuffix, "j", "foo", "p", "foo"))
			seriesLabels = append(seriesLabels, labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", strconv.Itoa(n)+labelLongSuffix, "j", "bar", "q", "foo"))
			seriesLabels = append(seriesLabels, labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "0_"+strconv.Itoa(n)+labelLongSuffix, "j", "bar", "r", "foo"))
			seriesLabels = append(seriesLabels, labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "1_"+strconv.Itoa(n)+labelLongSuffix, "j", "bar", "s", "foo"))
			seriesLabels = append(seriesLabels, labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "2_"+strconv.Itoa(n)+labelLongSuffix, "j", "foo", "t", "foo"))
		}
	}

	blockID, err := testhelper.CreateBlock(ctx, tmpDir, seriesLabels, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	require.NoError(b, err)
	require.NoError(b, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	filename := filepath.Join(tmpDir, "bkt", blockID.String(), block.IndexHeaderFilename)
	require.NoError(b, WriteBinary(ctx, bkt, blockID, filename))

	b.ResetTimer()
	b.Run("benchmark", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			br, err := newFileBinaryReader(filename, 32, Config{})
			require.NoError(b, err)
			require.NoError(b, br.Close())
		}
	})
}

func BenchmarkBinaryReader_LookupSymbol(b *testing.B) {
	for _, numSeries := range []int{valueSymbolsCacheSize, valueSymbolsCacheSize * 10} {
		b.Run(fmt.Sprintf("num series = %d", numSeries), func(b *testing.B) {
			benchmarkBinaryReaderLookupSymbol(b, numSeries)
		})
	}
}

func benchmarkBinaryReaderLookupSymbol(b *testing.B, numSeries int) {
	const postingOffsetsInMemSampling = 32

	ctx := context.Background()
	logger := log.NewNopLogger()

	tmpDir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(b, err)
	defer func() { require.NoError(b, bkt.Close()) }()

	// Generate series labels.
	seriesLabels := make([]labels.Labels, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesLabels = append(seriesLabels, labels.FromStrings("a", strconv.Itoa(i)))
	}

	// Create a block.
	id1, err := testhelper.CreateBlock(ctx, tmpDir, seriesLabels, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	require.NoError(b, err)
	require.NoError(b, block.Upload(ctx, logger, bkt, filepath.Join(tmpDir, id1.String()), metadata.NoneFunc))

	// Create an index reader.
	reader, err := NewBinaryReader(ctx, logger, bkt, tmpDir, id1, postingOffsetsInMemSampling, Config{})
	require.NoError(b, err)

	// Get the offset of each label value symbol.
	symbolsOffsets := make([]uint32, numSeries)
	for i := 0; i < numSeries; i++ {
		o, err := reader.symbols.ReverseLookup(strconv.Itoa(i))
		require.NoError(b, err)

		symbolsOffsets[i] = o
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for i := 0; i < len(symbolsOffsets); i++ {
			if _, err := reader.LookupSymbol(symbolsOffsets[i]); err != nil {
				b.Fail()
			}
		}
	}
}

func getSymbolTable(b index.ByteSlice) (map[uint32]string, error) {
	version := int(b.Range(4, 5)[0])

	if version != 1 && version != 2 {
		return nil, errors.Errorf("unknown index file version %d", version)
	}

	toc, err := index.NewTOCFromByteSlice(b)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	symbolsV2, symbolsV1, err := readSymbols(b, version, int(toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	symbolsTable := make(map[uint32]string, len(symbolsV1)+len(symbolsV2))
	for o, s := range symbolsV1 {
		symbolsTable[o] = s
	}
	for o, s := range symbolsV2 {
		symbolsTable[uint32(o)] = s
	}
	return symbolsTable, nil
}

// readSymbols reads the symbol table fully into memory and allocates proper strings for them.
// Strings backed by the mmap'd memory would cause memory faults if applications keep using them
// after the reader is closed.
func readSymbols(bs index.ByteSlice, version, off int) ([]string, map[uint32]string, error) {
	if off == 0 {
		return nil, nil, nil
	}
	d := encoding.NewDecbufAt(bs, off, castagnoliTable)

	var (
		origLen     = d.Len()
		cnt         = d.Be32int()
		basePos     = uint32(off) + 4
		nextPos     = basePos + uint32(origLen-d.Len())
		symbolSlice []string
		symbols     = map[uint32]string{}
	)
	if version == index.FormatV2 {
		symbolSlice = make([]string, 0, cnt)
	}

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		s := d.UvarintStr()

		if version == index.FormatV2 {
			symbolSlice = append(symbolSlice, s)
		} else {
			symbols[nextPos] = s
			nextPos = basePos + uint32(origLen-d.Len())
		}
		cnt--
	}
	return symbolSlice, symbols, errors.Wrap(d.Err(), "read symbols")
}
