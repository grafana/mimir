// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/header_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"fmt"
	"math"
	"os"
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
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/storegateway/testhelper"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestReaders(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "test-indexheader")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	defer func() { require.NoError(t, bkt.Close()) }()

	// Create block index version 2.
	id1, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "5"}},
		{{Name: "a", Value: "6"}},
		{{Name: "a", Value: "7"}},
		{{Name: "a", Value: "8"}},
		{{Name: "a", Value: "9"}},
		// Missing 10 on purpose.
		{{Name: "a", Value: "11"}},
		{{Name: "a", Value: "12"}},
		{{Name: "a", Value: "13"}},
		{{Name: "a", Value: "1"}, {Name: "longer-string", Value: "1"}},
		{{Name: "a", Value: "1"}, {Name: "longer-string", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	require.NoError(t, err)

	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, id1.String()), metadata.NoneFunc))

	// Copy block index version 1 for backward compatibility.
	/* The block here was produced at the commit
	    706602daed1487f7849990678b4ece4599745905 used in 2.0.0 with:
	   db, _ := Open("v1db", nil, nil, nil)
	   app := db.Appender()
	   app.Add(labels.FromStrings("foo", "bar"), 1, 2)
	   app.Add(labels.FromStrings("foo", "baz"), 3, 4)
	   app.Add(labels.FromStrings("foo", "meh"), 1000*3600*4, 4) // Not in the block.
	   // Make sure we've enough values for the lack of sorting of postings offsets to show up.
	   for i := 0; i < 100; i++ {
	     app.Add(labels.FromStrings("bar", strconv.FormatInt(int64(i), 10)), 0, 0)
	   }
	   app.Commit()
	   db.compact()
	   db.Close()
	*/

	m, err := metadata.ReadFromDir("./testdata/index_format_v1")
	require.NoError(t, err)
	test.Copy(t, "./testdata/index_format_v1", filepath.Join(tmpDir, m.ULID.String()))

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, m.ULID.String()), metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, &m.BlockMeta)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, m.ULID.String()), metadata.NoneFunc))

	for _, id := range []ulid.ULID{id1, m.ULID} {
		t.Run(id.String(), func(t *testing.T) {
			indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, id.String(), block.IndexFilename))
			require.NoError(t, err)
			defer func() { _ = indexFile.Close() }()

			b := realByteSlice(indexFile.Bytes())

			testBinaryReader := func(t *testing.T, cfg BinaryReaderConfig) {
				fn := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
				require.NoError(t, WriteBinary(ctx, bkt, id, fn))

				br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, cfg)
				require.NoError(t, err)

				defer func() { require.NoError(t, br.Close()) }()

				if id == id1 {
					require.Equal(t, 1, br.version)
					require.Equal(t, 2, br.indexVersion)
					require.Equal(t, &BinaryTOC{Symbols: headerLen, PostingsOffsetTable: 70}, br.toc)
					require.Equal(t, int64(710), br.indexLastPostingEnd)
					require.Equal(t, 8, br.symbols.Size())
					require.Equal(t, 0, len(br.postingsV1))
					require.Equal(t, 2, len(br.nameSymbols))
					require.Equal(t, map[string]*postingValueOffsets{
						"": {
							offsets:       []postingOffset{{value: "", tableOff: 4}},
							lastValOffset: 440,
						},
						"a": {
							offsets: []postingOffset{
								{value: "1", tableOff: 9},
								{value: "13", tableOff: 32},
								{value: "4", tableOff: 54},
								{value: "7", tableOff: 75},
								{value: "9", tableOff: 89},
							},
							lastValOffset: 640,
						},
						"longer-string": {
							offsets: []postingOffset{
								{value: "1", tableOff: 96},
								{value: "2", tableOff: 115},
							},
							lastValOffset: 706,
						},
					}, br.postings)

					vals, err := br.LabelValues("not-existing")
					require.NoError(t, err)
					require.Equal(t, []string(nil), vals)

					// Regression tests for https://github.com/thanos-io/thanos/issues/2213.
					// Most of not existing value was working despite bug, except in certain unlucky cases
					// it was causing "invalid size" errors.
					_, err = br.PostingsOffset("not-existing", "1")
					require.Equal(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("a", "0")
					require.Equal(t, NotFoundRangeErr, err)
					// Unlucky case, because the bug was causing unnecessary read & decode requiring more bytes than
					// available. For rest cases read was noop wrong, but at least not failing.
					_, err = br.PostingsOffset("a", "10")
					require.Equal(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("a", "121")
					require.Equal(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("a", "131")
					require.Equal(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("a", "91")
					require.Equal(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("longer-string", "0")
					require.Equal(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("longer-string", "11")
					require.Equal(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("longer-string", "21")
					require.Equal(t, NotFoundRangeErr, err)
				}

				compareIndexToHeader(t, b, br)
			}

			t.Run("binary reader", func(t *testing.T) {
				testBinaryReader(t, BinaryReaderConfig{})
			})

			t.Run("binary reader with map populate", func(t *testing.T) {
				testBinaryReader(t, BinaryReaderConfig{MapPopulateEnabled: true})
			})

			t.Run("lazy binary reader", func(t *testing.T) {
				fn := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
				require.NoError(t, WriteBinary(ctx, bkt, id, fn))

				br, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, BinaryReaderConfig{}, NewLazyBinaryReaderMetrics(nil), nil)
				require.NoError(t, err)

				defer func() { require.NoError(t, br.Close()) }()

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

		vals, err := headerReader.LabelValues(lname)
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
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, &m.BlockMeta)
	require.NoError(t, err)
	require.NoError(t, block.Upload(context.Background(), log.NewNopLogger(), bkt, filepath.Join(tmpDir, m.ULID.String()), metadata.NoneFunc))

	return m
}

func BenchmarkBinaryWrite(t *testing.B) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "bench-indexheader")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

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

func BenchmarkBinaryReader(t *testing.B) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "bench-indexheader")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)

	m := prepareIndexV2Block(t, tmpDir, bkt)
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexHeaderFilename)
	require.NoError(t, WriteBinary(ctx, bkt, m.ULID, fn))

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		br, err := newFileBinaryReader(fn, 32, BinaryReaderConfig{})
		require.NoError(t, err)
		require.NoError(t, br.Close())
	}
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

	tmpDir, err := os.MkdirTemp("", "benchmark-lookupsymbol")
	require.NoError(b, err)
	defer func() { require.NoError(b, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(b, err)
	defer func() { require.NoError(b, bkt.Close()) }()

	// Generate series labels.
	seriesLabels := make([]labels.Labels, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesLabels = append(seriesLabels, labels.Labels{{Name: "a", Value: strconv.Itoa(i)}})
	}

	// Create a block.
	id1, err := testhelper.CreateBlock(ctx, tmpDir, seriesLabels, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	require.NoError(b, err)
	require.NoError(b, block.Upload(ctx, logger, bkt, filepath.Join(tmpDir, id1.String()), metadata.NoneFunc))

	// Create an index reader.
	reader, err := NewBinaryReader(ctx, logger, bkt, tmpDir, id1, postingOffsetsInMemSampling, BinaryReaderConfig{})
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
