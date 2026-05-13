// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"testing"

	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/util/filepool"
)

type postingEntry struct {
	name   string
	value  string
	offset uint64
}

func TestPostingsOffsetsTableV2_PostingsOffset(t *testing.T) {
	encBuf, postingsListEnd, tableStart := setupPostingsOffsetsTableEncbuf()
	factories := setupFactoriesWithPostingsOffsetsTable(t, encBuf)

	// Put every other entry in the sparse offsets table. This way,
	// any entry we search for which isn't already in the sparse offsets table is also the "worst-case" scenario,
	// where the found entry is the last one before the next sparse offset.
	sparseSampleFactor := 2

	for factoryName, factory := range factories {
		t.Run(fmt.Sprintf("DecbufFactory=%s", factoryName), func(t *testing.T) {
			sparseOffsets, err := SparseValuesFromPostingsOffsetsTable(factory, tableStart, postingsListEnd, sparseSampleFactor, true)
			require.NoError(t, err)

			tbl, err := NewPostingsOffsetsTableReader(index.FormatV2, factory, tableStart, sparseOffsets, sparseSampleFactor)
			require.NoError(t, err)

			tests := []struct {
				labelName  string
				labelValue string
				found      bool
				expected   index.Range
			}{
				// Sparse offset entry exactly
				{"job", "foo", true, index.Range{Start: 500 + postingLengthFieldSize, End: 600 - crc32.Size}},
				// Value between sparse offset entries
				{"__name__", "baz", true, index.Range{Start: 200 + postingLengthFieldSize, End: 300 - crc32.Size}},
				// Nonexistent values
				{"__name__", "aaa", false, index.Range{}},
				{"__name__", "zzz", false, index.Range{}},
				{"__name__", "bzz", false, index.Range{}},
				// Nonexistent label
				{"pod", "foo", false, index.Range{}},
			}

			for _, tt := range tests {
				t.Run(fmt.Sprintf("%s=%s", tt.labelName, tt.labelValue), func(t *testing.T) {
					rng, found, err := tbl.PostingsOffset(tt.labelName, tt.labelValue)
					require.NoError(t, err)
					require.Equal(t, tt.found, found)
					if tt.found {
						require.Equal(t, tt.expected, rng)
					}
				})
			}
		})
	}
}

func TestPostingsOffsetsTableV2_LabelValuesOffsets(t *testing.T) {
	encBuf, postingsListEnd, tableStart := setupPostingsOffsetsTableEncbuf()
	factories := setupFactoriesWithPostingsOffsetsTable(t, encBuf)
	sparseSampleFactor := 2

	for factoryName, factory := range factories {
		t.Run(fmt.Sprintf("DecbufFactory=%s", factoryName), func(t *testing.T) {
			sparseOffsets, err := SparseValuesFromPostingsOffsetsTable(factory, tableStart, postingsListEnd, sparseSampleFactor, true)
			require.NoError(t, err)

			tbl, err := NewPostingsOffsetsTableReader(index.FormatV2, factory, tableStart, sparseOffsets, sparseSampleFactor)
			require.NoError(t, err)

			tests := []struct {
				testName    string
				labelName   string
				prefix      string
				filter      func(string) bool
				expected    []PostingListOffset
				expectedErr bool
			}{
				{
					testName:  "fetch all values without prefix or filter",
					labelName: "__name__",
					expected: []PostingListOffset{
						{LabelValue: "bar", Off: index.Range{Start: 100 + postingLengthFieldSize, End: 200 - crc32.Size}},
						{LabelValue: "baz", Off: index.Range{Start: 204, End: 296}},
						{LabelValue: "foo", Off: index.Range{Start: 304, End: 396}},
						{LabelValue: "zip", Off: index.Range{Start: 404, End: 496}},
					},
				},
				{
					testName:  "fetch all values with prefix",
					labelName: "__name__",
					prefix:    "b",
					expected: []PostingListOffset{
						{LabelValue: "bar", Off: index.Range{Start: 104, End: 196}},
						{LabelValue: "baz", Off: index.Range{Start: 204, End: 296}},
					},
				},
				{
					testName:  "prefix matches last value for name",
					labelName: "__name__",
					prefix:    "z",
					expected: []PostingListOffset{
						{LabelValue: "zip", Off: index.Range{Start: 404, End: 496}},
					},
				},
				{
					testName:  "filter matches single value",
					labelName: "__name__",
					filter: func(v string) bool {
						r, err := regexp.Compile("fo.*")
						require.NoError(t, err)
						return r.MatchString(v)
					},
					expected: []PostingListOffset{
						{LabelValue: "foo", Off: index.Range{Start: 304, End: 396}},
					},
				},
				{
					testName:  "nonexistent label",
					labelName: "pod",
					expected:  nil,
				},
			}

			for _, tt := range tests {
				t.Run(tt.testName, func(t *testing.T) {
					result, err := tbl.LabelValuesOffsets(context.Background(), tt.labelName, tt.prefix, tt.filter)
					if tt.expectedErr {
						require.Error(t, err)
					}

					require.Equal(t, tt.expected, result)
				})
			}
		})
	}
}

func setupFactoriesWithPostingsOffsetsTable(t *testing.T, postingsOffsetsEncbuf encoding.Encbuf) map[string]streamencoding.DecbufFactory {
	dir := t.TempDir()
	filePath := path.Join(dir, "index")
	require.NoError(t, os.WriteFile(filePath, postingsOffsetsEncbuf.Get(), 0700))

	bkt, err := filesystem.NewBucket(dir)
	require.NoError(t, err)
	instBkt := objstore.WithNoopInstr(bkt)
	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
	})

	reg := prometheus.WrapRegistererWithPrefix("indexheader_", prometheus.NewPedanticRegistry())
	diskDecbufFactory := streamencoding.NewFilePoolDecbufFactory(filePath, 0, filepool.NewFilePoolMetrics(reg))
	bucketDecbufFactory := streamencoding.NewBucketDecbufFactory(context.Background(), instBkt, "index")

	return map[string]streamencoding.DecbufFactory{
		"disk":   diskDecbufFactory,
		"bucket": bucketDecbufFactory,
	}
}

func setupPostingsOffsetsTableEncbuf() (e encoding.Encbuf, postingsListEnd uint64, tableOffset int) {
	entries := []postingEntry{
		{"", "", 0}, // index.AllPostingsKey
		{"__name__", "bar", 100},
		{"__name__", "baz", 200},
		{"__name__", "foo", 300},
		{"__name__", "zip", 400},
		{"job", "foo", 500},
	}
	var postingsList encoding.Encbuf
	postingsList.PutBE32int(len(entries))
	for _, entry := range entries {
		postingsList.PutUvarint(2)
		postingsList.PutUvarintStr(entry.name)
		postingsList.PutUvarintStr(entry.value)
		postingsList.PutUvarint64(entry.offset)
	}
	postingsListEnd = 600
	e.PutUvarintStr("filler") // simulate table being part of a larger file; we don't want to read these bytes
	tableStart := e.Len()
	e.PutBE32int(postingsList.Len())
	e.PutBytes(postingsList.Get())
	e.PutBE32(crc32.Checksum(postingsList.Get(), castagnoliTable))

	return e, postingsListEnd, tableStart
}
