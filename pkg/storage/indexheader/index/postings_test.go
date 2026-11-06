// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
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

	for factoryName, factory := range factories {
		sparseOffsets, err := SparseValuesFromPostingsOffsetsTable(context.Background(), factory, tableStart, postingsListEnd, sparseSampleFactor, true)
		require.NoError(t, err)

		tbl, err := NewPostingsOffsetsTableReader(index.FormatV2, factory, tableStart, sparseOffsets, sparseSampleFactor)
		require.NoError(t, err)

		for _, tt := range tests {
			t.Run(fmt.Sprintf("DecbufFactory=%s/%s=%s", factoryName, tt.labelName, tt.labelValue), func(t *testing.T) {
				rng, found, err := tbl.PostingsOffset(context.Background(), tt.labelName, tt.labelValue)
				require.NoError(t, err)
				require.Equal(t, tt.found, found)
				if tt.found {
					require.Equal(t, tt.expected, rng)
				}
			})
		}
	}
}

type labelValuesOffsetsTestCase struct {
	testName  string
	labelName string
	prefix    string
	filter    func(string) bool
	expected  []PostingListOffset
	// expectedSizeHint is the estimate that the scan passes to SizeHint. It is zero when no scan starts.
	expectedSizeHint int
}

// labelValuesOffsetsTestCases returns the test cases for the table that setupPostingsOffsetsTableEncbuf builds.
// The sparse sampling factor of the table is 2, and the sparse offsets of __name__ are bar, foo and zip.
// A nil expected value means that no scan starts, because the label does not exist or the prefix matches no sparse offset.
func labelValuesOffsetsTestCases(t *testing.T) []labelValuesOffsetsTestCase {
	return []labelValuesOffsetsTestCase{
		{
			testName:  "fetch all values without prefix or filter",
			labelName: "__name__",
			expected: []PostingListOffset{
				{LabelValue: "bar", Off: index.Range{Start: 100 + postingLengthFieldSize, End: 200 - crc32.Size}},
				{LabelValue: "baz", Off: index.Range{Start: 204, End: 296}},
				{LabelValue: "foo", Off: index.Range{Start: 304, End: 396}},
				{LabelValue: "zip", Off: index.Range{Start: 404, End: 496}},
			},
			expectedSizeHint: 6,
		},
		{
			testName:  "fetch all values with prefix",
			labelName: "__name__",
			prefix:    "b",
			expected: []PostingListOffset{
				{LabelValue: "bar", Off: index.Range{Start: 104, End: 196}},
				{LabelValue: "baz", Off: index.Range{Start: 204, End: 296}},
			},
			expectedSizeHint: 2,
		},
		{
			testName:  "prefix matches last value for name",
			labelName: "__name__",
			prefix:    "z",
			expected: []PostingListOffset{
				{LabelValue: "zip", Off: index.Range{Start: 404, End: 496}},
			},
			expectedSizeHint: 4,
		},
		{
			testName:         "prefix matches no value for name",
			labelName:        "__name__",
			prefix:           "q",
			expected:         []PostingListOffset{},
			expectedSizeHint: 2,
		},
		{
			testName:  "prefix sorts before the first value",
			labelName: "__name__",
			prefix:    "a",
			expected:  nil,
		},
		{
			testName:  "prefix sorts after the last value",
			labelName: "__name__",
			prefix:    "zzz",
			expected:  nil,
		},
		{
			testName:  "filter matches single value",
			labelName: "__name__",
			filter: func(v string) bool {
				r, err := regexp.Compile("..o")
				require.NoError(t, err)
				return r.MatchString(v)
			},
			expected: []PostingListOffset{
				{LabelValue: "foo", Off: index.Range{Start: 304, End: 396}},
			},
			expectedSizeHint: 6,
		},
		{
			testName:  "filter matches no value",
			labelName: "__name__",
			filter: func(v string) bool {
				r, err := regexp.Compile(".*q.*")
				require.NoError(t, err)
				return r.MatchString(v)
			},
			expected:         []PostingListOffset{},
			expectedSizeHint: 6,
		},
		{
			testName:  "filter matches every other value",
			labelName: "__name__",
			filter: func(v string) bool {
				return v == "bar" || v == "foo"
			},
			expected: []PostingListOffset{
				{LabelValue: "bar", Off: index.Range{Start: 104, End: 196}},
				{LabelValue: "foo", Off: index.Range{Start: 304, End: 396}},
			},
			expectedSizeHint: 6,
		},
		{
			// The only value of the last label ends at the end of the posting lists, which the table gets from the sparse offsets.
			testName:  "last label with a single value",
			labelName: "job",
			expected: []PostingListOffset{
				{LabelValue: "foo", Off: index.Range{Start: 504, End: 596}},
			},
			expectedSizeHint: 2,
		},
		{
			testName:  "nonexistent label",
			labelName: "pod",
			expected:  nil,
		},
	}
}

func TestPostingsOffsetsTableV2_LabelValuesOffsets(t *testing.T) {
	encBuf, postingsListEnd, tableStart := setupPostingsOffsetsTableEncbuf()
	factories := setupFactoriesWithPostingsOffsetsTable(t, encBuf)
	sparseSampleFactor := 2

	for factoryName, factory := range factories {
		sparseOffsets, err := SparseValuesFromPostingsOffsetsTable(context.Background(), factory, tableStart, postingsListEnd, sparseSampleFactor, true)
		require.NoError(t, err)

		tbl, err := NewPostingsOffsetsTableReader(index.FormatV2, factory, tableStart, sparseOffsets, sparseSampleFactor)
		require.NoError(t, err)

		for _, tt := range labelValuesOffsetsTestCases(t) {
			t.Run(fmt.Sprintf("DecbufFactory=%s/%s", factoryName, tt.testName), func(t *testing.T) {
				result, err := tbl.LabelValuesOffsets(context.Background(), tt.labelName, tt.prefix, tt.filter)
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			})
		}
	}
}

func TestPostingsOffsetsTableV2_ForEachLabelValueOffset(t *testing.T) {
	encBuf, postingsListEnd, tableStart := setupPostingsOffsetsTableEncbuf()
	factories := setupFactoriesWithPostingsOffsetsTable(t, encBuf)
	sparseSampleFactor := 2

	for factoryName, factory := range factories {
		sparseOffsets, err := SparseValuesFromPostingsOffsetsTable(t.Context(), factory, tableStart, postingsListEnd, sparseSampleFactor, true)
		require.NoError(t, err)

		tbl, err := NewPostingsOffsetsTableReader(index.FormatV2, factory, tableStart, sparseOffsets, sparseSampleFactor)
		require.NoError(t, err)

		for _, tt := range labelValuesOffsetsTestCases(t) {
			t.Run(fmt.Sprintf("DecbufFactory=%s/%s", factoryName, tt.testName), func(t *testing.T) {
				visited := []PostingListOffset{}
				var events []string

				err := tbl.ForEachLabelValueOffset(t.Context(), tt.labelName, tt.prefix, tt.filter, LabelValueVisitor{
					SizeHint: func(estimatedValues int) { events = append(events, fmt.Sprintf("hint %d", estimatedValues)) },
					Visit: func(value []byte, off index.Range) bool {
						// Copy the value, because the scan reuses the memory behind it.
						events = append(events, "visit")
						visited = append(visited, PostingListOffset{LabelValue: string(value), Off: off})
						return true
					},
				})
				require.NoError(t, err)

				if tt.expected == nil {
					// No scan starts, so the scan neither calls SizeHint nor Visit.
					require.Empty(t, events)
					return
				}

				// The scan calls SizeHint once, before the first call of Visit.
				require.Equal(t, fmt.Sprintf("hint %d", tt.expectedSizeHint), events[0])
				require.Len(t, events, 1+len(tt.expected))
				require.Equal(t, tt.expected, visited)
			})
		}
	}
}

func TestPostingsOffsetsTableV2_ForEachLabelValueOffset_StopsWhenVisitReturnsFalse(t *testing.T) {
	encBuf, postingsListEnd, tableStart := setupPostingsOffsetsTableEncbuf()
	factories := setupFactoriesWithPostingsOffsetsTable(t, encBuf)
	sparseSampleFactor := 2

	for factoryName, factory := range factories {
		t.Run("DecbufFactory="+factoryName, func(t *testing.T) {
			sparseOffsets, err := SparseValuesFromPostingsOffsetsTable(t.Context(), factory, tableStart, postingsListEnd, sparseSampleFactor, true)
			require.NoError(t, err)

			tbl, err := NewPostingsOffsetsTableReader(index.FormatV2, factory, tableStart, sparseOffsets, sparseSampleFactor)
			require.NoError(t, err)

			var (
				events      []string
				filterCalls int
			)
			err = tbl.ForEachLabelValueOffset(t.Context(), "__name__", "", func(string) bool {
				filterCalls++
				return true
			}, LabelValueVisitor{
				SizeHint: func(int) { events = append(events, "hint") },
				Visit: func(value []byte, _ index.Range) bool {
					events = append(events, "visit "+string(value))
					// Stop the scan at the second label value.
					return len(events) < 3
				},
			})
			require.NoError(t, err)
			require.Equal(t, []string{"hint", "visit bar", "visit baz"}, events)

			// The scan read bar, baz and foo. It reads one entry ahead of Visit, and it never read zip.
			require.Equal(t, 3, filterCalls)
		})
	}
}

func TestPostingsOffsetsTableV2_ForEachLabelValueOffset_ConcurrentScans(t *testing.T) {
	encBuf, postingsListEnd, tableStart := setupPostingsOffsetsTableEncbuf()
	factories := setupFactoriesWithPostingsOffsetsTable(t, encBuf)
	sparseSampleFactor := 2

	// The cases with a filter call require from the filter, which the goroutines below must not do.
	var cases []labelValuesOffsetsTestCase
	for _, tt := range labelValuesOffsetsTestCases(t) {
		if tt.filter == nil && tt.expected != nil {
			cases = append(cases, tt)
		}
	}

	for factoryName, factory := range factories {
		t.Run("DecbufFactory="+factoryName, func(t *testing.T) {
			sparseOffsets, err := SparseValuesFromPostingsOffsetsTable(t.Context(), factory, tableStart, postingsListEnd, sparseSampleFactor, true)
			require.NoError(t, err)

			tbl, err := NewPostingsOffsetsTableReader(index.FormatV2, factory, tableStart, sparseOffsets, sparseSampleFactor)
			require.NoError(t, err)

			// Each scan owns its scratch buffers, so concurrent scans of one table must not disturb each other.
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 200 {
						for _, tt := range cases {
							visited := []PostingListOffset{}
							err := tbl.ForEachLabelValueOffset(t.Context(), tt.labelName, tt.prefix, nil, LabelValueVisitor{
								Visit: func(value []byte, off index.Range) bool {
									visited = append(visited, PostingListOffset{LabelValue: string(value), Off: off})
									return true
								},
							})
							assert.NoError(t, err)
							assert.Equal(t, tt.expected, visited)
						}
					}
				}()
			}
			wg.Wait()
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
	bucketDecbufFactory := streamencoding.NewBucketDecbufFactory(instBkt, "index")

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
