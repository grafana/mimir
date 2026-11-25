// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bufio"
	"bytes"
	"errors"
	"hash/crc32"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

func frozen(tenant string, pairs [][2]string, bounds ...[2]int64) frozenSeries {
	metas := make([]ChunkMeta, len(bounds))
	for index, b := range bounds {
		metas[index] = ChunkMeta{Ref: uint64(index) * 100, MinTime: b[0], MaxTime: b[1], Len: 10, Encoding: xorEncoding}
	}
	return frozenSeries{tenant: tenant, labels: labels.FromSorted(pairs), chunks: chunkListFromMetas(metas)}
}

func TestColdBlocksFindSeriesByPostingsAndSurviveReopening(t *testing.T) {
	directory := t.TempDir()
	fixture := func() []frozenSeries {
		return []frozenSeries{
			frozen("b", [][2]string{{"__name__", "up"}, {"job", "api"}}, [2]int64{10, 20}),
			frozen("a", [][2]string{{"__name__", "up"}, {"job", "db"}}, [2]int64{5, 8}, [2]int64{30, 40}),
			frozen("a", [][2]string{{"__name__", "up"}, {"job", "api"}}, [2]int64{1, 2}),
			frozen("a", [][2]string{{"__name__", "down"}, {"job", "api"}}, [2]int64{50, 60}),
		}
	}
	inMemory, err := buildColdBlock("", 1, fixture())
	require.NoError(t, err)
	written, err := buildColdBlock(directory, 2, fixture())
	require.NoError(t, err)
	reopened, err := openColdBlock(coldBlockPath(directory, 2), 2, true)
	require.NoError(t, err)
	for _, block := range []*coldBlock{inMemory, written, reopened} {
		require.Equal(t, [2]int64{1, 60}, [2]int64{block.minTime, block.maxTime})
		require.Equal(t, 3, block.seriesCount("a"))
		require.True(t, block.overlaps("a", 35, 45) && !block.overlaps("b", 35, 45))
		table := block.tenant("a")
		names := func(indexes []uint32) []string {
			var out []string
			for _, index := range indexes {
				series := block.seriesIn(table, int(index))
				out = append(out, series.labels().String())
			}
			return out
		}
		// Sorted by labels.
		require.Equal(t, []string{
			`[("__name__", "down"), ("job", "api")]`,
			`[("__name__", "up"), ("job", "api")]`,
			`[("__name__", "up"), ("job", "db")]`,
		}, names([]uint32{0, 1, 2}))
		equal := func(name, value string) compiledMatcher {
			return compiledMatcher{kind: kindEqual, name: name, value: value}
		}
		require.Equal(t, []string{`[("__name__", "up"), ("job", "db")]`}, names(block.candidates("a", []compiledMatcher{equal("__name__", "up"), equal("job", "db")})))
		require.Empty(t, block.candidates("a", []compiledMatcher{equal("job", "missing")}))
		require.Empty(t, block.candidates("a", []compiledMatcher{equal("unknown", "x")}))
		db := block.seriesIn(table, 2)
		var bounds [][2]int64
		it := db.chunkIter()
		for chunk, more := it.next(); more; chunk, more = it.next() {
			bounds = append(bounds, [2]int64{chunk.MinTime, chunk.MaxTime})
		}
		require.Equal(t, [][2]int64{{5, 8}, {30, 40}}, bounds)
		require.True(t, db.hasData(9, 31) && !db.hasData(9, 29))
	}
	require.NoError(t, written.close())
	require.NoError(t, reopened.close())
	path := coldBlockPath(directory, 2)
	corrupt, err := os.ReadFile(path)
	require.NoError(t, err)
	corrupt[20] ^= 1
	require.NoError(t, os.WriteFile(path, corrupt, 0o644))
	_, err = openColdBlock(path, 2, true)
	require.Error(t, err)
}

func TestSnapshotLabelsWriteEachNameOnce(t *testing.T) {
	var file bytes.Buffer
	w := newSnapshotWriter(&file)
	written := []labels.Labels{
		labelsFromStrings("__name__", "up", "job", "a"),
		labelsFromStrings("__name__", "up", "job", "b"),
		labelsFromStrings("__name__", "up", "instance", "c", "job", "d"),
		labelsFromStrings(),
	}
	for _, stored := range written {
		require.NoError(t, w.labels(stored))
	}
	require.NoError(t, w.flush())
	require.Equal(t, 1, bytes.Count(file.Bytes(), []byte("job")), "a name is written by the first labels to use it")

	r := &snapshotReader{r: bufio.NewReader(bytes.NewReader(file.Bytes())), hasher: crc32.NewIEEE(), version: 8}
	for _, expected := range written {
		read, err := r.labels()
		require.NoError(t, err)
		require.Equal(t, expected.String(), read.String())
	}

	// Labels with a name id no labels before them named.
	var corrupt bytes.Buffer
	w = newSnapshotWriter(&corrupt)
	stored := labelsFromStrings("job", "a")
	require.NoError(t, errors.Join(w.uvarint(0), w.uvarint(uint64(len(stored))), w.write([]byte(stored))))
	require.NoError(t, w.flush())
	r = &snapshotReader{r: bufio.NewReader(bytes.NewReader(corrupt.Bytes())), hasher: crc32.NewIEEE(), version: 8}
	_, err := r.labels()
	require.EqualError(t, err, "head snapshot labels have an unknown name")
}

// Bytes bigger than the writer's buffer, as the engine's state after a long replay is, go out in their place and in the
// checksum as any others, without growing the buffer for the rest of the snapshot.
func TestSnapshotWriterWritesBytesBiggerThanItsBuffer(t *testing.T) {
	writeAll := func(w *snapshotWriter, big []byte) uint32 {
		require.NoError(t, errors.Join(w.u8(1), w.optionalBytes(big), w.u8(2)))
		checksum := w.checksum()
		require.NoError(t, w.flush())
		return checksum
	}
	var file bytes.Buffer
	w := newSnapshotWriter(&file)
	bufferCap := cap(w.buf)
	big := bytes.Repeat([]byte{7}, 2*bufferCap+1)
	checksum := writeAll(w, big)
	require.Equal(t, bufferCap, cap(w.buf))

	// The same, through a buffer big enough for all of it.
	var expected bytes.Buffer
	roomy := newSnapshotWriter(&expected)
	roomy.buf = make([]byte, 0, 4*bufferCap)
	require.Equal(t, writeAll(roomy, big), checksum)
	require.Equal(t, expected.Bytes(), file.Bytes())
}

// A field is read at its size, nil apart from empty, and a length past the snapshot's end is rejected before it
// allocates.
func TestSnapshotReaderOptionalBytes(t *testing.T) {
	reader := func(file []byte) *snapshotReader {
		return &snapshotReader{r: bufio.NewReader(bytes.NewReader(file)), hasher: crc32.NewIEEE(), version: 8, size: int64(len(file))}
	}
	var file bytes.Buffer
	w := newSnapshotWriter(&file)
	require.NoError(t, errors.Join(w.optionalBytes([]byte("state")), w.optionalBytes([]byte{}), w.optionalBytes(nil)))
	require.NoError(t, w.flush())
	r := reader(file.Bytes())
	read, err := r.optionalBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("state"), read)
	require.Equal(t, len(read), cap(read), "read at its size")
	read, err = r.optionalBytes()
	require.NoError(t, err)
	require.Equal(t, []byte{}, read, "present but empty stays non-nil")
	read, err = r.optionalBytes()
	require.NoError(t, err)
	require.Nil(t, read)

	var corrupt bytes.Buffer
	w = newSnapshotWriter(&corrupt)
	require.NoError(t, errors.Join(w.u8(1), w.length(1<<30)))
	require.NoError(t, w.flush())
	_, err = reader(corrupt.Bytes()).optionalBytes()
	require.ErrorContains(t, err, "exceeds the snapshot's")
}
