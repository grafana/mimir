// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	testutil "github.com/grafana/mimir/pkg/util/test"
)

// failureULIDs returns the failures' block ULIDs in report order.
func failureULIDs(failures []Failure) []string {
	out := make([]string, 0, len(failures))
	for _, f := range failures {
		out = append(out, f.BlockULID)
	}
	return out
}

// failureMessages returns the failures' messages in report order.
func failureMessages(failures []Failure) []string {
	out := make([]string, 0, len(failures))
	for _, f := range failures {
		out = append(out, f.Err.Error())
	}
	return out
}

// sampleAt returns a chunks.Sample suitable for chunks.ChunkFromSamples and
// block.GenerateBlockFromSpec.
func sampleAt(ts int64, v float64) chunks.Sample {
	return testutil.Sample{TS: ts, Val: v}
}

// generateValidBlock writes a real TSDB block with 3 series to parent and
// returns the block's on-disk directory (parent/<ULID>) and its Meta.
// samples contains the samples for each series; all three series get the
// same samples. The caller picks minTime/maxTime by choosing sample
// timestamps.
func generateValidBlock(t *testing.T, parent string, samples []chunks.Sample) (string, *block.Meta) {
	t.Helper()
	require.GreaterOrEqual(t, len(samples), 1, "need at least one sample")

	makeChunk := func() chunks.Meta {
		c, err := chunks.ChunkFromSamples(samples)
		require.NoError(t, err)
		return c
	}

	specs := []*block.SeriesSpec{
		{Labels: labels.FromStrings("__name__", "metric_a"), Chunks: []chunks.Meta{makeChunk()}},
		{Labels: labels.FromStrings("__name__", "metric_b"), Chunks: []chunks.Meta{makeChunk()}},
		{Labels: labels.FromStrings("__name__", "metric_c"), Chunks: []chunks.Meta{makeChunk()}},
	}
	meta, err := block.GenerateBlockFromSpec(parent, specs)
	require.NoError(t, err)
	return filepath.Join(parent, meta.ULID.String()), meta
}

// corruptChunkSegment truncates chunks/000001 to a very small size so the
// internal format is destroyed.
func corruptChunkSegment(t *testing.T, blockDir string) {
	t.Helper()
	path := filepath.Join(blockDir, "chunks", "000001")
	require.NoError(t, os.Truncate(path, 10))
}

// mangleIndex overwrites the first 100 bytes of the index file with zeros,
// which destroys the file header.
func mangleIndex(t *testing.T, blockDir string) {
	t.Helper()
	path := filepath.Join(blockDir, "index")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	zeros := make([]byte, 100)
	_, err = f.WriteAt(zeros, 0)
	require.NoError(t, err)
}

// flipChunkByte flips a single bit inside chunks/000001 at the given offset
// (measured from the start of the file; caller chooses an offset past the
// segment header so the file format stays valid and only CRC32 is broken).
func flipChunkByte(t *testing.T, blockDir string, offset int64) {
	t.Helper()
	path := filepath.Join(blockDir, "chunks", "000001")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	var b [1]byte
	_, err = f.ReadAt(b[:], offset)
	require.NoError(t, err)
	b[0] ^= 0x01
	_, err = f.WriteAt(b[:], offset)
	require.NoError(t, err)
}
