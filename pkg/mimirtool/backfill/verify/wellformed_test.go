// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWellFormedVerifier_ValidBlock(t *testing.T) {
	dir, meta := generateValidBlock(t, t.TempDir(), []chunks.Sample{
		sampleAt(1_000, 1.0),
		sampleAt(2_000, 2.0),
		sampleAt(3_000, 3.0),
	})

	ctx := context.Background()
	deep := NewWellFormedVerifier(log.NewNopLogger(), Deep)
	medium := NewWellFormedVerifier(log.NewNopLogger(), Medium)

	require.NoError(t, deep.Verify(ctx, dir, *meta))
	require.NoError(t, medium.Verify(ctx, dir, *meta))
}

func TestWellFormedVerifier_TruncatedChunk(t *testing.T) {
	dir, meta := generateValidBlock(t, t.TempDir(), []chunks.Sample{
		sampleAt(1_000, 1.0),
		sampleAt(2_000, 2.0),
		sampleAt(3_000, 3.0),
	})
	corruptChunkSegment(t, dir)

	ctx := context.Background()
	deep := NewWellFormedVerifier(log.NewNopLogger(), Deep)
	err := deep.Verify(ctx, dir, *meta)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "well-formed check failed")
}

func TestWellFormedVerifier_MangledIndex(t *testing.T) {
	dir, meta := generateValidBlock(t, t.TempDir(), []chunks.Sample{
		sampleAt(1_000, 1.0),
		sampleAt(2_000, 2.0),
		sampleAt(3_000, 3.0),
	})
	mangleIndex(t, dir)

	ctx := context.Background()
	deep := NewWellFormedVerifier(log.NewNopLogger(), Deep)
	medium := NewWellFormedVerifier(log.NewNopLogger(), Medium)

	require.Error(t, deep.Verify(ctx, dir, *meta))
	require.Error(t, medium.Verify(ctx, dir, *meta))
}

func TestWellFormedVerifier_ChecksumMismatch_DeepFailsMediumPasses(t *testing.T) {
	// Flip a byte inside the first chunk's data region. The chunks segment
	// layout is: 8-byte segment header, then repeating chunks each encoded
	// as [uvarint len][1-byte encoding][data...][4-byte CRC32]. For the
	// small first chunk produced here the data length uvarint is 1 byte, so
	// offset 10 (== 8 + 1 + 1) lands on the first byte of chunk data. This
	// breaks the stored CRC32 without touching the segment header, so deep
	// mode catches it and medium mode (which doesn't open the chunks dir at
	// all) does not.
	dir, meta := generateValidBlock(t, t.TempDir(), []chunks.Sample{
		sampleAt(1_000, 1.0),
		sampleAt(2_000, 2.0),
		sampleAt(3_000, 3.0),
		sampleAt(4_000, 4.0),
		sampleAt(5_000, 5.0),
	})
	flipChunkByte(t, dir, 10)

	ctx := context.Background()
	deep := NewWellFormedVerifier(log.NewNopLogger(), Deep)
	medium := NewWellFormedVerifier(log.NewNopLogger(), Medium)

	require.Error(t, deep.Verify(ctx, dir, *meta),
		"deep mode must catch chunk checksum mismatch")
	require.NoError(t, medium.Verify(ctx, dir, *meta),
		"medium mode must NOT catch silent checksum mismatch (by design)")
}
