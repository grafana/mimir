// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"
)

func newTestChunkedReader(t *testing.T, sizeLimit uint64, chunks ...[]byte) *streamedChunkReader {
	t.Helper()

	var buf bytes.Buffer
	w := remote.NewChunkedWriter(&buf, nil)
	for _, c := range chunks {
		_, err := w.Write(c)
		require.NoError(t, err)
	}

	chunkReader := remote.NewChunkedReader(&buf, sizeLimit, nil)
	return &streamedChunkReader{
		reader: chunkReader,
		closer: io.NopCloser(&buf),
	}
}

func TestMergeChunkedReader_DrainsReadersInOrder(t *testing.T) {
	r1 := newTestChunkedReader(t, math.MaxUint64, []byte("a1"), []byte("a2"))
	r2 := newTestChunkedReader(t, math.MaxUint64, []byte("b1"), []byte("b2"))

	m := NewMergeChunkedReader(r1, r2)

	var got []string
	for {
		chunk, idx, err := m.Next()
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		got = append(got, fmt.Sprintf("%d:%s", idx, chunk))
	}
	require.Equal(t, []string{"0:a1", "0:a2", "1:b1", "1:b2"}, got)

	// The EOF is sticky.
	_, _, err := m.Next()
	require.ErrorIs(t, err, io.EOF)
	_, _, err = m.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestMergeChunkedReader_ReaderErrorIsSticky(t *testing.T) {
	// r1's chunk is bigger than the size limit we give its reader, so its Next() call
	// returns a non-EOF error. r2 must not be consumed after that error.
	r1 := newTestChunkedReader(t, 1, []byte("too big for the limit"))
	r2 := newTestChunkedReader(t, math.MaxUint64, []byte("b1"), []byte("b2"), []byte("b3"))

	m := NewMergeChunkedReader(r1, r2)

	_, _, err := m.Next()
	require.NotErrorIs(t, err, io.EOF)

	// The error is sticky, and doesn't change on subsequent calls.
	_, _, err2 := m.Next()
	require.Equal(t, err, err2)
}
