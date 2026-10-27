// SPDX-License-Identifier: AGPL-3.0-only

package chunks

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A file of out-of-order chunks stays, whatever its chunks' times, until retention no longer keeps those chunks.
func TestKeepsFilesOfRetainedOutOfOrderChunks(t *testing.T) {
	defer func(size int) { FileSize = size }(FileSize)
	FileSize = 16
	directory := filepath.Join(t.TempDir(), "chunks")
	mapper, err := OpenDiskMapper(directory)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mapper.Close() })
	inOrder, err := mapper.Write(bytes.Repeat([]byte{1}, 16), 10)
	require.NoError(t, err)
	outOfOrder, err := mapper.Write(bytes.Repeat([]byte{2}, 16), 5)
	require.NoError(t, err)
	mapper.NoteOutOfOrder(outOfOrder)
	// The current file, which is never removed.
	_, err = mapper.Write(bytes.Repeat([]byte{3}, 16), 30)
	require.NoError(t, err)

	require.NoError(t, mapper.TruncateRetained(20, outOfOrder))
	require.NoFileExists(t, filepath.Join(directory, fileName(uint32(inOrder>>32))))
	require.FileExists(t, filepath.Join(directory, fileName(uint32(outOfOrder>>32))), "its chunk is retained")
	require.NoError(t, mapper.TruncateRetained(20, outOfOrder+1))
	require.NoFileExists(t, filepath.Join(directory, fileName(uint32(outOfOrder>>32))))
}

func TestReadsWrittenChunksAndDeletesExpiredFiles(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "chunks")
	mapper, err := OpenDiskMapper(directory)
	require.NoError(t, err)
	first, err := mapper.Write([]byte("first"), 10)
	require.NoError(t, err)
	second, err := mapper.Write([]byte("second"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), mapper.Read(first, 5))
	require.Equal(t, []byte("second"), mapper.Read(second, 6))

	big := bytes.Repeat([]byte{7}, FileSize-10)
	third, err := mapper.Write(big, 30)
	require.NoError(t, err)
	require.Equal(t, uint64(1), third>>32)
	require.FileExists(t, filepath.Join(directory, fileName(1)))

	require.NoError(t, mapper.TruncateBefore(25))
	require.NoFileExists(t, filepath.Join(directory, fileName(0)))
	require.Equal(t, []byte{7, 7, 7}, mapper.Read(third, 3))
	require.NoError(t, mapper.TruncateBefore(1<<62))
	require.FileExists(t, filepath.Join(directory, fileName(1)), "current file is kept")

	require.NoError(t, mapper.Close())
	reopened, err := OpenDiskMapper(directory)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestReopensTheFilesOfAHeadSnapshot(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "chunks")
	mapper, err := OpenDiskMapper(directory)
	require.NoError(t, err)
	ref, err := mapper.Write([]byte("kept"), 10)
	require.NoError(t, err)
	require.NoError(t, mapper.Sync())
	files, next := mapper.State()
	require.Equal(t, []FileState{{Sequence: 0, Written: 4, MaxTime: 10}}, files)
	require.NoError(t, mapper.Close())
	require.NoError(t, os.WriteFile(filepath.Join(directory, "stale"), nil, 0o644))

	reopened, err := ReopenDiskMapper(directory, files, next)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Equal(t, []byte("kept"), reopened.Read(ref, 4))
	require.NoFileExists(t, filepath.Join(directory, "stale"))
	// New chunks go after what the snapshot recorded.
	next2, err := reopened.Write([]byte("more"), 20)
	require.NoError(t, err)
	require.Equal(t, uint64(4), next2)
}

func TestAnonymousChunkFiles(t *testing.T) {
	mapper, err := OpenDiskMapper("")
	require.NoError(t, err)
	t.Cleanup(func() { _ = mapper.Close() })
	ref, err := mapper.Write([]byte("anonymous"), 1)
	require.NoError(t, err)
	require.Equal(t, []byte("anonymous"), mapper.Read(ref, 9))
}
